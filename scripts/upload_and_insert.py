from supabase import create_client
from dotenv import load_dotenv
import os
import re
from datetime import datetime
import sys
import time
from httpx import ReadTimeout

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)

PDF_DIR = "pdfs"
BUCKET_NAME = "exam-pdfs"
BATCH_SIZE = 20
RETRY_DELAY = 2

unmatched_solutions = []
inserted_exams = 0
inserted_solutions = 0


def parse_filename(filename):
    pattern = r"^([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_(EXAM|SOLUTION)\.pdf$"
    match = re.match(pattern, filename)
    if not match:
        return None
    course_code, date_str, file_type = match.groups()
    return course_code, datetime.strptime(date_str, "%Y-%m-%d").date(), file_type


def create_exam_name(course_code, exam_date, file_type):
    """
    Creates a human-readable name:
    - "Tentamen 2024-01-05" for exams
    - "Lösningar 2024-01-05" for solutions
    """
    date_str = exam_date.strftime("%Y-%m-%d")
    if file_type == "EXAM":
        return f"Tentamen {date_str}"
    return f"Lösningar {date_str}"


def validate_environment():
    print("🔍 Validating environment...")

    if not url or not key:
        print("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env")
        sys.exit(1)

    if not os.path.isdir(PDF_DIR):
        print(f"❌ PDF directory '{PDF_DIR}' not found")
        sys.exit(1)

    files = [f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")]
    if not files:
        print("❌ No PDFs found in the pdfs/ folder")
        sys.exit(1)

    invalid_files = [f for f in files if not parse_filename(f)]
    if invalid_files:
        print("⚠️ Invalid filenames detected:")
        for f in invalid_files:
            print(f"   - {f}")
        print("❌ Fix filenames before running")
        sys.exit(1)

    try:
        buckets = supabase.storage.list_buckets()
        if not any(bucket.name == BUCKET_NAME for bucket in buckets):
            print(f"❌ Bucket '{BUCKET_NAME}' does not exist")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Failed to list buckets: {e}")
        sys.exit(1)

    try:
        supabase.table("exams").select("*").limit(1).execute()
        supabase.table("solutions").select("*").limit(1).execute()
    except Exception as e:
        print(f"❌ Failed to query tables: {e}")
        sys.exit(1)

    print("✅ Environment validated successfully!\n")


def upload_file(filename, file_path):
    for attempt in range(2):
        try:
            with open(file_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path=filename,
                    file=f,
                    file_options={
                        "cache-control": "3600",
                        "upsert": "true",
                        "content-type": "application/pdf",  # ✅ ensures browser renders it
                    },
                )
            print(f"✅ Uploaded {filename}")
            return True
        except ReadTimeout:
            print(f"⚠️ Timeout uploading {filename}, retrying ({attempt+1}/2)...")
            time.sleep(RETRY_DELAY)
    print(f"❌ Failed to upload {filename} after retries")
    return False


def process_files():
    global inserted_exams, inserted_solutions

    files = [f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")]
    total_files = len(files)
    print(f"📂 Found {total_files} PDFs, processing in batches of {BATCH_SIZE}...\n")

    for i in range(0, total_files, BATCH_SIZE):
        batch = files[i : i + BATCH_SIZE]
        print(
            f"🔄 Processing batch {i//BATCH_SIZE + 1}/{(total_files-1)//BATCH_SIZE+1}"
        )

        for filename in batch:
            parsed = parse_filename(filename)
            if not parsed:
                continue

            course_code, exam_date, file_type = parsed
            file_path = os.path.join(PDF_DIR, filename)

            if not upload_file(filename, file_path):
                continue

            pdf_url = f"https://{url.split('//')[1]}/storage/v1/object/public/{BUCKET_NAME}/{filename}"
            exam_name = create_exam_name(course_code, exam_date, file_type)

            if file_type == "EXAM":
                res = (
                    supabase.table("exams")
                    .upsert(
                        {
                            "course_code": course_code,
                            "exam_date": str(exam_date),
                            "pdf_url": pdf_url,
                            "exam_name": exam_name,
                        },
                        on_conflict="pdf_url",
                    )
                    .execute()
                )
                exam_id = res.data[0]["id"]
                inserted_exams += 1
                print(f"✅ Inserted/Updated exam: {exam_name} (ID: {exam_id})")

            else:  # SOLUTION
                matching_exam = (
                    supabase.table("exams")
                    .select("id")
                    .eq("course_code", course_code)
                    .eq("exam_date", str(exam_date))
                    .execute()
                    .data
                )

                if not matching_exam:
                    unmatched_solutions.append(
                        (filename, course_code, exam_date, pdf_url)
                    )
                    print(f"⚠️ Buffered unmatched solution: {filename}")
                    continue

                exam_id = matching_exam[0]["id"]
                solution_name = create_exam_name(course_code, exam_date, file_type)

                supabase.table("solutions").upsert(
                    {
                        "exam_id": exam_id,
                        "pdf_url": pdf_url,
                        "solution_name": solution_name,
                    },
                    on_conflict="pdf_url",
                ).execute()
                inserted_solutions += 1
                print(
                    f"✅ Inserted/Updated solution: {solution_name} (Exam ID: {exam_id})"
                )

        time.sleep(1)  # prevent rate limiting


def retry_unmatched_solutions():
    global inserted_solutions
    print("\n🔄 Retrying unmatched solutions...")
    for filename, course_code, exam_date, pdf_url in unmatched_solutions:
        matching_exam = (
            supabase.table("exams")
            .select("id")
            .eq("course_code", course_code)
            .eq("exam_date", str(exam_date))
            .execute()
            .data
        )

        if not matching_exam:
            print(f"❌ Still no matching exam for: {filename}")
            continue

        exam_id = matching_exam[0]["id"]
        solution_name = create_exam_name(course_code, exam_date, "SOLUTION")

        supabase.table("solutions").upsert(
            {
                "exam_id": exam_id,
                "pdf_url": pdf_url,
                "solution_name": solution_name,
            },
            on_conflict="pdf_url",
        ).execute()
        inserted_solutions += 1
        print(
            f"✅ Linked previously unmatched solution: {solution_name} (Exam ID: {exam_id})"
        )


def print_summary():
    print("\n📊 Migration Summary:")
    print(f"   ✅ Exams inserted/updated: {inserted_exams}")
    print(f"   ✅ Solutions inserted/updated: {inserted_solutions}")
    print(f"   ⚠️ Remaining unmatched solutions: {len(unmatched_solutions)}")


if __name__ == "__main__":
    validate_environment()
    process_files()
    retry_unmatched_solutions()
    print_summary()
