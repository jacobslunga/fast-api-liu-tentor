from supabase import create_client
from dotenv import load_dotenv
import os
import re
from datetime import datetime

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)

PDF_DIR = "pdfs"


def parse_filename(filename):
    pattern = r"^([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_SOLUTION\.pdf$"
    match = re.match(pattern, filename)
    if not match:
        return None
    course_code, date_str = match.groups()
    return course_code, datetime.strptime(date_str, "%Y-%m-%d").date()


def verify_unmatched_solutions():
    unmatched = [f for f in os.listdir(PDF_DIR) if f.endswith("_SOLUTION.pdf")]

    found_matches = []
    still_unmatched = []

    for filename in unmatched:
        parsed = parse_filename(filename)
        if not parsed:
            continue

        course_code, exam_date = parsed
        matching_exam = (
            supabase.table("exams")
            .select("id, exam_name")
            .eq("course_code", course_code)
            .eq("exam_date", str(exam_date))
            .execute()
            .data
        )

        if matching_exam:
            found_matches.append(
                (filename, matching_exam[0]["id"], matching_exam[0]["exam_name"])
            )
        else:
            still_unmatched.append((filename, course_code, exam_date))

    print("\n📊 Verification Results:")
    print(f"✅ Found matches: {len(found_matches)}")
    for f, exam_id, name in found_matches[:20]:  # limit output
        print(f"   {f} → Exam ID {exam_id} ({name})")

    print(f"\n❌ Still unmatched: {len(still_unmatched)}")
    for f, course_code, date in still_unmatched[:20]:
        print(f"   {f} → No exam found for {course_code} on {date}")


if __name__ == "__main__":
    verify_unmatched_solutions()
