from supabase import create_client
import requests
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
API_BASE = "https://liutentor.lukasabbe.com/api/courses"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def normalize_date(date_str):
    """Normalize date to YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "")).strftime("%Y-%m-%d")
    except ValueError:
        return date_str.split("T")[0] if "T" in date_str else date_str


def fetch_all_exams():
    """Fetch all exams from Supabase."""
    print("🔄 Fetching exams...")
    exams = []
    page_size = 1000
    offset = 0

    while True:
        resp = (
            supabase.table("exams")
            .select("id, course_code, exam_date, exam_name")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        data = resp.data
        if not data:
            break
        exams.extend(data)
        offset += page_size

    print(f"✅ Found {len(exams)} exams")
    return exams


def fetch_course_modules(course_code):
    """Fetch course modules (TEN1, KTR1, etc.) from external API."""
    try:
        resp = requests.get(f"{API_BASE}/{course_code}", timeout=10)
        resp.raise_for_status()
        return resp.json().get("modules", [])
    except requests.RequestException:
        print(f"⚠️ Failed to fetch modules for {course_code}")
        return []


def update_exam_names():
    exams = fetch_all_exams()
    updated_count = 0

    # Group exams by course_code to avoid redundant API calls
    courses = {}
    for exam in exams:
        courses.setdefault(exam["course_code"], []).append(exam)

    for course_code, course_exams in courses.items():
        print(f"\n📚 Processing course {course_code}...")
        modules = fetch_course_modules(course_code)

        for exam in course_exams:
            exam_date = normalize_date(exam["exam_date"])
            new_name = f"Tentamen {exam_date}"  # Default name
            matched = False

            for module in modules:
                module_date = normalize_date(module.get("date"))
                if module_date == exam_date:
                    module_code = module.get("moduleCode", "").upper()
                    new_name = f"{module_code} {exam_date}"
                    matched = True
                    break

            if exam["exam_name"] != new_name:
                supabase.table("exams").update({"exam_name": new_name}).eq(
                    "id", exam["id"]
                ).execute()
                updated_count += 1
                print(f"✅ Updated exam {exam['id']} → {new_name}")
            elif matched:
                print(f"ℹ️ Already correct: {exam['exam_name']}")

    print(f"\n🎉 Done! Updated {updated_count} exams.")


if __name__ == "__main__":
    update_exam_names()
