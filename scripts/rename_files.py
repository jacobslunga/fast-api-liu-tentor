import requests
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(url, key)

API_BASE = "https://liutentor.lukasabbe.com/api/courses"


def detect_prefix(module_code):
    module_code = module_code.upper()
    if module_code.startswith("KTR"):
        return "Kontrollskrivning"
    if module_code.startswith("DUG"):
        return "Dugga"
    return "Tentamen"


def date_match(date1, date2):
    """Match allowing ±1 day tolerance."""
    d1 = datetime.strptime(date1, "%Y-%m-%d")
    d2 = datetime.strptime(date2.split("T")[0], "%Y-%m-%d")
    return abs((d1 - d2).days) <= 1


def update_exam_names():
    exams = (
        supabase.table("exams")
        .select("id, course_code, exam_date, exam_name")
        .execute()
        .data
    )
    courses = {}

    # Group exams by course_code
    for exam in exams:
        cc = exam["course_code"]
        if cc not in courses:
            resp = requests.get(f"{API_BASE}/{cc}")
            if resp.status_code == 200:
                courses[cc] = resp.json().get("modules", [])
            else:
                courses[cc] = []

    updated = 0
    for exam in exams:
        cc = exam["course_code"]
        matched_module = None
        for module in courses[cc]:
            if date_match(exam["exam_date"], module["date"]):
                matched_module = module
                break

        prefix = "Tentamen"
        if matched_module:
            prefix = detect_prefix(matched_module["moduleCode"])

        new_name = f"{prefix} {exam['exam_date']}"
        if exam["exam_name"] != new_name:
            supabase.table("exams").update({"exam_name": new_name}).eq(
                "id", exam["id"]
            ).execute()
            print(f"✅ Updated {exam['id']} → {new_name}")
            updated += 1

    print(f"Finished updating {updated} exams.")


if __name__ == "__main__":
    update_exam_names()
