import os
import asyncio
import httpx
from collections import defaultdict
from dotenv import load_dotenv
from supabase import create_client, Client
from tqdm.asyncio import tqdm_asyncio

# --- Configuration ---
COURSE_LIST_API = "https://liutentor.lukasabbe.com/api/courses"
STATS_API = "https://ysektionen.se/student/tentastatistik/exam_stats/"
PASSING_GRADES = {"3", "4", "5", "G"}
CONCURRENCY_LIMIT = 10

VALID_EXAM_PREFIXES = ("TEN", "KTR", "DUG", "DAT")
VALID_EXAM_NAMES = ("Skriftlig tentamen", "Skriftlig examination", "Kontrollskrivning")

# --- Helper Functions ---


async def fetch_all_course_codes(client: httpx.AsyncClient) -> list[str]:
    """Fetches the master list of all course codes."""
    try:
        resp = await client.get(COURSE_LIST_API, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except httpx.RequestError as e:
        print(f"FATAL: Could not fetch course code list: {e}")
        return []
    except Exception as e:
        print(f"FATAL: An error occurred while parsing the course code list: {e}")
        return []


async def fetch_and_process_course(
    supabase: Client,
    http_client: httpx.AsyncClient,
    course_code: str,
    semaphore: asyncio.Semaphore,
):
    """
    Fetches stats for a single course, transforms the data, and upserts it into Supabase.
    """
    async with semaphore:
        # 1. Fetch the raw stats data
        params = {"course_code": course_code}
        try:
            resp = await http_client.get(STATS_API, params=params, timeout=45)
            if resp.status_code != 200:
                return
            api_data = resp.json()
            if not api_data.get("success"):
                return
        except httpx.RequestError:
            return

        # 2. Transform the transposed API data into a standard format
        unpivoted_stats = defaultdict(lambda: defaultdict(int))
        for exam_code, exam_details in api_data.get("exams", {}).items():
            # --- FIX: HANDLE POTENTIAL `None` VALUE FOR EXAM_NAME ---
            # If `exam_details.get("name")` returns None, default to an empty string
            exam_name = exam_details.get("name") or ""
            # --- END OF FIX ---

            is_valid_prefix = exam_code.startswith(VALID_EXAM_PREFIXES)
            is_valid_name = any(
                valid_name in exam_name for valid_name in VALID_EXAM_NAMES
            )

            if not (is_valid_prefix or is_valid_name):
                continue

            dates_list = exam_details.get("dates", [])
            grade_data_list = exam_details.get("data", [])
            for grade_obj in grade_data_list:
                grade_name = grade_obj.get("name")
                quantities = grade_obj.get("data", [])
                for i, quantity in enumerate(quantities):
                    if i < len(dates_list):
                        date = dates_list[i]
                        unpivoted_stats[date][grade_name] += quantity
                        unpivoted_stats[date]["_exam_code"] = exam_code
                        unpivoted_stats[date]["_exam_name"] = exam_details.get(
                            "name", ""
                        )

        # 3. Build the list of rows to be inserted into Supabase
        rows_to_insert = []
        for date, grades in unpivoted_stats.items():
            total_students = sum(
                v for k, v in grades.items() if not str(k).startswith("_")
            )
            passed_count = sum(grades.get(g, 0) for g in PASSING_GRADES)
            pass_rate = (
                round((passed_count / total_students * 100), 1)
                if total_students > 0
                else 0.0
            )

            exam_code = grades.pop("_exam_code", None)
            exam_name = grades.pop("_exam_name", None)

            rows_to_insert.append(
                {
                    "course_code": course_code,
                    "exam_date": date,
                    "statistics": dict(grades),
                    "pass_rate": pass_rate,
                    "course_name_swe": api_data.get("course_name"),
                    "course_name_eng": api_data.get("course_name_eng"),
                    "exam_code": exam_code,
                    "exam_name": exam_name,
                }
            )

        # 4. Upsert the data into the new exam_stats table
        if rows_to_insert:
            try:
                supabase.table("exam_stats").upsert(rows_to_insert).execute()
            except Exception as e:
                print(f"  ERROR: Failed to upsert data for {course_code}: {e}")


async def main():
    """Main function to run the population script."""
    print("Starting script to populate the `exam_stats` table...")

    load_dotenv()
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not supabase_url or not supabase_key:
        print(
            "Error: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in a .env file."
        )
        return

    supabase: Client = create_client(supabase_url, supabase_key)

    async with httpx.AsyncClient() as http_client:
        all_course_codes = await fetch_all_course_codes(http_client)
        if not all_course_codes:
            print("No course codes found. Exiting.")
            return

        print(f"Found {len(all_course_codes)} total course codes to process.")

        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        print(
            f"Fetching stats and populating database (Concurrency: {CONCURRENCY_LIMIT})..."
        )

        tasks = [
            fetch_and_process_course(supabase, http_client, code, semaphore)
            for code in all_course_codes
        ]
        await tqdm_asyncio.gather(*tasks, desc="Processing Courses")

    print("\nStats population script finished successfully!")


if __name__ == "__main__":
    asyncio.run(main())
