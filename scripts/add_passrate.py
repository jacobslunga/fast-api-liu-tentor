import os
import asyncio
import httpx
from collections import defaultdict
from dotenv import load_dotenv
from supabase import create_client, Client
from tqdm.asyncio import tqdm_asyncio

# --- Configuration ---
EXTERNAL_API_BASE = "https://liutentor.lukasabbe.com/api/courses"
PASSING_GRADES = {"3", "4", "5", "G"}
CONCURRENCY_LIMIT = 10
PAGE_SIZE = 1000  # How many rows to fetch from Supabase at a time

# --- Helper Functions ---


async def fetch_course_stats(client: httpx.AsyncClient, course_code: str) -> dict:
    """Fetches the full statistics object for a course asynchronously."""
    api_url = f"{EXTERNAL_API_BASE}/{course_code}"
    try:
        resp = await client.get(api_url, timeout=30)
        if resp.status_code != 200:
            # This is now a non-critical warning, as some courses may not exist in the external API
            # print(f"API returned status {resp.status_code} for {course_code}. Skipping.")
            return {}
        return resp.json()
    except httpx.RequestError as e:
        print(f"Network error fetching stats for {course_code}: {e}")
        return {}


async def process_course(
    supabase: Client,
    http_client: httpx.AsyncClient,
    course_code: str,
    exams: list,
    semaphore: asyncio.Semaphore,
):
    """Processes all exams for a single course code, with enhanced debugging output."""
    async with semaphore:
        stats_data = await fetch_course_stats(http_client, course_code)

        # Create a lookup map for faster access: { 'exam_date': {stats_obj} }
        stats_map = {}
        if stats_data and "modules" in stats_data:
            for module in stats_data.get("modules", []):
                date = (
                    module.get("date", "").strip().split("T")[0]
                )  # .strip() added for safety
                grades = {g["grade"]: g["quantity"] for g in module.get("grades", [])}

                total_students = sum(grades.values())
                passed_count = sum(grades.get(g, 0) for g in PASSING_GRADES)

                pass_rate = (
                    round((passed_count / total_students * 100), 1)
                    if total_students > 0
                    else 0.0
                )
                stats_map[date] = {"statistics": grades, "pass_rate": pass_rate}

        # Update each exam in Supabase for the current course
        for exam in exams:
            exam_date = exam["exam_date"].strip()
            if exam_date in stats_map:
                update_payload = stats_map[exam_date]
                try:
                    supabase.table("exams").update(update_payload).eq(
                        "id", exam["id"]
                    ).execute()
                except Exception as e:
                    print(f"  ERROR: Failed to update exam ID {exam['id']}: {e}")


async def main():
    """Main function to run the backfill script."""
    print("Starting exam statistics backfill script...")

    load_dotenv()
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not supabase_url or not supabase_key:
        print("Error: SUPABASE_URL and SUPABASE_KEY must be set in a .env file.")
        return

    supabase: Client = create_client(supabase_url, supabase_key)

    # --- FIX: PAGINATE THE RESULTS TO FETCH ALL EXAMS ---
    print("Fetching ALL exams from Supabase using pagination...")
    all_exams = []
    current_page = 0
    while True:
        try:
            start_index = current_page * PAGE_SIZE
            end_index = start_index + PAGE_SIZE - 1

            response = (
                supabase.table("exams")
                .select("id, course_code, exam_date")
                .range(start_index, end_index)
                .execute()
            )

            page_data = response.data
            all_exams.extend(page_data)

            # If a page returns fewer than PAGE_SIZE results, it's the last page
            if len(page_data) < PAGE_SIZE:
                break

            current_page += 1

        except Exception as e:
            print(f"Error fetching exams from Supabase: {e}")
            return
    # --- END OF FIX ---

    if not all_exams:
        print("No exams found in the database.")
        return

    print(f"Found {len(all_exams)} total exams to process.")

    courses = defaultdict(list)
    for exam in all_exams:
        courses[exam["course_code"]].append(exam)

    print(f"Grouped exams into {len(courses)} unique course codes.")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    print(
        f"Fetching statistics and updating database (Concurrency limit: {CONCURRENCY_LIMIT})..."
    )
    async with httpx.AsyncClient() as http_client:
        tasks = [
            process_course(supabase, http_client, code, exam_list, semaphore)
            for code, exam_list in courses.items()
        ]
        # We can remove the debugging print statements now
        await tqdm_asyncio.gather(*tasks, desc="Processing Courses")

    print("\nBackfill script finished successfully!")


if __name__ == "__main__":
    asyncio.run(main())
