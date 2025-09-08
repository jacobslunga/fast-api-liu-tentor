import httpx
from fastapi import APIRouter, HTTPException, Request
from app.db.supabase import supabase
from app.core.rate_limiter import limiter

router = APIRouter()

EXTERNAL_API_BASE = "https://liutentor.lukasabbe.com/api/courses"


async def fetch_course_stats(course_code: str) -> dict:
    """
    Fetches the full statistics object for a course from the external API.
    This is the potentially slow network call.
    """
    api_url = f"{EXTERNAL_API_BASE}/{course_code}"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(api_url, timeout=10)
            resp.raise_for_status()
            return resp.json()
    except httpx.RequestError:
        return {}


@router.get("/courses/{course_code}/exams")
@limiter.limit("200/minute")
async def get_course_exams(request: Request, course_code: str):
    """
    Fetches all exam documents for a course and enriches them with statistics
    from the `exam_stats` table.
    """

    exams_response = (
        supabase.table("exams")
        .select("id, course_code, exam_date, pdf_url, exam_name, solutions(exam_id)")
        .eq("course_code", course_code)
        .order("exam_date", desc=True)
        .execute()
        .data
    )

    if not exams_response:
        raise HTTPException(
            status_code=404, detail="No exam documents found for this course"
        )

    stats_response = (
        supabase.table("exam_stats")
        .select("exam_date, statistics, pass_rate, course_name_swe, course_name_eng")
        .eq("course_code", course_code)
        .execute()
        .data
    )

    stats_map = {
        stat["exam_date"]: {
            "statistics": stat.get("statistics"),
            "pass_rate": stat.get("pass_rate"),
        }
        for stat in stats_response
    }

    exam_list = []
    for exam in exams_response:
        exam_stats = stats_map.get(exam["exam_date"], {})

        exam_list.append(
            {
                "id": exam["id"],
                "course_code": exam["course_code"],
                "exam_date": exam["exam_date"],
                "pdf_url": exam["pdf_url"],
                "exam_name": exam["exam_name"],
                "has_solution": bool(exam.get("solutions")),
                "statistics": exam_stats.get("statistics"),
                "pass_rate": exam_stats.get("pass_rate"),
            }
        )

    course_name_swe = (
        stats_response[0].get("course_name_swe", "") if stats_response else ""
    )
    course_name_eng = (
        stats_response[0].get("course_name_eng", "") if stats_response else ""
    )

    return {
        "course_code": course_code,
        "course_name_swe": course_name_swe,
        "course_name_eng": course_name_eng,
        "exams": exam_list,
    }


@router.get("/courses/{course_code}/details")
@limiter.limit("200/minute")
async def get_course_details(request: Request, course_code: str):
    stats_data = await fetch_course_stats(course_code)
    course_name_swe = stats_data.get("courseNameSwe", "")
    course_name_eng = stats_data.get("courseNameEng", "")

    return {
        "course_code": course_code,
        "course_name_swe": course_name_swe,
        "course_name_eng": course_name_eng,
    }


@router.get("/courses/{course_code}/stats/{exam_date}")
@limiter.limit("200/minute")
async def get_exam_statistics(request: Request, course_code: str, exam_date: str):
    stats_data = await fetch_course_stats(course_code)

    if not stats_data:
        return {"grades": {}, "pass_rate": 0.0}

    for module in stats_data.get("modules", []):
        if module.get("date", "").startswith(exam_date):
            grades = {g["grade"]: g["quantity"] for g in module.get("grades", [])}
            total_students = sum(grades.values())
            passed_count = sum(grades.get(g, 0) for g in ["3", "4", "5", "G"])

            pass_rate = (
                round((passed_count / total_students * 100), 1)
                if total_students > 0
                else 0.0
            )

            return {"grades": grades, "pass_rate": pass_rate}

    return {"grades": {}, "pass_rate": 0.0}


@router.get("/exams/{exam_id}")
@limiter.limit("200/minute")
async def get_exam_with_solutions(request: Request, exam_id: int):
    """
    Retrieves a single exam and its associated solutions in one database call.
    """
    response = (
        supabase.table("exams")
        .select("id, course_code, exam_date, pdf_url, solutions(*)")
        .eq("id", exam_id)
        .single()
        .execute()
        .data
    )

    if not response:
        raise HTTPException(status_code=404, detail="Exam not found")

    solutions = response.pop("solutions", [])
    exam = response

    return {"exam": exam, "solutions": solutions}
