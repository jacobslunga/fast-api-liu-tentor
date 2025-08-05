from fastapi import APIRouter, HTTPException, Request
from app.db.supabase import supabase
from app.core.rate_limiter import limiter
import requests
from datetime import datetime
import os

router = APIRouter()

EXTERNAL_API_BASE = "https://liutentor.lukasabbe.com/api/courses"
url = os.getenv("SUPABASE_URL")


def fetch_course_stats(course_code: str) -> dict:
    """Fetches course statistics from the external API."""
    api_url = f"{EXTERNAL_API_BASE}/{course_code}"
    resp = requests.get(api_url, timeout=10)
    resp.raise_for_status()
    return resp.json()


@router.get("/courses/{course_code}/exams")
@limiter.limit("30/minute")
def get_course_exams(request: Request, course_code: str):
    exams = (
        supabase.table("exams")
        .select("id, course_code, exam_date, pdf_url, exam_name")
        .eq("course_code", course_code)
        .order("exam_date", desc=True)
        .execute()
        .data
    )

    if not exams:
        raise HTTPException(status_code=404, detail="No exams found")

    exam_ids = [exam["id"] for exam in exams]
    solutions = (
        supabase.table("solutions")
        .select("exam_id")
        .in_("exam_id", exam_ids)
        .execute()
        .data
    )

    solution_map = {sol["exam_id"]: True for sol in solutions}

    course_name_swe = ""
    course_name_eng = ""
    stats_map = {}

    try:
        stats_data = fetch_course_stats(course_code)
        course_name_swe = stats_data.get("courseNameSwe", "")
        course_name_eng = stats_data.get("courseNameEng", "")

        for module in stats_data.get("modules", []):
            date = module.get("date", "").split("T")[0]
            grades = {g["grade"]: g["quantity"] for g in module.get("grades", [])}
            total = sum(grades.values())
            passed = grades.get("3", 0) + grades.get("4", 0) + grades.get("5", 0)
            pass_rate = round((passed / total * 100), 1) if total > 0 else 0.0

            stats_map[date] = {"grades": grades, "pass_rate": pass_rate}
    except requests.RequestException:
        pass

    exam_list = []
    for exam in exams:
        exam_date_str = exam["exam_date"]
        stat_entry = stats_map.get(exam_date_str, {"grades": {}, "pass_rate": 0.0})
        exam_list.append(
            {
                "id": exam["id"],
                "course_code": exam["course_code"],
                "exam_date": exam_date_str,
                "pdf_url": exam["pdf_url"],
                "exam_name": exam["exam_name"],
                "has_solution": solution_map.get(exam["id"], False),
                "statistics": stat_entry["grades"],
                "pass_rate": stat_entry["pass_rate"],
            }
        )

    # Sort: exams with solutions first, then by date (newest first)
    exam_list.sort(key=lambda x: (not x["has_solution"], x["exam_date"]), reverse=True)

    return {
        "course_code": course_code,
        "course_name_swe": course_name_swe,
        "course_name_eng": course_name_eng,
        "exams": exam_list,
    }


@router.get("/exams/{exam_id}")
@limiter.limit("30/minute")
def get_exam_with_solutions(request: Request, exam_id: int):
    exam = (
        supabase.table("exams")
        .select("id, course_code, exam_date, pdf_url")
        .eq("id", exam_id)
        .single()
        .execute()
        .data
    )

    if not exam:
        raise HTTPException(status_code=404, detail="Exam not found")

    solutions = (
        supabase.table("solutions")
        .select("id, exam_id, pdf_url")
        .eq("exam_id", exam_id)
        .execute()
        .data
    )

    return {"exam": exam, "solutions": solutions or []}


@router.post("/uploads")
@limiter.limit("30/minute")
def upload_document(
    request: Request, course_code: str, file_type: str, pdf_url: str, uploaded_by: str
):
    if file_type not in ["EXAM", "SOLUTION"]:
        raise HTTPException(status_code=400, detail="Invalid file type")
    if not pdf_url:
        raise HTTPException(status_code=400, detail="Missing pdf_url")

    res = (
        supabase.table("pending_uploads")
        .insert(
            {
                "course_code": course_code,
                "file_type": file_type,
                "pdf_url": pdf_url,
                "uploaded_by": uploaded_by,
                "original_filename": pdf_url.split("/")[-1],
                "status": "PENDING",
            }
        )
        .execute()
    )

    return {"message": "File uploaded for review", "data": res.data}


@router.post("/uploads/{upload_id}/approve")
def approve_upload(request: Request, upload_id: int):
    pending = (
        supabase.table("pending_uploads")
        .select("*")
        .eq("id", upload_id)
        .single()
        .execute()
        .data
    )
    if not pending:
        raise HTTPException(status_code=404, detail="Upload not found")

    filename = pending["original_filename"]

    try:
        file_data = supabase.storage.from_("pending-pdfs").download(filename)
    except Exception:
        raise HTTPException(
            status_code=500, detail="Failed to download file from pending bucket"
        )

    try:
        supabase.storage.from_("exam-pdfs").upload(
            filename, file_data, {"upsert": True}
        )
    except Exception:
        raise HTTPException(
            status_code=500, detail="Failed to upload file to exam bucket"
        )

    supabase.storage.from_("pending-pdfs").remove([filename])

    pdf_url = (
        f"https://{url.split('//')[1]}/storage/v1/object/public/exam-pdfs/{filename}"
    )

    if pending["file_type"] == "EXAM":
        supabase.table("exams").insert(
            {
                "course_code": pending["course_code"],
                "exam_date": str(datetime.now().date()),
                "pdf_url": pdf_url,
            }
        ).execute()
    else:
        supabase.table("solutions").insert(
            {"exam_id": None, "pdf_url": pdf_url}
        ).execute()

    supabase.table("pending_uploads").update({"status": "APPROVED"}).eq(
        "id", upload_id
    ).execute()

    return {"message": "Upload approved and moved successfully"}


@router.delete("/uploads/{upload_id}")
def reject_upload(request: Request, upload_id: int):
    pending = (
        supabase.table("pending_uploads")
        .select("*")
        .eq("id", upload_id)
        .single()
        .execute()
        .data
    )
    if not pending:
        raise HTTPException(status_code=404, detail="Upload not found")

    filename = pending["original_filename"]
    supabase.storage.from_("pending-pdfs").remove([filename])
    supabase.table("pending_uploads").delete().eq("id", upload_id).execute()

    return {"message": "Upload deleted successfully"}
