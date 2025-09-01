from google import genai
from dotenv import load_dotenv
import os
import io
import httpx

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    print("No gemini api key")
    exit(-1)

client = genai.Client()

exam = "https://mkunnogkkppsclxprvvb.supabase.co/storage/v1/object/public/exam-pdfs/TATA24_2025-08-22_EXAM.pdf"
solution = "https://mkunnogkkppsclxprvvb.supabase.co/storage/v1/object/public/exam-pdfs/TATA24_2025-08-22_SOLUTION.pdf"

doc_exam = io.BytesIO(httpx.get(exam).content)
doc_solution = io.BytesIO(httpx.get(solution).content)

exam_pdf = client.files.upload(file=doc_exam, config=dict(mime_type="application/pdf"))
solution_pdf = client.files.upload(
    file=doc_solution, config=dict(mime_type="application/pdf")
)

prompt = "Förklara fråga 1"

response = client.models.generate_content(
    model="gemini-2.5-flash", contents=[exam_pdf, solution_pdf, prompt]
)
print(response.text)
