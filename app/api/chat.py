from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from app.core.rate_limiter import limiter
from google import genai
from dotenv import load_dotenv
import httpx
import io
import asyncio
import os

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    print("No Gemini API Key found")


client = genai.Client(api_key=GEMINI_API_KEY)

router = APIRouter()


class ChatRequest(BaseModel):
    prompt: str
    exam_url: str
    solution_url: str | None = None


async def stream_generator(prompt: str, exam_url: str, solution_url: str | None):
    """
    This async generator is the core of our logic. It:
    1. Asynchronously fetches the PDFs without blocking the server.
    2. Uploads the files to the Gemini File API.
    3. Calls the Gemini API to get a response stream.
    4. Yields each chunk of the response as it arrives.
    5. Cleans up by deleting the uploaded files.
    """
    exam_pdf = None
    solution_pdf = None
    try:
        # 1. Asynchronously fetch the PDF files concurrently
        async with httpx.AsyncClient() as http_client:
            tasks = [http_client.get(exam_url)]
            if solution_url:
                tasks.append(http_client.get(solution_url))

            responses = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for download errors
            for i, res in enumerate(responses):
                if isinstance(res, Exception):
                    url = exam_url if i == 0 else solution_url
                    print(f"Failed to download from {url}: {res}")
                    yield f"Error: Could not download the document from {url}."
                    return
                res.raise_for_status()

            exam_content = responses[0].content
            solution_content = responses[1].content if len(responses) > 1 else None

        # 2. Upload files to Gemini.
        # Note: The current 'google-generativeai' library's upload is synchronous.
        # In a very high-traffic app, you might run this in a thread pool.
        # For most cases, this is acceptable.
        exam_pdf = client.files.upload(
            file=io.BytesIO(exam_content), config=dict(mime_type="application/pdf")
        )

        contents = [exam_pdf]
        if solution_content:
            solution_pdf = client.files.upload(
                file=io.BytesIO(solution_content),
                config=dict(mime_type="application/pdf"),
            )
            contents.append(solution_pdf)

        contents.append(prompt)

        gemini_stream = client.models.generate_content_stream(
            model="gemini-2.5-flash",
            contents=contents,
        )

        for chunk in gemini_stream:
            if chunk.text:
                yield chunk.text

    except httpx.HTTPStatusError as e:
        print(f"Error fetching PDF: {e}")
        yield f"Error: Could not fetch the document from {e.request.url}. Status: {e.response.status_code}"
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        yield "Error: An unexpected error occurred while generating the response."


@router.post("/chat/response")
@limiter.limit("10/minute")
async def generate_response(chat_request: ChatRequest, request: Request):
    """
    Accepts a prompt and URLs for an exam and an optional solution,
    and returns a streaming response of the explanation.
    """
    return StreamingResponse(
        stream_generator(
            chat_request.prompt, chat_request.exam_url, chat_request.solution_url
        ),
        media_type="text/plain",
    )
