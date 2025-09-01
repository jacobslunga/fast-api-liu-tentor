from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from slowapi.errors import RateLimitExceeded
from app.core.rate_limiter import limiter
from app.api import exams

ALLOWED_ORIGINS = [
    "https://liutentor.se",
    "https://www.liutentor.se",
    "https://admin.liutentor.se",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]


def origin_for(request: Request) -> str | None:
    o = request.headers.get("origin")
    return o if o in ALLOWED_ORIGINS else None


app = FastAPI(title="LiU Tentor Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
    max_age=86400,
)


@app.middleware("http")
async def ensure_cors_on_errors(request: Request, call_next):
    try:
        response = await call_next(request)
    except Exception:
        response = JSONResponse({"detail": "internal server error"}, status_code=500)
    o = origin_for(request)
    if o and "access-control-allow-origin" not in (
        k.lower() for k in response.headers.keys()
    ):
        response.headers["Access-Control-Allow-Origin"] = o
        response.headers["Vary"] = "Origin"
    return response


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    o = origin_for(request)
    headers = {}
    if o:
        headers["Access-Control-Allow-Origin"] = o
        headers["Vary"] = "Origin"
    return JSONResponse(
        {"detail": "rate limit exceeded"},
        status_code=429,
        headers=headers,
    )


app.state.limiter = limiter

app.include_router(exams.router, prefix="/api", tags=["Exams"])


@app.get("/")
@limiter.limit("30/minute")
def read_root(request: Request):
    return {
        "message": "Welcome to the LiU Tentor Backend API. Visit https://api.liutentor.se/docs for API documentation."
    }
