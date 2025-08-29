from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from app.api import exams, chat
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from fastapi.middleware.cors import CORSMiddleware
from app.core.rate_limiter import limiter

app = FastAPI(title="LiU Tentor Backend")
templates = Jinja2Templates(directory="app/templates")


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://liutentor.se",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.include_router(exams.router, prefix="/api", tags=["Exams"])
app.include_router(chat.router, prefix="/api", tags=["Chat"])


@app.get("/")
@limiter.limit("30/minute")
def read_root(request: Request):
    return {
        "message": "Welcome to the LiU Tentor Backend API. Visit https://api.liutentor.se/docs for API documentation."
    }
