from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from typing import List
from pydantic import BaseModel
from time import perf_counter

from app.api.endpoints import router as api_router

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent.parent

# Serve static files (e.g., downloaded cover images)
# Ensure base static directory exists
(BASE_DIR / "static").mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

@app.middleware("http")
async def log_process_time(request: Request, call_next):
    start = perf_counter()
    response = await call_next(request)
    duration = perf_counter() - start
    response.headers["X-Process-Time"] = f"{duration:.3f}s"
    try:
        print(f"{request.method} {request.url.path} {response.status_code} - {duration*1000:.1f} ms")
    except Exception:
        pass
    return response

app.include_router(api_router)


