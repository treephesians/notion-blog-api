from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from typing import List
from pydantic import BaseModel
from time import perf_counter

import os
from app.api.endpoints import router as api_router
from app.core.database import Base, engine
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
from app.notion import sync_notion_pages, sync_notion_projects

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure models are imported so that tables are registered
    import app.models  # noqa: F401
    Base.metadata.create_all(bind=engine)

    enable_scheduler = os.getenv("ENABLE_SCHEDULER", "1") == "1"
    scheduler = None
    if enable_scheduler:
        # Start background scheduler for hourly sync
        scheduler = BackgroundScheduler(timezone="UTC")

    def sync_all():
        try:
            print("[scheduler] syncing notion posts...")
            sync_notion_pages()
        except Exception as e:
            print(f"[scheduler] posts sync failed: {e}")
        try:
            print("[scheduler] syncing notion projects...")
            sync_notion_projects()
        except Exception as e:
            print(f"[scheduler] projects sync failed: {e}")

    if scheduler:
        job = scheduler.add_job(
            sync_all,
            IntervalTrigger(hours=1),
            id="hourly_notion_sync",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=600,
            max_instances=1,
        )
        scheduler.start()
        try:
            print(f"[scheduler] started. next_run_time={job.next_run_time}")
        except Exception:
            pass

    # keep reference for debugging/inspection
    try:
        app.state.scheduler = scheduler
    except Exception:
        pass

    # Optionally run initial sync once on startup (only when scheduler enabled)
    if scheduler:
        try:
            print("[startup] running initial notion sync...")
            sync_all()
        except Exception as e:
            print(f"[startup] initial sync failed: {e}")

    try:
        yield
    finally:
        if scheduler:
            scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)

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

