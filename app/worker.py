import os
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.notion import sync_notion_pages, sync_notion_projects


def sync_all():
    try:
        print("[worker] syncing notion posts...")
        sync_notion_pages()
    except Exception as e:
        print(f"[worker] posts sync failed: {e}")
    try:
        print("[worker] syncing notion projects...")
        sync_notion_projects()
    except Exception as e:
        print(f"[worker] projects sync failed: {e}")


def main():
    if os.getenv("ENABLE_SCHEDULER", "1") != "1":
        print("[worker] scheduler disabled via env; exiting")
        return

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        sync_all,
        IntervalTrigger(hours=1),
        id="hourly_notion_sync",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=600,
        max_instances=1,
    )
    print("[worker] scheduler started")
    try:
        print("[worker] running initial notion sync...")
        sync_all()
    except Exception as e:
        print(f"[worker] initial sync failed: {e}")
    scheduler.start()


if __name__ == "__main__":
    main()


