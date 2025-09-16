from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models import NotionPage, Status


def list_posts(session: Session) -> List[NotionPage]:
    return (
        session.query(NotionPage)
        .join(Status, NotionPage.status_id == Status.id)
        .filter(
            NotionPage.is_deleted.is_(False),
            Status.name == "완료",
        )
        .order_by(NotionPage.written_date.desc())
        .all()
    )


def get_post_by_id(session: Session, post_id: str) -> Optional[NotionPage]:
    return (
        session.query(NotionPage)
        .join(Status, NotionPage.status_id == Status.id)
        .filter(
            NotionPage.id == post_id,
            NotionPage.is_deleted.is_(False),
            Status.name == "완료",
        )
        .first()
    )


def list_projects(session: Session, project_database_id: str) -> List[NotionPage]:
    # Normalize DB ID by removing hyphens for robust matching
    normalized = (project_database_id or "").replace("-", "").lower()
    return (
        session.query(NotionPage)
        .filter(
            NotionPage.is_deleted.is_(False),
            func.lower(func.replace(NotionPage.database_id, "-", "")) == normalized,
        )
        .order_by(NotionPage.pin.desc(), NotionPage.last_edited_time.desc())
        .all()
    )


def get_project_by_id(session: Session, project_id: str) -> Optional[NotionPage]:
    return (
        session.query(NotionPage)
        .filter(
            NotionPage.id == project_id,
            NotionPage.is_deleted.is_(False),
        )
        .first()
    )

