from typing import List, Optional
from sqlalchemy.orm import Session

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


