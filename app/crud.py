from typing import List
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
        .order_by(NotionPage.created_time.desc())
        .all()
    )


