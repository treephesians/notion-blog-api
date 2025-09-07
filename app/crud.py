from typing import List
from sqlalchemy.orm import Session

from app.models import NotionPage


def list_posts(session: Session) -> List[NotionPage]:
    return (
        session.query(NotionPage)
        .filter(NotionPage.is_deleted.is_(False))
        .order_by(NotionPage.created_time.desc())
        .all()
    )


