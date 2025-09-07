from fastapi import APIRouter
from typing import List

from app.core.database import SessionLocal, print_database_tables
from app.schemas import PostCard, PostTag
from app.crud import list_posts
from app.models import NotionPage
from app.notion import sync_notion_pages


router = APIRouter()


@router.get("/")
async def root():
    print_database_tables()
    return {"message": "Hello World"}

@router.get("/notion/posts", response_model=List[PostCard])
async def get_posts():
    session = SessionLocal()
    try:
        pages = list_posts(session)

        def to_card(page: NotionPage) -> PostCard:
            return PostCard(
                id=page.id,
                coverUrl=page.cover_url,
                title=page.title,
                tags=[PostTag(id=t.id, name=t.name, color=t.color) for t in page.tags],
                createdDate=page.created_time.date().isoformat() if page.created_time else None,
                isPinned=bool(page.pin),
            )

        return [to_card(p) for p in pages]
    finally:
        session.close()


@router.post("/notion/sync")
async def trigger_sync():
    result = sync_notion_pages()
    return result


