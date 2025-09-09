from fastapi import APIRouter, HTTPException
from typing import List

from app.core.database import SessionLocal, print_database_tables
from app.schemas import PostCard, PostTag
from app.crud import list_posts, get_post_by_id
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
                createdDate=page.written_date.isoformat() if page.written_date else None,
                isPinned=bool(page.pin),
            )

        return [to_card(p) for p in pages]
    finally:
        session.close()


@router.post("/notion/sync")
async def trigger_sync():
    result = sync_notion_pages()
    return result


@router.get("/notion/posts/{post_id}", response_model=PostCard)
async def get_post(post_id: str):
    session = SessionLocal()
    try:
        post = get_post_by_id(session, post_id)
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")

        def to_card(page: NotionPage) -> PostCard:
            return PostCard(
                id=page.id,
                coverUrl=page.cover_url,
                title=page.title,
                tags=[PostTag(id=t.id, name=t.name, color=t.color) for t in page.tags],
                createdDate=page.written_date.isoformat() if page.written_date else None,
                isPinned=bool(page.pin),
            )

        return to_card(post)
    finally:
        session.close()


