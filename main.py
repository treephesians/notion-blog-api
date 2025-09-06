from fastapi import FastAPI
from typing import List
from pydantic import BaseModel

from db import init_db, print_database_tables, SessionLocal, NotionPage

from notion import sync_notion_pages

app = FastAPI()


@app.get("/")
async def root():
    print_database_tables()
    return {"message": "Hello World"}


class PostTag(BaseModel):
    id: str
    name: str
    color: str | None = None


class PostCard(BaseModel):
    id: str
    coverUrl: str | None = None
    title: str | None = None
    tags: List[PostTag]
    createdDate: str | None = None
    isPinned: bool = False


@app.get("/notion/posts", response_model=List[PostCard])
async def list_Posts():
    session = SessionLocal()
    try:
        pages = (
            session.query(NotionPage)
            .order_by(NotionPage.created_time.desc())
            .all()
        )

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


@app.post("/notion/sync")
async def trigger_sync():
    result = sync_notion_pages()
    return result