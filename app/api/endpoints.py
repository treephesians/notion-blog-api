from fastapi import APIRouter, HTTPException
import os
from typing import List

from app.core.database import SessionLocal, print_database_tables
from app.schemas import PostCard, PostTag, ProjectCard, ProjectDetail, TypeInfo
from app.crud import list_posts, get_post_by_id, list_projects, get_project_by_id
from app.models import NotionPage
from app.notion import sync_notion_pages, sync_notion_projects


router = APIRouter()


@router.get("/")
async def root():
    print_database_tables()
    return {"message": "Hello World"}


@router.post("/notion/sync")
async def trigger_sync():
    posts_result = sync_notion_pages()
    projects_result = sync_notion_projects()
    return {
        "posts": posts_result,
        "projects": projects_result,
        "total_created": posts_result.get("created", 0) + projects_result.get("created", 0),
        "total_updated": posts_result.get("updated", 0) + projects_result.get("updated", 0),
        "total_items": posts_result.get("total", 0) + projects_result.get("total", 0),
    }


@router.post("/notion/projects/sync")
async def trigger_project_sync():
    result = sync_notion_projects()
    return result


@router.post("/notion/posts/sync/")
async def trigger_posts_sync():
    result = sync_notion_pages()
    return result


@router.get("/notion/projects", response_model=List[ProjectCard])
async def get_projects():
    session = SessionLocal()
    try:
        project_db_id = os.getenv("NOTION_PROJECT_DATABASE_ID")
        if not project_db_id:
            raise HTTPException(status_code=500, detail="NOTION_PROJECT_DATABASE_ID not configured")

        pages = list_projects(session, project_db_id)

        # 종료일(없으면 시작일) 기준 내림차순 정렬
        from datetime import datetime
        def parse_iso(dt: str):
            if not dt:
                return None
            try:
                return datetime.fromisoformat(dt.replace("Z", "+00:00"))
            except Exception:
                return None
        def end_key(page: NotionPage):
            props = page.raw_properties or {}
            date_obj = (props.get("기간") or {}).get("date") or {}
            end = parse_iso(date_obj.get("end"))
            start = parse_iso(date_obj.get("start"))
            return end or start or datetime.min
        pages = sorted(pages, key=end_key, reverse=True)

        def fmt_period(start: str, end: str) -> str:
            if not start:
                return None
            try:
                # prefer YYYY-MM
                from datetime import datetime
                s = datetime.fromisoformat(start.replace("Z", "+00:00")).strftime("%Y.%m")
                e = (
                    datetime.fromisoformat(end.replace("Z", "+00:00")).strftime("%Y.%m")
                    if end
                    else None
                )
                return f"{s} → {e}" if e else s
            except Exception:
                return f"{start} → {end}" if end else start

        def to_card(page: NotionPage) -> ProjectCard:
            props = page.raw_properties or {}
            date_obj = (props.get("기간") or {}).get("date") or {}
            start = date_obj.get("start")
            end = date_obj.get("end")
            period = fmt_period(start, end) if start else None

            return ProjectCard(
                id=page.id,
                coverUrl=page.cover_url,
                title=page.title,
                tags=[PostTag(id=t.id, name=t.name, color=t.color) for t in page.tags],
                createdDate=period,
                url=page.url,
                isPinned=bool(page.pin),
            )

        return [to_card(p) for p in pages]
    finally:
        session.close()


@router.get("/notion/projects/{project_id}", response_model=ProjectDetail)
async def get_project(project_id: str):
    session = SessionLocal()
    try:
        page = get_project_by_id(session, project_id)
        if not page:
            raise HTTPException(status_code=404, detail="Project not found")

        props = page.raw_properties or {}

        # cover
        cover_url = page.cover_url

        # title
        title = page.title

        # tags from Tag relation
        tags = [PostTag(id=t.id, name=t.name, color=t.color) for t in page.tags]

        # createdDate and period from 기간
        from datetime import datetime
        date_obj = (props.get("기간") or {}).get("date") or {}
        start = date_obj.get("start")
        end = date_obj.get("end")
        def fmt_period(start: str, end: str) -> str:
            if not start:
                return None
            try:
                s = datetime.fromisoformat(start.replace("Z", "+00:00")).strftime("%Y-%m")
                e = (
                    datetime.fromisoformat(end.replace("Z", "+00:00")).strftime("%Y-%m")
                    if end else None
                )
                return f"{s} ~ {e}" if e else s
            except Exception:
                return f"{start} ~ {end}" if end else start
        period = fmt_period(start, end) if start else None

        # site/github urls
        site = (props.get("사이트") or {}).get("url")
        github = (props.get("GitHub") or {}).get("url")

        # review relation (회고) - first id if available
        relation = (props.get("회고") or {}).get("relation") or []
        review_page_id = relation[0].get("id") if relation else None

        # type from 종류 (select)
        type_select = (props.get("종류") or {}).get("select") or None
        type_info = None
        if type_select:
            type_info = TypeInfo(name=type_select.get("name"), color=type_select.get("color"))

        # createdDate: align with extractProjectData(createdDate=period)
        created_date = period

        return ProjectDetail(
            id=page.id,
            coverUrl=cover_url,
            title=title,
            tags=tags,
            createdDate=created_date,
            period=period,
            site=site,
            github=github,
            reviewPageId=review_page_id,
            type=type_info,
        )
    finally:
        session.close()


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


