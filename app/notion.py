import os
import requests
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional
from pathlib import Path
from datetime import datetime

from app.core.database import session_scope
from app.models import NotionPage, Tag, Status

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_API_KEY")
DATABASE_ID = os.getenv("NOTION_POST_DATABASE_ID")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

BASE_DIR = Path(__file__).resolve().parent.parent

def fetch_notion_data():
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    response = requests.post(url, headers=HEADERS)
    response.raise_for_status()
    results = response.json().get("results", [])
    return results


def _iso_to_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _get_text_title(prop: Dict[str, Any]) -> Optional[str]:
    if not prop:
        return None
    items = prop.get("title") or []
    if not items:
        return None
    # Concatenate plain_texts
    return "".join([i.get("plain_text") or i.get("text", {}).get("content", "") for i in items]) or None


def _get_rich_text(prop: Dict[str, Any]) -> Optional[str]:
    items = prop.get("rich_text") or []
    if not items:
        return None
    return "".join([i.get("plain_text") or i.get("text", {}).get("content", "") for i in items]) or None


def _extract_tags(prop: Dict[str, Any]) -> List[Dict[str, str]]:
    return prop.get("multi_select") or []


def _extract_status(prop: Dict[str, Any]) -> Optional[Dict[str, str]]:
    return prop.get("status") or None


def _ensure_static_covers_dir() -> Path:
    static_dir = BASE_DIR / "static" / "covers"
    static_dir.mkdir(parents=True, exist_ok=True)
    return static_dir


def _compute_incoming_ids(results: List[Dict[str, Any]]) -> set:
    incoming_ids = set()
    for item in results:
        page_id = item.get("id")
        if page_id:
            incoming_ids.add(page_id)
    return incoming_ids


def _mark_missing_pages_deleted(session, incoming_ids: set) -> None:
    if not incoming_ids:
        return
    existing_ids = {p.id for p in session.query(NotionPage.id).all()}
    missing_ids = existing_ids - incoming_ids
    if not missing_ids:
        return
    session.query(NotionPage).filter(NotionPage.id.in_(list(missing_ids))).update(
        {NotionPage.is_deleted: True}, synchronize_session=False
    )


def _parse_properties(item: Dict[str, Any]) -> Dict[str, Any]:
    parent = item.get("parent", {})
    database_id = parent.get("database_id")
    props = item.get("properties", {})

    title = _get_text_title(props.get("이름") or props.get("Name") or {})
    slug = _get_rich_text(props.get("slug") or {})

    written_date_prop = props.get("작성일") or props.get("Date") or {}
    written_date = None
    if written_date_prop and written_date_prop.get("date"):
        written_date = written_date_prop["date"].get("start")

    status_prop = _extract_status(props.get("상태") or props.get("Status") or {})
    tags_prop = _extract_tags(props.get("태그") or props.get("Tags") or {})

    cover = item.get("cover") or {}
    cover_url = None
    cover_expiry_time = None
    if cover and cover.get("type") == "file" and cover.get("file"):
        cover_url = cover["file"].get("url")
        cover_expiry_time = cover["file"].get("expiry_time")

    return {
        "database_id": database_id,
        "props": props,
        "title": title,
        "slug": slug,
        "written_date": written_date,
        "status_prop": status_prop,
        "tags_prop": tags_prop,
        "cover_url": cover_url,
        "cover_expiry_time": cover_expiry_time,
        "item_url": item.get("url"),
        "public_url": item.get("public_url"),
        "created_time": item.get("created_time"),
        "last_edited_time": item.get("last_edited_time"),
        "created_by_id": (item.get("created_by") or {}).get("id"),
        "last_edited_by_id": (item.get("last_edited_by") or {}).get("id"),
        "archived": item.get("archived"),
        "in_trash": item.get("in_trash"),
        "pin": (props.get("PIN") or {}).get("checkbox", False),
    }


def _upsert_status_if_needed(session, status_prop: Optional[Dict[str, Any]]) -> Optional[str]:
    if not status_prop:
        return None
    status_id = status_prop.get("id")
    if not status_id:
        return None
    existing_status = session.get(Status, status_id)
    if not existing_status:
        session.add(Status(id=status_id, name=status_prop.get("name"), color=status_prop.get("color")))
    return status_id


def _download_cover_if_available(cover_url: Optional[str], static_dir: Path, page_id: str) -> Optional[str]:
    if not cover_url:
        return None
    try:
        resp = requests.get(cover_url, timeout=15)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        ext = ""
        if "image/" in content_type:
            ext = "." + content_type.split("/")[-1].split(";")[0]
        else:
            ext = ".jpg"
        filename = f"{page_id}{ext}"
        file_path = static_dir / filename
        with open(file_path, "wb") as f:
            f.write(resp.content)
        return f"/static/covers/{filename}"
    except Exception:
        return None


def _upsert_page_and_relations(
    session,
    page_id: str,
    database_id: str,
    parsed: Dict[str, Any],
    local_cover_path: Optional[str],
) -> bool:
    page = session.get(NotionPage, page_id)
    is_new = False
    if not page:
        page = NotionPage(id=page_id, database_id=database_id)
        session.add(page)
        is_new = True

    page.url = parsed.get("item_url")
    page.public_url = parsed.get("public_url")
    page.created_time = _iso_to_dt(parsed.get("created_time"))
    page.last_edited_time = _iso_to_dt(parsed.get("last_edited_time"))
    page.created_by_user_id = parsed.get("created_by_id")
    page.last_edited_by_user_id = parsed.get("last_edited_by_id")
    page.archived = bool(parsed.get("archived"))
    page.in_trash = bool(parsed.get("in_trash"))
    page.cover_url = local_cover_path or parsed.get("cover_url")
    page.cover_expiry_time = _iso_to_dt(parsed.get("cover_expiry_time")) if parsed.get("cover_expiry_time") else None
    page.icon = None
    page.pin = bool(parsed.get("pin"))
    page.status_id = parsed.get("status_id")
    page.slug = parsed.get("slug")
    page.title = parsed.get("title")
    written_date = parsed.get("written_date")
    page.written_date = datetime.fromisoformat(written_date).date() if written_date else None
    page.raw_properties = parsed.get("props") or {}
    page.is_deleted = False

    page.tags.clear()
    for t in parsed.get("tags_prop") or []:
        tag_id = t.get("id")
        if not tag_id:
            continue
        tag = session.get(Tag, tag_id)
        if not tag:
            tag = Tag(id=tag_id, name=t.get("name"), color=t.get("color"))
            session.add(tag)
        page.tags.append(tag)
    return is_new


def sync_notion_pages() -> dict:
    results = fetch_notion_data()
    created = 0
    updated = 0
    static_dir = _ensure_static_covers_dir()
    with session_scope() as session:
        incoming_ids = _compute_incoming_ids(results)
        _mark_missing_pages_deleted(session, incoming_ids)

        for item in results:
            page_id = item.get("id")
            existing_page = session.get(NotionPage, page_id)
            item_last_edited = _iso_to_dt(item.get("last_edited_time"))

            if existing_page and existing_page.last_edited_time and item_last_edited and existing_page.last_edited_time == item_last_edited:
                continue

            parsed = _parse_properties(item)

            status_id = _upsert_status_if_needed(session, parsed.get("status_prop"))
            parsed["status_id"] = status_id

            local_cover_path = _download_cover_if_available(parsed.get("cover_url"), static_dir, page_id)
            is_new = _upsert_page_and_relations(
                session=session,
                page_id=page_id,
                database_id=parsed.get("database_id"),
                parsed=parsed,
                local_cover_path=local_cover_path,
            )
            if is_new:
                created += 1
            else:
                updated += 1

    return {"created": created, "updated": updated, "total": len(results)}


