import os
import requests
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional
from pathlib import Path
from datetime import datetime

from db import session_scope, NotionPage, Tag, Status

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_API_KEY")
DATABASE_ID = os.getenv("NOTION_POST_DATABASE_ID")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

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


def sync_notion_pages() -> dict:
    results = fetch_notion_data()
    created = 0
    updated = 0
    # Prepare local directory for cover images
    static_dir = Path("static/covers")
    static_dir.mkdir(parents=True, exist_ok=True)
    with session_scope() as session:
        for item in results:
            page_id = item.get("id")
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
            status_id = None
            if status_prop:
                status_id = status_prop.get("id")
                # upsert status
                if status_id:
                    existing_status = session.get(Status, status_id)
                    if not existing_status:
                        session.add(Status(id=status_id, name=status_prop.get("name"), color=status_prop.get("color")))

            tags_prop = _extract_tags(props.get("태그") or props.get("Tags") or {})

            cover = item.get("cover") or {}
            cover_url = None
            cover_expiry_time = None
            if cover and cover.get("type") == "file" and cover.get("file"):
                cover_url = cover["file"].get("url")
                cover_expiry_time = cover["file"].get("expiry_time")
            # Try downloading the cover locally (Notion provides time-limited signed URLs)
            local_cover_path = None
            if cover_url:
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
                    local_cover_path = f"/static/covers/{filename}"
                except Exception:
                    local_cover_path = None

            page = session.get(NotionPage, page_id)
            is_new = False
            if not page:
                page = NotionPage(id=page_id, database_id=database_id)
                session.add(page)
                is_new = True

            page.url = item.get("url")
            page.public_url = item.get("public_url")
            page.created_time = _iso_to_dt(item.get("created_time"))
            page.last_edited_time = _iso_to_dt(item.get("last_edited_time"))
            page.created_by_user_id = (item.get("created_by") or {}).get("id")
            page.last_edited_by_user_id = (item.get("last_edited_by") or {}).get("id")
            page.archived = bool(item.get("archived"))
            page.in_trash = bool(item.get("in_trash"))
            page.cover_url = local_cover_path or cover_url
            page.cover_expiry_time = _iso_to_dt(cover_expiry_time) if cover_expiry_time else None
            page.icon = None  # 간소화
            page.pin = bool((props.get("PIN") or {}).get("checkbox", False))
            page.status_id = status_id
            page.slug = slug
            page.title = title
            page.written_date = datetime.fromisoformat(written_date).date() if written_date else None
            page.raw_properties = props

            # upsert tags and association
            page.tags.clear()
            for t in tags_prop:
                tag_id = t.get("id")
                if not tag_id:
                    continue
                tag = session.get(Tag, tag_id)
                if not tag:
                    tag = Tag(id=tag_id, name=t.get("name"), color=t.get("color"))
                    session.add(tag)
                page.tags.append(tag)
            if is_new:
                created += 1
            else:
                updated += 1

        # commit via context manager
    return {"created": created, "updated": updated, "total": len(results)}