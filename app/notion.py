import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
import tempfile
import shutil
from PIL import Image
import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from app.core.database import session_scope
from app.models import NotionPage, Tag, Status

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_API_KEY")
POST_DATABASE_ID = os.getenv("NOTION_POST_DATABASE_ID")
PROJECT_DATABASE_ID = os.getenv("NOTION_PROJECT_DATABASE_ID")
REQUEST_TIMEOUT = float(os.getenv("NOTION_HTTP_TIMEOUT", "15"))

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

BASE_DIR = Path(__file__).resolve().parent.parent

# S3 configuration
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = (os.getenv("S3_PREFIX") or "").strip("/")  # e.g., cover-image
S3_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-northeast-2"
S3_ENDPOINT_URL = (os.getenv("S3_ENDPOINT_URL") or None)  # optional for custom endpoints; treat empty as None
S3_PUBLIC_BASE_URL = (os.getenv("S3_PUBLIC_BASE_URL") or "").rstrip("/")  # optional CDN/base url

# Revalidation webhook
REVALIDATE_URL = os.getenv("REVALIDATE_URL", "https://jblog.my/api/revalidate")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

_s3_client = None
_http_session = None

# Cover processing configs (env-driven)
COVER_DOWNLOAD_ENABLED = (os.getenv("COVER_DOWNLOAD_ENABLED", "1") == "1")
COVER_SOFT_MAX_BYTES = int(os.getenv("COVER_SOFT_MAX_BYTES", str(12 * 1024 * 1024)))  # 12MB
COVER_HARD_MAX_BYTES = int(os.getenv("COVER_HARD_MAX_BYTES", str(50 * 1024 * 1024)))  # 50MB
COVER_MAX_DIM = int(os.getenv("COVER_MAX_DIM", "1600"))  # max longer edge pixels
COVER_QUALITY = int(os.getenv("COVER_QUALITY", "85"))
DOWNLOAD_CHUNK_SIZE = int(os.getenv("COVER_CHUNK_SIZE", str(256 * 1024)))  # 256KB
# Debug logging toggles (현재는 분기와 무관하게 주요 단계는 모두 출력)
LOG_COVER_DEBUG = (os.getenv("LOG_COVER_DEBUG", "0") == "1")


def _post_revalidate(tag: str) -> None:
    """Fire-and-forget revalidation webhook. Never raise; just log."""
    url = (REVALIDATE_URL or "").strip()
    secret = (WEBHOOK_SECRET or "").strip()
    if not url or not secret:
        print(f"[revalidate] skip (missing url/secret) url={url!r} secret_present={bool(secret)} tag={tag}")
        return
    try:
        resp = requests.post(
            url,
            headers={
                "x-webhook-secret": secret,
                "Content-Type": "application/json",
            },
            json={"tag": tag},
            timeout=5,
        )
        try:
            resp.raise_for_status()
        except Exception as e:
            print(f"[revalidate] failed status={resp.status_code} body={resp.text[:200]} exc={e}")
            return
        print(f"[revalidate] success tag={tag} status={resp.status_code}")
    except Exception as e:
        print(f"[revalidate] error tag={tag} exc={e}")

def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            region_name=S3_REGION,
            endpoint_url=S3_ENDPOINT_URL,
            config=BotoConfig(s3={"addressing_style": "virtual"}),
        )
    return _s3_client


def _get_http_session() -> requests.Session:
    global _http_session
    if _http_session is None:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _http_session = session
    return _http_session


def _download_stream_to_temp(url: str) -> Optional[Tuple[str, str, int]]:
    """Download URL to a temp file by streaming. Returns (path, content_type, total_bytes).
    Obeys hard size limit to avoid excessive disk usage.
    """
    try:
        print(f"[cover][trace] stream_download start url={url[:120]}")
        with _get_http_session().get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
            r.raise_for_status()
            ctype = r.headers.get("Content-Type", "") or ""
            # Early reject by hard limit if Content-Length provided
            clen = r.headers.get("Content-Length")
            if clen and int(clen) > COVER_HARD_MAX_BYTES:
                print(f"[cover][trace] stream_download reject by Content-Length={clen} > HARD={COVER_HARD_MAX_BYTES}")
                return None

            total = 0
            tmp = tempfile.NamedTemporaryFile(delete=False)
            try:
                for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > COVER_HARD_MAX_BYTES:
                        print(f"[cover][trace] stream_download abort: total={total} > HARD={COVER_HARD_MAX_BYTES}")
                        try:
                            tmp.close()
                        finally:
                            try:
                                os.unlink(tmp.name)
                            except Exception:
                                pass
                        return None
                    tmp.write(chunk)
                tmp.flush()
                path = tmp.name
            finally:
                try:
                    tmp.close()
                except Exception:
                    pass
        print(f"[cover][trace] stream_download done bytes={total} ctype={ctype}")
        return path, ctype, total
    except Exception as e:
        print(f"[cover][trace] stream_download error exc={e}")
        return None


def _image_dims(path: str) -> Optional[Tuple[int, int]]:
    try:
        with Image.open(path) as img:
            return img.size  # (width, height)
    except Exception:
        return None


def _process_image_to_limit(src_path: str, soft_limit: int, max_dim: int, quality: int,
                            preferred_format: Optional[str] = None) -> Optional[Tuple[str, str, str]]:
    """Downscale and re-encode image if needed to satisfy size and dimension constraints.
    Returns (processed_path, content_type, ext)
    - If the original already satisfies constraints, returns original path without re-encode.
    - Uses JPEG by default for re-encoding to reduce size; keeps PNG/WebP when small enough.
    """
    try:
        orig_size = os.path.getsize(src_path)
        dims = _image_dims(src_path)

        # Decide pass-through
        if (orig_size <= soft_limit) and dims and max(dims) <= max_dim:
            print(f"[cover][trace] process passthrough size={orig_size} dims={dims}")
            # Keep original file and content type best-effort
            # Detect ext/content-type by Pillow
            with Image.open(src_path) as img:
                fmt = (img.format or "JPEG").upper()
            if fmt == "PNG":
                ctype = "image/png"
                ext = ".png"
            elif fmt == "WEBP":
                ctype = "image/webp"
                ext = ".webp"
            else:
                ctype = "image/jpeg"
                ext = ".jpg"
            return src_path, ctype, ext

        # Need processing: open and re-encode with max_dim + quality
        with Image.open(src_path) as img:
            # Convert to RGB for JPEG if needed
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            # Preserve aspect ratio, bound by max_dim
            img.thumbnail((max_dim, max_dim))

            out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
            out_path = out_tmp.name
            try:
                out_tmp.close()
            except Exception:
                pass

            img.save(out_path, format=preferred_format or "JPEG", quality=quality, optimize=True)
            ctype = "image/jpeg" if (preferred_format or "JPEG").upper() == "JPEG" else f"image/{(preferred_format or 'jpeg').lower()}"
            ext = ".jpg"
            try:
                psize = os.path.getsize(out_path)
            except Exception:
                psize = None
            print(f"[cover][trace] process reencode -> size={psize} max_dim={max_dim} quality={quality}")
            return out_path, ctype, ext
    except Exception as e:
        print(f"[cover][trace] process error exc={e}")
        return None

def fetch_notion_data_for_db(database_id: str):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    response = requests.post(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
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


def _mark_missing_pages_deleted(session, incoming_ids: set, database_id: str) -> None:
    if not incoming_ids:
        return
    existing_ids = {row[0] for row in session.query(NotionPage.id).filter(NotionPage.database_id == database_id).all()}
    missing_ids = existing_ids - incoming_ids
    if not missing_ids:
        return
    session.query(NotionPage).filter(
        NotionPage.database_id == database_id,
        NotionPage.id.in_(list(missing_ids))
    ).update({NotionPage.is_deleted: True}, synchronize_session=False)


def _parse_properties(item: Dict[str, Any]) -> Dict[str, Any]:
    parent = item.get("parent", {})
    database_id = parent.get("database_id")
    props = item.get("properties", {})

    title = _get_text_title(props.get("이름") or props.get("Name") or {})
    slug = _get_rich_text(props.get("slug") or {})

    written_date_prop = props.get("작성일") or props.get("Date") or props.get("기간") or {}
    written_date = None
    if written_date_prop and written_date_prop.get("date"):
        written_date = written_date_prop["date"].get("start")

    # For project DB, there might be a period with start/end
    period_start = None
    period_end = None
    if (props.get("기간") or {}).get("date"):
        period_start = (props.get("기간") or {})["date"].get("start")
        period_end = (props.get("기간") or {})["date"].get("end")

    status_prop = _extract_status(props.get("상태") or props.get("Status") or {})

    # Merge possible tag sources: 일반 태그/Tags, 기술(multi_select), 종류(select -> as single tag)
    tags_prop = []
    for candidate in [props.get("태그"), props.get("Tags"), props.get("기술")]:
        if candidate:
            tags_prop.extend(_extract_tags(candidate))
    # 종류(select)를 태그로 변환해 추가
    type_select = (props.get("종류") or {}).get("select")
    if type_select:
        tags_prop.append({
            "id": type_select.get("id"),
            "name": type_select.get("name"),
            "color": type_select.get("color"),
        })

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
        "period_start": period_start,
        "period_end": period_end,
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
        print(f"[cover][trace] skip(no_url) page_id={page_id}")
        return None
    if not COVER_DOWNLOAD_ENABLED:
        print(f"[cover][trace] skip(disabled) page_id={page_id}")
        return None
    try:
        # 1) Stream download to temp
        print(f"[cover][trace] downloading page_id={page_id}")
        dl = _download_stream_to_temp(cover_url)
        if not dl:
            print(f"[cover][trace] download_failed_or_hardlimit page_id={page_id}")
            return None
        tmp_path, content_type, total_bytes = dl
        print(f"[cover][trace] downloaded bytes={total_bytes} ctype={content_type} page_id={page_id}")

        # Ensure it's an image; if not clearly image, try opening via PIL as a final check
        if not (content_type.startswith("image/") if content_type else False):
            try:
                with Image.open(tmp_path):
                    pass
                # If Pillow can open, treat as jpeg by default
                content_type = "image/jpeg"
            except Exception:
                print(f"[cover][trace] not_image page_id={page_id}")
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
                return None

        # 2) Downscale/re-encode if needed to meet soft constraints
        proc = _process_image_to_limit(
            tmp_path,
            soft_limit=COVER_SOFT_MAX_BYTES,
            max_dim=COVER_MAX_DIM,
            quality=COVER_QUALITY,
            preferred_format="JPEG",
        )
        if not proc:
            print(f"[cover][trace] process_failed page_id={page_id}")
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            return None
        processed_path, processed_ctype, ext = proc
        try:
            psize = os.path.getsize(processed_path)
        except Exception:
            psize = None
        print(f"[cover][trace] processed size={psize} ext={ext} ctype={processed_ctype} page_id={page_id}")

        # 3) Save: S3 or local files, then cleanup
        filename = f"{page_id}{ext}"
        print(S3_BUCKET)
        if S3_BUCKET:
            print(f"[cover][trace] s3_branch bucket={S3_BUCKET} prefix={S3_PREFIX} page_id={page_id}")
            key = "/".join([p for p in [S3_PREFIX, filename] if p])
            s3 = _get_s3_client()

            try:
                # Use upload_file with controlled concurrency to reduce memory
                cfg = TransferConfig(
                    multipart_threshold=5 * 1024 * 1024,
                    multipart_chunksize=5 * 1024 * 1024,
                    max_concurrency=2,
                    use_threads=True,
                )
                s3.upload_file(
                    Filename=processed_path,
                    Bucket=S3_BUCKET,
                    Key=key,
                    ExtraArgs={
                        "ContentType": processed_ctype or "image/jpeg",
                        "CacheControl": "public, max-age=31536000",
                        "ACL": "public-read",
                    },
                    Config=cfg,
                )
                # success log
                try:
                    fsize = os.path.getsize(processed_path)
                except Exception:
                    fsize = None
                print(f"[cover][s3-upload] success bucket={S3_BUCKET} key={key} size={fsize} content_type={processed_ctype}")
            except ClientError as ce:
                try:
                    err_code = (ce.response or {}).get("Error", {}).get("Code")
                    err_msg = (ce.response or {}).get("Error", {}).get("Message")
                except Exception:
                    err_code = None
                    err_msg = None
                print(f"[cover][s3-upload] client error bucket={S3_BUCKET} key={key} code={err_code} msg={err_msg} exc={ce}")
                # Fallback to local save
                try:
                    dest_path = static_dir / filename
                    static_dir.mkdir(parents=True, exist_ok=True)
                    with open(processed_path, "rb") as src, open(dest_path, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=DOWNLOAD_CHUNK_SIZE)
                    ret = f"/static/covers/{filename}"
                    print(f"[cover][trace] fallback_local(client_error) url={ret}")
                    return ret
                except Exception as fe:
                    print(f"[cover][trace] fallback_local_failed exc={fe}")
                    raise
            except Exception as e:
                print(f"[cover][s3-upload] failed bucket={S3_BUCKET} key={key} exc={e}")
                # Fallback to local save
                try:
                    dest_path = static_dir / filename
                    static_dir.mkdir(parents=True, exist_ok=True)
                    with open(processed_path, "rb") as src, open(dest_path, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=DOWNLOAD_CHUNK_SIZE)
                    ret = f"/static/covers/{filename}"
                    print(f"[cover][trace] fallback_local(upload_failed) url={ret}")
                    return ret
                except Exception as fe:
                    print(f"[cover][trace] fallback_local_failed exc={fe}")
                    raise
            finally:
                try:
                    os.unlink(processed_path)
                except Exception:
                    pass
                try:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass

            if S3_PUBLIC_BASE_URL:
                print(f"[cover][trace] return_url cdn={S3_PUBLIC_BASE_URL}/{key}")
                return f"{S3_PUBLIC_BASE_URL}/{key}"
            print(f"[cover][trace] return_url s3=https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}")
            return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"
        else:
            print(f"[cover][trace] local_branch page_id={page_id} filename={filename}")
            dest_path = static_dir / filename
            # Ensure dir exists
            static_dir.mkdir(parents=True, exist_ok=True)
            try:
                with open(processed_path, "rb") as src, open(dest_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=DOWNLOAD_CHUNK_SIZE)
            finally:
                try:
                    os.unlink(processed_path)
                except Exception:
                    pass
                try:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
            ret = f"/static/covers/{filename}"
            print(f"[cover][trace] return_url local={ret}")
            return ret
    except Exception as e:
        print(f"[cover][trace] error page_id={page_id} exc={e}")
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
    if not POST_DATABASE_ID:
        raise RuntimeError("NOTION_POST_DATABASE_ID is not set")
    results = fetch_notion_data_for_db(POST_DATABASE_ID)
    created = 0
    updated = 0
    static_dir = _ensure_static_covers_dir()
    with session_scope() as session:
        incoming_ids = _compute_incoming_ids(results)
        _mark_missing_pages_deleted(session, incoming_ids, POST_DATABASE_ID)

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

    # trigger revalidation if any change
    try:
        if created or updated:
            _post_revalidate("posts")
    except Exception:
        pass

    return {"created": created, "updated": updated, "total": len(results)}


def sync_notion_projects() -> dict:
    if not PROJECT_DATABASE_ID:
        raise RuntimeError("NOTION_PROJECT_DATABASE_ID is not set")
    results = fetch_notion_data_for_db(PROJECT_DATABASE_ID)
    created = 0
    updated = 0
    static_dir = _ensure_static_covers_dir()
    with session_scope() as session:
        incoming_ids = _compute_incoming_ids(results)
        _mark_missing_pages_deleted(session, incoming_ids, PROJECT_DATABASE_ID)

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

    # trigger revalidation (projects로 구분 필요 시 태그 변경 가능)
    try:
        if created or updated:
            _post_revalidate("posts")
    except Exception:
        pass
    return {"created": created, "updated": updated, "total": len(results)}


