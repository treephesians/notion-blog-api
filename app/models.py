from sqlalchemy import Column, String, DateTime, Table, ForeignKey, Boolean, Date, Text
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime

from app.core.database import Base


class Status(Base):
    __tablename__ = "statuses"

    id = Column(String, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    color = Column(String)


class Tag(Base):
    __tablename__ = "tags"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    color = Column(String)


page_tags = Table(
    "page_tags",
    Base.metadata,
    Column("page_id", String, ForeignKey("notion_pages.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_id", String, ForeignKey("tags.id", ondelete="CASCADE"), primary_key=True),
)


class NotionPage(Base):
    __tablename__ = "notion_pages"

    id = Column(String, primary_key=True)
    database_id = Column(String, nullable=False)
    url = Column(Text)
    public_url = Column(Text)
    created_time = Column(DateTime(timezone=True), nullable=False)
    last_edited_time = Column(DateTime(timezone=True), nullable=False)
    created_by_user_id = Column(String)
    last_edited_by_user_id = Column(String)
    archived = Column(Boolean, nullable=False, default=False)
    in_trash = Column(Boolean, nullable=False, default=False)
    cover_url = Column(Text)
    cover_expiry_time = Column(DateTime(timezone=True))
    icon = Column(Text)
    pin = Column(Boolean, nullable=False, default=False)
    is_deleted = Column(Boolean, nullable=False, default=False)
    status_id = Column(String, ForeignKey("statuses.id", ondelete="SET NULL"))
    slug = Column(Text)
    title = Column(Text)
    written_date = Column(Date)
    raw_properties = Column(JSONB, nullable=False, default={})
    synced_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    status = relationship("Status")
    tags = relationship("Tag", secondary=page_tags, backref="pages")


