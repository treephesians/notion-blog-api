from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, ForeignKey, Boolean, Date, Text
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from contextlib import contextmanager
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

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
    status_id = Column(String, ForeignKey("statuses.id", ondelete="SET NULL"))
    slug = Column(Text)
    title = Column(Text)
    written_date = Column(Date)
    raw_properties = Column(JSONB, nullable=False, default={})
    synced_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    status = relationship("Status")
    tags = relationship("Tag", secondary=page_tags, backref="pages")


class PageRelation(Base):
    __tablename__ = "page_relations"

    from_page_id = Column(String, ForeignKey("notion_pages.id", ondelete="CASCADE"), primary_key=True)
    to_page_id = Column(String, ForeignKey("notion_pages.id", ondelete="CASCADE"), primary_key=True)

def init_db():
    Base.metadata.create_all(bind=engine)


def get_database_tables():
    inspector = inspect(engine)
    return inspector.get_table_names()


def print_database_tables():
    tables = get_database_tables()
    print("Database tables:")
    if tables:
        for table_name in tables:
            print(f"- {table_name}")
    else:
        print("(no tables found)")


@contextmanager
def session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()