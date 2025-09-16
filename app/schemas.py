from typing import List, Optional
from pydantic import BaseModel


class PostTag(BaseModel):
    id: str
    name: str
    color: Optional[str] = None


class PostCard(BaseModel):
    id: str
    coverUrl: Optional[str] = None
    title: Optional[str] = None
    tags: List[PostTag]
    createdDate: Optional[str] = None
    isPinned: bool = False



class ProjectCard(BaseModel):
    id: str
    coverUrl: Optional[str] = None
    title: Optional[str] = None
    tags: List[PostTag]
    createdDate: Optional[str] = None
    url: Optional[str] = None
    isPinned: bool = False


class TypeInfo(BaseModel):
    name: str
    color: Optional[str] = None


class ProjectDetail(BaseModel):
    id: str
    coverUrl: Optional[str] = None
    title: Optional[str] = None
    tags: List[PostTag]
    createdDate: Optional[str] = None
    period: Optional[str] = None
    site: Optional[str] = None
    github: Optional[str] = None
    reviewPageId: Optional[str] = None
    type: Optional[TypeInfo] = None

