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


