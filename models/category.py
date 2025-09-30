from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Union

class Category(BaseModel):
    id: str
    parentId: str
    oldExternalIds: List[str] = []
    name: str
    description: Optional[str] = None
    tagline: Optional[str] = None
    sort: int
    active: bool
    hidden: bool
    approved: bool
    releaseDate: Optional[str] = None
    type: str
    importance: Optional[str] = None
    icon: Optional[str] = None
    attributes: Dict[str, Any] = {}
    #90052
    secondaryParents: List[str] = []

class CategoryListResponse(BaseModel):
    meta: dict
    items: List[Category]
