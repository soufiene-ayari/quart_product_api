"""Pydantic models for category endpoints."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class Category(BaseModel):
    id: str
    parentId: str
    oldExternalIds: list[str] = Field(default_factory=list)
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
    attributes: dict[str, Any] = Field(default_factory=dict)
    secondaryParents: list[str] = Field(default_factory=list)


class CategoryListMeta(BaseModel):
    offset: int
    limit: int
    total: int


class CategoryListResponse(BaseModel):
    meta: CategoryListMeta
    items: list[Category]


__all__ = ["Category", "CategoryListMeta", "CategoryListResponse"]
