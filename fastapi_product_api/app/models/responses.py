from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class Meta(BaseModel):
    total: int
    count: int
    page: Optional[int] = None
    pageSize: Optional[int] = None


class HealthResponse(BaseModel):
    database: bool
    elasticsearch: bool


__all__ = ["Meta", "HealthResponse"]
