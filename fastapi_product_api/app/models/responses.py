from __future__ import annotations

from pydantic import BaseModel


class Meta(BaseModel):
    total: int
    count: int
    page: int | None = None
    pageSize: int | None = None


class HealthResponse(BaseModel):
    database: bool
    elasticsearch: bool


__all__ = ["Meta", "HealthResponse"]
