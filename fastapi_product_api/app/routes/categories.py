"""FastAPI routes for category resources."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Path, Query, Request

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import Category, CategoryListResponse
from ..services.category_service import get_category_by_id, list_categories


router = APIRouter(prefix="/rest", tags=["categories"])


def get_gateways(request: Request) -> tuple[ElasticsearchGateway, Database]:
    return request.app.state.es, request.app.state.db


@router.get("/{brand}/{locale}/category/{category_id}", response_model=Category)
async def fetch_category(
    request: Request,
    category_id: str,
    brand: str = Path(...),
    locale: str = Path(...),
):
    es, _ = get_gateways(request)
    category = await get_category_by_id(es, category_id, locale, brand)
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")
    return category


@router.get("/{brand}/{locale}/categories", response_model=CategoryListResponse)
async def fetch_categories(
    request: Request,
    brand: str = Path(...),
    locale: str = Path(...),
    offset: int = Query(0, ge=0),
    limit: int = Query(10, gt=0, le=100),
    parent_id: str | None = Query(None, alias="parentId"),
):
    es, _ = get_gateways(request)
    return await list_categories(es, offset, limit, locale, brand, parent_id)


__all__ = ["router"]
