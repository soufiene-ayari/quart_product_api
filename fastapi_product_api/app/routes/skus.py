from __future__ import annotations

from fastapi import APIRouter, HTTPException, Path, Query, Request

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import Sku, SkuListResponse
from ..services.sku_service import get_sku, list_skus

router = APIRouter(prefix="/rest", tags=["skus"])


def get_gateways(request: Request) -> tuple[ElasticsearchGateway, Database]:
    return request.app.state.es, request.app.state.db


@router.get("/{brand}/{locale}/sku/{sku_id}", response_model=Sku)
async def fetch_sku(
    request: Request,
    sku_id: str,
    brand: str = Path(...),
    locale: str = Path(...),
    market: str = Query("", description="Market identifier"),
):
    es, db = get_gateways(request)
    sku = await get_sku(es, db, sku_id, locale, brand, market)
    if not sku:
        raise HTTPException(status_code=404, detail="SKU not found")
    return sku


@router.get("/{brand}/{locale}/skus", response_model=SkuListResponse)
async def fetch_skus(
    request: Request,
    brand: str = Path(...),
    locale: str = Path(...),
    market: str = Query(""),
    offset: int = Query(0, ge=0),
    limit: int = Query(10, gt=0, le=100),
):
    es, db = get_gateways(request)
    return await list_skus(es, db, offset, limit, locale, brand, market)


__all__ = ["router"]
