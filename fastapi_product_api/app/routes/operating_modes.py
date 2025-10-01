"""FastAPI routes for operating mode resources."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Path, Query, Request

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import OperatingMode, OperatingModeListResponse
from ..services.operating_mode_service import (
    get_operating_mode_by_id,
    list_operating_modes,
)


router = APIRouter(prefix="/rest", tags=["operating-modes"])


def get_gateways(request: Request) -> tuple[ElasticsearchGateway, Database]:
    return request.app.state.es, request.app.state.db


@router.get("/{brand}/{locale}/operating-mode/{operating_mode_id}", response_model=OperatingMode)
async def fetch_operating_mode(
    request: Request,
    operating_mode_id: str,
    brand: str = Path(...),
    locale: str = Path(...),
    market: str = Query(""),
):
    es, db = get_gateways(request)
    operating_mode = await get_operating_mode_by_id(
        es, db, operating_mode_id, locale, brand, market
    )
    if not operating_mode:
        raise HTTPException(status_code=404, detail="Operating mode not found")
    return operating_mode


@router.get("/{brand}/{locale}/operating-modes", response_model=OperatingModeListResponse)
async def fetch_operating_modes(
    request: Request,
    brand: str = Path(...),
    locale: str = Path(...),
    offset: int = Query(0, ge=0),
    limit: int = Query(10, gt=0, le=100),
    market: str = Query(""),
    product_id: str | None = Query(None, alias="productId"),
    sku_id: str | None = Query(None, alias="skuId"),
):
    es, db = get_gateways(request)
    return await list_operating_modes(
        es, db, offset, limit, locale, brand, market, product_id, sku_id
    )


__all__ = ["router"]
