from __future__ import annotations

from fastapi import APIRouter, HTTPException, Path, Query, Request

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import Product, ProductDocumentsResponse, ProductListResponse, SkuListResponse
from ..services.product_service import (
    get_product_by_id,
    get_product_documents,
    get_product_skus,
    list_products,
)

router = APIRouter(prefix="/rest", tags=["products"])


def get_gateways(request: Request) -> tuple[ElasticsearchGateway, Database]:
    return request.app.state.es, request.app.state.db


@router.get("/{brand}/{locale}/products", response_model=ProductListResponse)
async def fetch_products(
    request: Request,
    brand: str = Path(..., description="Brand identifier"),
    locale: str = Path(..., description="Locale code"),
    offset: int = Query(0, ge=0),
    limit: int = Query(10, gt=0, le=100),
):
    es, _ = get_gateways(request)
    return await list_products(es, offset, limit, locale, brand)


@router.get("/{brand}/{locale}/product/{product_id}", response_model=Product)
async def fetch_product(
    request: Request,
    product_id: str,
    brand: str = Path(...),
    locale: str = Path(...),
):
    es, _ = get_gateways(request)
    product = await get_product_by_id(es, product_id, locale, brand)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


@router.get("/{brand}/{locale}/product/{product_id}/documents", response_model=ProductDocumentsResponse)
async def fetch_product_documents(
    product_id: str,
    brand: str,
    locale: str,
):
    return await get_product_documents(product_id, locale, brand)


@router.get("/{brand}/{locale}/product/{product_id}/skus", response_model=SkuListResponse)
async def fetch_product_skus(
    request: Request,
    product_id: str,
    brand: str = Path(...),
    locale: str = Path(...),
    market: str = Query(""),
):
    es, db = get_gateways(request)
    response = await get_product_skus(es, db, product_id, locale, brand, market)
    if not response.items:
        raise HTTPException(status_code=404, detail="No SKUs found for product")
    return response


__all__ = ["router"]
