"""Operating mode service wrappers."""

from __future__ import annotations

from typing import Optional

from services.operating_mode_builder import OperatingModeBuilder

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import OperatingMode, OperatingModeListResponse
from ..models.operating_mode import OperatingModeListMeta
from ..services.builder_runner import run_builder
from ..utils.mapping import map_locale, map_market


async def get_operating_mode_by_id(
    es: ElasticsearchGateway,
    db: Database,
    identifier: str,
    locale: str,
    brand: str,
    market: str,
) -> Optional[OperatingMode]:
    builder = OperatingModeBuilder(es.legacy, db.legacy)
    lang = map_locale(locale)
    market_code = market or map_market(brand, lang)
    return await run_builder(builder, "build_operating_mode", identifier, lang, brand, market_code)


async def list_operating_modes(
    es: ElasticsearchGateway,
    db: Database,
    offset: int,
    limit: int,
    locale: str,
    brand: str,
    market: str,
    product_id: str | None,
    sku_id: str | None,
) -> OperatingModeListResponse:
    builder = OperatingModeBuilder(es.legacy, db.legacy)
    lang = map_locale(locale)
    market_code = market or map_market(brand, lang)

    index = f"systemair_ds_variants_{lang}"
    filters = []
    if product_id:
        filters.append({"term": {"parentHierarchy": product_id}})
    if sku_id:
        filters.append({"term": {"skuId": sku_id}})

    query: dict[str, object] = {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "must": [{"term": {"planningLevel": "Product"}}],
            }
        },
        "_source": ["epimId"],
        "sort": [{"seqorderNr": "asc"}],
    }

    if filters:
        query["query"] = {"bool": {"must": query["query"]["bool"]["must"], "filter": filters}}

    response = await es.search(index, query)
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)

    items: list[OperatingMode] = []
    for hit in hits:
        mode_id = hit.get("_source", {}).get("epimId")
        if mode_id:
            operating_mode = await run_builder(
                builder, "build_operating_mode", mode_id, lang, brand, market_code
            )
            if operating_mode:
                items.append(operating_mode)

    meta = OperatingModeListMeta(offset=offset, limit=limit, total=total)
    return OperatingModeListResponse(meta=meta, items=items)


__all__ = ["get_operating_mode_by_id", "list_operating_modes"]
