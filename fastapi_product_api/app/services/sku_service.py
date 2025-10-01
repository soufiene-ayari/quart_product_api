from __future__ import annotations


from __future__ import annotations


import asyncio
from typing import Optional

from services.sku_builder import SkuBuilder

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import Sku, SkuListResponse
from ..models.responses import Meta

from ..services.builder_runner import run_builder
from ..utils.mapping import map_locale, map_market



async def get_sku(
    es: ElasticsearchGateway,
    db: Database,
    identifier: str,
    locale: str,
    brand: str,
    market: str,
) -> Optional[Sku]:
    builder = SkuBuilder(es.legacy, db.legacy)
    lang = map_locale(locale)

    return await run_builder(builder, "build_sku", identifier, lang, brand, market)



async def list_skus(
    es: ElasticsearchGateway,
    db: Database,
    offset: int,
    limit: int,
    locale: str,
    brand: str,
    market: str,
) -> SkuListResponse:
    builder = SkuBuilder(es.legacy, db.legacy)
    lang = map_locale(locale)

    index = f"systemair_ds_products_{lang}"
    filters = [
        {
            "nested": {
                "path": "hierarchies",
                "query": {
                    "script": {
                        "script": {
                            "source": "doc['hierarchies.hierarchy'].value.toLowerCase().startsWith(params.brand)",
                            "params": {"brand": brand.lower()},
                            "lang": "painless",
                        }
                    }
                },
            }
        }
    ]
    market_code = map_market(brand, lang)
    if market_code:
        filters.append({"term": {"markets": market_code}})

    body = {
        "from": offset,
        "size": limit,
        "query": {"bool": {"filter": filters}},
        "sort": [{"epimId": "asc"}],
    }

    response = await es.search(index, body)
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)

    items: list[Sku] = []
    for hit in hits:
        sku_id = hit.get("_source", {}).get("epimId")
        if sku_id:

            sku = await run_builder(builder, "build_sku", sku_id, lang, brand, market)

            if sku:
                items.append(sku)

    meta = Meta(total=total, count=len(items), page=None, pageSize=None).model_dump()
    return SkuListResponse(meta=meta, items=items)


__all__ = ["get_sku", "list_skus"]
