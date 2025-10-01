from __future__ import annotations

import asyncio

from services.product_builder import ProductBuilder
from services.sku_builder import SkuBuilder

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import (
    Product,
    ProductDocument,
    ProductDocumentsResponse,
    ProductListResponse,
    Sku,
    SkuListResponse,
)
from ..models.responses import Meta
from ..queries.product_queries import query_child_objects
from ..services.builder_runner import run_builder
from ..utils.mapping import map_locale, map_market


async def get_product_by_id(
    es: ElasticsearchGateway, identifier: str, locale: str, brand: str
) -> Product | None:
    builder = ProductBuilder(es.legacy)
    lang = map_locale(locale)
    return await run_builder(builder, "build_product", identifier, lang, brand)


async def list_products(
    es: ElasticsearchGateway,
    offset: int,
    limit: int,
    locale: str,
    brand: str,
) -> ProductListResponse:
    builder = ProductBuilder(es.legacy)
    lang = map_locale(locale)
    index = f"systemair_ds_hierarchies_{lang}"

    body = {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"planningLevel": "Product"}},
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
                    },
                ]
            }
        },
        "_source": ["epimId"],
        "sort": [{"seqorderNr": "asc"}],
    }

    response = await es.search(index, body)
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)

    items: list[Product] = []
    for hit in hits:
        product_id = hit.get("_source", {}).get("epimId")
        if product_id:
            product = await run_builder(builder, "build_product", product_id, lang, brand)
            if product:
                items.append(product)

    return ProductListResponse(meta=Meta(total=total, count=len(items), page=None, pageSize=None).model_dump(), items=items)


async def get_product_documents(identifier: str, locale: str, brand: str) -> ProductDocumentsResponse:
    # Placeholder implementation retains legacy sample behaviour
    documents = [
        ProductDocument(
            type="brochures",
            name=f"Catalogue_{identifier}_{locale}.pdf",
            url=f"https://shop.{brand}.com/documents/{identifier}/{locale}/catalogue.pdf",
            mime="application/pdf",
            viewable=True,
        )
    ]
    return ProductDocumentsResponse(meta={"items": len(documents)}, items=documents)


async def get_product_skus(
    es: ElasticsearchGateway,
    db: Database,
    identifier: str,
    locale: str,
    brand: str,
    market: str | None,
) -> SkuListResponse:
    builder = SkuBuilder(es.legacy, db.legacy)
    lang = map_locale(locale)
    market_code = market or map_market(brand, lang)

    index = f"systemair_ds_products_{lang}"
    raw_hits = await es.get_scroll(index, query_child_objects(identifier), 10000, "1m")
    sku_ids = [hit.get("_source", {}).get("epimId") for hit in raw_hits if hit.get("_source")]

    if not sku_ids:
        return SkuListResponse(meta={"total": 0}, items=[])

    semaphore = asyncio.Semaphore(64)

    async def build_one(sku_id: str) -> Sku | None:
        async with semaphore:
            return await run_builder(builder, "build_sku", sku_id, lang, brand, market_code)

    tasks = [asyncio.create_task(build_one(sku_id)) for sku_id in sku_ids]

    items: list[Sku] = []
    for task in asyncio.as_completed(tasks):
        sku = await task
        if sku:
            items.append(sku)

    return SkuListResponse(meta={"total": len(items)}, items=items)


__all__ = [
    "get_product_by_id",
    "get_product_skus",
    "list_products",
    "get_product_documents",
]
