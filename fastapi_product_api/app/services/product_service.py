from __future__ import annotations

import asyncio
from typing import Optional

from services.product_builder import ProductBuilder

from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import Product, ProductDocument, ProductDocumentsResponse, ProductListResponse
from ..models.responses import Meta
from ..utils.mapping import map_locale


async def _run_builder(builder: ProductBuilder, method: str, *args, **kwargs):
    async def _invoke() -> Product | None:
        func = getattr(builder, method)
        return await func(*args, **kwargs)

    return await asyncio.to_thread(asyncio.run, _invoke())


async def get_product_by_id(
    es: ElasticsearchGateway, identifier: str, locale: str, brand: str
) -> Product | None:
    builder = ProductBuilder(es.legacy)
    lang = map_locale(locale)
    return await _run_builder(builder, "build_product", identifier, lang, brand)


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
            product = await _run_builder(builder, "build_product", product_id, lang, brand)
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


__all__ = ["get_product_by_id", "list_products", "get_product_documents"]
