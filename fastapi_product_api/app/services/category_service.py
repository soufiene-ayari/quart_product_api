"""Category service wrappers that adapt legacy builders for FastAPI."""

from __future__ import annotations

from typing import Optional

from services.category_builder import CategoryBuilder

from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import Category, CategoryListResponse
from ..models.category import CategoryListMeta
from ..queries.category_queries import query_categories, query_categories_by_parentId
from ..services.builder_runner import run_builder
from ..utils.mapping import map_locale


async def get_category_by_id(
    es: ElasticsearchGateway, identifier: str, locale: str, brand: str
) -> Optional[Category]:
    builder = CategoryBuilder(es.legacy)
    lang = map_locale(locale)
    return await run_builder(builder, "build_category", identifier, lang, brand)


async def list_categories(
    es: ElasticsearchGateway,
    offset: int,
    limit: int,
    locale: str,
    brand: str,
    parent_id: str | None,
) -> CategoryListResponse:
    builder = CategoryBuilder(es.legacy)
    lang = map_locale(locale)
    index = f"systemair_ds_hierarchies_{lang}"

    if parent_id:
        body = query_categories_by_parentId(offset, limit, brand, parent_id)
    else:
        body = query_categories(offset, limit, brand)

    response = await es.search(index, body)
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)

    items: list[Category] = []
    for hit in hits:
        category_id = hit.get("_source", {}).get("epimId")
        if category_id:
            category = await run_builder(builder, "build_category", category_id, lang, brand)
            if category:
                items.append(category)

    meta = CategoryListMeta(offset=offset, limit=limit, total=total)
    return CategoryListResponse(meta=meta, items=items)


__all__ = ["get_category_by_id", "list_categories"]
