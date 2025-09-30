import logging
from typing import List, Optional
from models.operating_mode import OperatingMode, OperatingModeListResponse
from services.operating_mode_builder import OperatingModeBuilder
from core.environment import env
from services.elasticsearch_service import ESConnection
from queries.operating_mode_queries import query_operating_modes
from quart import current_app
logger = logging.getLogger(__name__)



# Single OperatingMode by ID or slug
async def get_operating_mode_by_id(identifier: str, lang: str, brand: str, market:str) -> Optional[OperatingMode]:
    builder = OperatingModeBuilder(current_app.es,current_app.db)
    return await builder.build_operating_mode(identifier, lang, brand, market)

# Paginated list of operating_modes
async def get_operating_modes_old(offset: int, limit: int, lang: str) -> OperatingModeListResponse:
    index = f"systemair_ds_variants_{lang}"
    response = es.search(index=index, body=query_operating_modes(offset, limit))
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)

    items: List[OperatingMode] = []
    for hit in hits:
        operating_mode_id = hit["_source"]["id"]
        operating_mode = await builder.build_operating_mode(operating_mode_id, lang)
        if operating_mode:
            items.append(operating_mode)

    return OperatingModeListResponse(
        meta={"total": total, "offset": offset, "limit": limit},
        items=items
    )
async def get_operating_modes(offset=0, limit=10, brand="systemair", lang="deu_deu") -> OperatingModeListResponse:
    es = current_app.es
    db=current_app.db
    builder = OperatingModeBuilder(es,db)
    index = f"systemair_ds_variants_{lang}"

    body = {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "must": [
                    {"term": {"planningLevel": "Product"}}
                ]
            }
        },
        "_source": ["epimId"]  # Corrected here
    }

    response = es.search(index, body)
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)

    items: List[OperatingMode] = []
    for hit in hits:
        operating_mode_id = hit["_source"].get("epimId")
        if operating_mode_id:
            operating_mode = await builder.build_operating_mode(operating_mode_id, lang, brand)
            if operating_mode:
                items.append(operating_mode)

    return OperatingModeListResponse(
        meta={"offset": offset, "limit": limit, "total": total},
        items=items
    )
# Async generator for streaming
async def stream_operating_modes(lang: str):
    index = f"systemair_ds_hierarchies_{lang}"
    offset = 0
    size = 50

    while True:
        response = es.search(index=index, body=query_operating_modes(offset, size))
        hits = response.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            operating_mode_id = hit["_source"]["id"]
            operating_mode = await builder.build_operating_mode(operating_mode_id, lang)
            if operating_mode:
                yield operating_mode

        offset += size
