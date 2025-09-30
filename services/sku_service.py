import logging
from typing import List, Optional
from models.sku import Sku, SkuListResponse
from models.product import ProductDocument
from services.sku_builder import SkuBuilder
from core.environment import env
from services.elasticsearch_service import ESConnection
from queries.sku_queries import query_skus,query_shopSku_market,query_sku_by_refrence_id
from quart import current_app
from utils.mapping import map_brand, map_locale, map_market
logger = logging.getLogger(__name__)



# Single SKU by ID or slug
async def get_sku_by_id(identifier: str, lang: str, brand: str, market: str) -> Optional[Sku]:
    """Get SKU by its internal identifier."""
    builder = SkuBuilder(current_app.es, current_app.db)
    return await builder.build_sku(identifier, lang, brand, market)

# Single SKU by vendor ID
async def get_sku_by_vendor_id(vendor_id: str, lang: str, brand: str, market: str) -> Optional[Sku]:
    """Get SKU by vendor ID (product number)."""
    from queries.sku_queries import query_sku_by_vendor_id
    
    # First, find the internal ID using the vendor ID
    index = f"systemair_ds_products_{lang}"
    response = current_app.es.search(index, query_sku_by_vendor_id(vendor_id))
    hits = response.get("hits", {}).get("hits", [])
    
    if not hits:
        return None
        
    # Get the first matching SKU
    sku_id = hits[0]["_source"].get("epimId")

    if not sku_id:
        return None
    ref_response = current_app.es.search(index, query_sku_by_refrence_id(sku_id, brand))
    ref_hits = ref_response.get("hits", {}).get("hits", [])
    if not ref_hits:
        return None
    ref_id=ref_hits[0]["_source"].get("epimId")
    if not ref_id:
        return None

    # Now build the full SKU using the internal ID
    return await get_sku_by_id(ref_id, lang, brand, market)

# Unified function to get SKU by either ID or vendor ID
async def get_sku(identifier: str, lang: str, brand: str, market: str, use_vendor_id: bool = False) -> Optional[Sku]:
    """Get SKU by either internal ID or vendor ID.
    
    Args:
        identifier: Either the internal SKU ID or vendor ID
        lang: Language code
        brand: Brand identifier
        market: Market identifier
        use_vendor_id: If True, treats the identifier as a vendor ID
        
    Returns:
        Sku object if found, None otherwise
    """
    if use_vendor_id:
        return await get_sku_by_vendor_id(identifier, lang, brand, market)
    return await get_sku_by_id(identifier, lang, brand, market)

# Paginated list of SKUs
async def get_skus_old(offset: int, limit: int, lang: str, market:str) -> SkuListResponse:
    index = f"systemair_ds_products_{lang}"
    response = es.search(index=index, body=query_skus(offset, limit))
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)

    items: List[Sku] = []
    for hit in hits:
        sku_id = hit["_source"]["id"]
        sku = await builder.build_sku(sku_id, lang)
        if sku:
            items.append(sku)

    return SkuListResponse(
        meta={"total": total, "offset": offset, "limit": limit},
        items=items
    )
async def get_skus(offset=0, limit=10, brand="systemair", lang="deu_deu", market="") -> SkuListResponse:
    es = current_app.es
    db=current_app.db
    builder = SkuBuilder(es,db)
    index = f"systemair_ds_products_{lang}"

    body = {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "filter": [
                    {
                        "nested": {
                            "path": "hierarchies",
                            "query": {
                                "script": {
                                    "script": {
                                        "source": f"doc['hierarchies.hierarchy'].value.toLowerCase().startsWith('{brand}')",
                                        "lang": "painless"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "nested": {
                            "path": "hierarchies",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "hierarchies.hierarchy": "ECOM NG"
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "epimId": "asc"
            }
        ]
    }

    response = es.search(index, body)
    #response = list(es.getScrollObject(index, body, 10000, "1m"))
    hits = response.get("hits", {}).get("hits", [])
    #total = len(hits)
    total = response.get("hits", {}).get("total", {}).get("value", 0)
    mapped_brand = map_brand(brand)
    items: List[Sku] = []
    for hit in hits:
        sku_id = hit["_source"].get("epimId")
        if sku_id:
            sku = await builder.build_sku(sku_id, lang, brand,market)
            if sku:
                items.append(sku)

    return SkuListResponse(
        meta={"offset": offset, "limit": limit, "total": total},
        items=items
    )

async def get_shop_sku_ids(lang: str, brand: str, market: str, size: int = 1000, offset: int = 0, timestamp: Optional[int] = None) -> tuple:
    """
    Return a list of SKU IDs for shop view from Elasticsearch for the given brand/locale/market, optionally filtered by timestamp (epoch ms).
    """
    from quart import current_app
    es = current_app.es
    index = f"systemair_ds_products_{lang}"
    filters = [
        {
            "nested": {
                "path": "hierarchies",
                "query": {
                    "script": {
                        "script": {
                            "source": f"doc['hierarchies.hierarchy'].value.toLowerCase().startsWith('{brand}')",
                            "lang": "painless"
                        }
                    }
                }
            }
        },
        {
            "nested": {
                "path": "hierarchies",
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "term": {
                                    "hierarchies.hierarchy": "ECOM NG"
                                }
                            }
                        ]
                    }
                }
            }
        }
    ]
    if timestamp is not None:
        filters.append({
            "range": {
                "timestamp": {"gte": timestamp}
            }
        })
    query = {
        "from": offset,
        "size": size,
        "query": {
            "bool": {
                "filter": filters
            }
        },
        "sort": [
            {"epimId": "asc"}
        ],
        "_source": ["epimId","timestamp","referenceId"]
    }
    queryCount = {

        "query": {
            "bool": {
                "filter": filters
            }
        }
    }
    try:

        total = es.getIndexCount(index, queryCount,None)
        response = es.search(index, query)
        hits = response.get("hits", {}).get("hits", [])
        ids = [hit["_source"]["epimId"] for hit in hits if "epimId" in hit["_source"]]
        ref_ids = [hit["_source"]["referenceId"] for hit in hits if "referenceId" in hit["_source"]]
        marketCorrected=[market.lower().replace("_","-")]
        # Query attributes for each ref_id (market attribute)
        attIndex = f"systemair_ds_attributes_{lang}"
        attr_query = query_shopSku_market(marketCorrected, ref_ids)
        attr_response = es.search(attIndex, attr_query)
        attr_hits = attr_response.get("hits", {}).get("hits", [])
        # Map parentId (ref_id) to value
        attr_map = {hit["_source"]["parentId"]: hit["_source"].get("values", [{}])[0].get("value", 0) for hit in attr_hits}
        # Build result: id, value (attribute), ref_id, parentId (ref_id)
        result = []
        for id_, ref_id in zip(ids, ref_ids):
            if brand in ("frico","fantech"):
                value = attr_map.get(ref_id, 0)
            else:
                value = attr_map.get(ref_id, 0)
            result.append({
                "id": id_,
                marketCorrected[0]: value
            })
        return result, total
    except Exception as e:
        import logging
        logging.getLogger(__name__).exception(f"Failed to fetch shop SKU IDs: {e}")
        return [], 0
