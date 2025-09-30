from typing import Optional, List
from quart import current_app
from models.product import Product, ProductListResponse, ProductDocument, ProductDocumentsResponse
from services.product_builder import ProductBuilder
import logging

logger = logging.getLogger(__name__)

async def get_products(offset=0, limit=10, brand="systemair", lang="deu_deu") -> ProductListResponse:
    es = current_app.es
    builder = ProductBuilder(es)
    index = f"systemair_ds_hierarchies_{lang}"

    body = {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
              "filter": [
                {
                  "term": {
                    "planningLevel": "Product"
                  }
                },
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
                }
              ]
            }
          },
        "_source": ["epimId"]
        ,
        "sort": [
            {
                "seqorderNr": "asc"
            }
        ]
        # Corrected here
    }

    response = es.search(index, body)
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)

    items: List[Product] = []
    for hit in hits:
        product_id = hit["_source"].get("epimId")
        if product_id:
            product = await builder.build_product(product_id, lang, brand)
            if product:
                items.append(product)

    return ProductListResponse(
        meta={"offset": offset, "limit": limit, "total": total},
        items=items
    )

async def stream_products(lang: str, brand: str):
    es = current_app.es
    builder = ProductBuilder(es)
    index = f"systemair_ds_hierarchies_{lang}"
    size = 1000
    scroll = "2m"
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"planningLevel": "Product"}}
                ]
            }
        },
        "_source": ["epimId"]  # Corrected here
    }

    try:
        hits = es.getScrollObject(index=index, querySource=query, scrollSize=size, scrollTimeout=scroll)

        for hit in hits:  # Normal for-loop!
            src = hit["_source"]
            product_id = src.get("epimId")
            if product_id:
                product = await builder.build_product(product_id, lang, brand)
                if product:
                    yield product
    except Exception as e:
        logger.exception(f"Failed during streaming products for {lang}: {e}")

async def get_product_by_id(identifier: str, lang: str, brand: str) -> Optional[Product]:
    builder = ProductBuilder(current_app.es)
    return await builder.build_product(identifier, lang, brand)


async def get_product_documents(identifier: str, lang: str, brand: str) -> Optional[dict]:
    """
    Retrieve documents associated with a product
    
    Args:
        identifier: The product ID or SKU
        lang: The language code (e.g., 'deu_deu')
        brand: The brand identifier (e.g., 'systemair')
        
    Returns:
        Dictionary containing 'meta' and 'items' keys with document data,
        or None if no documents found
    """
    if not identifier:
        return None
    
    # TODO: Replace with actual document retrieval from your data source
    # This is a sample implementation that returns example data
    
    # Sample documents that match the example format
    sample_documents = [
        {
            "type": "brochures",
            "name": f"Catalogue_Controllers_2013-1_{lang[:2].upper()}.pdf",
            "url": f"https://shop.{brand}.com/upload/assets/CATALOGUE_CONTROLLERS_2013-1_EN-IT_20190426_003821100.PDF?14bca674",
            "mime": "application/pdf",
            "viewable": True
        },
        {
            "type": "brochures",
            "name": f"Performance_SYSCOIL_2_pipe_{lang[:2].upper()}.pdf",
            "url": f"https://shop.{brand}.com/upload/assets/PERFORMANCE_SYSCOIL_2_PIPE_20190426_003822787.PDF?eca5750c",
            "mime": "application/pdf",
            "viewable": True
        },
        {
            "type": "manuals",
            "name": f"TM_SYSCOIL_2012_{lang[:2].upper()}.pdf",
            "url": f"https://shop.{brand}.com/upload/assets/TM_SYSCOIL_2012_EN.PDF?241401d4",
            "mime": "application/pdf",
            "viewable": True
        }
    ]
    
    # Create the response structure
    response = {
        "meta": {
            "items": len(sample_documents)
        },
        "items": []
    }
    
    # Validate and add documents to the response
    try:
        response["items"] = [ProductDocument(**doc) for doc in sample_documents]
        return response
    except Exception as e:
        logger.error(f"Error creating product documents: {str(e)}")
        return None

# Removed: get_sku_documents, now in sku_service.py
