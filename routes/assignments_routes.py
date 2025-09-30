from quart import Blueprint, request, jsonify, current_app
from quart_schema import validate_response
from typing import List
from models.sku import Sku, Relation, Document, Certification, RelationListResponse, DocumentListResponse, CertificationListResponse
from utils.mapping import map_brand, map_locale, map_market
from services.sku_builder import SkuBuilder
from services.assignments_builder import AssignmentsBuilder
import asyncio

assignments_bp = Blueprint('assignments_routes', __name__)

# Helper to get all SKU ids for a brand/locale/market
async def get_sku_ids(es, mapped_locale, mapped_brand, market):
    from queries.sku_queries import query_skus
    index = f"systemair_ds_products_{mapped_locale}"
    # Use a simple match_all or filter by brand/market if needed
    body = {
        "query": {
            "bool": {
              "filter": [
                {
                  "nested": {
                    "path": "hierarchies",
                    "query": {
                      "script": {
                        "script": {
                          "source": f"doc['hierarchies.hierarchy'].value.toLowerCase().startsWith('{mapped_brand}')",
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
    hits = list(es.getScrollObject(index, body, 10000, "1m"))
    sku_ids = [hit["_source"].get("epimId") for hit in hits if hit.get("_source", {}).get("epimId")]
    return sku_ids

#@assignments_bp.route("/rest/<brand>/<locale>/relations", methods=["GET"])
#@validate_response(RelationListResponse, 200)
async def get_all_relations(brand: str, locale: str):
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
        market = map_market(mapped_brand, mapped_locale)
    except ValueError as e:
        return {"error": str(e)}, 400
    es = current_app.es
    db = current_app.db
    builder = SkuBuilder(es, db)
    sku_ids = await get_sku_ids(es, mapped_locale, mapped_brand, market)
    results = await asyncio.gather(*[builder.get_relations(sku_id, mapped_locale, brand) for sku_id in sku_ids])
    # Flatten and filter None
    relations = [item for sublist in results if sublist for item in sublist]
    return RelationListResponse(meta={"items": len(relations)}, items=relations)

#@assignments_bp.route("/rest/<brand>/<locale>/documents", methods=["GET"])
#@validate_response(DocumentListResponse, 200)
async def get_all_documents(brand: str, locale: str):
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
        market = map_market(mapped_brand, mapped_locale)
    except ValueError as e:
        return {"error": str(e)}, 400
    es = current_app.es
    es = current_app.es
    db = current_app.db
    builder = SkuBuilder(es, db)
    sku_ids = await get_sku_ids(es, mapped_locale, brand, market)
    results = await asyncio.gather(*[builder.get_documents(sku_id, mapped_locale,brand) for sku_id in sku_ids])
    documents = [item for sublist in results if sublist for item in sublist]
    return DocumentListResponse(meta={"items": len(documents)}, items=documents)

@assignments_bp.route("/rest/<brand>/<locale>/certifications", methods=["GET"])
@validate_response(CertificationListResponse, 200)
async def get_all_certifications(brand: str, locale: str):
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
        market = map_market(mapped_brand, mapped_locale)
    except ValueError as e:
        return {"error": str(e)}, 400
    es = current_app.es
    db = current_app.db
    builder = AssignmentsBuilder(es, db)

    results = await asyncio.gather(builder.parse_certifications_async( mapped_locale))
    certifications = [item for sublist in results if sublist for item in sublist]
    return CertificationListResponse(meta={"items": len(certifications)}, items=certifications)
