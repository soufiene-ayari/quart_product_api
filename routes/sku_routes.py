from quart import Blueprint, request, Response, jsonify
from quart_schema import validate_response, validate_querystring
from pydantic import BaseModel, Field
from models.sku import Sku, SkuListResponse, Relation,Document
from services.sku_service import get_sku_by_id, get_skus, get_shop_sku_ids
from utils.auth import require_auth
from utils.pagination import extract_pagination
from utils.mapping import map_brand, map_locale, map_market
from utils.utilities import json_response
from core.environment import env
from typing import Optional
import json

sku_bp = Blueprint("sku_routes", __name__)

class SkuVendorIdQuery(BaseModel):
    vendorid: bool = Field(
        False,
        description="If true, treats the identifier as a vendor ID (product number) instead of internal SKU ID"
    )
    
    class Config:
        extra = "forbid"  # This will raise an error if extra fields are provided

@sku_bp.route("/rest/<brand>/<locale>/sku/<identifier>", methods=["GET"])
#@require_auth
@validate_querystring(SkuVendorIdQuery)
#@validate_response(Sku, 200)
async def get_sku_endpoint(locale: str, identifier: str, brand: str, query_args: SkuVendorIdQuery):
    """
    Get a single SKU by ID or vendor ID
    
    Retrieves detailed information about a specific SKU including its attributes,
    pricing, and availability. Can look up by internal SKU ID or vendor ID.
    
    ---
    tags:
      - SKUs
    parameters:
      - name: brand
        in: path
        required: true
        schema:
          type: string
          example: systemair
        description: The brand identifier
      - name: locale
        in: path
        required: true
        schema:
          type: string
          enum: [de-DE, en-GB, fr-FR, it-IT, nl-NL, pl-PL, sv-SE]
          example: en-GB
        description: The locale code for language and region
      - name: identifier
        in: path
        required: true
        schema:
          type: string
          example: "12345-67"
        description: The unique identifier of the SKU or vendor ID if vendorid=true
      - name: vendorid
        in: query
        required: false
        schema:
          type: boolean
          default: false
        description: If true, treats the identifier as a vendor ID (product number) instead of internal SKU ID
    responses:
      200:
        description: The SKU data
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Sku'
      400:
        description: Invalid brand, locale, or market parameters
      404:
        description: SKU not found
    """
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
        market = map_market(brand, mapped_locale)
    except ValueError as e:
        return {"error": str(e)}, 400  # Bad Request if invalid
    
    # Use the validated query parameter
    use_vendor_id = query_args.vendorid
    
    # Import here to avoid circular imports
    from services.sku_service import get_sku
    
    sku = await get_sku(identifier, mapped_locale, brand, market, use_vendor_id=use_vendor_id)
    if sku:
        return json_response(sku)
    return {"error": "SKU not found"}, 404

class SkuQueryParams(BaseModel):
    offset: int = Field(0, ge=0, description="The number of items to skip before starting to collect the result set")
    limit: int = Field(10, ge=1, le=100, description="The numbers of items to return")
    product_id: Optional[str] = Field(None, description="Filter SKUs by product ID")
    operating_mode: Optional[str] = Field(None, description="Filter SKUs by operating mode ID")

    class Config:
        extra = "forbid"  # This will raise an error if extra fields are provided



@sku_bp.route("/rest/<brand>/<locale>/skus", methods=["GET"])
@validate_querystring(SkuQueryParams)
@validate_response(SkuListResponse, 200)
async def get_skus_endpoint(locale: str, brand: str, query_args: SkuQueryParams):
    """
    Get a paginated list of SKUs
    
    Retrieves a paginated list of SKUs with their basic information.
    Use the pagination parameters to navigate through large result sets.
    
    ---
    tags:
      - SKUs
    parameters:
      - name: brand
        in: path
        required: true
        schema:
          type: string
        description: The brand identifier (e.g., systemair, frico, etc.)
      - name: locale
        in: path
        required: true
        schema:
          type: string
          enum: [de-DE, en-GB, fr-FR, it-IT, nl-NL, pl-PL, sv-SE]
        description: The locale code for language and region
      - name: offset
        in: query
        required: false
        schema:
          type: integer
          minimum: 0
          default: 0
          example: 0
        description: The number of items to skip before starting to collect the result set
      - name: limit
        in: query
        required: false
        schema:
          type: integer
          minimum: 1
          maximum: 100
          default: 10
          example: 10
        description: The number of items to return per page
      - name: product_id
        in: query
        required: false
        schema:
          type: string
        description: Filter SKUs by product ID
      - name: operating_mode
        in: query
        required: false
        schema:
          type: string
        description: Filter SKUs by operating mode ID
    responses:
      200:
        description: A paginated list of SKUs
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/SkuListResponse'
      400:
        description: Invalid brand, locale, or pagination parameters
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "Invalid pagination parameters"
    """
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
        market = map_market(brand, mapped_locale)
    except ValueError as e:
        return {"error": str(e)}, 400  # Bad Request if invalid
    response = await get_skus(
        offset=query_args.offset,
        limit=query_args.limit,
        lang=mapped_locale,
        brand=brand,
        market=market
        #product_id=query_args.product_id
        #operating_mode=query_args.operating_mode
    )
    return response



@sku_bp.route("/rest/<brand>/<locale>/sku/<identifier>/certifications", methods=["GET"])
async def get_sku_certifications_endpoint(locale: str, identifier: str, brand: str):
    """
    Get certifications for a SKU
    Returns only the parsed certifications for the specified SKU in the required format.
    """
    from quart import current_app
    from services.sku_builder import SkuBuilder
    from utils.mapping import map_locale
    mapped_locale = map_locale(locale)
    builder = SkuBuilder(current_app.es, current_app.db)
    identifiers = await builder.extract_identifiers(identifier, mapped_locale,brand)
    certifications = await builder.parse_certifications_async(identifiers, mapped_locale)
    items = [cert.model_dump() for cert in certifications]
    return {
        "meta": {"items": len(items)},
        "items": items
    }

@sku_bp.route("/rest/<brand>/<locale>/sku/<identifier>/documents", methods=["GET"])

async def get_sku_documents_endpoint(locale: str, identifier: str, brand: str):
    """
    Get documents associated with a specific SKU
    ---
    tags:
      - SKUs
    parameters:
      - name: brand
        in: path
        required: true
        schema:
          type: string
        description: The brand identifier (e.g., systemair, frico, etc.)
      - name: locale
        in: path
        required: true
        schema:
          type: string
          enum: [de-DE, en-GB, fr-FR, it-IT, nl-NL, pl-PL, sv-SE]
        description: The locale code for language and region
      - name: identifier
        in: path
        required: true
        schema:
          type: string
        description: The SKU ID or identifier
    responses:
      200:
        description: List of documents associated with the SKU
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ProductDocumentsResponse'
      400:
        description: Invalid brand or locale
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "Invalid brand or locale"
      404:
        description: SKU not found or no documents available
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "No documents found for this SKU"
    """
    from quart import current_app
    from services.sku_builder import SkuBuilder
    from models.sku import Document
    from utils.mapping import map_locale
    mapped_locale = map_locale(locale)
    builder = SkuBuilder(current_app.es, current_app.db)
    documents = await builder.get_documents(identifier, mapped_locale,brand)
    items = [Document(**doc).model_dump() if not isinstance(doc, Document) else doc.model_dump() for doc in documents]
    return {
        "meta": {"items": len(items)},
        "items": items
    }


@sku_bp.route("/rest/<brand>/<locale>/sku/<identifier>/relations", methods=["GET"])
async def get_sku_relations_endpoint(locale: str, identifier: str, brand: str):
    """
    Get relations for a SKU
    ---
    summary: Get relations for a SKU
    description: |
      Returns a list of related items (such as accessories) for the specified SKU, including meta information about the number of items.
    tags:
      - SKUs
    parameters:
      - name: brand
        in: path
        required: true
        schema:
          type: string
        description: The brand identifier (e.g., systemair, frico, etc.)
      - name: locale
        in: path
        required: true
        schema:
          type: string
          enum: [de-DE, en-GB, fr-FR, it-IT, nl-NL, pl-PL, sv-SE]
        description: The locale code for language and region
      - name: identifier
        in: path
        required: true
        schema:
          type: string
        description: The unique identifier of the SKU
    responses:
      200:
        description: A list of related items for the SKU
        content:
          application/json:
            schema:
              type: object
              properties:
                meta:
                  type: object
                  properties:
                    items:
                      type: integer
                      description: Number of relation items returned
                items:
                  type: array
                  items:
                    $ref: '#/components/schemas/Relation'
      404:
        description: SKU not found or no relations available
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "No relations found for this SKU"
    """
    from quart import current_app
    from services.sku_builder import SkuBuilder
    from models.sku import Relation
    from utils.mapping import map_locale
    mapped_locale = map_locale(locale)
    builder = SkuBuilder(current_app.es, current_app.db)
    relations = await builder.get_relations(identifier, mapped_locale,brand)
    items = [Relation(**rel).model_dump() if not isinstance(rel, Relation) else rel.model_dump() for rel in relations]
    return {
        "meta": {"items": len(items)},
        "items": items
    }

@sku_bp.route("/rest/<brand>/<locale>/shopSKU/<identifier>", methods=["GET"])
async def get_shop_sku_endpoint(locale: str, identifier: str, brand: str):
    """
    Get a partial SKU for shop view
    ---
    tags:
      - SKUs
    parameters:
      - name: brand
        in: path
        required: true
        schema:
          type: string
        description: The brand identifier
      - name: locale
        in: path
        required: true
        schema:
          type: string
        description: The locale code for language and region
      - name: identifier
        in: path
        required: true
        schema:
          type: string
        description: The SKU identifier
    responses:
      200:
        description: Partial SKU data for shop
        content:
          application/json:
            schema:
              type: object
    """
    from utils.mapping import map_brand, map_market
    from services.sku_builder import SkuBuilder
    from quart import current_app

    # Validate locale format: 7 chars, 3 lower, _, 3 upper
    if len(locale) != 7 or not (locale[:3].islower() and locale[3] == '_' and locale[4:].isupper()):
        return {"error": "Locale must be in format xxx_XXX (3 lowercase letters, underscore, 3 uppercase letters)"}, 400

    try:
        mapped_brand = map_brand(brand)
        # DO NOT map locale, force lower case
        forced_locale = locale.lower()
        market = map_market(brand, locale)
    except ValueError as e:
        return {"error": str(e)}, 400
    es = current_app.es
    db = current_app.db
    builder = SkuBuilder(es, db)
    sku = await builder.build_shop_sku(identifier, forced_locale, brand, market)
    if not sku:
        return {"error": "SKU not found"}, 404
        # Convert the model to a dict *without* using .json() (which can sort)
    return json_response(sku)


class ShopSkuQueryParams(BaseModel):
    offset: int = Field(0, ge=0, description="The number of items to skip before starting to collect the result set")
    limit: int = Field(10, ge=1, le=100, description="The numbers of items to return")
    timestamp: Optional[str] = Field(None, description="Only SKUs with a timestamp >= this ISO8601 value (e.g. 2025-05-29T12:00:34.668147384Z)")

    class Config:
        extra = "forbid"  # This will raise an error if extra fields are provided
@sku_bp.route("/rest/<brand>/<locale>/shopSKUs", methods=["GET"])
@validate_querystring(ShopSkuQueryParams)
async def get_shop_sku_ids_endpoint(locale: str, brand: str, query_args: ShopSkuQueryParams):
    """
    Get list of shop SKU IDs for a brand and locale
    ---
    tags:
      - SKUs
    parameters:
      - name: brand
        in: path
        required: true
        schema:
          type: string
        description: The brand identifier
      - name: locale
        in: path
        required: true
        schema:
          type: string
        description: The locale code for language and region
    responses:
      200:
        description: List of shop SKU IDs
        content:
          application/json:
            schema:
              type: object
    """
    from utils.mapping import map_brand, map_market
    from quart import request
    # Validate locale format: 7 chars, 3 lower, _, 3 upper
    if len(locale) != 7 or not (locale[:3].islower() and locale[3] == '_' and locale[4:].isupper()):
        return {"error": "Locale must be in format xxx_XXX (3 lowercase letters, underscore, 3 uppercase letters)"}, 400
    try:
        mapped_brand = map_brand(brand)
        forced_locale = locale.lower()
        market = map_market(brand, locale)
    except ValueError as e:
        return {"error": str(e)}, 400
    # Optionally support ?offset and ?size query params
    args = request.args
    size = query_args.limit
    offset = query_args.offset
    timestamp = query_args.timestamp
    ids, total = await get_shop_sku_ids(forced_locale, brand, market, size=size, offset=offset, timestamp=timestamp)
    return {
        "meta": {"items": total},
        "items": ids
    }
