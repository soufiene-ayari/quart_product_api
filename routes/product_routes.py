from quart import Blueprint, request, Response, jsonify
from quart_schema import validate_response, validate_querystring
from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List
from services.product_service import get_products, get_product_by_id, get_product_documents
from models.product import Product, ProductListResponse, ProductDocumentsResponse
from models.sku import  SkuListResponse
from utils.auth import require_auth
from utils.pagination import extract_pagination
from utils.mapping import map_brand, map_locale, map_market
from core.environment import env
import json
product_bp = Blueprint('product_routes', __name__)


@product_bp.route("/rest/<brand>/<locale>/product/<identifier>", methods=["GET"])
@validate_response(Product, 200)
async def get_product(locale: str, identifier: str, brand: str):
    """
    Get a single product by ID
    
    ---
    tags:
      - Products
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
        description: The product ID or SKU
    responses:
      200:
        description: The product data
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Product'
      400:
        description: Invalid brand or locale
      404:
        description: Product not found
    """
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
    except ValueError as e:
        return {"error": str(e)}, 400  # Bad Request if invalid
    product = await get_product_by_id(identifier, mapped_locale, brand)
    if product:
        return product
    return {"error": "Product not found"}, 404

class ProductQueryParams(BaseModel):
    offset: int = Field(0, ge=0, description="The number of items to skip before starting to collect the result set")
    limit: int = Field(10, ge=1, le=100, description="The numbers of items to return")
    category_id: Optional[str] = Field(None, description="Filter products by category ID")

    class Config:
        extra = "forbid"  # This will raise an error if extra fields are provided

@product_bp.route("/rest/<brand>/<locale>/products", methods=["GET"])
@validate_querystring(ProductQueryParams)
@validate_response(ProductListResponse, 200)
async def get_products_endpoint(locale: str, brand: str, query_args: ProductQueryParams):
    """
    Get a paginated list of products
    
    Retrieves a paginated list of products with their basic information.
    Use the pagination parameters to navigate through large result sets.
    
    ---
    tags:
      - Products
    parameters:
      - name: brand
        in: path
        required: true
        schema:
          type: string
          example: systemair
        description: The brand identifier (e.g., systemair, frico, etc.)
      - name: locale
        in: path
        required: true
        schema:
          type: string
          enum: [de-DE, en-GB, fr-FR, it-IT, nl-NL, pl-PL, sv-SE]
          example: en-GB
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
      - name: category_id
        in: query
        required: false
        schema:
          type: string
          example: "12345"
        description: Filter products by category ID
    responses:
      200:
        description: A paginated list of products
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ProductListResponse'
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
    except ValueError as e:
        return {"error": str(e)}, 400  # Bad Request if invalid
    products = await get_products(
        offset=query_args.offset, 
        limit=query_args.limit, 
        brand=brand,
        lang=mapped_locale
    )
    return products




#@product_bp.route("/rest/<brand>/<locale>/product/<identifier>/documents", methods=["GET"])
#@validate_response(ProductDocumentsResponse, 200)
async def get_product_documents_endpoint(locale: str, identifier: str, brand: str):
    """
    Get documents associated with a specific product
    
    ---
    tags:
      - Products
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
        description: The product ID or SKU
    responses:
      200:
        description: List of documents associated with the product
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
        description: Product not found or no documents available
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "No documents found for this product"
    """
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
    except ValueError as e:
        return {"error": str(e)}, 400
    
    response = await get_product_documents(identifier, mapped_locale, mapped_brand)
    if not response or not response.get('items'):
        return {"error": "No documents found for this product"}, 404
        
    return response

@product_bp.route("/rest/<brand>/<locale>/product/<identifier>/skus", methods=["GET"])
@validate_response(SkuListResponse, 200)
async def get_product_skus(locale: str, identifier: str, brand: str):

    """
    Get all SKUs belonging to a product
    ---
    tags:
      - Products
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
        description: The product ID
    responses:
      200:
        description: List of SKUs for the product
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/SkuListResponse'
      400:
        description: Invalid brand or locale
      404:
        description: No SKUs found for this product
    """
    from quart import current_app
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
        market = map_market(brand, mapped_locale)
    except ValueError as e:
        return {"error": str(e)}, 400

    es = current_app.es
    db = current_app.db
    # Get all SKUs for the product identifier
    from queries.product_queries import query_child_objects
    index = f"systemair_ds_products_{mapped_locale}"
    body = query_child_objects(identifier)
    sku_hits = await es.agetScrollObject(index, body, 10000, "1m")
    sku_ids = [hit["_source"].get("epimId") for hit in sku_hits if hit.get("_source", {}).get("epimId")]
    if not sku_ids:
        return {"error": "No SKUs found for this product"}, 404

    from services.sku_builder import SkuBuilder
    import asyncio
    builder = SkuBuilder(es, db)
    '''
    # Build all SKUs asynchronously
    skus = await asyncio.gather(*[builder.build_sku(sku_id, mapped_locale, brand, market) for sku_id in sku_ids])
    skus = [sku for sku in skus if sku]
    '''
    concurrency = 256  # tune 64–256
    queue= asyncio.Queue()

    for sid in sku_ids:
        queue.put_nowait(sid)

    results= []
    results_lock = asyncio.Lock()  # protect list during concurrent appends

    async def build_one(sku_id: str):
        try:
            return await builder.build_sku(sku_id, mapped_locale, brand, market)
        except Exception:
            current_app.logger.exception("SKU build failed for %s", sku_id)
            return None

    async def worker():
        while True:
            try:
                sid = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                sku = await build_one(sid)
                if sku:
                    async with results_lock:
                        results.append(sku)
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await asyncio.gather(*workers)

    skus = results
    return SkuListResponse(meta={"total": len(skus)}, items=skus)

# Removed: SKU documents endpoint, now in sku_routes.py
