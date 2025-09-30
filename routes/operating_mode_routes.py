from quart import Blueprint, request, Response, jsonify
from quart_schema import validate_response, validate_querystring
from pydantic import BaseModel, Field
from models.operating_mode import OperatingMode, OperatingModeListResponse
from services.operating_mode_service import get_operating_mode_by_id, get_operating_modes, stream_operating_modes
from utils.auth import require_auth
from utils.mapping import map_brand, map_locale, map_market
from utils.pagination import extract_pagination
from core.environment import env
from typing import Optional
import json

operating_mode_bp = Blueprint("operating_mode_routes", __name__)




@operating_mode_bp.route("/rest/<brand>/<locale>/operating-mode/<identifier>", methods=["GET"])
#@require_auth
@validate_response(OperatingMode, 200)
async def get_operating_mode_endpoint(locale: str, identifier: str, brand: str):
    """
    Get a single operating mode by ID
    
    Retrieves detailed information about a specific operating mode including its
    attributes, pricing, and associated metadata. This endpoint is essential for
    displaying operating mode details in product configuration interfaces.
    
    ---
    tags:
      - Operating Modes
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
      - name: identifier
        in: path
        required: true
        schema:
          type: string
          example: "12345"
        description: The unique identifier of the operating mode
    responses:
      200:
        description: The operating mode data
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/OperatingMode'
      400:
        description: Invalid brand or locale parameter
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "Invalid locale code"
      404:
        description: Operating mode not found
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "Operating mode not found"
    """
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
        market = map_market(brand, mapped_locale)
    except ValueError as e:
        return {"error": str(e)}, 400  # Bad Request if invalid
        
    operating_mode = await get_operating_mode_by_id(identifier, mapped_locale, mapped_brand, market)
    if operating_mode:
        return operating_mode
    return {"error": "Operating mode not found"}, 404

class OperatingModeQueryParams(BaseModel):
    offset: int = Field(0, ge=0, description="The number of items to skip before starting to collect the result set")
    limit: int = Field(10, ge=1, le=100, description="The numbers of items to return")
    product_id: Optional[str] = Field(None, description="Filter operating modes by product ID")
    sku_id: Optional[str] = Field(None, description="Filter operating modes by SKU ID")

    class Config:
        extra = "forbid"  # This will raise an error if extra fields are provided

@operating_mode_bp.route("/rest/<brand>/<locale>/operating-modes", methods=["GET"])
@validate_querystring(OperatingModeQueryParams)
@validate_response(OperatingModeListResponse, 200)
async def get_operating_modes_endpoint(locale: str, brand: str, query_args: OperatingModeQueryParams):
    """
    Get a paginated list of operating modes
    
    Retrieves a paginated list of operating modes with their configurations.
    Use the pagination parameters to navigate through large result sets.
    
    ---
    tags:
      - Operating Modes
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
      - name: product_id
        in: query
        required: false
        schema:
          type: string
          example: "12345"
        description: Filter operating modes by product ID
      - name: sku_id
        in: query
        required: false
        schema:
          type: string
          example: "SKU123"
        description: Filter operating modes by SKU ID
    responses:
      200:
        description: A paginated list of operating modes
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/OperatingModeListResponse'
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
        return {"error": str(e)}, 400
        
    response = await get_operating_modes(
        offset=query_args.offset,
        limit=query_args.limit,
        locale=mapped_locale,
        brand=mapped_brand,
        market=market,
        product_id=query_args.product_id,
        sku_id=query_args.sku_id
    )
    return response


