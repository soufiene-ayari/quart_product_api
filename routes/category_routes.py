from quart import Blueprint, request, Response, jsonify
from quart_schema import validate_response, validate_querystring
from pydantic import BaseModel, Field
from services.category_service import get_categories, get_category_by_id
from models.category import Category, CategoryListResponse
from utils.auth import require_auth
from utils.pagination import extract_pagination
from utils.mapping import map_brand, map_locale
import json
from typing import Optional, List, Dict, Any

category_bp = Blueprint('category_routes', __name__)

class CategoryQueryParams(BaseModel):
    offset: int = Field(0, ge=0, description="The number of items to skip before starting to collect the result set")
    limit: int = Field(10, ge=1, le=100, description="The numbers of items to return")
    parent_id: Optional[str] = Field(None, description="Filter categories by parent ID")

    class Config:
        extra = "forbid"  # This will raise an error if extra fields are provided

@category_bp.route("/rest/<brand>/<locale>/category/<identifier>", methods=["GET"])
@validate_response(Category, 200)
async def get_category(locale: str, identifier: str, brand: str):
    """
    Get a single category by ID
    
    Retrieves detailed information about a specific category including its hierarchy,
    attributes, and associated metadata. This endpoint is useful for displaying
    category details in a product catalog or navigation menu.
    
    ---
    tags:
      - Categories
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
        description: The unique identifier of the category
    responses:
      200:
        description: The category data
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Category'
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
        description: Category not found
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "Category not found"
    """
    try:
        mapped_brand = map_brand(brand)
        mapped_locale = map_locale(locale)
    except ValueError as e:
        return {"error": str(e)}, 400  # Bad Request if invalid
        
    category = await get_category_by_id(identifier, mapped_locale, mapped_brand)
    if category:
        return category
    return {"error": "Category not found"}, 404

@category_bp.route("/rest/<brand>/<locale>/categories", methods=["GET"])
@validate_querystring(CategoryQueryParams)
@validate_response(CategoryListResponse, 200)
async def get_categories_endpoint(locale: str, brand: str, query_args: CategoryQueryParams):
    """
    Get a paginated list of categories
    
    Retrieves a paginated list of categories with their basic information.
    Use the parent_id parameter to filter categories by their parent in the hierarchy.
    
    ---
    tags:
      - Categories
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
      - name: parent_id
        in: query
        required: false
        schema:
          type: string
          example: "12345"
        description: Filter categories by parent ID. Omit to get root categories.
    responses:
      200:
        description: A paginated list of categories
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CategoryListResponse'
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
    
    # Get pagination parameters from the validated query args
    offset = query_args.offset
    limit = query_args.limit
    parent_id = query_args.parent_id
    
    # Get categories with pagination and optional parent filter
    categories = await get_categories(
        offset=offset, 
        limit=limit, 
        brand=brand,
        lang=mapped_locale,
        parent_id=parent_id
    )
    return categories


