from typing import List, Optional, AsyncGenerator, Dict, Any
from models.category import Category, CategoryListResponse
from services.category_builder import CategoryBuilder
from services.elasticsearch_service import ESConnection
from services.database_service import DBConnection
from quart import current_app
from queries.category_queries import query_categories, query_category_by_id,query_categories_by_parentId



async def get_category_by_id(identifier: str, lang: str, brand: str) -> Optional[Category]:
    """
    Get a single category by its ID
    """
    builder = CategoryBuilder(current_app.es)
    return await builder.build_category(identifier, lang, brand)

async def get_categories(
    offset: int = 0, 
    limit: int = 10, 
    brand: str = "systemair", 
    lang: str = "deu_deu",
    parent_id: Optional[str] = None
) -> CategoryListResponse:
    """
    Get a list of categories with pagination and optional parent filtering
    
    Args:
        offset: Number of items to skip
        limit: Maximum number of items to return (1-100)
        brand: Brand identifier
        lang: Language code
        parent_id: Optional parent category ID to filter by
        
    Returns:
        CategoryListResponse containing the list of categories and metadata
    """

    index = f"systemair_ds_hierarchies_{lang}"  # Categories are stored in hierarchies index
    es = current_app.es
    builder = CategoryBuilder(es)
    # Build query based on whether we're getting root or child categories
    if parent_id:
        query = query_categories_by_parentId(offset, limit, brand,parent_id)
    else:
        query = query_categories(offset, limit, brand)
    
    try:
        response = es.search(index, query)
        hits = response.get("hits", {}).get("hits", [])
        total = response.get("hits", {}).get("total", {}).get("value", 0)
        
        # Build category objects
        items: List[Category] = []
        for hit in hits:
            category_id = hit["_source"].get("epimId")
            if category_id:
                category = await builder.build_category(category_id, lang, brand)
                if category:
                    items.append(category)
        
        return CategoryListResponse(
            meta={"offset": offset, "limit": limit, "total": total},
            items=items
        )
    except Exception as e:
        current_app.logger.error(f"Error fetching categories: {str(e)}")
        return CategoryListResponse(
            meta={"offset": offset, "limit": limit, "total": 0, "error": str(e)},
            items=[]
        )


