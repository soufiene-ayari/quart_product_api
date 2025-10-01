from .assignments import router as assignments_router
from .categories import router as category_router
from .health import router as health_router
from .operating_modes import router as operating_mode_router
from .products import router as product_router
from .skus import router as sku_router

__all__ = [
    "assignments_router",
    "category_router",
    "health_router",
    "operating_mode_router",
    "product_router",
    "sku_router",
]
