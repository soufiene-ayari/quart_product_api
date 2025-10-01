from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .core.config import get_settings
from .core.lifespan import app_lifespan
from .routes import health_router, product_router, sku_router
from .utils.error_handlers import install_error_handlers


settings = get_settings()
app = FastAPI(title=settings.api.app_name, version=settings.api.version, lifespan=app_lifespan)

if settings.api.enable_cors:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

install_error_handlers(app)
app.include_router(health_router)
app.include_router(product_router)
app.include_router(sku_router)


__all__ = ["app"]
