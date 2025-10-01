from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from .config import get_settings
from .logging import configure_logging


@asynccontextmanager
def app_lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage startup/shutdown of integrations."""

    settings = get_settings()
    configure_logging()

    db = Database(settings.database)
    es = ElasticsearchGateway(settings.elasticsearch)

    await db.connect()
    await es.connect()

    app.state.settings = settings
    app.state.db = db
    app.state.es = es

    try:
        yield
    finally:
        await es.disconnect()
        await db.disconnect()


__all__ = ["app_lifespan"]
