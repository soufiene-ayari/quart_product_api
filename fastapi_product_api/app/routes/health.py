from __future__ import annotations

from fastapi import APIRouter, Request

from ..models import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    import asyncio

    db = request.app.state.db
    es = request.app.state.es
    try:
        await db.fetch_all("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    try:
        await asyncio.to_thread(es.legacy.check_connection)
        es_ok = True
    except Exception:
        es_ok = False
    return HealthResponse(database=db_ok, elasticsearch=es_ok)


__all__ = ["router"]
