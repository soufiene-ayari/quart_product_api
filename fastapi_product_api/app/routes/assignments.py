"""Routes exposing assignment aggregates such as certifications."""

from __future__ import annotations

from fastapi import APIRouter, Path, Query, Request

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import CertificationListResponse
from ..services.assignments_service import list_certifications


router = APIRouter(prefix="/rest", tags=["assignments"])


def get_gateways(request: Request) -> tuple[ElasticsearchGateway, Database]:
    return request.app.state.es, request.app.state.db


@router.get("/{brand}/{locale}/certifications", response_model=CertificationListResponse)
async def fetch_certifications(
    request: Request,
    brand: str = Path(...),
    locale: str = Path(...),
    market: str = Query(""),
):
    # brand and market parameters are kept for parity with the legacy API even
    # though the current certification builder only relies on the locale.
    es, db = get_gateways(request)
    return await list_certifications(es, db, locale)


__all__ = ["router"]
