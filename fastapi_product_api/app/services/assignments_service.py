"""Assignments related service helpers."""

from __future__ import annotations

from services.assignments_builder import AssignmentsBuilder

from ..integrations.database import Database
from ..integrations.elasticsearch import ElasticsearchGateway
from ..models import CertificationListResponse
from ..services.builder_runner import run_builder
from ..utils.mapping import map_locale


async def list_certifications(
    es: ElasticsearchGateway, db: Database, locale: str
) -> CertificationListResponse:
    builder = AssignmentsBuilder(es.legacy, db.legacy)
    lang = map_locale(locale)
    certifications = await run_builder(builder, "parse_certifications_async", lang)
    items = certifications or []
    return CertificationListResponse(meta={"items": len(items)}, items=items)


__all__ = ["list_certifications"]
