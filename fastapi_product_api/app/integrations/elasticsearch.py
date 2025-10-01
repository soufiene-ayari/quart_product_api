from __future__ import annotations

import asyncio
from typing import Any

from services.elasticsearch_service import ESConnection

from ..core.config import ElasticsearchSettings


class ElasticsearchGateway:
    """Async wrapper around the legacy Elasticsearch connection."""

    def __init__(self, settings: ElasticsearchSettings) -> None:
        self._settings = settings
        self._conn: ESConnection | None = None

    async def connect(self) -> None:
        if self._conn is not None:
            return
        self._conn = ESConnection(
            {
                "url": self._settings.hosts[0] if self._settings.hosts else "http://localhost:9200",
                "user": self._settings.username or "",
                "pass": self._settings.password or "",
                "timeout": self._settings.request_timeout,
            }
        )
        await asyncio.to_thread(self._conn.connect)

    async def disconnect(self) -> None:
        if self._conn is not None:
            client = self._conn.get_client()
            if client is not None:
                await asyncio.to_thread(client.close)
            self._conn = None

    async def search(self, index: str, body: dict[str, Any]) -> dict[str, Any]:
        if self._conn is None:
            raise RuntimeError("Elasticsearch not connected")
        return await self._conn.asearch(index, body)

    async def get_scroll(self, index: str, query: dict[str, Any], scroll_size: int, scroll_timeout: str) -> list[dict[str, Any]]:
        if self._conn is None:
            raise RuntimeError("Elasticsearch not connected")
        return await self._conn.agetScrollObject(index, query, scroll_size, scroll_timeout)

    @property
    def legacy(self) -> ESConnection:
        if self._conn is None:
            raise RuntimeError("Elasticsearch not connected")
        return self._conn


__all__ = ["ElasticsearchGateway"]
