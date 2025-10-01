from __future__ import annotations

import asyncio
from typing import Any

from services.database_service import DBConnection

from ..core.config import DatabaseSettings


class Database:
    """Async wrapper around the legacy DBConnection."""

    def __init__(self, settings: DatabaseSettings) -> None:
        self._settings = settings
        self._conn: DBConnection | None = None

    async def connect(self) -> None:
        if self._conn is not None:
            return
        self._conn = DBConnection(
            self._settings.driver,
            self._settings.host,
            self._settings.user,
            self._settings.password,
            self._settings.database,
        )
        await asyncio.to_thread(self._conn.connect)

    async def disconnect(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._conn.disconnect)
            self._conn = None

    async def fetch_all(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        if self._conn is None:
            raise RuntimeError("Database not connected")
        return await self._conn.aexecute_query(sql, params)

    @property
    def legacy(self) -> DBConnection:
        if self._conn is None:
            raise RuntimeError("Database not connected")
        return self._conn


__all__ = ["Database"]
