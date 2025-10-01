"""Utilities for executing legacy async builders without blocking the loop."""

from __future__ import annotations

import asyncio
from typing import Any


async def run_builder(builder: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    """Execute a legacy async builder method in a worker thread.

    The legacy builders are implemented as ``async def`` coroutines that perform
    blocking I/O using synchronous Elasticsearch and database clients. Running
    them directly on the event loop would therefore block FastAPI's event loop.
    To keep the API responsive we spin up a dedicated event loop in a worker
    thread for every invocation and execute the coroutine there.
    """

    async def _invoke() -> Any:
        func = getattr(builder, method)
        return await func(*args, **kwargs)

    return await asyncio.to_thread(asyncio.run, _invoke())


__all__ = ["run_builder"]
