from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Request


@asynccontextmanager
def request_timer(request: Request) -> AsyncIterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        request.state.duration = duration


def duration_logger(threshold: float) -> Callable[[Request], None]:
    def _log(request: Request) -> None:
        duration = getattr(request.state, "duration", 0.0)
        if duration > threshold:
            request.app.logger.warning(
                "SLOW %s %s took %.2fs", request.method, request.url.path, duration
            )

    return _log


__all__ = ["request_timer", "duration_logger"]
