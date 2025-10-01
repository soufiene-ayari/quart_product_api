from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("app.errors")


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:  # type: ignore[override]
        logger.exception("Unhandled exception for %s %s", request.method, request.url.path, exc_info=exc)
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


__all__ = ["install_error_handlers"]
