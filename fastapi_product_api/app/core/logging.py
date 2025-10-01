from __future__ import annotations

import logging
from logging.config import dictConfig

from pythonjsonlogger import jsonlogger

from .config import get_settings


class JsonFormatter(jsonlogger.JsonFormatter):
    """JSON formatter that enforces ISO timestamps."""

    def add_fields(self, log_record, record, message_dict):  # type: ignore[override]
        super().add_fields(log_record, record, message_dict)
        log_record.setdefault("timestamp", record.created)
        log_record.setdefault("level", record.levelname)
        log_record.setdefault("logger", record.name)


def configure_logging() -> None:
    settings = get_settings()
    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {
                    "()": JsonFormatter,
                    "fmt": "%(timestamp)s %(level)s %(name)s %(message)s",
                },
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "formatter": "json" if settings.api.environment == "prod" else "standard",
                }
            },
            "root": {
                "handlers": ["default"],
                "level": settings.api.log_level,
            },
        }
    )


__all__ = ["configure_logging"]
