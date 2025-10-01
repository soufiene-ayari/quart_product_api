from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database connection configuration."""

    driver: Literal["postgresql", "mssql"] = "postgresql"
    host: str = "localhost"
    port: int = 5432
    user: str = "systemair"
    password: str = "systemair"
    database: str = "product"
    pool_min_size: int = 5
    pool_max_size: int = 20

    model_config = SettingsConfigDict(env_prefix="DB_", extra="ignore")


class ElasticsearchSettings(BaseSettings):
    """Elasticsearch cluster configuration."""

    hosts: list[str] = ["http://localhost:9200"]
    cloud_id: str | None = None
    api_key: str | None = None
    username: str | None = None
    password: str | None = None
    request_timeout: float = 30.0

    model_config = SettingsConfigDict(env_prefix="ES_", extra="ignore")


class APISettings(BaseSettings):
    """FastAPI application settings."""

    app_name: str = "Systemair Product API"
    version: str = "1.0.0"
    environment: Literal["local", "dev", "qa", "prod"] = "local"
    debug: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    enable_cors: bool = True
    allowed_origins: list[str] = ["*"]

    model_config = SettingsConfigDict(env_prefix="API_", env_file=".env", extra="ignore")


class Settings(BaseSettings):
    """Aggregated application configuration."""

    api: APISettings = APISettings()
    database: DatabaseSettings = DatabaseSettings()
    elasticsearch: ElasticsearchSettings = ElasticsearchSettings()

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def root_path(self) -> Path:
        return Path(__file__).resolve().parents[2]


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""

    return Settings()


__all__ = ["Settings", "get_settings"]
