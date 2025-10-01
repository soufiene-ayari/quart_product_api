from __future__ import annotations

import asyncio
import pathlib
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

sys.modules.setdefault(
    "orjson",
    types.SimpleNamespace(dumps=lambda obj: b"{}", loads=lambda data: {}),
)


class _BaseSettings:
    model_config: dict[str, object] = {}

    def __init__(self, **values: object) -> None:
        for name, value in self.__class__.__dict__.items():
            if name.startswith("_") or callable(value):
                continue
            setattr(self, name, value)
        for key, value in values.items():
            setattr(self, key, value)


sys.modules.setdefault(
    "pydantic_settings", types.SimpleNamespace(BaseSettings=_BaseSettings, SettingsConfigDict=dict)
)

from ..app.models import (
    Category,
    CategoryListResponse,
    Certification,
    OperatingMode,
    OperatingModeListResponse,
    Sku,
    SkuListResponse,
)
from ..app.services import builder_runner, product_service
from ..app.services import assignments_service, category_service, operating_mode_service


class StubElasticsearch:
    def __init__(self, hits: list[dict]):
        self._hits = hits

    async def get_scroll(self, index: str, body: dict, size: int, timeout: str) -> list[dict]:
        return self._hits

    async def search(self, index: str, body: dict) -> dict:
        return {"hits": {"hits": self._hits, "total": {"value": len(self._hits)}}}

    @property
    def legacy(self) -> object:
        return object()


class StubDatabase:
    @property
    def legacy(self) -> object:
        return object()


def test_get_product_skus_uses_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    es = StubElasticsearch([
        {"_source": {"epimId": "sku-1"}},
        {"_source": {"epimId": "sku-2"}},
    ])
    db = StubDatabase()

    async def fake_run_builder(builder, method, identifier, lang, brand, market):
        return Sku(
            id=identifier,
            parentId="parent",
            vendorId="vendor",
            maintenanceId="maint",
            name=f"SKU {identifier}",
            active=True,
            expired=False,
            approved=True,
        )

    monkeypatch.setattr(builder_runner, "run_builder", fake_run_builder)
    monkeypatch.setattr(product_service, "run_builder", fake_run_builder)

    async def _run() -> None:
        response = await product_service.get_product_skus(
            es, db, "prod-1", "en-GB", "systemair", ""
        )

        assert isinstance(response, SkuListResponse)
        assert response.meta["total"] == 2
        assert [sku.id for sku in response.items] == ["sku-1", "sku-2"]

    asyncio.run(_run())


def test_list_categories_builds_full_objects(monkeypatch: pytest.MonkeyPatch) -> None:
    es = StubElasticsearch([
        {"_source": {"epimId": "cat-1"}},
        {"_source": {"epimId": "cat-2"}},
    ])

    async def fake_run_builder(builder, method, identifier, lang, brand):
        return Category(
            id=identifier,
            parentId="root",
            name=f"Category {identifier}",
            sort=0,
            active=True,
            hidden=False,
            approved=True,
            type="category",
        )

    monkeypatch.setattr(builder_runner, "run_builder", fake_run_builder)
    monkeypatch.setattr(category_service, "run_builder", fake_run_builder)

    async def _run() -> None:
        response = await category_service.list_categories(
            es, 0, 10, "en-GB", "systemair", parent_id=None
        )

        assert isinstance(response, CategoryListResponse)
        assert response.meta.total == 2
        assert response.items[0].id == "cat-1"

    asyncio.run(_run())


def test_list_operating_modes_invokes_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    es = StubElasticsearch([
        {"_source": {"epimId": "mode-1"}},
    ])
    db = StubDatabase()

    async def fake_run_builder(builder, method, identifier, lang, brand, market):
        return OperatingMode(
            id=identifier,
            parentId="parent",
            vendorId="vendor",
            name="Operating Mode",
            active=True,
            expired=False,
            approved=True,
            skuId="sku-1",
        )

    monkeypatch.setattr(builder_runner, "run_builder", fake_run_builder)
    monkeypatch.setattr(operating_mode_service, "run_builder", fake_run_builder)

    async def _run() -> None:
        response = await operating_mode_service.list_operating_modes(
            es, db, 0, 10, "en-GB", "systemair", market="", product_id=None, sku_id=None
        )

        assert isinstance(response, OperatingModeListResponse)
        assert response.meta.total == 1
        assert response.items[0].id == "mode-1"

    asyncio.run(_run())


def test_list_certifications_wraps_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    es = StubElasticsearch([])
    db = StubDatabase()

    async def fake_run_builder(builder, method, locale):
        return [Certification(id="1", name="cert", label="Cert", image=None, text=None)]

    monkeypatch.setattr(builder_runner, "run_builder", fake_run_builder)
    monkeypatch.setattr(assignments_service, "run_builder", fake_run_builder)

    async def _run() -> None:
        response = await assignments_service.list_certifications(es, db, "en-GB")

        assert response.meta["items"] == 1
        assert response.items[0].name == "cert"

    asyncio.run(_run())
