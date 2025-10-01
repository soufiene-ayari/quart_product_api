# Systemair Product API (FastAPI Edition)

This repository contains a modernized FastAPI implementation of the Systemair Product API.
It preserves the business rules implemented in the legacy Quart service while delivering
higher throughput, stricter validation, and a maintainable architecture.

The legacy Quart application remains untouched in the repository root. The new code lives
under `fastapi_product_api/` so both versions can be compared side-by-side during the
migration.

## Highlights

* **FastAPI** service shell with automatic OpenAPI documentation and async lifespan hooks.
* **Async-friendly persistence adapters** that wrap the legacy SQL and Elasticsearch
  connectors without blocking the FastAPI event loop.
* **Structured configuration** managed through Pydantic settings with environment overrides.
* **Thread-offloaded product and SKU services** that reuse the mature builder logic while
  enabling concurrent request handling.
* **Mutable-default safe Pydantic models** powered by `Field(default_factory=...)` to prevent
  state leakage under concurrency.

## Project layout

```
fastapi_product_api/
├── README.md
├── pyproject.toml
├── requirements.txt
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── core/
│   │   ├── config.py
│   │   ├── lifespan.py
│   │   └── logging.py
│   ├── integrations/
│   │   ├── database.py
│   │   └── elasticsearch.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── product.py
│   │   ├── responses.py
│   │   └── sku.py
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── health.py
│   │   ├── products.py
│   │   └── skus.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── product_service.py
│   │   └── sku_service.py
│   └── utils/
│       ├── error_handlers.py
│       ├── mapping.py
│       ├── pagination.py
│       └── timing.py
└── tests/
    └── __init__.py
```

## Running locally

```bash
uv sync
uv run fastapi dev app/main.py
```

Alternatively, using pip/venv:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Configuration is controlled via environment variables described in
`app/core/config.py`. During development you can create a `.env` file at the
project root to supply settings.

## Testing

```bash
pytest
```

Unit tests would use async fixtures and run against lightweight fakes for the database
and Elasticsearch to guarantee deterministic payloads.
