# Systemair Product API (Quart)

## Features
- OpenAPI auto-generated docs (`/docs`)
- Blueprint routing
- Dataclass models for structured data
- MSSQL and Elasticsearch integration stubs
- Logging with level output
- Run with Uvicorn and multiple workers

## Run

```bash
uvicorn app:app --host 0.0.0.0 --port 5000 --workers 5
```

## Install dependencies

```bash
pip install -r requirements.txt
```
