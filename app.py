from wsgiref.util import request_uri

from quart import Quart,request,g,jsonify
from quart_schema import QuartSchema, Info, OpenAPIProvider
import logging
from services.database_service import DBConnection
from services.elasticsearch_service import ESConnection
from core.environment import env
from routes.product_routes import product_bp
from routes.sku_routes import sku_bp
from routes.operating_mode_routes import operating_mode_bp
from routes.category_routes import category_bp
from routes.assignments_routes import assignments_bp
from utils.error_handler import register_error_handlers
from utils.utilities import shop_statistics
from utils.mapping import unmap_locale,get_epimLang_by_market
from quart_compress import Compress
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import asyncio
from deepdiff import DeepDiff
import httpx
import json
import os
from concurrent.futures import ThreadPoolExecutor
from quart import send_file
#class ThreeZeroProvider(OpenAPIProvider):
#    def schema(self):
#       spec = super().schema()
#        spec["openapi"] = "3.0.0"    # force 3.0.3 instead of 3.1.0
#        return spec
#app = Quart(__name__,static_folder="static")
#app.config["QUART_SCHEMA_SWAGGER_JS_URL"]  = "/static/swagger-ui-bundle.js"
#app.config["QUART_SCHEMA_SWAGGER_CSS_URL"] = "/static/swagger-ui.css"
# Initialize Quart-Schema with the app
#QuartSchema(
#    app,
#    info=Info(
#        title="Systemair Product API",
#        version="1.0.0",
#        description="API documentation for Systemair Product Service"
#    ),
#    openapi_provider_class=ThreeZeroProvider
#)
app = Quart(__name__)
app.config["JSON_SORT_KEYS"] = False
QuartSchema(app)
#
# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Global Resources
db_conn = None
es_conn = None





@app.before_serving
async def startup():
    global db_conn, es_conn
    config = env.getConfig()
    db_cfg = config["epim_db"]
    es_cfg = config["elastic_source"]

    db_conn = DBConnection(db_cfg["type"], db_cfg["host"], db_cfg["user"], db_cfg["pass"], db_cfg["name"])
    db_conn.connect()

    es_conn = ESConnection(es_cfg)
    es_conn.connect()
    app.db = db_conn
    app.es = es_conn
    await register_error_handlers(app)

@app.after_serving
async def shutdown():
    global db_conn, es_conn
    if db_conn:
        db_conn.disconnect()
    if es_conn and es_conn.get_client():
        await es_conn.get_client().close()


@app.before_request
async def start_timer():
    request.start_time = time.perf_counter()

@app.after_request
async def log_request(response):
    duration = time.perf_counter() - request.start_time
    user = getattr(g, "auth_user", "anonymous")
    if duration > 0.5:
        app.logger.warning(f"SLOW {request.method} {request.path} by {user} took {duration:.2f}s")
    return response






@app.route("/rest/compare/<brand>/<locale>/<types>/<id>", methods=["GET"])
async def compare_endpoints(brand: str, locale: str, types: str, id: str):
    """
    Compare API responses between old and new service
    
    This endpoint helps with migration by comparing responses from the old and new API services.
    It retrieves the same resource from both services and returns a detailed diff of the responses.
    
    ---
    tags:
      - System
    parameters:
      - name: brand
        in: path
        required: true
        schema:
          type: string
          enum: [systemair, frico, fantech, dvs, vts, vts-clima, systemair_uk]
          example: systemair
        description: The brand identifier
      - name: locale
        in: path
        required: true
        schema:
          type: string
          example: en-GB
        description: The locale code (e.g., en-GB, de-DE, fr-FR)
      - name: types
        in: path
        required: true
        schema:
          type: string
          enum: [product, sku, category, operating-mode]
          example: product
        description: The type of resource to compare
      - name: id
        in: path
        required: true
        schema:
          type: string
          example: "12345"
        description: The ID of the resource to compare
    responses:
      200:
        description: Successful comparison
        content:
          application/json:
            schema:
              type: object
              properties:
                old_url:
                  type: string
                  example: "https://shop.systemair.com/rest/en-GB/product/12345"
                  description: URL of the old service endpoint
                new_url:
                  type: string
                  example: "http://10.31.11.6:5000/rest/systemair/en-GB/product/12345"
                  description: URL of the new service endpoint
                differences:
                  type: object
                  description: Detailed diff between the two responses
                  example:
                    values_changed:
                      "root['name']":
                        new_value: "New Product Name"
                        old_value: "Old Product Name"
      400:
        description: Invalid request parameters
      500:
        description: Internal server error during comparison
      502:
        description: Failed to connect to one of the services
      504:
        description: Timeout while waiting for a service to respond
    """
    # Use brand in the old service URL
    if brand in ["frico", "fantech"]:
        ext = "net"
    else:
        ext = "com"
    OLD_SERVICE_BASE = f"https://shop.{brand}.{ext}/rest"
    NEW_SERVICE_BASE = f"http://10.31.10.6:5000/rest"

    old_url = f"{OLD_SERVICE_BASE}/{locale}/{types}/{id}"
    new_url = f"{NEW_SERVICE_BASE}/{brand}/{locale}/{types}/{id}"

    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            old_response = await client.get(old_url)
            new_response = await client.get(new_url)
    except httpx.ReadTimeout as e:
        return jsonify({"error": "Timeout while accessing one of the services", "details": str(e)}), 504
    except httpx.RequestError as e:
        return jsonify({"error": "Request failed", "details": str(e)}), 502

    try:
        old_json = old_response.json()
        new_json = new_response.json()
    except Exception as e:
        return jsonify({"error": "Invalid JSON response", "details": str(e)}), 500

    try:
        diff = DeepDiff(old_json, new_json, ignore_order=True)
        serializable_diff = json.loads(diff.to_json())
    except Exception as e:
        return jsonify({"error": "Diff serialization error", "details": str(e)}), 500

    return jsonify({
        "old_url": str(old_url),
        "new_url": str(new_url),
        "differences": serializable_diff,
    })

@app.route("/health")
async def health():
    """
    Health check endpoint
    
    Performs health checks on critical dependencies including the database and Elasticsearch.
    Returns the status of each component and an overall status.
    
    ---
    tags:
      - System
    responses:
      200:
        description: Health check passed
        content:
          application/json:
            schema:
              type: object
              properties:
                status:
                  type: string
                  enum: [ok, fail]
                  example: ok
                  description: Overall health status
                db:
                  type: string
                  enum: [ok, fail]
                  example: ok
                  description: Database connection status
                elasticsearch:
                  type: string
                  enum: [ok, down]
                  example: ok
                  description: Elasticsearch connection status
      500:
        description: Health check failed
        content:
          application/json:
            schema:
              type: object
              properties:
                status:
                  type: string
                  example: fail
                error:
                  type: string
                  example: "Database connection failed"
    """
    try:
        config = env.getConfig()["epim_db"]
        # Use a temporary DB connection for the health check
        temp_db = DBConnection(config["type"], config["host"], config["user"], config["pass"], config["name"])
        temp_db.connect()

        # Run a simple query
        db_result = await asyncio.to_thread(temp_db.execute_query, "SELECT 1")
        db_status = "ok" if db_result else "fail"

        temp_db.disconnect()

        # Check Elasticsearch
        es_info = app.es.es.info() if app.es else None
        es_status = "ok" if es_info else "down"

        return {
            "status": "ok",
            "db": db_status,
            "elasticsearch": es_status
        }

    except Exception as e:
        return {"status": "fail", "error": str(e)}, 500

app.register_blueprint(product_bp)
app.register_blueprint(sku_bp)
app.register_blueprint(operating_mode_bp)
app.register_blueprint(category_bp)
app.register_blueprint(assignments_bp)


STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

async def load_static_json(filename: str):
    file_path = os.path.join(STATIC_DIR, filename)
    if not os.path.isfile(file_path):
        return None, f"File {filename} not found"

    try:
        def read_json():
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        data = await asyncio.to_thread(read_json)
        return data, None
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON format in {filename}: {str(e)}"
    except Exception as e:
        return None, str(e)

@app.route("/rest/<brand>/shops")
async def get_brand_shops(brand: str):
    filename = f"{brand}_shops.json"
    data, error = await load_static_json(filename)
    if error:
        return jsonify({"error": error}), 500
    return jsonify(data)

#@app.route("/rest/<brand>/statistics")
async def get_brand_statistics(brand: str):
    filename = f"{brand}_statistics.json"
    data, error = await load_static_json(filename)
    if error:
        return jsonify({"error": error}), 500
    return jsonify(data)

@app.route("/rest/<brand>/statistics")
async def get_brand_statistics_dynamic(brand: str):

    # --- FAST: no script; use case-insensitive wildcard (works on ES 7.10+) ---
    # If you have a lowercased keyword subfield, prefer `prefix` on that field.
    brand_lower = brand.lower()
    proQuery = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "nested": {
                            "path": "hierarchies",
                            "query": {
                                "script": {
                                    "script": {
                                        "source": f"doc['hierarchies.hierarchy'].value.toLowerCase().startsWith('{brand.lower()}')",
                                        "lang": "painless"
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        },
        "_source": [
            "epimId",
            "referenceId"
        ]
    }

    pro_index = f"systemair_ds_products_eng_glo"
    pro_response = list(app.es.getScrollObject(pro_index, proQuery, 10000, "1m"))

    products = []
    for hit in pro_response:
        src = hit.get("_source", {})
        reference_id = src.get("referenceId")
        if reference_id:
            products.append(reference_id)

    # --- Attributes query (same filters), but run chunks concurrently ---
    att_index = f"systemair_ds_attributes_eng_glo"
    CHUNK = 10000
    chunks = [products[i:i + CHUNK] for i in range(0, len(products), CHUNK)]

    async def fetch_attr_chunk(chunk):
        q = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "terms": {
                                "parentId": chunk
                            }
                        },
                        {
                            "nested": {
                                "path": "collections",
                                "query": {
                                    "term": {
                                        "collections.collection": f"market-{brand}-COL"
                                    }
                                }
                            }
                        }
                    ]
                }
            },"_source":["parentId","name","values"]
        }
        # getScrollObject is likely sync; run in a worker thread
        return await asyncio.to_thread(
            lambda: list(app.es.getScrollObject(att_index, q, 10000, "1m"))
        )

    # Limit parallelism to avoid hammering ES (tune 4–8 based on cluster)
    SEMAPHORE = asyncio.Semaphore(10)
    async def guarded_fetch(chunk):
        async with SEMAPHORE:
            return await fetch_attr_chunk(chunk)

    # Run all chunks concurrently
    att_parts = await asyncio.gather(*(guarded_fetch(c) for c in chunks))
    att_response = [h for part in att_parts for h in part]

    shops = shop_statistics(att_response)

    langs = []
    for shop in shops:
        langsMarket = get_epimLang_by_market(shop)
        for lang in langsMarket:
            lang_mapped = unmap_locale(lang)
            if lang_mapped and lang_mapped not in langs:
                langs.append(lang_mapped)

    now = datetime.now(ZoneInfo("Europe/Berlin"))
    result = {
        "locales": langs,
        "markets": shops,
        "published": int(now.timestamp()),
        "version": now.strftime("%Y.%m.%d")
    }
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, workers=1, reload=False)
