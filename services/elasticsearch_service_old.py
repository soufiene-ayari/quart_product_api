import logging
from elasticsearch import Elasticsearch, helpers, ApiError
from elastic_transport import ConnectionError as ESConnectionError, ConnectionTimeout, TransportError

class ESConnection:
    def __init__(self, config: dict):
        self.logger = logging.getLogger("services.elasticsearch")
        self.config = config
        self.es: Elasticsearch | None = None

    def connect(self):
        url = self.config["url"]                 # e.g. "https://es.host:9200"
        user = self.config["user"]
        password = self.config["pass"]
        certs = self.config.get("certs")         # path to http_ca.crt (optional)
        request_timeout = int(self.config.get("timeout", 30))
        retries = int(self.config.get("retries", 3))

        common = dict(
            request_timeout=request_timeout,     # v8: global timeout
            max_retries=retries,
            retry_on_timeout=True,
            http_compress=True,
        )

        if certs:
            self.es = Elasticsearch(
                hosts=[url],
                basic_auth=(user, password),     # v8
                verify_certs=True,
                ca_certs=certs,
                **common,
            )
        else:
            self.es = Elasticsearch(
                hosts=[url],
                basic_auth=(user, password),     # v8
                **common,
            )

        return self.check_connection()

    def check_connection(self):
        try:
            return self.es.info()
        except Exception as e:
            self.logger.error("Failed to connect to Elasticsearch", exc_info=e)
            return False

    def get_client(self) -> Elasticsearch:
        return self.es

    # ---- v8-friendly search ----
    def search(self, index, body_or_query: dict, **kwargs):
        """
        Accepts either:
          • a plain Query DSL dict (the value you'd pass to 'query=...')
          • or a full 'search body' dict containing keys like 'query', 'aggs', 'sort', 'size', ...
        We map both forms to v8 keyword params.
        """
        try:
            # If caller passed a *full* body (with 'query' or other top-level keys),
            # just splat it into kwargs. Otherwise treat it as the query DSL.
            if any(k in body_or_query for k in ("query", "aggs", "sort", "size", "_source", "collapse", "highlight")):
                return self.es.search(index=index, **body_or_query, **kwargs)
            else:
                return self.es.search(index=index, query=body_or_query, **kwargs)
        except (ESConnectionError, ConnectionTimeout, TransportError):
            self.logger.exception(f"Error with ES connection during search. Index: {index}")
            raise
        except ApiError:
            # API-level error (400/404/500...)
            self.logger.exception(f"Elasticsearch API error during search. Index: {index}")
            raise

    def searchAggregations(self, query_fn, index, size, fullFlag, lastRunTime):
        """
        Composite agg pagination helper. 'query_fn' should be the *name* of a function in globals()
        that returns a dict with keys like {'aggs': {...}, 'size': ..., 'query': {...}, 'composite': {...}}.
        """
        after_key = None
        query_fn = globals().get(query_fn)

        while True:
            q = query_fn(fullFlag, lastRunTime, size, after_key)
            response = self.search(index, q)
            buckets = response["aggregations"]["group_by_ids"].get("buckets", [])
            if not buckets:
                break

            for bucket in buckets:
                for hit in bucket["latest_document"]["hits"]["hits"]:
                    yield hit

            after_key = response["aggregations"]["group_by_ids"].get("after_key")
            if not after_key:
                break

    def getScrollObject(self, index, querySource, scrollSize, scrollTimeout):
        # 'helpers.scan' is still valid in v8 and takes 'query='
        return helpers.scan(
            self.es,
            index=index,
            query=querySource,
            size=scrollSize,
            scroll=scrollTimeout,
            preserve_order=True,
        )

    def getIndexCount(self, index, q, aggregation=None):
        if aggregation:
            response = self.search(index, q)
            return response["aggregations"]["unique_group_count"]["value"]
        else:
            # v8 'count' accepts query=...
            resp = self.es.count(index=index, query=(q.get("query") if isinstance(q, dict) and "query" in q else q))
            return resp["count"]
