import logging
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError, ConnectionError, ConnectionTimeout
import asyncio
class ESConnection:
    def __init__(self, config):
        self.logger = logging.getLogger("services.elasticsearch")
        self.config = config
        self.es = None

    def connect(self):
        url = self.config['url']
        user = self.config['user']
        password = self.config['pass']
        certs = self.config.get('certs')
        timeout = int(self.config.get('timeout', 30))
        retries = int(self.config.get('retries', 3))

        if certs:
            self.es = Elasticsearch(
                hosts=[url],
                http_auth=(user, password),
                timeout=timeout,
                max_retries=retries,
                retry_on_timeout=True,
                verify_certs=True,
                ca_certs=certs,
                http_compress=True,
            )
        else:
            self.es = Elasticsearch(
                hosts=[url],
                http_auth=(user, password),
                timeout=timeout,
                max_retries=retries,
                retry_on_timeout=True,
                http_compress=True,
            )

        return self.check_connection()

    def check_connection(self):
        try:
            return self.es.info()
        except Exception as e:
            self.logger.error("Failed to connect to Elasticsearch", exc_info=e)
            return False

    def get_client(self):
        return self.es


    def search(self, index, query):
        try:
            return self.es.search(index=index, body=query)
        except (ConnectionError, ConnectionTimeout):
            self.logger.exception(f"Error with ES connection during search. Index: {index}")
            raise

    async def asearch(self, index, query):
        try:
            # Why: elasticsearch-py is sync; run it in a worker thread
            return await asyncio.to_thread(self.es.search, index=index, body=query)
        except (ConnectionError, ConnectionTimeout):
            self.logger.exception("Error with ES connection during search. Index: %s", index)
            raise

    async def agetScrollObject(self, index, querySource, scrollSize, scrollTimeout):
        def _scan_sync():
            return list(helpers.scan(self.es, query=querySource, scroll=scrollTimeout, size=scrollSize, index=index))

        return await asyncio.to_thread(_scan_sync)
    def searchAggregations(self, query_fn, index, size, fullFlag, lastRunTime):
        """
        Generator function to fetch results using composite aggregation pagination.

        :param query_fn: Function that returns the query dict (with dynamic parameters)
        :param size: Number of results per page
        :yield: Each document bucket from Elasticsearch
        """
        after_key = None  # Used for pagination
        query_fn = globals().get(query_fn)

        while True:
            query = query_fn(fullFlag, lastRunTime, size, after_key)  # Call the user-defined function to get the query

            response = self.search(index, query)
            buckets = response["aggregations"]["group_by_ids"].get("buckets", [])

            if not buckets:
                break  # Stop iteration when no more data

            # Extract each document (hit) from the top_hits aggregation
            for bucket in buckets:
                hits = bucket["latest_document"]["hits"]["hits"]
                for hit in hits:
                    yield hit  # Yield the actual document (_source)

            after_key = response["aggregations"]["group_by_ids"].get("after_key")
            if not after_key:
                break  # Stop when there are no more pages
    # setup scrolling and return results object
    def getScrollObject(self, index, querySource, scrollSize, scrollTimeout):
        res = helpers.scan(self.es, query=querySource, scroll=scrollTimeout, size=scrollSize, index=index)
        return res
    # counts documents in index depending on query (delta/full)
    def getIndexCount(self, index, query, aggregation=None):

        if aggregation:
            response = self.search(index, query)
            count=response["aggregations"]["unique_group_count"]["value"]
            return count
        else:
            # indexlist = self.es.indices.get(index=index + '*')
            count = self.es.count(index=index, body=query)
            return count['count']

        # gets list of source indices that need to be processed depending on config settings (all languages or list)
        # all languages: resolve aliases since resolving indices would get all indices instead of only the most resent one
        # list languages: resolve alias name as index names since the name is defined without possible timestamp at the end