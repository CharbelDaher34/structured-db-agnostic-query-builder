"""
Elasticsearch query executor.

Executes Elasticsearch DSL queries and returns normalized results.
"""

import logging
from typing import Any, Optional

from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


class ESQueryExecutor:
    """
    Executes Elasticsearch queries.

    Implements the IQueryExecutor interface for Elasticsearch.
    """

    def __init__(
        self,
        es_host: str,
        index_name: str,
        client: Optional[Elasticsearch] = None,
    ):
        """
        Initialize Elasticsearch query executor.

        Args:
            es_host: Elasticsearch host URL
            index_name: Name of the index to query
            client: Optional shared Elasticsearch client. Pass one in to share
                a single connection pool with the schema extractor.
        """
        self.es_host = es_host
        self.index_name = index_name
        self._owns_client = client is None
        self.es_client = client or Elasticsearch(hosts=[es_host])

    def close(self) -> None:
        """Close the ES client if owned by this executor."""
        if self._owns_client:
            try:
                self.es_client.close()
            except Exception:
                logger.warning("Failed to close Elasticsearch client", exc_info=True)

    def execute(
        self,
        queries: list[dict[str, Any]],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Execute multiple Elasticsearch queries.

        Args:
            queries: List of Elasticsearch DSL query objects

        Returns:
            List of result dictionaries
        """
        if not queries:
            return []

        # One query → just call search() (faster code path; msearch has a small
        # body-parsing overhead and worse error attribution).
        if len(queries) == 1:
            return [self._execute_single(queries[0], offset=offset, limit=limit)]

        # Multiple slices (comparisons, multi-bucket queries) → one msearch
        # round-trip instead of N sequential search() calls.
        return self._execute_msearch(queries, offset=offset, limit=limit)

    def _execute_msearch(
        self,
        queries: list[dict[str, Any]],
        offset: int,
        limit: Optional[int],
    ) -> list[dict[str, Any]]:
        """Batch multiple slices into a single _msearch call."""
        # msearch wants a flat list of alternating header / body dicts. We pin
        # the index in the executor's constructor, so the header is just {}.
        searches: list[dict[str, Any]] = []
        for query in queries:
            request = dict(query)
            if offset and "from" not in request:
                request["from"] = offset
            if limit is not None and "size" not in request:
                request["size"] = limit
            searches.append({})
            searches.append(request)

        try:
            response = self.es_client.msearch(index=self.index_name, searches=searches)
        except Exception as e:
            logger.exception("Elasticsearch msearch failed")
            return [
                {
                    "total_hits": 0,
                    "documents": [],
                    "error": str(e),
                    "success": False,
                }
                for _ in queries
            ]

        results: list[dict[str, Any]] = []
        for sub in response.get("responses", []):
            if "error" in sub:
                results.append(
                    {
                        "total_hits": 0,
                        "documents": [],
                        "error": str(sub["error"]),
                        "success": False,
                    }
                )
                continue
            hits = sub.get("hits", {})
            total = hits.get("total", 0)
            total_hits = total["value"] if isinstance(total, dict) else total
            result: dict[str, Any] = {
                "total_hits": total_hits,
                "documents": [h["_source"] for h in hits.get("hits", [])],
                "success": True,
            }
            if "aggregations" in sub:
                result["aggregations"] = sub["aggregations"]
            results.append(result)
        return results

    def _execute_single(
        self,
        query: dict[str, Any],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        """Execute a single query."""
        try:
            # Don't mutate the caller's dict — inject pagination only when the
            # query doesn't already specify it.
            request = dict(query)
            if offset and "from" not in request:
                request["from"] = offset
            if limit is not None and "size" not in request:
                request["size"] = limit

            response = self.es_client.search(index=self.index_name, **request)

            total = response["hits"]["total"]
            total_hits = total["value"] if isinstance(total, dict) else total

            result = {
                "total_hits": total_hits,
                "documents": [hit["_source"] for hit in response["hits"]["hits"]],
                "success": True,
            }

            if "aggregations" in response:
                result["aggregations"] = response["aggregations"]

            return result

        except Exception as e:
            logger.exception("Elasticsearch search failed")
            return {
                "total_hits": 0,
                "documents": [],
                "error": str(e),
                "success": False,
            }

    def execute_raw(self, query: dict[str, Any], size: int = 100) -> dict[str, Any]:
        """
        Execute a raw Elasticsearch query.

        Args:
            query: Raw Elasticsearch DSL query
            size: Number of results to return

        Returns:
            Result dictionary
        """
        try:
            # Don't mutate the caller's dict — they may reuse it.
            request = {**query}
            if "size" not in request:
                request["size"] = size

            response = self.es_client.search(index=self.index_name, **request)

            result = {
                "total_hits": (
                    response["hits"]["total"]["value"]
                    if isinstance(response["hits"]["total"], dict)
                    else response["hits"]["total"]
                ),
                "documents": [hit["_source"] for hit in response["hits"]["hits"]],
                "query": request,
                "success": True,
            }

            if "aggregations" in response:
                result["aggregations"] = response["aggregations"]

            return result

        except Exception as e:
            return {
                "error": str(e),
                "query": query,
                "success": False,
                "total_hits": 0,
                "documents": [],
            }
