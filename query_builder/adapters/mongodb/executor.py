"""
MongoDB query executor.

Executes MongoDB aggregation pipelines and returns normalized results.
"""

import logging
from typing import Any, Dict, List, Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

logger = logging.getLogger(__name__)


class MongoQueryExecutor:
    """
    Executes MongoDB queries.

    Implements the IQueryExecutor interface for MongoDB.
    """

    def __init__(
        self,
        mongo_uri: str,
        database_name: str,
        collection_name: str,
        client: Optional[MongoClient] = None,
    ):
        """
        Initialize MongoDB query executor.

        Args:
            mongo_uri: MongoDB connection URI
            database_name: Name of the database
            collection_name: Name of the collection
            client: Optional shared MongoClient. If provided, this executor will not own
                the client (it won't be closed on cleanup). Pass one in to share a single
                connection pool with the schema extractor.
        """
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name

        self._owns_client = client is None
        self.client: MongoClient = client or MongoClient(mongo_uri)
        self.db: Database = self.client[database_name]
        self.collection: Collection = self.db[collection_name]

    def close(self) -> None:
        """Close the MongoDB client if owned by this executor."""
        if self._owns_client:
            try:
                self.client.close()
            except Exception:
                logger.warning("Failed to close MongoClient", exc_info=True)

    def execute(
        self,
        queries: List[Dict[str, Any]],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple MongoDB aggregation pipelines.

        Args:
            queries: List of MongoDB query objects (each with "pipeline" key)
            offset: Pagination offset injected as a $skip stage (after pipeline-defined sorts).
            limit: Pagination limit injected as a $limit stage. If the pipeline already
                contains a $limit, this is ignored.

        Returns:
            List of result dictionaries
        """
        if not queries:
            return []

        results = []
        for query in queries:
            result = self._execute_single(query, offset=offset, limit=limit)
            results.append(result)

        return results

    def _execute_single(
        self,
        query: Dict[str, Any],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Execute a single MongoDB aggregation pipeline."""
        try:
            pipeline = list(query.get("pipeline", []))

            # Inject pagination only if the pipeline doesn't already define a $limit
            has_limit = any("$limit" in stage for stage in pipeline)
            if offset:
                pipeline.append({"$skip": offset})
            if limit is not None and not has_limit:
                pipeline.append({"$limit": limit})

            if not pipeline:
                effective_limit = limit if limit is not None else 100
                documents = list(self.collection.find().skip(offset).limit(effective_limit))
            else:
                documents = list(self.collection.aggregate(pipeline, allowDiskUse=True))

            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

            return {
                "total_hits": len(documents),
                "documents": documents,
                "success": True,
            }

        except Exception as e:
            logger.exception("MongoDB aggregation failed")
            return {
                "total_hits": 0,
                "documents": [],
                "error": str(e),
                "success": False,
            }

    def execute_raw(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        """
        Execute a raw MongoDB query or aggregation.

        Args:
            query: Raw MongoDB query or aggregation pipeline
                   Format: {"pipeline": [...]} for aggregation
                   or {"filter": {...}} for find()
            size: Number of results to return (for find() queries)
        """
        try:
            if "pipeline" in query:
                documents = list(
                    self.collection.aggregate(query["pipeline"], allowDiskUse=True)
                )
            elif "filter" in query:
                documents = list(self.collection.find(query["filter"]).limit(size))
            else:
                documents = list(self.collection.find().limit(size))

            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

            return {
                "total_hits": len(documents),
                "documents": documents,
                "query": query,
                "success": True,
            }

        except Exception as e:
            logger.exception("MongoDB raw query failed")
            return {
                "error": str(e),
                "query": query,
                "success": False,
                "total_hits": 0,
                "documents": [],
            }
