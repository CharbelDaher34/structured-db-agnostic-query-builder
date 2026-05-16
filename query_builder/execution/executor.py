"""
Query execution coordinator.

Handles execution of queries through database-specific executors.
"""

import asyncio
from typing import Any, Optional

from query_builder._logging import QueryBuilderLogger
from query_builder.core.interfaces import IQueryExecutor

logger = QueryBuilderLogger.get(__name__)


class QueryExecutor:
    """
    Coordinates query execution.

    Wraps a database-specific query executor and provides common
    execution logic like error handling and async dispatch.
    """

    def __init__(self, executor: IQueryExecutor):
        """
        Initialize query executor.

        Args:
            executor: Database-specific query executor implementation
        """
        self.executor = executor

    def execute(
        self,
        queries: list[dict[str, Any]],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Execute multiple queries synchronously."""
        if not queries:
            return []

        try:
            return self.executor.execute(queries, offset=offset, limit=limit)
        except Exception as e:
            logger.exception("Query execution failed")
            return [
                {
                    "total_hits": 0,
                    "documents": [],
                    "error": str(e),
                    "success": False,
                }
                for _ in queries
            ]

    async def execute_async(
        self,
        queries: list[dict[str, Any]],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Execute queries on a worker thread so blocking DB drivers don't stall the event loop.
        """
        return await asyncio.to_thread(self.execute, queries, offset, limit)

    def execute_raw(self, query: dict[str, Any], size: int = 100) -> dict[str, Any]:
        """Execute a raw database query."""
        try:
            return self.executor.execute_raw(query, size)
        except Exception as e:
            logger.exception("Raw query execution failed")
            return {
                "total_hits": 0,
                "documents": [],
                "error": str(e),
                "success": False,
                "query": query,
            }

    async def execute_raw_async(self, query: dict[str, Any], size: int = 100) -> dict[str, Any]:
        """Async variant of execute_raw."""
        return await asyncio.to_thread(self.execute_raw, query, size)
