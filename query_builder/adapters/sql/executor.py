"""
SQL query executor.

Executes SQLAlchemy ``Select`` statements (produced by SQLQueryTranslator) and
normalizes results. ``total_hits`` is computed pre-pagination by wrapping the
statement in ``SELECT COUNT(*) FROM (<stmt>) sub`` — matching the semantics of
the other adapters.
"""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import Engine, Select, func, select, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class SQLQueryExecutor:
    """
    Executes SQL queries via a SQLAlchemy engine.

    Implements the IQueryExecutor interface for relational databases.
    """

    def __init__(self, engine: Engine, owns_engine: bool = False):
        """
        Args:
            engine: SQLAlchemy engine. When ``owns_engine`` is True the executor
                disposes of it on ``close()`` — set this when the executor
                itself created the engine.
            owns_engine: Whether the executor should dispose the engine on close.
        """
        self.engine = engine
        self._owns_engine = owns_engine

    def close(self) -> None:
        if self._owns_engine:
            try:
                self.engine.dispose()
            except Exception:
                logger.warning("Failed to dispose SQLAlchemy engine", exc_info=True)

    def execute(
        self,
        queries: list[dict[str, Any]],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        if not queries:
            return []

        results: list[dict[str, Any]] = []
        with self.engine.connect() as conn:
            for query in queries:
                results.append(self._execute_single(conn, query, offset, limit))
        return results

    def _execute_single(
        self,
        conn,
        query: dict[str, Any],
        offset: int,
        limit: Optional[int],
    ) -> dict[str, Any]:
        stmt: Optional[Select] = query.get("statement")
        if stmt is None:
            return {
                "total_hits": 0,
                "documents": [],
                "error": "missing 'statement' in query",
                "success": False,
            }

        try:
            # Pre-pagination count — wrap the (unpaginated) query as a subquery
            # and SELECT COUNT(*) over it. This matches the Mongo/CSV semantics
            # where total_hits reflects the filtered row count, not the page.
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_hits = conn.execute(count_stmt).scalar() or 0

            paginated = stmt
            if offset:
                paginated = paginated.offset(offset)
            if limit is not None:
                paginated = paginated.limit(limit)

            result = conn.execute(paginated)
            keys = list(result.keys())
            rows = [self._row_to_dict(row, keys) for row in result]

            response: dict[str, Any] = {
                "total_hits": int(total_hits),
                "documents": rows,
                "success": True,
            }
            if query.get("is_aggregation"):
                # Match the other adapters' shape: the executor itself doesn't
                # invent a bucket structure, but consumers know to read
                # `documents` as bucket rows when is_aggregation is True.
                response["aggregations"] = {"buckets": rows}
            return response

        except SQLAlchemyError as e:
            logger.exception("SQL execution failed")
            return {
                "total_hits": 0,
                "documents": [],
                "error": str(e),
                "success": False,
            }

    # ------------------------------------------------------------- raw query

    def execute_raw(self, query: dict[str, Any], size: int = 100) -> dict[str, Any]:
        """
        Execute a raw SQL query.

        Accepts either:
        - ``{"statement": <Select>}`` — same shape as ``execute()`` produces.
        - ``{"sql": "<raw SQL string>", "params": {...}}`` — for ad-hoc text SQL.
        """
        try:
            with self.engine.connect() as conn:
                if "statement" in query:
                    return self._execute_single(conn, query, offset=0, limit=size)

                sql = query.get("sql")
                if not sql:
                    return {
                        "total_hits": 0,
                        "documents": [],
                        "error": "execute_raw needs either 'statement' or 'sql'",
                        "success": False,
                        "query": query,
                    }
                params = query.get("params") or {}
                result = conn.execute(text(sql), params)
                keys = list(result.keys())
                rows = [self._row_to_dict(row, keys) for row in result.fetchmany(size)]
                return {
                    "total_hits": len(rows),
                    "documents": rows,
                    "query": query,
                    "success": True,
                }
        except SQLAlchemyError as e:
            logger.exception("SQL raw query failed")
            return {
                "total_hits": 0,
                "documents": [],
                "error": str(e),
                "query": query,
                "success": False,
            }

    # -------------------------------------------------------- result hygiene

    def _row_to_dict(self, row: Any, keys: list[str]) -> dict[str, Any]:
        return {k: self._json_safe(v) for k, v in zip(keys, row, strict=False)}

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        return value
