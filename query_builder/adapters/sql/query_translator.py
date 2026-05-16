"""
SQL query translator.

Translates the LLM's structured ``QueryFilters`` into SQLAlchemy ``Select``
statements. Output dicts wrap the compiled statement plus a small amount of
metadata (whether the slice is an aggregation, which columns are group-by /
aggregate result names) so the executor can shape the response correctly.

Date-bucket grouping is dialect-aware: PostgreSQL gets ``date_trunc``, SQLite
gets ``strftime``, MySQL gets ``date_format``. Any other dialect falls back to
``cast(col, Date)`` which only supports day-level granularity.
"""

from typing import Any, Optional

from sqlalchemy import Date, Select, Table, and_, asc, cast, desc, func, select
from sqlalchemy.sql.elements import ColumnElement

from query_builder._logging import QueryBuilderLogger

logger = QueryBuilderLogger.get(__name__)


class SQLQueryTranslator:
    """
    Translates structured filters to SQLAlchemy Select statements.

    Implements the IQueryTranslator interface for SQL databases. Output:
    a list of ``{"statement": <Select>, "is_aggregation": bool, ...}`` dicts.
    """

    SUPPORTED_OPERATORS = {
        "string": {"is", "different", "contains", "isin", "notin", "exists"},
        "number": {"<", ">", "is", "different", "between", "isin", "notin", "exists"},
        "date": {"<", ">", "is", "different", "between", "exists"},
        "boolean": {"is", "different", "exists"},
        "enum": {"is", "different", "isin", "notin", "exists"},
    }

    def __init__(self, table: Table, dialect_name: str = "postgresql"):
        """
        Args:
            table: SQLAlchemy ``Table`` to build queries against.
            dialect_name: ``engine.dialect.name`` (e.g. "postgresql", "sqlite",
                "mysql"). Drives the date-truncation function choice.
        """
        self.table = table
        self.dialect_name = dialect_name

    def translate(
        self, filters: dict[str, Any], model_info: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if not filters or "filters" not in filters:
            return [self._build_query({}, model_info)]

        return [self._build_query(slc, model_info) for slc in filters["filters"]]

    # ------------------------------------------------------------------ build

    def _build_query(
        self, slice_filters: dict[str, Any], model_info: dict[str, Any]
    ) -> dict[str, Any]:
        conditions = slice_filters.get("conditions") or []
        sort = slice_filters.get("sort") or []
        limit = slice_filters.get("limit")
        group_by = [g for g in (slice_filters.get("group_by") or []) if g in model_info]
        aggregations = slice_filters.get("aggregations") or []
        interval = slice_filters.get("interval") if group_by else None

        where_clauses = self._build_conditions(conditions, model_info)

        if group_by or aggregations:
            return self._build_aggregation_query(
                where_clauses=where_clauses,
                group_by=group_by,
                aggregations=aggregations,
                interval=interval,
                sort=sort,
                limit=limit,
                model_info=model_info,
            )
        return self._build_row_query(where_clauses, sort, limit)

    def _build_row_query(
        self,
        where_clauses: list[ColumnElement],
        sort: list[dict[str, Any]],
        limit: Optional[int],
    ) -> dict[str, Any]:
        stmt: Select = select(self.table)
        if where_clauses:
            stmt = stmt.where(and_(*where_clauses))
        stmt = self._apply_sort(stmt, sort)
        if limit is not None:
            stmt = stmt.limit(limit)

        return {"statement": stmt, "is_aggregation": False}

    def _build_aggregation_query(
        self,
        where_clauses: list[ColumnElement],
        group_by: list[str],
        aggregations: list[dict[str, Any]],
        interval: Optional[str],
        sort: list[dict[str, Any]],
        limit: Optional[int],
        model_info: dict[str, Any],
    ) -> dict[str, Any]:
        # Build SELECT and GROUP BY expressions together — they must match.
        select_exprs: list[ColumnElement] = []
        group_exprs: list[ColumnElement] = []

        for gb in group_by:
            col = self.table.columns.get(gb)
            if col is None:
                continue
            if interval and self._is_date_field(gb, model_info):
                trunc = self._date_trunc(interval, col)
                select_exprs.append(trunc.label(gb))
                group_exprs.append(trunc)
            else:
                select_exprs.append(col.label(gb))
                group_exprs.append(col)

        agg_result_names: dict[str, str] = {}
        having_clauses: list[ColumnElement] = []
        for agg in aggregations:
            agg_type = agg.get("type")
            field = agg.get("field")
            col = self.table.columns.get(field)

            expr = self._aggregation_expr(agg_type, col)
            if expr is None:
                logger.warning("Unknown aggregation type %r — skipping", agg_type)
                continue

            result_name = f"{agg_type}_{field}"
            agg_result_names[field] = result_name
            select_exprs.append(expr.label(result_name))

            having_op = agg.get("having_operator")
            having_val = agg.get("having_value")
            if having_op is not None and having_val is not None:
                having_clauses.append(self._having_clause(expr, having_op, having_val))

        stmt: Select = select(*select_exprs)
        if where_clauses:
            stmt = stmt.where(and_(*where_clauses))
        if group_exprs:
            stmt = stmt.group_by(*group_exprs)
        if having_clauses:
            stmt = stmt.having(and_(*having_clauses))

        stmt = self._apply_sort(stmt, sort, group_by=group_by, agg_names=agg_result_names)
        if limit is not None:
            stmt = stmt.limit(limit)

        return {
            "statement": stmt,
            "is_aggregation": True,
            "group_by": group_by,
            "agg_result_names": agg_result_names,
        }

    # ------------------------------------------------------------ conditions

    def _build_conditions(
        self, conditions: list[dict[str, Any]], model_info: dict[str, Any]
    ) -> list[ColumnElement]:
        clauses: list[ColumnElement] = []
        for cond in conditions:
            field = cond.get("field")
            if field not in model_info:
                logger.warning("Dropping condition on unknown field %r", field)
                continue
            col = self.table.columns.get(field)
            if col is None:
                logger.warning("Field %r in model_info but not in table — skipping", field)
                continue

            clause = self._condition_clause(col, cond.get("operator"), cond.get("value"))
            if clause is not None:
                clauses.append(clause)
        return clauses

    @staticmethod
    def _condition_clause(
        col: ColumnElement, operator: Optional[str], value: Any
    ) -> Optional[ColumnElement]:
        if operator == "is":
            return col == value
        if operator == "different":
            return col != value
        if operator == ">":
            return col > value
        if operator == "<":
            return col < value
        if operator == "between":
            if isinstance(value, list) and len(value) == 2:
                return col.between(value[0], value[1])
            return None
        if operator == "isin":
            if isinstance(value, list):
                # Two-element date list is sometimes used as a range by the LLM
                # but the CSV adapter handles that — SQL keeps strict IN().
                return col.in_(value)
            return col == value
        if operator == "notin":
            if isinstance(value, list):
                return col.notin_(value)
            return col != value
        if operator == "contains":
            return col.ilike(f"%{value}%")
        if operator == "exists":
            return col.isnot(None) if value else col.is_(None)

        logger.warning("Unknown SQL operator %r — ignoring condition", operator)
        return None

    # ---------------------------------------------------------- aggregations

    @staticmethod
    def _aggregation_expr(
        agg_type: Optional[str], col: Optional[ColumnElement]
    ) -> Optional[ColumnElement]:
        if agg_type == "count":
            # COUNT(*) when no column is specified (or column missing)
            return func.count() if col is None else func.count(col)
        if col is None:
            return None
        if agg_type == "sum":
            return func.sum(col)
        if agg_type == "avg":
            return func.avg(col)
        if agg_type == "min":
            return func.min(col)
        if agg_type == "max":
            return func.max(col)
        return None

    @staticmethod
    def _having_clause(expr: ColumnElement, op: str, val: Any) -> ColumnElement:
        if op == ">":
            return expr > val
        if op == "<":
            return expr < val
        if op == "is":
            return expr == val
        if op == "different":
            return expr != val
        return expr > val  # safe default — only the four operators above are valid

    # --------------------------------------------------------- date intervals

    def _date_trunc(self, interval: str, col: ColumnElement) -> ColumnElement:
        if self.dialect_name == "postgresql":
            return func.date_trunc(interval, col)
        if self.dialect_name == "sqlite":
            fmt = {
                "day": "%Y-%m-%d",
                "week": "%Y-%W",
                "month": "%Y-%m-01",
                "year": "%Y-01-01",
            }.get(interval, "%Y-%m-%d")
            return func.strftime(fmt, col)
        if self.dialect_name in ("mysql", "mariadb"):
            fmt = {
                "day": "%Y-%m-%d",
                "week": "%Y-%u",
                "month": "%Y-%m-01",
                "year": "%Y-01-01",
            }.get(interval, "%Y-%m-%d")
            return func.date_format(col, fmt)
        # Generic fallback: day-level only.
        return cast(col, Date)

    @staticmethod
    def _is_date_field(field: str, model_info: dict[str, Any]) -> bool:
        return model_info.get(field, {}).get("type") == "date"

    # ------------------------------------------------------------------ sort

    def _apply_sort(
        self,
        stmt: Select,
        sort: list[dict[str, Any]],
        group_by: Optional[list[str]] = None,
        agg_names: Optional[dict[str, str]] = None,
    ) -> Select:
        group_by = group_by or []
        agg_names = agg_names or {}

        for entry in sort:
            field = entry.get("field")
            order = entry.get("order", "asc")
            direction = desc if order == "desc" else asc

            # For aggregation queries, sort by the aggregation alias rather than
            # the raw column — that's the only thing in the result set.
            if field in agg_names:
                stmt = stmt.order_by(direction(agg_names[field]))
                continue
            if group_by and field in group_by:
                stmt = stmt.order_by(direction(field))
                continue

            col = self.table.columns.get(field)
            if col is None:
                logger.warning("Sort field %r not in table — skipping", field)
                continue
            stmt = stmt.order_by(direction(col))

        return stmt
