"""
CSV query executor.

Executes a normalized execution plan (produced by CSVQueryTranslator) against a
pandas DataFrame and returns normalized results.
"""

import math
from datetime import date, datetime
from typing import Any, Optional

import pandas as pd

from query_builder._logging import QueryBuilderLogger

logger = QueryBuilderLogger.get(__name__)


def _looks_like_iso_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


_PANDAS_FREQ = {
    "day": "D",
    "week": "W",
    "month": "MS",
    "year": "YS",
}


class CSVQueryExecutor:
    """
    Executes CSV query plans by applying pandas operations.

    Implements the IQueryExecutor interface for CSV files.
    """

    def __init__(
        self,
        csv_path: str,
        df: Optional[pd.DataFrame] = None,
        date_columns: Optional[list[str]] = None,
        read_csv_kwargs: Optional[dict[str, Any]] = None,
    ):
        """
        Args:
            csv_path: Path to the CSV file (used for read_csv when df is not provided).
            df: Pre-loaded DataFrame. Pass the one from CSVSchemaExtractor to share state.
            date_columns: Columns to parse as datetimes when loading.
            read_csv_kwargs: Extra keyword args for pd.read_csv.
        """
        self.csv_path = csv_path
        self.date_columns = date_columns or []
        if df is not None:
            self._df = df
        else:
            kwargs = dict(read_csv_kwargs or {})
            if self.date_columns:
                kwargs.setdefault("parse_dates", self.date_columns)
            self._df = pd.read_csv(csv_path, **kwargs)

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    def close(self) -> None:
        return None

    def execute(
        self,
        queries: list[dict[str, Any]],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        if not queries:
            return []
        return [self._execute_single(q, offset=offset, limit=limit) for q in queries]

    def _execute_single(
        self,
        query: dict[str, Any],
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        try:
            plan: dict[str, Any] = query.get("plan", {})
            df = self._df

            # 1) Match — apply chained AND of conditions
            mask = self._build_mask(df, plan.get("conditions") or [])
            if mask is not None:
                df = df[mask]

            # 2) Grouping + aggregations + having
            agg_result_names: dict[str, str] = {}
            if plan.get("group_by"):
                df, agg_result_names = self._apply_group(df, plan)

            # 3) Sort (remap fields if grouped)
            if plan.get("sort"):
                df = self._apply_sort(
                    df,
                    plan["sort"],
                    is_grouped=bool(plan.get("group_by")),
                    group_by_fields=list(plan.get("group_by") or []),
                    agg_result_names=agg_result_names,
                )

            # Capture the row count BEFORE pagination so callers can compute
            # pages. After filter/group/sort but before offset/limit.
            total_hits = len(df)

            # 4) Pagination: per-slice `limit` wins over the request-level one
            slice_limit = plan.get("limit") if plan.get("limit") is not None else limit
            if offset:
                df = df.iloc[offset:]
            if slice_limit is not None:
                df = df.head(slice_limit)

            documents = [self._sanitize_doc(d) for d in df.to_dict(orient="records")]
            return {
                "total_hits": total_hits,
                "documents": documents,
                "success": True,
            }
        except Exception as e:
            logger.exception("CSV query failed")
            return {
                "total_hits": 0,
                "documents": [],
                "error": str(e),
                "success": False,
            }

    # ------------------------------------------------------------------ match

    def _build_mask(
        self, df: pd.DataFrame, conditions: list[dict[str, Any]]
    ) -> Optional[pd.Series]:
        if not conditions:
            return None

        mask = pd.Series(True, index=df.index)
        for cond in conditions:
            clause = self._condition_mask(df, cond)
            if clause is not None:
                mask &= clause
        return mask

    def _condition_mask(self, df: pd.DataFrame, condition: dict[str, Any]) -> Optional[pd.Series]:
        field = condition.get("field")
        operator = condition.get("operator")
        value = condition.get("value")

        if field not in df.columns:
            logger.warning("Condition on unknown column %r — ignoring", field)
            return None

        col = df[field]

        # Coerce string-like values for date columns so comparisons work.
        if pd.api.types.is_datetime64_any_dtype(col):
            value = self._coerce_value_to_datetime(value)

        if operator == ">":
            return col > value
        if operator == "<":
            return col < value
        if operator == "is":
            return col == value
        if operator == "different":
            return col != value
        if operator == "isin":
            if isinstance(value, list):
                if len(value) == 2 and all(_looks_like_iso_date(v) for v in value):
                    lo, hi = value
                    if pd.api.types.is_datetime64_any_dtype(col):
                        lo, hi = pd.Timestamp(lo), pd.Timestamp(hi)
                    return (col >= lo) & (col <= hi)
                return col.isin(value)
            return col == value
        if operator == "notin":
            if isinstance(value, list):
                return ~col.isin(value)
            return col != value
        if operator == "between":
            if isinstance(value, list) and len(value) == 2:
                return (col >= value[0]) & (col <= value[1])
            return None
        if operator == "contains":
            return col.astype(str).str.contains(str(value), case=False, na=False)
        if operator == "exists":
            return col.notna() if value is True else col.isna()

        logger.warning("Unknown operator %r — ignoring condition", operator)
        return None

    @staticmethod
    def _coerce_value_to_datetime(value: Any) -> Any:
        try:
            if isinstance(value, list):
                return [pd.Timestamp(v) if _looks_like_iso_date(v) else v for v in value]
            if _looks_like_iso_date(value):
                return pd.Timestamp(value)
        except Exception:
            return value
        return value

    # --------------------------------------------------------- group + having

    def _apply_group(
        self, df: pd.DataFrame, plan: dict[str, Any]
    ) -> tuple[pd.DataFrame, dict[str, str]]:
        group_by: list[str] = list(plan["group_by"])
        interval = plan.get("interval")
        aggregations = plan.get("aggregations") or []

        # Build a grouper per group_by field — pd.Grouper(freq=...) for date columns
        groupers: list[Any] = []
        for gb in group_by:
            if gb not in df.columns:
                continue
            if interval and pd.api.types.is_datetime64_any_dtype(df[gb]):
                freq = _PANDAS_FREQ.get(interval, "MS")
                groupers.append(pd.Grouper(key=gb, freq=freq))
            else:
                groupers.append(gb)

        grouped = df.groupby(groupers, dropna=False)

        agg_result_names: dict[str, str] = {}
        agg_kwargs: dict[str, pd.NamedAgg] = {}
        for agg in aggregations:
            field = agg["field"]
            agg_type = agg["type"]
            result_name = f"{agg_type}_{field.replace('.', '_')}"
            agg_result_names[field] = result_name

            func_map = {
                "sum": "sum",
                "avg": "mean",
                "count": "count",
                "min": "min",
                "max": "max",
            }
            func = func_map.get(agg_type)
            if func is None:
                logger.warning("Unknown aggregation type %r — skipping", agg_type)
                continue
            agg_kwargs[result_name] = pd.NamedAgg(column=field, aggfunc=func)

        if agg_kwargs:
            df_grouped = grouped.agg(**agg_kwargs).reset_index()
        else:
            df_grouped = grouped.size().reset_index(name="count")

        # Apply having
        for agg in aggregations:
            op = agg.get("having_operator")
            val = agg.get("having_value")
            if op is None or val is None:
                continue
            col_name = agg_result_names.get(agg["field"])
            if col_name is None or col_name not in df_grouped.columns:
                continue
            if op == ">":
                df_grouped = df_grouped[df_grouped[col_name] > val]
            elif op == "<":
                df_grouped = df_grouped[df_grouped[col_name] < val]
            elif op == "is":
                df_grouped = df_grouped[df_grouped[col_name] == val]
            elif op == "different":
                df_grouped = df_grouped[df_grouped[col_name] != val]

        return df_grouped, agg_result_names

    # ----------------------------------------------------------------- sort

    def _apply_sort(
        self,
        df: pd.DataFrame,
        sort_entries: list[dict[str, Any]],
        is_grouped: bool,
        group_by_fields: list[str],
        agg_result_names: dict[str, str],
    ) -> pd.DataFrame:
        columns: list[str] = []
        ascending: list[bool] = []

        for entry in sort_entries:
            field = entry["field"]
            order = entry.get("order", "asc")

            if not is_grouped:
                target = field
            elif field in group_by_fields:
                target = field  # group_by fields keep their name after reset_index()
            elif field in agg_result_names:
                target = agg_result_names[field]
            else:
                logger.warning(
                    "Sort field %r is not a group_by/aggregation result; dropping.",
                    field,
                )
                continue

            if target in df.columns:
                columns.append(target)
                ascending.append(order == "asc")
            else:
                logger.warning("Sort target column %r missing from result; dropping.", target)

        if not columns:
            return df
        return df.sort_values(by=columns, ascending=ascending)

    # ------------------------------------------------------------ raw query

    def execute_raw(self, query: dict[str, Any], size: int = 100) -> dict[str, Any]:
        """
        Execute a raw "filter" against the CSV.

        Accepts either {"plan": {...}} (the same shape as execute()) or
        {"filter": [<condition>, ...]} for a quick AND-mask.
        """
        try:
            if "plan" in query:
                return self._execute_single(query, limit=size)

            df = self._df
            conditions = query.get("filter") or []
            mask = self._build_mask(df, conditions)
            if mask is not None:
                df = df[mask]

            documents = [self._sanitize_doc(d) for d in df.head(size).to_dict(orient="records")]
            return {
                "total_hits": len(documents),
                "documents": documents,
                "query": query,
                "success": True,
            }
        except Exception as e:
            logger.exception("CSV raw query failed")
            return {
                "error": str(e),
                "query": query,
                "success": False,
                "total_hits": 0,
                "documents": [],
            }

    # -------------------------------------------------------- result hygiene

    @staticmethod
    def _sanitize_doc(doc: dict[str, Any]) -> dict[str, Any]:
        """Convert pandas-specific types (Timestamp, NaN, numpy scalars) to JSON-safe values."""
        out: dict[str, Any] = {}
        for key, value in doc.items():
            if (isinstance(value, float) and math.isnan(value)) or value is pd.NaT:
                out[key] = None
            elif isinstance(value, (pd.Timestamp, datetime, date)):
                out[key] = value.isoformat()
            elif hasattr(value, "item") and callable(value.item):
                # numpy scalar
                try:
                    out[key] = value.item()
                except Exception:
                    out[key] = value
            else:
                out[key] = value
        return out
