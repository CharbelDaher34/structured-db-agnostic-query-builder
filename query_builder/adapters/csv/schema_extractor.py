"""
CSV schema extraction.

Implements ISchemaExtractor for CSV files using pandas.
"""

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _looks_like_iso_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


class CSVSchemaExtractor:
    """
    Extracts schema information from a CSV file by inspecting pandas dtypes
    and sampling values.

    The full CSV is loaded into a DataFrame at construction time so the executor
    can share it — for very large files use the `df` argument to pass in a
    pre-built DataFrame (e.g. from a chunked or memory-mapped read).
    """

    # Number of non-null values inspected to confirm a string column is ISO dates
    _DATE_SAMPLE_SIZE = 20

    def __init__(
        self,
        csv_path: str,
        category_fields: Optional[list[str]] = None,
        date_columns: Optional[list[str]] = None,
        df: Optional[pd.DataFrame] = None,
        read_csv_kwargs: Optional[dict[str, Any]] = None,
    ):
        """
        Args:
            csv_path: Path to the CSV file.
            category_fields: Columns to expose as enums.
            date_columns: Columns to coerce to datetime when loading (also forces
                the schema type to "date").
            df: Pre-loaded DataFrame. When provided the file is NOT re-read; pass
                this in to share a DataFrame with `CSVQueryExecutor`.
            read_csv_kwargs: Extra keyword args forwarded to pd.read_csv.
        """
        self.csv_path = csv_path
        self.category_fields = category_fields or []
        self.date_columns = date_columns or []
        self._read_csv_kwargs = read_csv_kwargs or {}

        if df is not None:
            self._df = df
        else:
            kwargs = dict(self._read_csv_kwargs)
            if self.date_columns:
                kwargs.setdefault("parse_dates", self.date_columns)
            self._df = pd.read_csv(csv_path, **kwargs)

        self._schema_cache: Optional[dict[str, Any]] = None
        self._enum_cache: Optional[dict[str, list[Any]]] = None

    @property
    def df(self) -> pd.DataFrame:
        """Underlying DataFrame — pass this into the executor to share state."""
        return self._df

    def close(self) -> None:
        """No-op for parity with DB adapters; nothing to close for an in-memory CSV."""
        return None

    def extract_schema(self) -> dict[str, Any]:
        if self._schema_cache is not None:
            return self._schema_cache

        schema: dict[str, Any] = {}
        for column in self._df.columns:
            normalized_type = self._normalize_dtype(column)
            field_info: dict[str, Any] = {"type": normalized_type}
            if column in self.category_fields:
                field_info["type"] = "enum"
            schema[column] = field_info

        self._schema_cache = schema
        return schema

    def invalidate_cache(self) -> None:
        """Drop cached schema/enums so the next call re-extracts."""
        self._schema_cache = None
        self._enum_cache = None

    def _normalize_dtype(self, column: str) -> str:
        if column in self.date_columns:
            return "date"

        series = self._df[column]
        dtype = series.dtype

        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "date"
        if pd.api.types.is_bool_dtype(dtype):
            return "boolean"
        if pd.api.types.is_numeric_dtype(dtype):
            return "number"

        if pd.api.types.is_string_dtype(dtype) or dtype is object:
            # Object columns: confirm whether the values are ISO dates.
            sample = series.dropna().head(self._DATE_SAMPLE_SIZE)
            if not sample.empty and all(_looks_like_iso_date(v) for v in sample):
                return "date"
            return "string"

        return "string"

    def get_distinct_values(self, field_path: str, size: int = 1000) -> list[Any]:
        if self._enum_cache and field_path in self._enum_cache:
            return self._enum_cache[field_path]

        if field_path not in self._df.columns:
            logger.warning("get_distinct_values: column %r not in CSV", field_path)
            return []

        try:
            values = self._df[field_path].dropna().unique().tolist()
            if len(values) > size:
                values = values[:size]

            if self._enum_cache is None:
                self._enum_cache = {}
            self._enum_cache[field_path] = values
            return values
        except Exception:
            logger.warning("Error getting distinct values for %r", field_path, exc_info=True)
            return []

    def get_field_type(self, field_path: str) -> str:
        schema = self.extract_schema()
        return schema.get(field_path, {}).get("type", "unknown")
