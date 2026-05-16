"""
Schema extraction coordinator.

Coordinates schema extraction from database adapters and prepares it for model building.
"""

import time
from typing import Any, Optional

from query_builder._logging import QueryBuilderLogger
from query_builder.core.interfaces import ISchemaExtractor

logger = QueryBuilderLogger.get(__name__)


class SchemaExtractor:
    """
    Coordinates schema extraction and normalization.

    This class wraps a database-specific schema extractor and provides
    additional coordination logic like caching and category field handling.
    """

    def __init__(
        self,
        extractor: ISchemaExtractor,
        category_fields: Optional[list[str]] = None,
    ):
        """
        Initialize schema extractor.

        Args:
            extractor: Database-specific schema extractor implementation
            category_fields: List of field paths to treat as categories (enum types)
        """
        self.extractor = extractor
        self.category_fields = category_fields or []
        self._cached_schema: Optional[dict[str, Any]] = None
        self._cached_enum_fields: Optional[dict[str, list[Any]]] = None

    def get_schema(self, force_refresh: bool = False) -> dict[str, Any]:
        """
        Get normalized schema.

        Args:
            force_refresh: If True, bypass cache and re-extract schema

        Returns:
            Normalized schema dictionary
        """
        if self._cached_schema is None or force_refresh:
            logger.info(
                "schema.extract: via %s (force_refresh=%s)",
                type(self.extractor).__name__,
                force_refresh,
            )
            t0 = time.perf_counter()
            self._cached_schema = self.extractor.extract_schema()
            logger.info(
                "schema.extract_done: %d field(s) in %.0f ms",
                len(self._cached_schema),
                (time.perf_counter() - t0) * 1000,
            )
        return self._cached_schema

    def get_enum_fields(self, force_refresh: bool = False) -> dict[str, list[Any]]:
        """
        Get enum values for category fields.

        Args:
            force_refresh: If True, bypass cache and re-fetch values

        Returns:
            Dictionary mapping field paths to list of distinct values
        """
        if self._cached_enum_fields is None or force_refresh:
            self._cached_enum_fields = {}

            if self.category_fields:
                logger.info(
                    "schema.enum_values: fetching for %d category field(s)",
                    len(self.category_fields),
                )
            t0 = time.perf_counter()
            for field_path in self.category_fields:
                try:
                    values = self.extractor.get_distinct_values(field_path)
                    if values:
                        self._cached_enum_fields[field_path] = values
                        logger.debug(
                            "schema.enum_values: %s -> %d value(s)",
                            field_path,
                            len(values),
                        )
                except Exception:
                    logger.warning("Could not get enum values for %r", field_path, exc_info=True)
            if self.category_fields:
                logger.info(
                    "schema.enum_values_done: %d/%d field(s) populated in %.0f ms",
                    len(self._cached_enum_fields),
                    len(self.category_fields),
                    (time.perf_counter() - t0) * 1000,
                )

        return self._cached_enum_fields

    def get_field_type(self, field_path: str) -> str:
        """
        Get normalized type for a field.

        Args:
            field_path: Path to the field

        Returns:
            Normalized type string
        """
        return self.extractor.get_field_type(field_path)
