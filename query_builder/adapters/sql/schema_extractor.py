"""
SQL schema extraction.

Implements ISchemaExtractor for any SQLAlchemy-compatible database (PostgreSQL,
MySQL, SQLite, …) by reading column metadata from a SQLAlchemy ``Table``.

Works with anything SQLModel/SQLAlchemy can describe: a SQLModel class, a
SQLAlchemy ``Table`` instance, or a table name resolved against the engine.
"""

import logging
from typing import Any, Optional

from sqlalchemy import Engine, MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

from query_builder.schema.type_mappings import TypeMapper

logger = logging.getLogger(__name__)


class SQLSchemaExtractor:
    """
    Extracts schema information from a SQL table via SQLAlchemy metadata.

    Implements the ISchemaExtractor interface for relational databases.
    Column types are mapped to normalized types by looking up the column's
    ``python_type`` against :class:`TypeMapper.PYTHON_TYPE_MAP` — so SQLAlchemy
    handles the dialect-specific type system and we only deal with Python types.
    """

    def __init__(
        self,
        engine: Engine,
        table: Table,
        category_fields: Optional[list[str]] = None,
    ):
        """
        Args:
            engine: SQLAlchemy engine bound to the target database. Shared with
                the executor so a single connection pool is reused.
            table: SQLAlchemy ``Table`` instance for the target table. The
                orchestrator's ``from_sqlmodel`` factory resolves SQLModel
                classes / table names into a Table for you.
            category_fields: Columns to expose as enums.
        """
        self.engine = engine
        self.table = table
        self.category_fields = category_fields or []

        self._schema_cache: Optional[dict[str, Any]] = None
        self._enum_cache: Optional[dict[str, list[Any]]] = None

    def close(self) -> None:
        """No-op: the engine is owned by the orchestrator's ``_shared_client``."""
        return None

    def extract_schema(self) -> dict[str, Any]:
        if self._schema_cache is not None:
            return self._schema_cache

        schema: dict[str, Any] = {}
        for col in self.table.columns:
            normalized = self._normalize_column_type(col)
            field_info: dict[str, Any] = {
                "type": normalized,
                # Stash the SQL type name so debugging / future translator
                # decisions can reference it (analogous to es_type / mongo_type).
                "sql_type": str(col.type),
            }
            if col.name in self.category_fields:
                field_info["type"] = "enum"
            schema[col.name] = field_info

        self._schema_cache = schema
        return schema

    def invalidate_cache(self) -> None:
        """Drop cached schema / enums so the next call re-extracts."""
        self._schema_cache = None
        self._enum_cache = None

    @staticmethod
    def _normalize_column_type(column: Any) -> str:
        try:
            python_type = column.type.python_type
        except (NotImplementedError, AttributeError):
            return "string"

        return TypeMapper.normalize_python_type(python_type.__name__)

    def get_distinct_values(self, field_path: str, size: int = 1000) -> list[Any]:
        if self._enum_cache and field_path in self._enum_cache:
            return self._enum_cache[field_path]

        col = self.table.columns.get(field_path)
        if col is None:
            logger.warning("get_distinct_values: column %r not in table", field_path)
            return []

        try:
            stmt = select(col).where(col.isnot(None)).distinct().limit(size)
            with self.engine.connect() as conn:
                values = [row[0] for row in conn.execute(stmt) if row[0] is not None]

            if self._enum_cache is None:
                self._enum_cache = {}
            self._enum_cache[field_path] = values
            return values
        except SQLAlchemyError:
            logger.warning("Error getting distinct values for %r", field_path, exc_info=True)
            return []

    def get_field_type(self, field_path: str) -> str:
        schema = self.extract_schema()
        return schema.get(field_path, {}).get("type", "unknown")


def resolve_table(engine: Engine, table: Any) -> Table:
    """
    Resolve a SQLModel class, Table instance, or table name into a Table.

    - SQLModel / SQLAlchemy declarative class: returns ``cls.__table__``
    - ``Table`` instance: returned unchanged
    - ``str``: reflected from the live database via ``MetaData.reflect``
    """
    if isinstance(table, Table):
        return table
    if hasattr(table, "__table__"):
        return table.__table__
    if isinstance(table, str):
        metadata = MetaData()
        try:
            metadata.reflect(bind=engine, only=[table])
        except SQLAlchemyError as exc:
            raise ValueError(f"Table {table!r} not found in database") from exc
        if table not in metadata.tables:
            raise ValueError(f"Table {table!r} not found in database")
        return metadata.tables[table]
    raise TypeError(
        f"Cannot resolve table from {type(table).__name__}; "
        "pass a SQLModel class, sqlalchemy.Table, or table name string."
    )
