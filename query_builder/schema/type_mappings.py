"""
Type mapping utilities for converting database types to Python types.
"""

from datetime import date, datetime
from typing import Any


class TypeMapper:
    """Maps database-specific types to Python types."""

    # Common type mappings across databases
    COMMON_TYPE_MAP = {
        "string": str,
        "number": float,
        "integer": int,
        "boolean": bool,
        "date": datetime,
        "datetime": datetime,
        "timestamp": datetime,
        "object": dict[str, Any],
        "array": list,
    }

    # Elasticsearch-specific type mappings
    ELASTICSEARCH_TYPE_MAP = {
        "text": str,
        "keyword": str,
        "integer": int,
        "long": int,
        "short": int,
        "byte": int,
        "double": float,
        "float": float,
        "half_float": float,
        "scaled_float": float,
        "boolean": bool,
        "date": datetime,
        "object": dict[str, Any],
        "nested": list,
    }

    # MongoDB-specific type mappings
    MONGODB_TYPE_MAP = {
        "string": str,
        "int": int,
        "long": int,
        "double": float,
        "decimal": float,
        "bool": bool,
        "date": datetime,
        "timestamp": datetime,
        "object": dict[str, Any],
        "array": list,
    }

    # PostgreSQL-specific type mappings
    POSTGRESQL_TYPE_MAP = {
        "varchar": str,
        "text": str,
        "char": str,
        "integer": int,
        "bigint": int,
        "smallint": int,
        "decimal": float,
        "numeric": float,
        "real": float,
        "double precision": float,
        "boolean": bool,
        "date": date,
        "timestamp": datetime,
        "timestamptz": datetime,
        "json": dict[str, Any],
        "jsonb": dict[str, Any],
        "array": list,
    }

    # Python runtime type names → normalized type strings.
    # Used by schema extractors that infer types from sampled Python values
    # (e.g. MongoSchemaExtractor sampling documents with type(value).__name__).
    PYTHON_TYPE_MAP: dict[str, str] = {
        "str": "string",
        "int": "number",
        "float": "number",
        "bool": "boolean",
        "datetime": "date",
        "date": "date",
        "dict": "object",
        "list": "array",
        "ObjectId": "string",
    }

    @classmethod
    def normalize_python_type(cls, type_name: str) -> str:
        """
        Normalize a Python runtime type name to a common type string.

        Args:
            type_name: Python type name (e.g. from ``type(value).__name__``)

        Returns:
            Normalized type string (string, number, date, boolean, object, array, unknown)
        """
        return cls.PYTHON_TYPE_MAP.get(type_name, "unknown")

    @classmethod
    def get_python_type(cls, db_type: str, source_db: str = "common") -> type:
        """
        Get Python type for a database type.

        Args:
            db_type: Database-specific type string
            source_db: Source database (elasticsearch, mongodb, postgresql, common)

        Returns:
            Python type class
        """
        type_map = cls._get_type_map(source_db)
        return type_map.get(db_type.lower(), Any)

    @classmethod
    def normalize_type(cls, db_type: str, source_db: str = "common") -> str:
        """
        Normalize database type to common type name.

        Args:
            db_type: Database-specific type string
            source_db: Source database

        Returns:
            Normalized type string (string, number, date, boolean, object, array)
        """
        python_type = cls.get_python_type(db_type, source_db)

        if python_type is str:
            return "string"
        elif python_type in (int, float):
            return "number"
        elif python_type is bool:
            return "boolean"
        elif python_type in (date, datetime):
            return "date"
        elif python_type is dict or python_type == dict[str, Any]:
            return "object"
        elif python_type is list:
            return "array"
        else:
            return "unknown"

    @classmethod
    def _get_type_map(cls, source_db: str) -> dict[str, Any]:
        """Get the appropriate type map for a database."""
        by_source = {
            "elasticsearch": cls.ELASTICSEARCH_TYPE_MAP,
            "mongodb": cls.MONGODB_TYPE_MAP,
            "postgresql": cls.POSTGRESQL_TYPE_MAP,
        }
        return by_source.get(source_db, cls.COMMON_TYPE_MAP)
