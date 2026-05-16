"""
MongoDB schema extraction.

Implements ISchemaExtractor for MongoDB by sampling documents.
"""

import logging
import re
from typing import Any, Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

logger = logging.getLogger(__name__)

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?Z?)?$")


class MongoSchemaExtractor:
    """
    Extracts schema information from MongoDB collections.

    Implements the ISchemaExtractor interface for MongoDB.
    Since MongoDB is schemaless, we infer schema by sampling documents.
    """

    def __init__(
        self,
        mongo_uri: str,
        database_name: str,
        collection_name: str,
        category_fields: Optional[list[str]] = None,
        sample_size: int = 1000,
        client: Optional[MongoClient] = None,
    ):
        """
        Initialize MongoDB schema extractor.

        Args:
            mongo_uri: MongoDB connection URI
            database_name: Name of the database
            collection_name: Name of the collection
            category_fields: List of fields to treat as categories (enums)
            sample_size: Number of documents to randomly sample for schema inference
            client: Optional shared MongoClient. If provided, this extractor will not own
                the client (it won't be closed on cleanup). Pass one in to share a single
                connection pool with the executor.
        """
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.category_fields = category_fields or []
        self.sample_size = sample_size

        self._owns_client = client is None
        self.client: MongoClient = client or MongoClient(mongo_uri)
        self.db: Database = self.client[database_name]
        self.collection: Collection = self.db[collection_name]

        self._schema_cache: Optional[dict[str, Any]] = None
        self._enum_cache: Optional[dict[str, list[Any]]] = None

    def close(self) -> None:
        """Close the MongoDB client if owned by this extractor."""
        if self._owns_client:
            try:
                self.client.close()
            except Exception:
                logger.warning("Failed to close MongoClient", exc_info=True)

    def extract_schema(self) -> dict[str, Any]:
        """
        Extract normalized schema from MongoDB by randomly sampling documents.

        Uses $sample so the inferred schema represents the whole collection rather than
        the first N inserted documents.
        """
        if self._schema_cache is not None:
            return self._schema_cache

        try:
            sample_docs = list(
                self.collection.aggregate(
                    [{"$sample": {"size": self.sample_size}}],
                    allowDiskUse=True,
                )
            )
        except Exception:
            logger.warning("Random sampling failed, falling back to sequential read", exc_info=True)
            sample_docs = list(self.collection.find().limit(self.sample_size))

        if not sample_docs:
            self._schema_cache = {}
            return self._schema_cache

        schema = self._infer_schema(sample_docs)
        self._schema_cache = schema
        return schema

    def invalidate_cache(self) -> None:
        """Drop cached schema and enum values so the next call re-extracts."""
        self._schema_cache = None
        self._enum_cache = None

    def _infer_schema(self, documents: list[dict[str, Any]], prefix: str = "") -> dict[str, Any]:
        """Infer schema from a list of documents."""
        schema: dict[str, Any] = {}
        field_types: dict[str, set] = {}

        for doc in documents:
            self._collect_field_types(doc, field_types, prefix)

        for field_path, types in field_types.items():
            normalized_type = self._normalize_field_types(types)
            field_info: dict[str, Any] = {"type": normalized_type}

            if field_path in self.category_fields:
                field_info["type"] = "enum"

            schema[field_path] = field_info

        return schema

    def _collect_field_types(
        self,
        obj: Any,
        field_types: dict[str, set],
        prefix: str = "",
    ):
        """Recursively collect field types from a document."""
        if not isinstance(obj, dict):
            return

        for key, value in obj.items():
            if key.startswith("_"):
                continue

            full_path = f"{prefix}.{key}" if prefix else key
            field_types.setdefault(full_path, set())

            if isinstance(value, dict):
                field_types[full_path].add("object")
                self._collect_field_types(value, field_types, full_path)
            elif isinstance(value, list):
                field_types[full_path].add("array")
                # Sample the first 10 items so the inferred item type isn't decided by a
                # single (possibly null) leading element.
                for item in value[:10]:
                    item_type = type(item).__name__
                    field_types[full_path].add(f"array<{item_type}>")
                    if isinstance(item, dict):
                        self._collect_field_types(item, field_types, full_path)
            else:
                if isinstance(value, str) and self._is_date_string(value):
                    field_types[full_path].add("date_string")
                else:
                    field_types[full_path].add(type(value).__name__)

    @staticmethod
    def _is_date_string(value: str) -> bool:
        """Check if string looks like an ISO date."""
        return bool(_ISO_DATE_RE.match(value))

    def _normalize_field_types(self, types: set) -> str:
        """Normalize a set of Python types to a common type string."""
        types = {t for t in types if t != "NoneType"}

        if not types:
            return "unknown"

        if "date_string" in types:
            return "date"

        if "array" in types or any(t.startswith("array<") for t in types):
            return "array"

        if "dict" in types or "object" in types:
            return "object"

        type_map = {
            "str": "string",
            "int": "number",
            "float": "number",
            "bool": "boolean",
            "datetime": "date",
            "date": "date",
            "ObjectId": "string",
        }

        for t in types:
            if t in type_map:
                return type_map[t]

        return "string"

    def get_distinct_values(self, field_path: str, size: int = 1000) -> list[Any]:
        """
        Get distinct values for a field from MongoDB.

        Uses an aggregation pipeline with `$limit` so we don't pull the full distinct set
        into memory for high-cardinality fields.
        """
        if self._enum_cache and field_path in self._enum_cache:
            return self._enum_cache[field_path]

        try:
            pipeline = [
                {"$match": {field_path: {"$ne": None, "$exists": True}}},
                {"$group": {"_id": f"${field_path}"}},
                {"$limit": size},
            ]
            cursor = self.collection.aggregate(pipeline, allowDiskUse=True)
            distinct_values = [doc["_id"] for doc in cursor if doc["_id"] is not None]

            if self._enum_cache is None:
                self._enum_cache = {}
            self._enum_cache[field_path] = distinct_values

            return distinct_values

        except Exception:
            logger.warning("Error getting distinct values for %r", field_path, exc_info=True)
            return []

    def get_field_type(self, field_path: str) -> str:
        """Get normalized type for a field."""
        schema = self.extract_schema()
        return schema.get(field_path, {}).get("type", "unknown")
