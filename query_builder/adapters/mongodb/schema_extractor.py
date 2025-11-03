"""
MongoDB schema extraction.

Implements ISchemaExtractor for MongoDB by sampling documents.
"""

from typing import Any, Dict, List, Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

from query_builder.schema.type_mappings import TypeMapper


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
        category_fields: Optional[List[str]] = None,
        sample_size: int = 1000,
    ):
        """
        Initialize MongoDB schema extractor.
        
        Args:
            mongo_uri: MongoDB connection URI
            database_name: Name of the database
            collection_name: Name of the collection
            category_fields: List of fields to treat as categories (enums)
            sample_size: Number of documents to sample for schema inference
        """
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.category_fields = category_fields or []
        self.sample_size = sample_size
        
        self.client: MongoClient = MongoClient(mongo_uri)
        self.db: Database = self.client[database_name]
        self.collection: Collection = self.db[collection_name]
        
        self._schema_cache: Optional[Dict[str, Any]] = None
        self._enum_cache: Optional[Dict[str, List[Any]]] = None
    
    def extract_schema(self) -> Dict[str, Any]:
        """
        Extract normalized schema from MongoDB by sampling documents.
        
        Returns:
            Dictionary mapping field paths to field information
        """
        if self._schema_cache is not None:
            return self._schema_cache
        
        # Sample documents to infer schema
        sample_docs = list(self.collection.find().limit(self.sample_size))
        
        if not sample_docs:
            return {}
        
        # Infer schema from sampled documents
        schema = self._infer_schema(sample_docs)
        
        self._schema_cache = schema
        return schema
    
    def _infer_schema(
        self, documents: List[Dict[str, Any]], prefix: str = ""
    ) -> Dict[str, Any]:
        """
        Infer schema from a list of documents.
        
        Args:
            documents: List of MongoDB documents
            prefix: Current field path prefix for nested objects
            
        Returns:
            Normalized schema dictionary
        """
        schema: Dict[str, Any] = {}
        field_types: Dict[str, set] = {}
        
        # Collect all fields and their types from all documents
        for doc in documents:
            self._collect_field_types(doc, field_types, prefix)
        
        # Normalize field types
        for field_path, types in field_types.items():
            # Determine the most common type
            normalized_type = self._normalize_field_types(types)
            
            field_info: Dict[str, Any] = {"type": normalized_type}
            
            # Check if this is a category field
            if field_path in self.category_fields:
                field_info["type"] = "enum"
                # Values will be fetched on demand
            
            schema[field_path] = field_info
        
        return schema
    
    def _collect_field_types(
        self,
        obj: Any,
        field_types: Dict[str, set],
        prefix: str = "",
    ):
        """Recursively collect field types from a document."""
        if not isinstance(obj, dict):
            return
        
        for key, value in obj.items():
            # Skip MongoDB internal fields (including _id)
            if key.startswith("_"):
                continue
            
            full_path = f"{prefix}.{key}" if prefix else key
            
            if full_path not in field_types:
                field_types[full_path] = set()
            
            value_type = type(value).__name__
            
            if isinstance(value, dict):
                field_types[full_path].add("object")
                # Recursively process nested objects
                self._collect_field_types(value, field_types, full_path)
            elif isinstance(value, list):
                field_types[full_path].add("array")
                # Check array item types
                if value and len(value) > 0:
                    item_type = type(value[0]).__name__
                    field_types[full_path].add(f"array<{item_type}>")
                    # If array of objects, process nested structure
                    if isinstance(value[0], dict):
                        for item in value[:10]:  # Sample first 10 items
                            if isinstance(item, dict):
                                self._collect_field_types(item, field_types, full_path)
            else:
                # Check if string looks like a date (ISO format)
                if isinstance(value, str) and self._is_date_string(value):
                    field_types[full_path].add("date_string")
                else:
                    field_types[full_path].add(value_type)
    
    @staticmethod
    def _is_date_string(value: str) -> bool:
        """Check if string looks like a date (ISO format)."""
        if not isinstance(value, str):
            return False
        
        # Check ISO date format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS[Z]
        import re
        iso_date_pattern = r'^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?Z?)?$'
        return bool(re.match(iso_date_pattern, value))
        
    def _normalize_field_types(self, types: set) -> str:
        """
        Normalize a set of Python types to a common type string.
        
        Args:
            types: Set of Python type names
            
        Returns:
            Normalized type string
        """
        # Remove None
        types = {t for t in types if t != "NoneType"}
        
        if not types:
            return "unknown"
        
        # Check for date strings first (ISO format timestamps)
        if "date_string" in types:
            return "date"
        
        # Check for array
        if "array" in types or any(t.startswith("array<") for t in types):
            return "array"
        
        # Check for object
        if "dict" in types or "object" in types:
            return "object"
        
        # Map Python types to normalized types
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
        
        # Default
        return "string"
    
    def get_distinct_values(
        self, field_path: str, size: int = 1000
    ) -> List[Any]:
        """
        Get distinct values for a field from MongoDB.
        
        Args:
            field_path: Path to the field
            size: Maximum number of distinct values to return
            
        Returns:
            List of distinct values
        """
        # Check cache first
        if self._enum_cache and field_path in self._enum_cache:
            return self._enum_cache[field_path]
        
        try:
            # Use MongoDB distinct() for efficiency
            distinct_values = self.collection.distinct(field_path)
            
            # Limit size
            if len(distinct_values) > size:
                distinct_values = distinct_values[:size]
            
            # Cache the result
            if self._enum_cache is None:
                self._enum_cache = {}
            self._enum_cache[field_path] = distinct_values
            
            return distinct_values
        
        except Exception as e:
            print(f"Error getting distinct values for '{field_path}': {e}")
            return []
    
    def get_field_type(self, field_path: str) -> str:
        """
        Get normalized type for a field.
        
        Args:
            field_path: Path to the field
            
        Returns:
            Normalized type string
        """
        schema = self.extract_schema()
        return schema.get(field_path, {}).get("type", "unknown")

