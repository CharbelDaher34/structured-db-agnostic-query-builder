"""
Elasticsearch schema extraction.

Implements ISchemaExtractor for Elasticsearch.
"""

from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch

from query_builder.schema.type_mappings import TypeMapper


class ESSchemaExtractor:
    """
    Extracts schema information from Elasticsearch indices.
    
    Implements the ISchemaExtractor interface for Elasticsearch.
    """
    
    ES_TYPE_MAP = {
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
        "date": "datetime",
        "object": dict,
        "nested": list,
    }
    
    IGNORED_FIELD_TYPES = {"alias"}
    
    def __init__(
        self,
        es_host: str,
        index_name: str,
        category_fields: Optional[List[str]] = None,
        use_utils_schema: bool = True,
    ):
        """
        Initialize Elasticsearch schema extractor.
        
        Args:
            es_host: Elasticsearch host URL
            index_name: Name of the index to extract schema from
            category_fields: List of fields to treat as categories (enums)
            use_utils_schema: If True, use utils.get_es_schema_for_api
        """
        self.es_host = es_host
        self.index_name = index_name
        self.category_fields = category_fields or []
        self.use_utils_schema = use_utils_schema
        
        self.es_client = Elasticsearch(hosts=[es_host])
        
        self._schema_cache: Optional[Dict[str, Any]] = None
        self._mapping_cache: Optional[Dict[str, Any]] = None
        self._enum_cache: Optional[Dict[str, List[Any]]] = None
    
    def extract_schema(self) -> Dict[str, Any]:
        """
        Extract normalized schema from Elasticsearch.
        
        Returns:
            Dictionary mapping field paths to field information
        """
        if self._schema_cache is not None:
            return self._schema_cache
        
        # Try using utils function first for better enum support
        if self.use_utils_schema:
            try:
                from utils import get_es_schema_for_api
                
                schema_data = get_es_schema_for_api(
                    es_host=self.es_host,
                    index_name=self.index_name,
                    category_fields=self.category_fields,
                )
                
                self._mapping_cache = schema_data["elasticsearch_mapping"]
                self._enum_cache = schema_data["enum_fields"]
                
            except Exception as e:
                print(f"Warning: Could not use utils schema function: {e}")
                print("Falling back to direct ES mapping extraction...")
                self._extract_mapping_from_es()
        else:
            self._extract_mapping_from_es()
        
        # Convert ES mapping to normalized schema
        self._schema_cache = self._normalize_mapping(
            self._mapping_cache or {}, ""
        )
        
        return self._schema_cache
    
    def _extract_mapping_from_es(self):
        """Extract mapping directly from Elasticsearch."""
        mappings = self.es_client.indices.get_mapping(index=self.index_name)
        index_mapping = mappings.get(self.index_name, {}).get("mappings", {})
        self._mapping_cache = index_mapping.get("properties", {})
        self._enum_cache = {}
    
    def _normalize_mapping(
        self, mapping: Dict[str, Any], prefix: str
    ) -> Dict[str, Any]:
        """
        Normalize ES mapping to common schema format.
        
        Args:
            mapping: ES mapping properties
            prefix: Current field path prefix
            
        Returns:
            Normalized schema dictionary
        """
        schema = {}
        
        for field_name, field_props in mapping.items():
            full_path = f"{prefix}.{field_name}" if prefix else field_name
            
            if field_props.get("type") in self.IGNORED_FIELD_TYPES:
                continue
            
            es_type = field_props.get("type", "object")
            
            # Normalize type
            normalized_type = TypeMapper.normalize_type(es_type, "elasticsearch")
            
            field_info: Dict[str, Any] = {"type": normalized_type}
            
            # Handle nested objects
            if "properties" in field_props:
                nested_schema = self._normalize_mapping(
                    field_props["properties"], full_path
                )
                schema.update(nested_schema)
                
                # Also add the parent field
                if es_type == "nested":
                    field_info["type"] = "array"
                    field_info["item_type"] = "object"
                else:
                    field_info["type"] = "object"
            
            # Check for enum values
            if self._enum_cache and full_path in self._enum_cache:
                field_info["type"] = "enum"
                field_info["values"] = self._enum_cache[full_path]
            
            schema[full_path] = field_info
        
        return schema
    
    def get_distinct_values(
        self, field_path: str, size: int = 1000
    ) -> List[Any]:
        """
        Get distinct values for a field from Elasticsearch.
        
        Args:
            field_path: Path to the field
            size: Maximum number of distinct values to return
            
        Returns:
            List of distinct values
        """
        # Check cache first
        if self._enum_cache and field_path in self._enum_cache:
            return self._enum_cache[field_path]
        
        # Check without .keyword suffix
        base_field = field_path.replace(".keyword", "")
        if self._enum_cache and base_field in self._enum_cache:
            return self._enum_cache[base_field]
        
        # Fallback to ES aggregation
        try:
            mappings = self.es_client.indices.get_mapping(index=self.index_name)
            index_mapping = (
                mappings.get(self.index_name, {})
                .get("mappings", {})
                .get("properties", {})
            )
            
            # Check if field is nested
            field_parts = field_path.split(".")
            nested_path = None
            current_mapping = index_mapping
            current_path = []
            
            for part in field_parts:
                current_path.append(part)
                if part in current_mapping:
                    field_props = current_mapping[part]
                    if field_props.get("type") == "nested":
                        nested_path = ".".join(current_path)
                        break
                    elif "properties" in field_props:
                        current_mapping = field_props["properties"]
            
            # Build aggregation query
            if nested_path:
                query = {
                    "size": 0,
                    "aggs": {
                        "nested_agg": {
                            "nested": {"path": nested_path},
                            "aggs": {
                                "distinct_values": {
                                    "terms": {"field": field_path, "size": size}
                                }
                            },
                        }
                    },
                }
                response = self.es_client.search(index=self.index_name, **query)
                buckets = (
                    response.get("aggregations", {})
                    .get("nested_agg", {})
                    .get("distinct_values", {})
                    .get("buckets", [])
                )
            else:
                query = {
                    "size": 0,
                    "aggs": {
                        "distinct_values": {
                            "terms": {"field": field_path, "size": size}
                        }
                    },
                }
                response = self.es_client.search(index=self.index_name, **query)
                buckets = (
                    response.get("aggregations", {})
                    .get("distinct_values", {})
                    .get("buckets", [])
                )
            
            return [bucket["key"] for bucket in buckets]
        
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

