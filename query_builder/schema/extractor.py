"""
Schema extraction coordinator.

Coordinates schema extraction from database adapters and prepares it for model building.
"""

from typing import Any, Dict, List, Optional

from query_builder.core.interfaces import ISchemaExtractor


class SchemaExtractor:
    """
    Coordinates schema extraction and normalization.
    
    This class wraps a database-specific schema extractor and provides
    additional coordination logic like caching and category field handling.
    """
    
    def __init__(
        self,
        extractor: ISchemaExtractor,
        category_fields: Optional[List[str]] = None,
    ):
        """
        Initialize schema extractor.
        
        Args:
            extractor: Database-specific schema extractor implementation
            category_fields: List of field paths to treat as categories (enum types)
        """
        self.extractor = extractor
        self.category_fields = category_fields or []
        self._cached_schema: Optional[Dict[str, Any]] = None
        self._cached_enum_fields: Optional[Dict[str, List[Any]]] = None
    
    def get_schema(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Get normalized schema.
        
        Args:
            force_refresh: If True, bypass cache and re-extract schema
            
        Returns:
            Normalized schema dictionary
        """
        if self._cached_schema is None or force_refresh:
            self._cached_schema = self.extractor.extract_schema()
        return self._cached_schema
    
    def get_enum_fields(self, force_refresh: bool = False) -> Dict[str, List[Any]]:
        """
        Get enum values for category fields.
        
        Args:
            force_refresh: If True, bypass cache and re-fetch values
            
        Returns:
            Dictionary mapping field paths to list of distinct values
        """
        if self._cached_enum_fields is None or force_refresh:
            self._cached_enum_fields = {}
            
            for field_path in self.category_fields:
                try:
                    values = self.extractor.get_distinct_values(field_path)
                    if values:
                        self._cached_enum_fields[field_path] = values
                except Exception as e:
                    print(f"Warning: Could not get enum values for {field_path}: {e}")
        
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

