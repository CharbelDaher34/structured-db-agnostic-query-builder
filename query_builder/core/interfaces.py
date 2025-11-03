"""
Abstract interfaces for database adapters.

These protocols define the contract that all database adapters must implement
to work with the query builder system.
"""

from typing import Any, Dict, List, Protocol


class ISchemaExtractor(Protocol):
    """
    Extract schema information from a database.
    
    Implementations should provide normalized schema information that can be
    used to build Pydantic models and generate query filters.
    """
    
    def extract_schema(self) -> Dict[str, Any]:
        """
        Extract and return normalized schema.
        
        Returns:
            Dictionary mapping field paths to field information.
            Format: {
                "field.path": {
                    "type": "string|number|date|boolean|enum|array",
                    "values": [...],  # For enum types
                    "item_type": "...",  # For array types
                    "is_array_item": bool,  # If part of array structure
                }
            }
        """
        ...
    
    def get_distinct_values(self, field_path: str, size: int = 1000) -> List[Any]:
        """
        Get distinct values for a field (for enum/category fields).
        
        Args:
            field_path: Path to the field (e.g., "user.status")
            size: Maximum number of distinct values to return
            
        Returns:
            List of distinct values found in the field
        """
        ...
    
    def get_field_type(self, field_path: str) -> str:
        """
        Get normalized type for a field.
        
        Args:
            field_path: Path to the field
            
        Returns:
            Normalized type string (string, number, date, boolean, enum, array, object)
        """
        ...


class IQueryTranslator(Protocol):
    """
    Translate normalized filters to database-specific queries.
    
    Takes the structured filter output from the LLM and converts it to
    database-specific query format (e.g., Elasticsearch DSL, MongoDB aggregation).
    """
    
    def translate(
        self, 
        filters: Dict[str, Any], 
        model_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Convert normalized filters to database-specific queries.
        
        Args:
            filters: Structured filters from LLM (QueryFilters format)
            model_info: Field information for validation and query building
            
        Returns:
            List of database-specific query objects (one per filter slice)
        """
        ...


class IQueryExecutor(Protocol):
    """
    Execute database queries and return normalized results.
    
    Handles the actual execution of queries against the database and
    normalizes the results into a consistent format.
    """
    
    def execute(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute multiple queries and return results.
        
        Args:
            queries: List of database-specific query objects
            
        Returns:
            List of result dictionaries with format:
            {
                "total_hits": int,
                "documents": [...],
                "aggregations": {...},  # Optional
                "error": str,  # Optional, if execution failed
            }
        """
        ...
    
    def execute_raw(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        """
        Execute a raw database query.
        
        Args:
            query: Raw database query object
            size: Number of results to return
            
        Returns:
            Result dictionary with same format as execute()
        """
        ...

