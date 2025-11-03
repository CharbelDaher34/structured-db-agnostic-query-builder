"""
Type mapping utilities for converting database types to Python types.
"""

from typing import Any, Dict, Type
from datetime import datetime, date


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
        "object": Dict[str, Any],
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
        "object": Dict[str, Any],
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
        "object": Dict[str, Any],
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
        "json": Dict[str, Any],
        "jsonb": Dict[str, Any],
        "array": list,
    }
    
    @classmethod
    def get_python_type(cls, db_type: str, source_db: str = "common") -> Type:
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
        elif python_type is dict or python_type == Dict[str, Any]:
            return "object"
        elif python_type is list:
            return "array"
        else:
            return "unknown"
    
    @classmethod
    def _get_type_map(cls, source_db: str) -> Dict[str, Type]:
        """Get the appropriate type map for a database."""
        if source_db == "elasticsearch":
            return cls.ELASTICSEARCH_TYPE_MAP
        elif source_db == "mongodb":
            return cls.MONGODB_TYPE_MAP
        elif source_db == "postgresql":
            return cls.POSTGRESQL_TYPE_MAP
        else:
            return cls.COMMON_TYPE_MAP

