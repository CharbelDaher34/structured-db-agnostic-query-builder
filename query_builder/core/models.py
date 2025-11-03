"""
Shared data models for the query builder system.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class SchemaField(BaseModel):
    """Represents a single field in the schema."""
    
    name: str
    type: str  # string, number, date, boolean, enum, array, object
    values: Optional[List[Any]] = None  # For enum types
    item_type: Optional[str] = None  # For array types
    is_array_item: bool = False
    properties: Optional[Dict[str, "SchemaField"]] = None  # For nested objects


class NormalizedSchema(BaseModel):
    """Normalized schema representation across databases."""
    
    fields: Dict[str, Dict[str, Any]]
    source_type: str  # elasticsearch, mongodb, postgresql, etc.
    metadata: Dict[str, Any] = Field(default_factory=dict)


class QueryResult(BaseModel):
    """Standardized query result format."""
    
    total_hits: int = 0
    documents: List[Dict[str, Any]] = Field(default_factory=list)
    aggregations: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    success: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class LLMConfig(BaseModel):
    """Configuration for LLM client."""
    
    model: str
    api_key: str
    temperature: float = 0.0
    max_tokens: Optional[int] = None

