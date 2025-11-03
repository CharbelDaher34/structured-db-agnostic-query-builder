"""
Build Pydantic filter models for LLM query parsing.

Creates structured filter models based on schema information.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import date
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator, ValidationInfo


class FilterModelBuilder:
    """
    Builds Pydantic filter models for LLM processing.
    
    Creates a structured model that the LLM uses to output query filters
    based on the available fields in the schema.
    """
    
    def __init__(self, model_info: Dict[str, Any]):
        """
        Initialize filter model builder.
        
        Args:
            model_info: Flattened field information from ModelBuilder
        """
        self.model_info = model_info
        self._filter_model_class: Optional[type[BaseModel]] = None
    
    def build_filter_model(self) -> type[BaseModel]:
        """
        Build Pydantic model for query filters.
        
        Returns:
            QueryFilters model class for LLM structured output
        """
        if self._filter_model_class is not None:
            return self._filter_model_class
        
        class OperatorEnum(str, Enum):
            """Supported query operators."""
            lt = "<"
            gt = ">"
            isin = "isin"
            notin = "notin"
            eq = "is"
            ne = "different"
            be = "between"
            contains = "contains"
            exists = "exists"
        
        class SortOrderEnum(str, Enum):
            """Sort order options."""
            asc = "asc"
            desc = "desc"
        
        class AggregationEnum(str, Enum):
            """Aggregation types."""
            SUM = "sum"
            AVG = "avg"
            COUNT = "count"
            MIN = "min"
            MAX = "max"
        
        class TimeIntervalEnum(str, Enum):
            """Time interval for date grouping."""
            DAY = "day"
            WEEK = "week"
            MONTH = "month"
            YEAR = "year"
        
        _model_info = self.model_info
        field_enum_members = {k: k for k in self.model_info.keys()}
        FieldEnum = Enum("FieldEnum", field_enum_members)
        
        class SortField(BaseModel):
            """Sort field specification."""
            field: FieldEnum
            order: SortOrderEnum = SortOrderEnum.asc
        
        class Aggregation(BaseModel):
            """Aggregation specification with optional having clause."""
            field: FieldEnum
            type: AggregationEnum
            having_operator: Optional[OperatorEnum] = Field(
                default=None,
                description="Operator for post-aggregation filtering (e.g., '>')",
            )
            having_value: Optional[Union[str, int, float]] = Field(
                default=None,
                description="Value for post-aggregation filtering (e.g., 1)",
            )
        
        class Query(BaseModel):
            """Single filter condition."""
            field: FieldEnum
            operator: OperatorEnum
            value: Union[
                str,
                float,
                int,
                bool,
                date,
                List[Union[str, float, int, date]],
                None,
            ]
            
            @field_validator("value")
            def validate_value(cls, v, info: ValidationInfo):
                """Validate value against field type and operator."""
                if "field" not in info.data or "operator" not in info.data:
                    return v
                
                field = info.data["field"].value
                op = info.data["operator"].value
                field_info = _model_info.get(field, {})
                ftype = field_info.get("type", "unknown")
                
                def fail(msg: str):
                    raise ValueError(f"Invalid value for '{field}' ({ftype}): {msg}")
                
                # Validate based on operator
                if op in ("<", ">", "between"):
                    if ftype not in ("number", "date"):
                        fail(f"Operator '{op}' only for number/date")
                    if op == "between" and (not isinstance(v, list) or len(v) != 2):
                        fail("Expected list of 2 values")
                elif op in ("isin", "notin"):
                    if not isinstance(v, list):
                        fail("Expected list")
                    if ftype == "enum" and not all(
                        x in field_info.get("values", []) for x in v
                    ):
                        fail(f"Values must be in enum: {field_info['values']}")
                elif op == "contains":
                    if ftype != "string" or not isinstance(v, str):
                        fail("Expected string for contains")
                elif op == "exists":
                    if not isinstance(v, bool):
                        fail("Expected bool (True=exists, False=not exists)")
                
                return v
        
        class QuerySlice(BaseModel):
            """Single query slice with conditions and options."""
            conditions: List[Query] = Field(
                description="AND-joined filter conditions"
            )
            sort: Optional[List[SortField]] = None
            limit: Optional[int] = None
            group_by: Optional[List[FieldEnum]] = None
            aggregations: Optional[List[Aggregation]] = None
            interval: Optional[TimeIntervalEnum] = Field(
                default=TimeIntervalEnum.MONTH,
                description="Time interval for date grouping",
            )
            
            @model_validator(mode="after")
            def validate_slice(self) -> "QuerySlice":
                """Validate and correct query slice parameters."""
                # Remove null field conditions
                for query in self.conditions:
                    if query.field.value == "null":
                        self.conditions.remove(query)
                
                # Remove aggregations/interval if no group_by
                if not self.group_by:
                    if self.aggregations:
                        self.aggregations = None
                    if self.interval:
                        self.interval = None
                
                # Remove interval if not grouping by date field
                if self.interval and self.group_by:
                    is_date_grouped = any(
                        _model_info.get(f.value, {}).get("type") == "date"
                        for f in self.group_by
                    )
                    if not is_date_grouped:
                        self.interval = None
                
                return self
        
        class QueryFilters(BaseModel):
            """Top-level query filters container."""
            filters: List[QuerySlice] = Field(
                description="List of query slices (for comparisons)"
            )
        
        self._filter_model_class = QueryFilters
        return self._filter_model_class

