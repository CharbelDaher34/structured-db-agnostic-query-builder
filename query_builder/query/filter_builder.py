"""
Build Pydantic filter models for LLM query parsing.

Creates structured filter models based on schema information.
"""

from typing import Any, Dict, List, Optional, Union, Annotated, Literal
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
        
        class HavingOperatorEnum(str, Enum):
            """Operators for having clause in aggregations."""
            lt = "<"
            gt = ">"
            eq = "is"
            ne = "different"
        
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
            having_operator: Optional[HavingOperatorEnum] = Field(
                default=None,
                description="Operator for post-aggregation filtering (e.g., '>')",
            )
            having_value: Optional[Union[str, int, float]] = Field(
                default=None,
                description="Value for post-aggregation filtering (e.g., 1)",
            )
        
        class StringOperatorEnum(str, Enum):
            """Operators for string fields."""
            eq = "is"
            ne = "different"
            contains = "contains"
            isin = "isin"
            notin = "notin"
            exists = "exists"

        class NumberOperatorEnum(str, Enum):
            """Operators for number fields."""
            lt = "<"
            gt = ">"
            eq = "is"
            ne = "different"
            be = "between"
            isin = "isin"
            notin = "notin"
            exists = "exists"

        class DateOperatorEnum(str, Enum):
            """Operators for date fields."""
            lt = "<"
            gt = ">"
            eq = "is"
            ne = "different"
            be = "between"
            exists = "exists"

        class BooleanOperatorEnum(str, Enum):
            """Operators for boolean fields."""
            eq = "is"
            ne = "different"
            exists = "exists"

        class EnumOperatorEnum(str, Enum):
            """Operators for enum fields."""
            eq = "is"
            ne = "different"
            isin = "isin"
            notin = "notin"
            exists = "exists"

        class StringFilter(BaseModel):
            """Filter for string fields."""
            type: Literal["StringFilter"] = "StringFilter"
            field: FieldEnum
            operator: StringOperatorEnum
            value: Union[str, List[str], bool, None]

            @model_validator(mode="after")
            def validate_filter_type(self):
                """Ensure the filter type matches the field's schema type."""
                field = self.field.value
                field_info = _model_info.get(field, {})
                ftype = field_info.get("type", "unknown")
                
                if ftype != "string":
                    raise ValueError(
                        f"StringFilter used for non-string field '{field}' ({ftype}). "
                        f"Use the appropriate filter type for {ftype} fields."
                    )
                return self

        class NumberFilter(BaseModel):
            """Filter for number fields."""
            type: Literal["NumberFilter"] = "NumberFilter"
            field: FieldEnum
            operator: NumberOperatorEnum
            value: Union[float, int, List[Union[float, int]], bool, None]

            @model_validator(mode="after")
            def validate_filter_type(self):
                """Ensure the filter type matches the field's schema type."""
                field = self.field.value
                field_info = _model_info.get(field, {})
                ftype = field_info.get("type", "unknown")
                
                if ftype != "number":
                    raise ValueError(
                        f"NumberFilter used for non-number field '{field}' ({ftype}). "
                        f"Use the appropriate filter type for {ftype} fields."
                    )
                return self

        class DateFilter(BaseModel):
            """Filter for date fields."""
            type: Literal["DateFilter"] = "DateFilter"
            field: FieldEnum
            operator: DateOperatorEnum
            value: Union[date, List[date], bool, None]

            @model_validator(mode="after")
            def validate_filter_type(self):
                """Ensure the filter type matches the field's schema type."""
                field = self.field.value
                field_info = _model_info.get(field, {})
                ftype = field_info.get("type", "unknown")
                
                if ftype != "date":
                    raise ValueError(
                        f"DateFilter used for non-date field '{field}' ({ftype}). "
                        f"Use the appropriate filter type for {ftype} fields."
                    )
                return self

        class BooleanFilter(BaseModel):
            """Filter for boolean fields."""
            type: Literal["BooleanFilter"] = "BooleanFilter"
            field: FieldEnum
            operator: BooleanOperatorEnum
            value: Union[bool, None]

            @model_validator(mode="after")
            def validate_filter_type(self):
                """Ensure the filter type matches the field's schema type."""
                field = self.field.value
                field_info = _model_info.get(field, {})
                ftype = field_info.get("type", "unknown")
                
                if ftype != "boolean":
                    raise ValueError(
                        f"BooleanFilter used for non-boolean field '{field}' ({ftype}). "
                        f"Use the appropriate filter type for {ftype} fields."
                    )
                return self

        class EnumFilter(BaseModel):
            """Filter for enum fields."""
            type: Literal["EnumFilter"] = "EnumFilter"
            field: FieldEnum
            operator: EnumOperatorEnum
            value: Union[str, List[str], bool, None]

            @model_validator(mode="after")
            def validate_filter_type(self):
                """Ensure the filter type matches the field's schema type."""
                field = self.field.value
                field_info = _model_info.get(field, {})
                ftype = field_info.get("type", "unknown")
                
                if ftype != "enum":
                    raise ValueError(
                        f"EnumFilter used for non-enum field '{field}' ({ftype}). "
                        f"Use the appropriate filter type for {ftype} fields."
                    )
                
                # Validate enum values
                valid_values = field_info.get("values", [])
                if valid_values and self.value is not None and self.operator.value not in ("exists",):
                    if isinstance(self.value, list):
                        invalid = [v for v in self.value if v not in valid_values]
                        if invalid:
                            raise ValueError(
                                f"Invalid enum values for field '{field}': {invalid}. "
                                f"Valid values: {valid_values}"
                            )
                    elif not isinstance(self.value, bool) and self.value not in valid_values:
                        raise ValueError(
                            f"Invalid enum value for field '{field}': '{self.value}'. "
                            f"Valid values: {valid_values}"
                        )
                
                return self

        # Union of all filter types
        FilterType = Annotated[
            Union[StringFilter, NumberFilter, DateFilter, BooleanFilter, EnumFilter],
            Field(discriminator="type")
        ]

        class QuerySlice(BaseModel):
            """Single query slice with conditions and options."""
            conditions: List[FilterType] = Field(
                description="AND-joined filter conditions (use appropriate filter type for each field)"
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
                if self.interval and self.group_by is not None:
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

