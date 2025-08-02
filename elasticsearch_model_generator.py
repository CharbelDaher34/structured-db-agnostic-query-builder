import json
import sys
import time
from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, List, Optional, Union, get_args, get_origin
import inspect
from pathlib import Path

from llm.llm_agent import LLM
from elasticsearch import Elasticsearch
from pydantic import BaseModel, Field, create_model, field_validator, model_validator, ValidationInfo


import time
import asyncio


class ModelBuilder:
    """Builds Pydantic models from Elasticsearch mappings, with or without a database connection."""
    
    ES_TYPE_MAP = {
        "text": str, "keyword": str, "integer": int, "long": int, "short": int, 
        "byte": int, "double": float, "float": float, "half_float": float, 
        "scaled_float": float, "boolean": bool, "date": datetime, 
        "object": Dict[str, Any], "nested": List[Any],
    }
    IGNORED_FIELD_TYPES = {"alias"}

    def __init__(self, 
                 es_client: Optional[Elasticsearch] = None, 
                 index_name: Optional[str] = None, 
                 category_fields: Optional[List[str]] = None, 
                 fields_to_ignore: Optional[List[str]] = None,
                 mapping: Optional[Dict[str, Any]] = None,
                 enum_fields: Optional[Dict[str, List[Any]]] = None,
                 es_host: Optional[str] = None):

        if es_client and index_name:
            self.mode = "es"
        elif mapping is not None:
            self.mode = "mapping"
        else:
            raise ValueError("Must provide either (es_client, index_name) for DB mode or a `mapping` for mapping-only mode.")

        self.es_client = es_client
        self.index_name = index_name
        self.category_fields = category_fields or []
        self.fields_to_ignore = fields_to_ignore or []
        self.provided_mapping = mapping
        self.provided_enum_fields = enum_fields or {}
        self.es_host = es_host
        
        self._model_class: Optional[type[BaseModel]] = None
        self._model_info = None
        self._mapping_cache = None
        self._schema_data = None

    def _GetIndexMapping(self) -> Dict[str, Any]:
        """Get mapping from ES or from provided mapping."""
        if self._mapping_cache:
            return self._mapping_cache

        if self.mode == "es":
            
                        # Static type checkers (e.g. mypy/pyright) cannot infer that `es_client` and
            # `index_name` are non-null when `self.mode == "es"`. An explicit assertion
            # makes this guarantee clear and eliminates the "attribute of None" warning.
            # assert self.es_client is not None and self.index_name is not None, "Elasticsearch client or index name is missing"
            # mappings = self.es_client.indices.get_mapping(index=self.index_name)
            # index_mapping = mappings.get(self.index_name, {}).get("mappings", {})
            # self._mapping_cache = index_mapping.get("properties", {})
            # Use the utility function to get schema data
            if not self._schema_data:
                from utils import get_es_schema_for_api
                assert self.es_host is not None and self.index_name is not None, "ES host or index name is missing"
                self._schema_data = get_es_schema_for_api(
                    es_host=self.es_host,
                    index_name=self.index_name,
                    category_fields=self.category_fields
                )
            
            self._mapping_cache = self._schema_data["elasticsearch_mapping"]
            # Update provided_enum_fields with the fetched enum values
            self.provided_enum_fields.update(self._schema_data["enum_fields"])
        else: # mapping mode
            self._mapping_cache = self.provided_mapping or {}
            
        return self._mapping_cache

    def GetDistinctValues(self, field_path: str, size: int = 1000) -> List[Any]:
        """Get distinct values for a field from Elasticsearch. Requires 'es' mode."""
        if self.mode != "es":
            raise RuntimeError("Cannot get distinct values without an Elasticsearch client.")
        
        # If we have schema data from utils function, try to get values from there first
        if self._schema_data and "enum_fields" in self._schema_data:
            # Try to find the field in enum_fields
            for field_name, values in self._schema_data["enum_fields"].items():
                if field_name == field_path or field_name == field_path.replace(".keyword", ""):
                    return values
        
        # Fallback to direct ES query if not found in schema data
        try:
            assert self.es_client is not None and self.index_name is not None, "Elasticsearch client or index name is missing"
            mappings = self.es_client.indices.get_mapping(index=self.index_name)
            index_mapping = (
                mappings.get(self.index_name, {})
                .get("mappings", {})
                .get("properties", {})
            )

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

            if nested_path:
                query = {"size": 0, "aggs": {"nested_agg": {"nested": {"path": nested_path}, "aggs": {"distinct_values": {"terms": {"field": field_path, "size": size}}}}}}
                response = self.es_client.search(index=self.index_name, **query)
                buckets = response.get("aggregations", {}).get("nested_agg", {}).get("distinct_values", {}).get("buckets", [])
            else:
                query = {"size": 0, "aggs": {"distinct_values": {"terms": {"field": field_path, "size": size}}}}
                response = self.es_client.search(index=self.index_name, **query)
                buckets = response.get("aggregations", {}).get("distinct_values", {}).get("buckets", [])

            return [bucket["key"] for bucket in buckets]

        except Exception as e:
            print(f"Error getting distinct values for field '{field_path}': {e}")
            return []

    def Build(self, model_name: Optional[str] = None) -> type[BaseModel]:
        if self._model_class is None:
            es_mapping = self._GetIndexMapping()
            model_name = model_name or (f"ES_{self.index_name.capitalize()}" if self.index_name else "CustomModel")
            self._model_class = self._BuildPydanticModel(es_mapping, model_name)
        return self._model_class

    def _BuildPydanticModel(self, es_mapping: Dict[str, Any], model_name: str, current_path: str = "") -> type[BaseModel]:
        fields: Dict[str, tuple] = {}
        for field_name, field_props in es_mapping.items():
            full_field_path = f"{current_path}.{field_name}" if current_path else field_name

            if (field_props.get("type") in self.IGNORED_FIELD_TYPES or field_name in self.fields_to_ignore):
                continue

            es_type = field_props.get("type")
            py_type = None

            if "properties" in field_props:
                nested_model_name = f"{model_name}_{field_name.capitalize()}"
                nested_model = self._BuildPydanticModel(field_props["properties"], nested_model_name, full_field_path)
                py_type = List[nested_model] if es_type == "nested" else nested_model
            else:
                enum_values = self._get_enum_values(full_field_path, field_name, es_type)
                if enum_values:
                    py_type = self._CreateEnumTypeFromValues(field_name, model_name, enum_values)
                else:
                    py_type = self.ES_TYPE_MAP.get(es_type, Any)
            
            fields[field_name] = self._GetFieldDefinition(py_type)
        
        # The **fields mapping is valid at runtime; we silence the type checker.
        return create_model(model_name, **fields)  # type: ignore[arg-type]

    def _get_enum_values(self, full_field_path: str, field_name: str, es_type: str) -> Optional[List[Any]]:
        """Get enum values from provided dictionary or from ES."""
        if self.mode == "mapping":
            return self.provided_enum_fields.get(full_field_path) or self.provided_enum_fields.get(field_name)
        
        if self.mode == "es" and (full_field_path in self.category_fields or field_name in self.category_fields):
            # First try to get from provided_enum_fields (populated by utils function)
            enum_values = self.provided_enum_fields.get(full_field_path) or self.provided_enum_fields.get(field_name)
            if enum_values:
                return enum_values
            
            # Fallback to direct ES query
            field_path_for_es = f"{full_field_path}.keyword" if es_type == "text" else full_field_path
            return self.GetDistinctValues(field_path_for_es)
        
        return None

    def _CreateEnumTypeFromValues(self, field_name: str, model_name: str, values: List[Any]):
        enum_class_name = f"{model_name}_{field_name.capitalize()}Enum"
        enum_members = {}
        for i, value in enumerate(values):
            member_name = str(value)
            if isinstance(value, str):
                member_name = value.replace(" ", "_").replace("-", "_").replace("'", "").replace(".", "_")
                if not member_name or (not member_name[0].isalpha() and member_name[0] != "_"):
                    member_name = f"_{member_name}"
                member_name = "".join(c for c in member_name if c.isalnum() or c == "_") or f"VALUE_{i}"
            else:
                member_name = f"VALUE_{i}"
            enum_members[member_name.upper()] = value
        return Enum(enum_class_name, enum_members)

    def _GetFieldDefinition(self, py_type):
        if isinstance(py_type, type) and issubclass(py_type, Enum):
            return (Optional[py_type], Field(default=None))
        if isinstance(py_type, type) and issubclass(py_type, BaseModel):
            return (py_type, Field(...))
        if hasattr(py_type, "__origin__") and py_type.__origin__ is list:
            args = getattr(py_type, "__args__", ())
            if args and isinstance(args[0], type) and issubclass(args[0], BaseModel):
                return (py_type, Field(...))
            return (Optional[py_type], Field(default=None))
        return (Optional[py_type], Field(default=None))

    def GetModelInfo(self) -> Dict[str, Any]:
        if self._model_info is None:
            model = self.Build()
            self._model_info = self._ExtractModelInfo(model)
        return self._model_info

    def _ExtractModelInfo(self, model_class: type[BaseModel], prefix: str = "") -> Dict[str, Any]:
        info = {}
        for field_name, field_info in model_class.model_fields.items():
            field_type = field_info.annotation
            origin, args = get_origin(field_type), get_args(field_type)
            full_field_name = f"{prefix}.{field_name}" if prefix else field_name

            if origin is Union:
                non_none_types = [arg for arg in args if arg is not type(None)]
                if non_none_types:
                    field_type, origin, args = non_none_types[0], get_origin(non_none_types[0]), get_args(non_none_types[0])
            
            if inspect.isclass(field_type) and issubclass(field_type, Enum):
                info[full_field_name] = {"type": "enum", "values": [e.value for e in field_type]}
            elif inspect.isclass(field_type) and issubclass(field_type, BaseModel):
                info.update(self._ExtractModelInfo(field_type, full_field_name))
            elif origin is list or origin is List:
                list_info = self._GetListFieldInfo(args, full_field_name)
                if isinstance(list_info, dict) and any("is_array_item" in v for v in list_info.values()):
                    info.update(list_info)
                else:
                    info[full_field_name] = list_info
            elif field_type is str: info[full_field_name] = {"type": "string"}
            elif field_type in (int, float): info[full_field_name] = {"type": "number"}
            elif field_type is bool: info[full_field_name] = {"type": "boolean"}
            elif field_type in (date, datetime): info[full_field_name] = {"type": "date"}
            else: info[full_field_name] = {"type": self._GetSimpleTypeName(field_type)}
        return info

    def _GetListFieldInfo(self, args, full_field_name: str) -> Dict[str, Any]:
        if not args: return {"type": "array", "item_type": "unknown"}
        
        list_item_type = args[0]
        if inspect.isclass(list_item_type) and issubclass(list_item_type, BaseModel):
            nested_info = self._ExtractModelInfo(list_item_type, full_field_name)
            for nested_field_info in nested_info.values():
                nested_field_info["is_array_item"] = True
            return nested_info
        
        if inspect.isclass(list_item_type) and issubclass(list_item_type, Enum):
            return {"type": "array", "item_type": "enum", "values": [e.value for e in list_item_type]}
        
        return {"type": "array", "item_type": self._GetSimpleTypeName(list_item_type)}

    def _GetSimpleTypeName(self, field_type) -> str:
        if hasattr(field_type, "__name__"): return field_type.__name__
        if hasattr(field_type, "_name"): return field_type._name
        return str(field_type)


class FilterModelBuilder:
    """Builds filter models and system prompts for LLM processing."""
    
    def __init__(self, model_info: Dict[str, Any]):
        self.model_info = model_info
        self._filter_model_class = None

    def BuildFilterModel(self) -> type[BaseModel]:
        """Build Pydantic model for query filters with improved type safety and features."""
        class OperatorEnum(str, Enum):
            lt = "<"         # Existing
            gt = ">"         # Existing
            isin = "isin"    # Existing
            notin = "notin"  # Existing
            eq = "is"        # Existing
            ne = "different" # Existing
            be = "between"   # Existing
            contains = "contains"  # NEW: For string partial matches
            exists = "exists"      # NEW: Check if field exists (non-null)

        class SortOrderEnum(str, Enum):
            asc = "asc"
            desc = "desc"

        class AggregationEnum(str, Enum):  # NEW: For explicit aggregations in group_by
            SUM = "sum"
            AVG = "avg"
            COUNT = "count"
            MIN = "min"
            MAX = "max"

        class TimeIntervalEnum(str, Enum):  # NEW: For date_histogram intervals
            DAY = "day"
            WEEK = "week" 
            MONTH = "month"
            YEAR = "year"

        _model_info = self.model_info
        field_enum_members = {k: k for k in self.model_info.keys()}
        FieldEnum = Enum("FieldEnum", field_enum_members)

        class SortField(BaseModel):  # NEW: Typed sort field (allows multiple)
            field: FieldEnum
            order: SortOrderEnum = SortOrderEnum.asc

        class Aggregation(BaseModel):  # NEW: Typed aggregation spec
            field: FieldEnum
            type: AggregationEnum
            # NEW: Optional having clause for post-aggregation filtering
            having_operator: Optional[OperatorEnum] = Field(default=None, description="Operator for post-aggregation filtering (e.g., '>')")
            having_value: Optional[Union[str, int, float]] = Field(default=None, description="Value for post-aggregation filtering (e.g., 1)")

        class Query(BaseModel):
            field: FieldEnum
            operator: OperatorEnum
            value: Union[  # IMPROVED: More precise unions based on common types
                str, float, int, bool, date,
                List[Union[str, float, int, date]],  # For isin/notin/between
                None  # For exists (value=True/False for exists/not exists)
            ]

            @field_validator("value")
            def validate_value(cls, v, info: ValidationInfo):
                if "field" not in info.data or "operator" not in info.data:
                    return v
                
                field = info.data["field"].value
                op = info.data["operator"].value
                field_info = _model_info[field]  # Assuming _model_info is class-level or passed
                ftype = field_info["type"]

                def fail(msg: str):
                    raise ValueError(f"Invalid value for '{field}' ({ftype}): {msg}")
               
                # IMPROVED: Stricter checks, including new operators
                if op in ("<", ">", "between"):
                    if ftype not in ("number", "date"):
                        fail(f"Operator '{op}' only for number/date")
                    if op == "between" and (not isinstance(v, list) or len(v) != 2):
                        fail("Expected list of 2 values")
                elif op in ("isin", "notin"):
                    if not isinstance(v, list):
                        fail("Expected list")
                    if ftype == "enum" and not all(x in field_info.get("values", []) for x in v):
                        fail(f"Values must be in enum: {field_info['values']}")
                elif op == "contains":
                    if ftype != "string" or not isinstance(v, str):
                        fail("Expected string for contains")
                elif op == "exists":
                    if not isinstance(v, bool):
                        fail("Expected bool (True=exists, False=not exists)")
                # ... (expand with existing logic for eq/ne, numbers, dates, etc.)
                return v

        class QuerySlice(BaseModel):  # NEW: Per-slice model for clarity
            conditions: List[Query] = Field(description="AND-joined filter conditions")
            sort: Optional[List[SortField]] = None  # IMPROVED: Multi-field sort at slice level
            limit: Optional[int] = None
            group_by: Optional[List[FieldEnum]] = None  # IMPROVED: Multi-field group_by
            aggregations: Optional[List[Aggregation]] = None  # NEW: Explicit aggs, now with optional having clause
            interval: Optional[TimeIntervalEnum] = Field(default=TimeIntervalEnum.MONTH, description="Time interval for date grouping")

            @model_validator(mode='after')
            def validate_slice(self) -> 'QuerySlice':
                """
                Ensures query slice parameters are used logically.
                This validator corrects invalid combinations instead of raising errors.
                """
                
                for query in self.conditions:
                    if query.field.value == "null":
                        self.conditions.remove(query)
                
                # Rule: aggregations and interval require group_by. If no group_by, remove them.
                if not self.group_by:
                    if self.aggregations:
                        self.aggregations = None
                    # The user mentioned this case specifically.
                    if self.interval:
                        self.interval = None
                
                # Rule: interval is only applicable when grouping by a date field.
                # If interval is set, but no date field is in group_by, remove interval.
                if self.interval and self.group_by:
                    is_date_grouped = any(
                        _model_info.get(f.value, {}).get("type") == "date" for f in self.group_by
                    )
                    if not is_date_grouped:
                        self.interval = None
                
                return self

        class QueryFilters(BaseModel):
            filters: List[QuerySlice] = Field(description="List of query slices (for comparisons)")

        self._filter_model_class = QueryFilters
        return self._filter_model_class

    def GenerateSystemPrompt(self) -> str:
        """Generate system prompt for LLM filter extraction."""
        # Note: All literal curly braces in this f-string are doubled `{{` `}}`
        # to prevent format errors, as this string is processed by Python's
        # .format() or f-string mechanism.
        return f"""
Today is {datetime.now().strftime("%Y-%m-%d")}

### 1. Your Goal
You are an expert assistant that converts a user's natural-language question into a structured JSON filter. Your output MUST strictly follow the JSON schema provided below.

### 2. Available Data Schema
This is the data you can query. Fields are specified as `object.field`.

{json.dumps(self.model_info, indent=2)}

### 3. How to Build the JSON Filter
Your entire output must be a single JSON object with one key, `filters`. This key holds a list of "slices". Each slice represents a set of data.

#### Supported Operators
| Symbol | Meaning | Allowed on |
|---|---|---|
| `<` | less than | number, date |
| `>` | greater than | number, date |
| `isin` | value in list | any |
| `notin` | value not in list | any |
| `is` | equals | any |
| `different` | not equal | any |
| `between` | range (for dates) | date |
| `contains` | partial string match | string |
| `exists` | field is not null | any (use `true`) |

#### Slice Options
Each slice in the `filters` list can have these keys:
- `conditions`: A list of filter conditions.
- `sort`: A list of fields to sort by asc or desc (e.g., `[{{\\"field\\": \\"transaction.amount\\", \\"order\\": \\"desc\\"}}]`).
- `limit`: The maximum number of results to return.
- `group_by`: A list of fields to group by.
- `aggregations`: A list of calculations to perform on groups (e.g., `[{{\\"field\\": \\"transaction.amount\\", \\"type\\": \\"sum\\"}}]`). An aggregation can also include a `having_operator` and `having_value` to filter groups based on the result.
- `interval`: Use for date grouping (`day`, `week`, `month`, `year`). Defaults to `month`.

### 4. Critical Rules & Guardrails
- **ALWAYS use the field names from the schema.** Do not invent fields. If a user's term is ambiguous (e.g., "category"), map it to the most likely schema field (e.g., `transaction.receiver.category_type`).
- **`aggregations` and `interval` ONLY work with `group_by`.** If there is no `group_by`, do not use `aggregations` or `interval`.
- **`interval` is ONLY for date fields.** Do not use it when grouping by non-date fields.
- **Comparisons mean multiple slices.** If the user says "compare A with B", create two slices in the `filters` list. The first for A, the second for B.
- **Be precise with dates.** Convert relative dates like "last month" or "this year" into specific date ranges (e.g., `"operator": "between", "value": ["2024-01-01", "2024-12-31"]`).

### 5. Realistic Examples

#### Example 1: Simple Filtering
**User**: "what were my transactions at starbucks?"
```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.receiver.name", "operator": "is", "value": "Starbucks" }}
      ]
    }}
  ]
}}
```

#### Example 2: Time-Based Aggregation
**User**: "How much did I spend on food each month this year?"
```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.receiver.category_type", "operator": "is", "value": "food" }},
        {{ "field": "transaction.timestamp", "operator": "between", "value": ["2024-01-01", "2024-12-31"] }}
      ],
      "group_by": ["transaction.timestamp"],
      "interval": "month",
      "aggregations": [
        {{ "field": "transaction.amount", "type": "sum" }}
      ]
    }}
  ]
}}
```

#### Example 3: Two-Slice Comparison
**User**: "Compare my spending on flights vs hotels for my gold card last year."
```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.receiver.category_type", "operator": "is", "value": "flight" }},
        {{ "field": "card_type", "operator": "is", "value": "GOLD" }},
        {{ "field": "transaction.timestamp", "operator": "between", "value": ["2023-01-01", "2023-12-31"] }}
      ]
    }},
    {{
      "conditions": [
        {{ "field": "transaction.receiver.category_type", "operator": "is", "value": "hotel" }},
        {{ "field": "card_type", "operator": "is", "value": "GOLD" }},
        {{ "field": "transaction.timestamp", "operator": "between", "value": ["2023-01-01", "2023-12-31"] }}
      ]
    }}
  ]
}}
```

#### Example 4: Complex Query with Sorting and Limiting
**User**: "What were my top 5 most expensive transactions in London, and when did they happen?"
```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.receiver.location", "operator": "is", "value": "London" }}
      ],
      "sort": [
        {{ "field": "transaction.amount", "order": "desc" }}
      ],
      "limit": 5
    }}
  ]
}}
```

Below are the **re‑aligned “hard‑mode” examples**.
Every field now exists in the supplied `user_transactions` mapping.

---

### Example 5 – Quarter‑over‑Quarter Grocery Spend by Category

```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "grocery" }},
        {{ "field": "transaction.timestamp",
          "operator": "between",
          "value": ["2025-01-01", "2025-03-31"] }}
      ],
      "group_by": ["transaction.receiver.category_type"],
      "aggregations": [
        {{ "field": "transaction.amount", "type": "sum" }},
        {{ "field": "transaction.amount", "type": "avg" }}
      ]
    }},
    {{
      "conditions": [
        {{ "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "grocery" }},
        {{ "field": "transaction.timestamp",
          "operator": "between",
          "value": ["2024-01-01", "2024-03-31"] }}
      ],
      "group_by": ["transaction.receiver.category_type"],
      "aggregations": [
        {{ "field": "transaction.amount", "type": "sum" }},
        {{ "field": "transaction.amount", "type": "avg" }}
      ]
    }}
  ]
}}
```

---

### Example 6 – High‑Value Receiver‑Name Search with Existence Check

```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.amount",
          "operator": ">",
          "value": 1000 }},
        {{ "field": "transaction.receiver.name",
          "operator": "contains",
          "value": "airfare" }},
        {{ "field": "transaction.receiver.name",
          "operator": "exists",
          "value": true }}
      ],
      "sort": [
        {{ "field": "transaction.timestamp", "order": "desc" }}
      ],
      "limit": 10
    }}
  ]
}}
```

---

### Example 7 – Monthly Withdrawal Counts & Totals (USD)

```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.type",
          "operator": "is",
          "value": "Withdrawal" }},
        {{ "field": "transaction.currency",
          "operator": "is",
          "value": "USD" }},
        {{ "field": "transaction.timestamp",
          "operator": "between",
          "value": ["2023-01-01", "2023-06-30"] }}
      ],
      "group_by": ["transaction.timestamp"],
      "interval": "month",
      "aggregations": [
        {{ "field": "transaction.amount", "type": "count" }},
        {{ "field": "transaction.amount", "type": "sum" }}
      ]
    }}
  ]
}}
```

---

### Example 8 – Multi‑Level Grouping of Non‑USD/EUR Deposits (2024)

```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.type",
          "operator": "is",
          "value": "Deposit" }},
        {{ "field": "transaction.currency",
          "operator": "notin",
          "value": ["USD", "EUR"] }},
        {{ "field": "transaction.timestamp",
          "operator": "between",
          "value": ["2024-01-01", "2024-12-31"] }}
      ],
      "group_by": [
        "transaction.currency",
        "transaction.receiver.location"
      ],
      "aggregations": [
        {{ "field": "transaction.amount", "type": "min" }},
        {{ "field": "transaction.amount", "type": "max" }}
      ]
    }}
  ]
}}
```

---

### Example 9 – Daily Spend Comparison: Paris vs New York (May 2025)

```json
{{
  "filters": [
    {{
      "conditions": [
        {{ "field": "transaction.receiver.location",
          "operator": "is",
          "value": "Paris" }},
        {{ "field": "transaction.timestamp",
          "operator": "between",
          "value": ["2025-05-01", "2025-05-31"] }}
      ],
      "group_by": ["transaction.timestamp"],
      "interval": "day",
      "aggregations": [
        {{ "field": "transaction.amount", "type": "sum" }}
      ]
    }},
    {{
      "conditions": [
        {{ "field": "transaction.receiver.location",
          "operator": "is",
          "value": "New York" }},
        {{ "field": "transaction.timestamp",
          "operator": "between",
          "value": ["2025-05-01", "2025-05-31"] }}
      ],
      "group_by": ["transaction.timestamp"],
      "interval": "day",
      "aggregations": [
        {{ "field": "transaction.amount", "type": "sum" }}
      ]
    }}
  ]
}}
```

---

### Example 10 – Find Days With Multiple Transactions (Having Clause)
**User**: "Show me all transactions on days where I made more than one purchase."
```json
{{
  "filters": [
    {{
      "group_by": ["transaction.timestamp"],
      "interval": "day",
      "aggregations": [
        {{
          "field": "transaction.id",
          "type": "count",
          "having_operator": ">",
          "having_value": 1
        }}
      ]
    }}
  ]
}}
```
━━━━━━━━━━━━━━━━━━━━━━━
📤 **Your Task**
After reading the user question, output **only** the corresponding JSON object (starting with `{{ "filters": [...] }}`). No extra explanation.
━━━━━━━━━━━━━━━━━━━━━━━
"""


class LlmClientFactory:
    """Creates and manages LLM clients for query processing."""
    
    def __init__(self, model_name: str, api_key: str):
        if not model_name:
            raise ValueError("model_name is required")
        if not api_key:
            raise ValueError("api_key is required")
        
        self.model_name = model_name
        self.api_key = api_key
        self._client = None

    def GetClient(self, result_type: type[BaseModel], system_prompt: str) -> LLM:
        """Get or create LLM client."""
        if self._client is None:
            self._client = LLM(
                model=self.model_name,
                result_type=result_type,
                system_prompt=system_prompt,
                api_key=self.api_key,
            )
        return self._client

    def ParseQuery(self, query: str, filter_model: type[BaseModel], system_prompt: str):
        """Parse natural language query synchronously."""
        client = self.GetClient(filter_model, system_prompt)
        return client.llm_agent.run_sync([query])

    async def ParseQueryAsync(self, query: str, filter_model: type[BaseModel], system_prompt: str):
        """Parse natural language query asynchronously."""
        client = self.GetClient(filter_model, system_prompt)
        return await client.llm_agent.run([query])


def FiltersToDsl(query_filters: dict, model_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert QueryFilters to Elasticsearch query DSL."""
    if not query_filters or "filters" not in query_filters:
        return [{"query": {"match_all": {}}}]

    def _keyword_field(f: str) -> str:
        if f.endswith(".keyword"):
            return f
        return f"{f}.keyword"

    elastic_queries: List[Dict[str, Any]] = []

    for filter_slice in query_filters["filters"]:
        must_clauses: List[Dict[str, Any]] = []
        sort_configs = []
        limit_config = None
        aggs_config = None

        # Process conditions
        for condition in filter_slice.get("conditions", []):
            field = condition["field"]
            operator = condition["operator"]
            value = condition["value"]

            is_string = isinstance(value, str) and not (len(value) == 10 and value[4] == "-" and value[7] == "-")
            exact_field = _keyword_field(field) if is_string else field

            if operator == ">":
                must_clauses.append({"range": {field: {"gt": value}}})
            elif operator == "<":
                must_clauses.append({"range": {field: {"lt": value}}})
            elif operator == "is":
                must_clauses.append({"term": {exact_field: value}})
            elif operator == "different":
                must_clauses.append({"bool": {"must_not": {"term": {exact_field: value}}}})
            elif operator == "isin":
                if isinstance(value, list):
                    if len(value) == 2 and all(isinstance(v, str) and len(v) == 10 and v.count("-") == 2 for v in value):
                        must_clauses.append({"range": {field: {"gte": value[0], "lte": value[1]}}})
                    else:
                        must_clauses.append({"terms": {exact_field: value}})
                else:
                    must_clauses.append({"term": {exact_field: value}})
            elif operator == "notin":
                if isinstance(value, list):
                    must_clauses.append({"bool": {"must_not": {"terms": {exact_field: value}}}})
                else:
                    must_clauses.append({"bool": {"must_not": {"term": {exact_field: value}}}})
            elif operator == "between":
                if isinstance(value, list) and len(value) == 2:
                    must_clauses.append({"range": {field: {"gte": value[0], "lte": value[1]}}})
            elif operator == "contains":  # NEW: Partial string match
                must_clauses.append({"wildcard": {exact_field: {"value": f"*{value}*", "case_insensitive": True}}})
            elif operator == "exists":  # NEW: Exists check
                if value is True:
                    must_clauses.append({"exists": {"field": field}})
                elif value is False:
                    must_clauses.append({"bool": {"must_not": {"exists": {"field": field}}}})

        elastic_query = {"query": {"bool": {"must": must_clauses}}} if must_clauses else {"query": {"match_all": {}}}

        # Process sort (multi-field)
        if "sort" in filter_slice and filter_slice["sort"]:
            for s in filter_slice["sort"]:
                sort_configs.append({s["field"]: {"order": s.get("order", "asc")}})
            elastic_query["sort"] = sort_configs  # type: ignore

        # Process limit
        if "limit" in filter_slice:
            limit_config = filter_slice["limit"]
            elastic_query["size"] = limit_config  # type: ignore

        # Process group_by and aggregations
        if "group_by" in filter_slice and filter_slice["group_by"]:
            group_fields = filter_slice["group_by"]
            aggs = {}
            current_agg = aggs

            for i, gf in enumerate(group_fields):
                agg_name = f"group_by_{i}"
                field_type = model_info.get(gf, {}).get("type")

                if field_type == "date":
                    interval = filter_slice.get("interval", "month")  # Default to month, expect string value
                    
                    # Set format based on interval
                    format_map = {
                        "day": "yyyy-MM-dd",
                        "week": "yyyy-'W'ww", 
                        "month": "yyyy-MM",
                        "year": "yyyy"
                    }
                    format_str = format_map.get(interval, "yyyy-MM")
                    
                    current_agg[agg_name] = {
                        "date_histogram": {
                            "field": gf,
                            "calendar_interval": interval,
                            "format": format_str
                        }
                    }
                else:
                    agg_field = _keyword_field(gf) if field_type in ("string", "enum", "text") else gf
                    current_agg[agg_name] = {
                        "terms": {
                            "field": agg_field,
                            "size": limit_config or 100
                        }
                    }

                if i < len(group_fields) - 1:
                    current_agg[agg_name]["aggs"] = {}
                    current_agg = current_agg[agg_name]["aggs"]

            # Navigate to the deepest aggregation level (always for group_by)
            target_for_sub_aggs = aggs
            for i in range(len(group_fields)):
                group_agg_name = f"group_by_{i}"
                if "aggs" in target_for_sub_aggs[group_agg_name]:
                    target_for_sub_aggs = target_for_sub_aggs[group_agg_name]["aggs"]
                else:
                    target_for_sub_aggs = target_for_sub_aggs[group_agg_name]
                    break
            
            sub_aggs = target_for_sub_aggs.setdefault("aggs", {})

            # Always add top_hits to get documents per bucket (moved here to be unconditional)
            sub_aggs["documents"] = {
                "top_hits": {
                    "size": 100
                }
            }

            # Process aggregations if present
            having_clauses = []
            if "aggregations" in filter_slice and filter_slice["aggregations"]:
                for agg in filter_slice["aggregations"]:
                    agg_metric_name = f"{agg['type'].lower()}_{agg['field'].replace('.', '_')}"
                    
                    field_for_agg = agg['field']
                    field_info = model_info.get(field_for_agg, {})
                    field_type = field_info.get("type")

                    if agg["type"] == "count" and field_type in ("string", "enum", "text"):
                        field_for_agg = _keyword_field(field_for_agg)

                    if agg["type"] == "sum":
                        sub_aggs[agg_metric_name] = {"sum": {"field": field_for_agg}}
                    elif agg["type"] == "avg":
                        sub_aggs[agg_metric_name] = {"avg": {"field": field_for_agg}}
                    elif agg["type"] == "count":
                        sub_aggs[agg_metric_name] = {"value_count": {"field": field_for_agg}}
                    elif agg["type"] == "min":
                        sub_aggs[agg_metric_name] = {"min": {"field": field_for_agg}}
                    elif agg["type"] == "max":
                        sub_aggs[agg_metric_name] = {"max": {"field": field_for_agg}}

                    if agg.get("having_operator") and agg.get("having_value") is not None:
                        having_clauses.append({
                            "metric_name": agg_metric_name,
                            "operator": agg["having_operator"],
                            "value": agg["having_value"]
                        })

                # Add bucket_selector if there are having clauses
                if having_clauses:
                    buckets_path = {}
                    script_parts = []
                    op_map = {">": ">", "<": "<", "is": "==", "different": "!=", ">=": ">=", "<=": "<="}

                    for i, clause in enumerate(having_clauses):
                        script_var = f"var_{i}"
                        buckets_path[script_var] = clause["metric_name"]
                        op_symbol = op_map.get(clause["operator"], "==")
                        value = clause['value']
                        script_value = f"'{value}'" if isinstance(value, str) else value
                        script_parts.append(f"params.{script_var} {op_symbol} {script_value}")
                    
                    script = " && ".join(script_parts)
                    
                    sub_aggs["having_filter"] = {
                        "bucket_selector": {
                            "buckets_path": buckets_path,
                            "script": script
                        }
                    }

            elastic_query["aggs"] = aggs  # type: ignore
            elastic_query["size"] = 0  # type: ignore

        elastic_queries.append(elastic_query)

    return elastic_queries


def RunElasticQueries(es_client: Elasticsearch, index_name: str, elastic_queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Execute Elasticsearch queries and return results."""
    if not elastic_queries:
        return []

    results = []
    for query in elastic_queries:
        try:
            response = es_client.search(index=index_name, **query)
            result = {
                "total_hits": response["hits"]["total"]["value"],
                "documents": [hit["_source"] for hit in response["hits"]["hits"]]
            }
            
            # Handle aggregations (for group by queries)
            if "aggregations" in response:
                result["aggregations"] = response["aggregations"]
                # For group by queries, format the aggregation results nicely
                if "group_by" in response["aggregations"]:
                    group_data = []
                    for bucket in response["aggregations"]["group_by"]["buckets"]:
                        group_item = {
                            "key": bucket["key"],
                            "doc_count": bucket["doc_count"],
                            "total_amount": bucket.get("total_amount", {}).get("value", 0),
                        }
                        group_data.append(group_item)
                    result["grouped_results"] = group_data
            
            results.append(result)
        except Exception as e:
            results.append({
                "error": str(e),
                "total_hits": 0,
                "documents": []
            })
    
    return results


class ElasticsearchModelGenerator:
    """Orchestrates ES mapping to model generation and natural language querying."""

    def __init__(
        self,
        index_name: str,
        es_host: str = "http://elastic:rvs59tB_VVANUy4rC-kd@84.16.230.94:9200",
        fields_to_ignore: List[str] = [],
        category_fields: List[str] = [],
        model_name: str = "",
        api_key: str = "",
    ):
        if not index_name:
            raise ValueError("index_name is required")
        
        self.index_name = index_name
        self.es_client = Elasticsearch(hosts=[es_host])
        
        self.model_builder = ModelBuilder(
            self.es_client, index_name, category_fields, fields_to_ignore, es_host=es_host
        )
        self.filter_builder = FilterModelBuilder(self.model_builder.GetModelInfo())
        
        self.llm_factory = None
        if model_name and api_key:
            self.llm_factory = LlmClientFactory(model_name, api_key)

    def GenerateModel(self, model_name: Optional[str] = None) -> type[BaseModel]:
        """Generate Pydantic model from ES mapping."""
        return self.model_builder.Build(model_name)

    def GetModelInfo(self) -> Dict[str, Any]:
        """Get flattened field information."""
        return self.model_builder.GetModelInfo()

    def PrintModelSummary(self):
        """Print summary of generated model."""
        model = self.GenerateModel()
        model_info = self.GetModelInfo()

        print(f"\n=== Model Summary for Index: {self.index_name} ===")
        print(f"Model Class: {model.__name__}")
        print(f"Total Fields: {len(model_info)}")

        print("\n=== Field Details ===")
        for field_name, field_info in model_info.items():
            field_type = field_info["type"]
            if field_type == "enum":
                values = field_info.get("values", [])
                print(f"  {field_name}: {field_type} ({len(values)} values)")
                if len(values) <= 10:
                    print(f"    Values: {values}")
                else:
                    print(f"    Sample Values: {values}")
            elif field_type == "array":
                item_type = field_info.get("item_type", "unknown")
                print(f"  {field_name}: {field_type}[{item_type}]")
                if "values" in field_info:
                    values = field_info["values"]
                    print(f"    Enum Values: {values}")
            else:
                print(f"  {field_name}: {field_type}")
                if field_info.get("is_array_item"):
                    print("    (Part of array structure)")

    def Query(self, query: str, execute: bool = True) -> Dict[str, Any]:
        """Complete pipeline: natural language to ES results."""
        if not self.llm_factory:
            raise ValueError("LLM factory not initialized. Provide model_name and api_key.")
        
        filter_model = self.filter_builder.BuildFilterModel()
        system_prompt = self.filter_builder.GenerateSystemPrompt()
        
        filters = self.llm_factory.ParseQuery(query, filter_model, system_prompt)
        elastic_queries = FiltersToDsl(filters, self.filter_builder.model_info)  # type: ignore[arg-type]
        
        response = {
            "natural_language_query": query,
            "extracted_filters": filters,
            "elasticsearch_queries": elastic_queries
        }
        
        if execute and elastic_queries:
            results = RunElasticQueries(self.es_client, self.index_name, elastic_queries)
            response["results"] = results
            
        return response

    async def QueryAsync(self, query: str, execute: bool = True) -> Dict[str, Any]:
        """Async version of complete pipeline."""
        if not self.llm_factory:
            raise ValueError("LLM factory not initialized. Provide model_name and api_key.")
        
        filter_model = self.filter_builder.BuildFilterModel()
        system_prompt = self.filter_builder.GenerateSystemPrompt()
        
        filters = await self.llm_factory.ParseQueryAsync(query, filter_model, system_prompt)
        elastic_queries = FiltersToDsl(filters, self.filter_builder.model_info)  # type: ignore[arg-type]
        
        response = {
            "natural_language_query": query,
            "extracted_filters": filters,
            "elasticsearch_queries": elastic_queries
        }
        
        if execute and elastic_queries:
            results = RunElasticQueries(self.es_client, self.index_name, elastic_queries)
            response["results"] = results
            
        return response

    def RunRawQuery(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        """Execute raw Elasticsearch query."""
        try:
            if "size" not in query:
                query["size"] = size
            
            response = self.es_client.search(index=self.index_name, **query)
            
            result = {
                "total_hits": response["hits"]["total"]["value"] if isinstance(response["hits"]["total"], dict) else response["hits"]["total"],
                "documents": [hit["_source"] for hit in response["hits"]["hits"]],
                "query": query,
                "success": True
            }
            
            if "aggregations" in response:
                result["aggregations"] = response["aggregations"]
            
            return result
            
        except Exception as e:
            return {
                "error": str(e),
                "query": query,
                "success": False,
                "total_hits": 0,
                "documents": []
            }

    # Legacy method aliases for backward compatibility
    def generate_model(self, model_name: Optional[str] = None) -> type[BaseModel]:
        return self.GenerateModel(model_name)
    
    def get_model_info(self) -> Dict[str, Any]:
        return self.GetModelInfo()
    
    def print_model_summary(self):
        return self.PrintModelSummary()
    
    def generate_filters_from_query(self, query: str):
        return self.Query(query, execute=False)["extracted_filters"]
    
    async def generate_filters_from_query_async(self, query: str):
        result = await self.QueryAsync(query, execute=False)
        return result["extracted_filters"]
    
    def FilterToElasticQuery(self, query_filters: dict) -> List[Dict[str, Any]]:
        return FiltersToDsl(query_filters, self.filter_builder.model_info)  # type: ignore[arg-type]
    
    def ExecuteElasticQueries(self, elastic_queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return RunElasticQueries(self.es_client, self.index_name, elastic_queries)
    
    def QueryFromNaturalLanguage(self, query: str, execute: bool = True, size: int = 100) -> Dict[str, Any]:
        return self.Query(query, execute)
    
    async def QueryFromNaturalLanguageAsync(self, query: str, execute: bool = True, size: int = 100) -> Dict[str, Any]:
        return await self.QueryAsync(query, execute)
    
    def run_raw_elastic_query(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        return self.RunRawQuery(query, size)

    # Legacy debugging methods
    def debug_category_fields(self) -> Dict[str, Any]:
        debug_info = {
            "configured_category_fields": self.model_builder.category_fields,
            "processed_fields": {},
            "errors": []
        }
        
        try:
            properties = self.model_builder._GetIndexMapping()
            
            for category_field in self.model_builder.category_fields:
                try:
                    field_path = f"{category_field}.keyword" if "." in category_field else category_field
                    distinct_values = self.model_builder.GetDistinctValues(field_path)
                    
                    debug_info["processed_fields"][category_field] = {
                        "field_path_used": field_path,
                        "distinct_values_found": len(distinct_values),
                        "sample_values": distinct_values[:5] if distinct_values else [],
                        "all_values": distinct_values
                    }
                    
                    print(f"✅ Category field '{category_field}' -> {len(distinct_values)} values: {distinct_values[:5]}")
                    
                except Exception as e:
                    error_msg = f"Error processing category field '{category_field}': {str(e)}"
                    debug_info["errors"].append(error_msg)
                    print(f"❌ {error_msg}")
            
            debug_info["available_fields"] = self._get_all_field_paths(properties)
            
        except Exception as e:
            debug_info["errors"].append(f"General error in debug_category_fields: {str(e)}")
            print(f"❌ General error: {str(e)}")
        
        return debug_info
    
    def _get_all_field_paths(self, mapping: Dict[str, Any], prefix: str = "") -> List[str]:
        paths = []
        
        for field_name, field_props in mapping.items():
            full_path = f"{prefix}.{field_name}" if prefix else field_name
            paths.append(full_path)
            
            if "properties" in field_props:
                nested_paths = self._get_all_field_paths(field_props["properties"], full_path)
                paths.extend(nested_paths)
        
        return paths

    def populate_with_examples(self, model_class: type[BaseModel]) -> dict:
        if model_class is None:
            model_class = self.GenerateModel()

        result = {}
        for name, field_info in model_class.model_fields.items():
            annotation = field_info.annotation

            origin = get_origin(annotation)
            if origin is Union:
                args = get_args(annotation)
                inner_types = [arg for arg in args if arg is not type(None)]
                if inner_types:
                    annotation = inner_types[0]

            if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                result[name] = self.populate_with_examples(annotation)
            else:
                result[name] = self.get_example_value(annotation)
        return result

    def get_example_value(self, annotation):
        # This method would need to be implemented based on your needs
        if annotation is str:
            return "example_string"
        elif annotation in (int, float):
            return 42
        elif annotation is bool:
            return True
        elif annotation in (date, datetime):
            return datetime.now()
        return None


def AppendQueriesToJson(queries: list[str], filename: str = "queries.json") -> None:
    """
    Append queries with their filters and elastic queries to JSON file using async processing.
    Skips existing queries and executes new ones to check if they work.
    """
    client = ElasticsearchModelGenerator(
        index_name="user_transactions",
        category_fields=[
            "card_kind",
            "card_type", 
            "transaction.receiver.category_type",
            "transaction.receiver.location",
            "transaction.type",
            "transaction.currency"
        ],
        fields_to_ignore=["user_id", "card_number"],
        model_name="ollama/qwen3:8b",
        api_key="sk-1234567890"
    )
    file_path = Path(filename)

    # Load existing data once to check for duplicates
    existing_data = []
    if file_path.exists():
        try:
            existing_data = json.loads(file_path.read_text())
        except (json.JSONDecodeError, FileNotFoundError):
            existing_data = []
    
    existing_queries = {item.get("input") for item in existing_data}

    async def process_query(q: str):
        """Processes a single query, executes it, and prepares the entry for saving."""
        try:
            # Execute the query to check if it works
            result = await client.QueryFromNaturalLanguageAsync(q, execute=True)
            
            # Determine execution status
            execution_status = "success"
            execution_error = None
            if "results" in result:
                for res in result["results"]:
                    if "error" in res:
                        execution_status = "execution_error"
                        execution_error = res["error"]
                        break
            
            new_entry = {
                "timestamp": datetime.now().isoformat(),
                "input": q,
                "filter": result["extracted_filters"],
                "elastic_query": result["elasticsearch_queries"],
                "execution_results": result.get("results", []),
                "status": execution_status
            }
            if execution_error:
                new_entry["error"] = execution_error

            print(f"✅ Processed and executed: {q}")
            
        except Exception as e:
            new_entry = {
                "timestamp": datetime.now().isoformat(),
                "input": q,
                "filter": None,
                "elastic_query": None,
                "execution_results": None,
                "status": "generation_error",
                "error": str(e)
            }
            print(f"❌ Error generating query for: {q} - {e}")
        
        return new_entry

    async def process_all():
        queries_to_process = [q for q in queries if q not in existing_queries]
        
        # Identify skipped queries
        for q in queries:
            if q not in queries_to_process:
                print(f"⏭️ Skipping existing query: {q}")

        if not queries_to_process:
            print("No new queries to process.")
            return
            
        print(f"Found {len(queries_to_process)} new queries to process.")

        new_entries = []
        for q in queries_to_process:
            entry = await process_query(q)
            new_entries.append(entry)
            await asyncio.sleep(2)  # Keep rate limiting

        # Append new entries to existing data and save
        if new_entries:
            all_data = existing_data + new_entries
            file_path.write_text(json.dumps(all_data, indent=2))
            print(f"Appended {len(new_entries)} new results to {filename}")

        print("Completed processing all queries.")

    asyncio.run(process_all())


def main(queries: list[str], filename: str = "queries.json") -> None:
    """Process list of queries and append results to JSON file using async processing."""
    AppendQueriesToJson(queries, filename)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Command line usage: python elasticsearch_model_generator.py "query1" "query2" ...
        queries = sys.argv[1:]
        main(queries, "queries.json")
    else:
        # Example queries for testing
        example_queries = [
            "Can you show me all my transactions from April 2024?",
            "What deposits have I made recently?",
            "How much have I spent on clothing?",
            "What's the total amount I withdrew in May 2024?",
            "Can you give me a breakdown of my spending by category?",
            "What are all the transactions I made at Starbucks?",
            "Which of my transactions were made online?",
            "How many times did I deposit money, and what's the total amount?",
            "Which of my withdrawals were over $500?",
            "How much have I spent each month?",
            "What are my travel-related transactions?",
            "How much money did I spend in London?",
            "What's the highest single transaction I've made?",
            "List all my subscriptions and how much I've paid for them.",
            "Can you group my transactions by location?",
            "Show me everything I've spent on food and drinks.",
            "What are all my transactions from June 2024?",
            "How much have I spent at restaurants versus cafes?",
            "Group my spending by card type",
            "Breakdown my transactions by merchant category",
            "Show me spending patterns by location and category",
            "Group my deposits by month",
            "Breakdown spending by currency type",
            "Group my transactions by type and show total amounts",
            "What is the monthly spending breakdown by card type?",
            "Show me a summary of spending by location and currency",
            "Group withdrawals by month and show counts"
        ]
        main(example_queries, "example_queries.json")
