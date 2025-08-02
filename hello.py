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
from pydantic import BaseModel, Field, create_model, field_validator, ValidationInfo


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
                 enum_fields: Optional[Dict[str, List[Any]]] = None):

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
        
        self._model_class: Optional[type[BaseModel]] = None
        self._model_info = None
        self._mapping_cache = None

    def _GetIndexMapping(self) -> Dict[str, Any]:
        """Get mapping from ES or from provided mapping."""
        if self._mapping_cache:
            return self._mapping_cache

        if self.mode == "es":
            # Static type checkers (e.g. mypy/pyright) cannot infer that `es_client` and
            # `index_name` are non-null when `self.mode == "es"`. An explicit assertion
            # makes this guarantee clear and eliminates the "attribute of None" warning.
            assert self.es_client is not None and self.index_name is not None, "Elasticsearch client or index name is missing"
            mappings = self.es_client.indices.get_mapping(index=self.index_name)
            index_mapping = mappings.get(self.index_name, {}).get("mappings", {})
            self._mapping_cache = index_mapping.get("properties", {})
        else: # mapping mode
            self._mapping_cache = self.provided_mapping or {}
            
        return self._mapping_cache

    def GetDistinctValues(self, field_path: str, size: int = 1000) -> List[Any]:
        """Get distinct values for a field from Elasticsearch. Requires 'es' mode."""
        if self.mode != "es":
            raise RuntimeError("Cannot get distinct values without an Elasticsearch client.")
        try:
            # See note in `_GetIndexMapping` – we assert for the type checker.
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
        """Build Pydantic model for query filters."""
        if self._filter_model_class is None:
            class OperatorEnum(str, Enum):
                lt = "<"
                gt = ">"
                isin = "isin"
                notin = "notin"
                eq = "is"
                ne = "different"
                be = "between"

            class SortOrderEnum(str, Enum):
                asc = "asc"
                desc = "desc"
            
            
            field_enum_members = {k: k for k in self.model_info.keys()}
            FieldEnum = Enum("FieldEnum", field_enum_members)
            
            class sortField(FieldEnum):
                sort_order: SortOrderEnum
            
            _model_info = self.model_info

            class Query(BaseModel):
                field: FieldEnum
                operator: OperatorEnum
                value: Union[str, float, int, List[str], List[date]]
                sort_field: Optional[list[sortField]] = Field(default=None, description="Field to sort by")
                limit: Optional[int] = Field(default=None, description="Maximum number of results to return")
                group_by: Optional[FieldEnum] = Field(default=None, description="Field to group results by")

                @field_validator("value")
                def validate_value(cls, v, info: ValidationInfo):
                    if "field" not in info.data or "operator" not in info.data:
                        return v

                    field = info.data["field"].value
                    op = info.data["operator"].value
                    field_info = _model_info[field]
                    ftype = field_info["type"]

                    def fail(msg):
                        raise ValueError(f"Invalid value for field '{field}' with type '{ftype}': {msg}")

                    if op in ("<", ">"):
                        if ftype not in ("number", "date"):
                            fail(f"Operator '{op}' only valid for number/date fields")
                        if ftype == "number":
                            try:
                                float(v)
                            except:
                                fail("Expected numeric value")
                        elif ftype == "date":
                            try:
                                if isinstance(v, str):
                                    date.fromisoformat(v)
                            except:
                                fail("Expected ISO date string (YYYY-MM-DD)")

                    elif op in ("isin", "notin"):
                        if not isinstance(v, list):
                            fail("Expected list of values")
                        if ftype == "enum":
                            allowed = field_info.get("values", [])
                            if not all(x in allowed for x in v):
                                fail(f"Values must be in enum: {allowed}")

                    elif op in ("is", "different"):
                        if ftype == "enum":
                            if v not in field_info.get("values", []):
                                fail(f"Must be one of {field_info.get('values', [])}")
                        elif ftype == "number":
                            try:
                                float(v)
                            except:
                                fail("Expected number")
                        elif ftype == "boolean":
                            if v not in [True, False, "true", "false", "True", "False"]:
                                fail("Expected boolean")
                        elif ftype == "date":
                            try:
                                date.fromisoformat(v)
                            except:
                                fail("Expected date in ISO format")
                    return v

            class QueryFilters(BaseModel):
                filters: list[list[Query]] = Field(description="Filtering conditions for the query")

            self._filter_model_class = QueryFilters
        return self._filter_model_class

    def GenerateSystemPrompt(self) -> str:
        """Generate system prompt for LLM filter extraction."""
        return f"""
Today is {datetime.now().strftime("%Y-%m-%d")}

You are an expert assistant that converts **natural-language questions into JSON filters** for an analytics engine.

The user might ask for:
• a **single slice** of data – e.g. "show my restaurant purchases", or  
• a **comparison of multiple slices** – e.g. "compare hotel spend last year with food spend on my gold card".

### 1. Available Schema
{json.dumps(self.model_info, indent=2)}

### 2. Supported Operators
| Symbol | Meaning      | Allowed on          |
|--------|--------------|---------------------|
| <      | less than    | number, date        |
| >      | greater than | number, date        |
| isin   | value in     | any                 |
| notin  | value not in | any                 |
| is     | equals       | any                 |
| different | not equal | any                 |
| between  | range      | number, date (TBD)  |

### 3. Additional Query Options
- **Sorting**: Use `sort_field` and `sort_order` ("asc" or "desc") to sort results
- **Limiting**: Use `limit` to restrict the number of results returned
- **Grouping**: Use `group_by` to aggregate results by a field (e.g., "expenses by location")

### 4. Reasoning steps
1. Detect time references and convert to ISO dates.  
2. Extract entities (amounts, categories, card types …).  
3. Determine intent: single slice vs multi-slice comparison.  
4. Map entities to schema fields.  
5. Choose operators.  
6. Validate values (enum membership, numeric type, date format).

### 5. Output format  ❗️
Return **only** a JSON object that matches this schema:

{{
  "filters": [
    [ /* first slice – AND-joined conditions */ ],
    [ /* second slice (if comparing)         */ ],
    /* more slices if user asks for them     */
  ]
}}

* One inner list ⇒ single slice.
* Two or more inner lists ⇒ comparison slices, in the order mentioned by the user.
* Do **not** add extra keys.

### 6. Examples

#### Single slice

**User**: "transactions over $100 in December"

{{
  "filters": [
    [
      {{ "field": "amount",            "operator": ">",  "value": 100 }},
      {{ "field": "transaction_date",  "operator": "isin",
        "value": ["2024-12-01","2024-12-31"] }}
    ]
  ]
}}

#### Two-slice comparison

**User**: "compare hotel spend last year with food spend on my gold card"

{{
  "filters": [
    [
      {{ "field": "transaction.receiver.category_type", "operator": "is", "value": "hotel" }},
      {{ "field": "transaction_date",                   "operator": "isin",
        "value": ["2024-01-01","2024-12-31"] }}
    ],
    [
      {{ "field": "transaction.receiver.category_type", "operator": "is", "value": "food" }},
      {{ "field": "card_type",                          "operator": "is", "value": "GOLD" }}
    ]
  ]
}}

#### Query with sorting and limiting

**User**: "show me my top 10 highest transactions sorted by amount"

{{
  "filters": [
    [
      {{ "field": "amount", "operator": ">", "value": 0, "sort_field": "amount", "sort_order": "desc", "limit": 10 }}
    ]
  ]
}}

#### Query with grouping

**User**: "give me my expenses by location"

{{
  "filters": [
    [
      {{ "field": "amount", "operator": ">", "value": 0, "group_by": "transaction.receiver.location" }}
    ]
  ]
}}

### 7. Edge cases & rules

* If the query clearly says "compare A with B", output exactly two slices.
* For ≥3 comparisons, output one slice per dataset.
* If the query references unknown fields, reply with an empty `filters` list.
* Never produce invalid enum values or non-ISO dates.

Now read the user's question and output **only** the JSON object described above.
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


def FiltersToDsl(query_filters: dict) -> List[Dict[str, Any]]:
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
        sort_config = None
        limit_config = None
        group_by_config = None

        if not filter_slice:
            elastic_query = {"query": {"match_all": {}}}
        else:
            for filter_condition in filter_slice:
                field = filter_condition["field"]
                operator = filter_condition["operator"]
                value = filter_condition["value"]

                # Extract sort, limit, and group_by from the first condition that has them
                if filter_condition.get("sort_field") and not sort_config:
                    sort_field = filter_condition["sort_field"]
                    sort_order = filter_condition.get("sort_order", "asc")
                    sort_config = {sort_field: {"order": sort_order}}

                if filter_condition.get("limit") and not limit_config:
                    limit_config = filter_condition["limit"]

                if filter_condition.get("group_by") and not group_by_config:
                    group_by_config = filter_condition["group_by"]

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

            elastic_query = {"query": {"bool": {"must": must_clauses}}} if must_clauses else {"query": {"match_all": {}}}

        # Add sorting if specified
        if sort_config:
            elastic_query["sort"] = [sort_config]  # type: ignore

        # Add limit if specified
        if limit_config:
            elastic_query["size"] = limit_config  # type: ignore

        # Add group by aggregation if specified
        if group_by_config:
            group_field = _keyword_field(group_by_config) if isinstance(group_by_config, str) else group_by_config
            elastic_query["aggs"] = {  # type: ignore
                "group_by": {
                    "terms": {
                        "field": group_field,
                        "size": limit_config or 100
                    },
                    "aggs": {
                        "total_amount": {
                            "sum": {"field": "amount"}
                        },
                        "count": {
                            "value_count": {"field": group_field}
                        }
                    }
                }
            }
            # For groupby queries, we typically want 0 hits and just aggregations
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
            self.es_client, index_name, category_fields, fields_to_ignore
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
        elastic_queries = FiltersToDsl(filters)  # type: ignore[arg-type]
        
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
        elastic_queries = FiltersToDsl(filters)  # type: ignore[arg-type]
        
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
        return FiltersToDsl(query_filters)
    
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
