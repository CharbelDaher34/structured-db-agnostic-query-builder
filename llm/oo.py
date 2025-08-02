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


class ElasticsearchModelGenerator:
    """
    A class that generates Pydantic models from Elasticsearch index mappings
    and creates corresponding filter extraction models for natural language queries.
    """

    _EXAMPLE_VALUES = {
        str: "example_string",
        int: 123,
        float: 1.23,
        bool: True,
        datetime: "2024-01-01T00:00:00Z",
        dict: {"key": "value"},
        list: ["item1", "item2"],
        Any: "any_value",
    }
    # Comprehensive type mapping from Elasticsearch to Python types
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
        "date": datetime,
        "object": Dict[str, Any],
        "nested": List[Any],
    }
    # Fields that should be ignored in the model
    IGNORED_FIELD_TYPES = {"alias"}

    # Example values for different types
    EXAMPLE_VALUES = {
        str: "example_string",
        int: 123,
        float: 1.23,
        bool: True,
        datetime: "2024-01-01T00:00:00Z",
        dict: {"key": "value"},
        list: ["item1", "item2"],
        Any: "any_value",
    }

    def __init__(
        self,
        index_name: str,
        es_host: str = "http://elastic:rvs59tB_VVANUy4rC-kd@84.16.230.94:9200",
        fields_to_ignore: List[str] = None,
        category_fields: List[str] = None,
    ):
        """
        Initialize the model generator.

        Args:
            index_name: Name of the Elasticsearch index
            es_host: Elasticsearch host URL
            fields_to_ignore: List of field names to ignore in model generation
            category_fields: List of field names that should be converted to Enums
        """
        self.index_name = index_name
        self.es_host = es_host
        self.fields_to_ignore = fields_to_ignore or []
        self.category_fields = category_fields or []
        self.es_client = Elasticsearch(hosts=[es_host])
        self._model_class = None
        self._model_info = None
        self._filter_model_class = None
        self._llm_client = None

    def get_distinct_field_values(self, field_path: str, size: int = 1000) -> List[Any]:
        """
        Get distinct values for any field in an Elasticsearch index.
        Automatically handles regular fields, nested fields, and multi-fields.

        Args:
            index_name: Name of the index to query
            field_path: Full path to the field (e.g., "card_type.keyword", "transaction.receiver.name.keyword")
            es_host: Elasticsearch host URL
            size: Maximum number of distinct values to return (default: 1000)

        Returns:
            List of distinct values for the specified field
        """
        try:
            # Check if field is nested by looking for nested path in mapping
            mappings = self.es_client.indices.get_mapping(index=self.index_name)
            index_mapping = (
                mappings.get(self.index_name, {})
                .get("mappings", {})
                .get("properties", {})
            )

            # Determine if we need nested aggregation
            field_parts = field_path.split(".")
            nested_path = None

            # Check if any part of the path is a nested field
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

            # Build query based on whether field is nested or not
            if nested_path:
                # Nested field query
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

                response = self.es_client.search(index=self.index_name, body=query)
                buckets = (
                    response.get("aggregations", {})
                    .get("nested_agg", {})
                    .get("distinct_values", {})
                    .get("buckets", [])
                )
            else:
                # Regular field query
                query = {
                    "size": 0,
                    "aggs": {
                        "distinct_values": {
                            "terms": {"field": field_path, "size": size}
                        }
                    },
                }

                response = self.es_client.search(index=self.index_name, body=query)
                buckets = (
                    response.get("aggregations", {})
                    .get("distinct_values", {})
                    .get("buckets", [])
                )

            # Extract distinct values
            distinct_values = [bucket["key"] for bucket in buckets]
            return distinct_values

        except Exception as e:
            print(f"Error getting distinct values for field '{field_path}': {e}")
            return []

    def get_example_value(self, py_type):
        # Handle typing.Union, typing.List, etc.
        origin = getattr(py_type, "__origin__", None)
        if origin is Union:
            # Pick the first non-None type
            for arg in py_type.__args__:
                if arg is not type(None):
                    return self.get_example_value(arg)
        elif origin is list or origin is List:
            return [self.get_example_value(py_type.__args__[0])]
        elif origin is dict or origin is Dict:
            return {"key": self.get_example_value(py_type.__args__[1])}
        elif isinstance(py_type, type) and issubclass(py_type, Enum):
            # Handle Enum types - return the first enum value
            enum_values = list(py_type)
            if enum_values:
                try:
                    return enum_values[0].value
                except:
                    return enum_values[0].value
            return "enum_value"
        elif isinstance(py_type, type):
            return self._EXAMPLE_VALUES.get(py_type, f"example_{py_type.__name__}")
        return "example_value"

    def _get_index_mapping(
        self,
    ) -> Dict[str, Any]:
        """
        Extract properties from index mapping, handling different ES versions.
        """
        mappings = self.es_client.indices.get_mapping(index=self.index_name)
        index_mapping = mappings.get(self.index_name, {}).get("mappings", {})

        # Handle modern ES structure
        if "properties" in index_mapping:
            return index_mapping["properties"]

        # Handle older ES versions
        return index_mapping.get("properties", {})

    def _es_type_to_pydantic(
        self,
        es_mapping: Dict[str, Any],
        model_name: str = "ESModel",
        current_path: str = "",
    ) -> BaseModel:
        """
        Convert Elasticsearch mapping to a Pydantic model, handling nested structures recursively.
        Creates Enum fields for specified category fields using distinct values from ES.
        """
        fields: Dict[str, tuple] = {}

        for field_name, field_props in es_mapping.items():
            # Build the full field path for nested fields
            full_field_path = (
                f"{current_path}.{field_name}" if current_path else field_name
            )

            # Skip ignored field types
            if (
                field_props.get("type") in self.IGNORED_FIELD_TYPES
                or field_name in self.fields_to_ignore
            ):
                continue

            es_type = field_props.get("type")
            sub_props = field_props.get("properties")

            # Handle nested/object types with properties (recursive case)
            if sub_props:
                # Recursively create nested model
                nested_model_name = f"{model_name}_{field_name.capitalize()}"
                nested_model = self._es_type_to_pydantic(
                    sub_props,
                    nested_model_name,
                    full_field_path,  # Pass the current path to nested calls
                )

                # Handle nested arrays vs objects
                if es_type == "nested":
                    py_type = List[nested_model]
                else:
                    # For object type or when no type is specified but properties exist
                    py_type = nested_model

            # Check if this field is a category field that should be an Enum
            elif (
                field_name in self.category_fields
                or full_field_path in self.category_fields
            ):
                try:
                    # Get distinct values for this field using the full path
                    field_path = (
                        f"{full_field_path}.keyword"
                        if es_type == "text"
                        else full_field_path
                    )
                    distinct_values = self.get_distinct_field_values(field_path)
                    print(f"Distinct values for {field_path}: {distinct_values}")

                    if distinct_values:
                        # Create enum class name
                        enum_class_name = f"{model_name}_{field_name.capitalize()}Enum"

                        # Create enum members dict - handle special characters in values
                        enum_members = {}
                        for i, value in enumerate(distinct_values):
                            # Create valid Python identifier for enum member
                            if isinstance(value, str):
                                # Replace special characters and spaces
                                member_name = (
                                    value.replace(" ", "_")
                                    .replace("-", "_")
                                    .replace("'", "")
                                    .replace(".", "_")
                                )
                                # Ensure it starts with letter or underscore
                                if (
                                    not member_name[0].isalpha()
                                    and member_name[0] != "_"
                                ):
                                    member_name = f"_{member_name}"
                                # Remove any remaining invalid characters
                                member_name = "".join(
                                    c for c in member_name if c.isalnum() or c == "_"
                                )
                                # Ensure it's not empty
                                if not member_name:
                                    member_name = f"VALUE_{i}"
                            else:
                                member_name = f"VALUE_{i}"

                            enum_members[member_name.upper()] = value

                        # Create the Enum class
                        category_enum = Enum(enum_class_name, enum_members)
                        py_type = category_enum
                    else:
                        # Fallback to string if no distinct values found
                        base_type = self.ES_TYPE_MAP.get(es_type, str)
                        py_type = base_type
                except Exception as e:
                    print(f"Error creating enum for field '{full_field_path}': {e}")
                    # Fallback to string type
                    base_type = self.ES_TYPE_MAP.get(es_type, str)
                    py_type = base_type

            # Handle fields that don't have properties but might have multi-fields
            elif "fields" in field_props:
                # Use the main field type, ignore sub-fields for now
                base_type = self.ES_TYPE_MAP.get(
                    es_type, str
                )  # Default to str for text fields
                py_type = base_type

            # Handle primitive fields
            else:
                # Get base type or default to Any
                base_type = self.ES_TYPE_MAP.get(es_type, Any)
                py_type = base_type

            # All fields are optional with None default
            if isinstance(py_type, type) and issubclass(py_type, Enum):
                # Enum fields are optional with None default
                fields[field_name] = (Optional[py_type], Field(default=None))
            elif isinstance(py_type, type) and issubclass(py_type, BaseModel):
                # Nested model fields are required (non-optional)
                fields[field_name] = (py_type, Field(...))
            elif hasattr(py_type, "__origin__") and py_type.__origin__ is list:
                # Check if it's List[BaseModel] - nested model arrays are required
                args = getattr(py_type, "__args__", ())
                if (
                    args
                    and isinstance(args[0], type)
                    and issubclass(args[0], BaseModel)
                ):
                    # List of nested models are required (non-optional)
                    fields[field_name] = (py_type, Field(...))
                else:
                    # Other lists are optional with None default
                    fields[field_name] = (Optional[py_type], Field(default=None))
            else:
                # Other fields are optional with None default
                fields[field_name] = (Optional[py_type], Field(default=None))

        # Create the model class using create_model
        model_class = create_model(model_name, **fields)  # type: ignore

        # Return the class itself
        return model_class

    def generate_model(self, model_name: Optional[str] = None) -> BaseModel:
        """
        Generate Pydantic model from Elasticsearch index mapping.

        Args:
            model_name: Optional custom name for the model

        Returns:
            Pydantic BaseModel class
        """
        if self._model_class is None:
            properties = self._get_index_mapping()
            model_name = model_name or f"ES_{self.index_name.capitalize()}"
            self._model_class = self._es_type_to_pydantic(
                properties, model_name=model_name, current_path=""
            )
        return self._model_class

    def _extract_model_info(self, model_class: type[BaseModel], prefix: str = ""):
        """
        Extract field information from a Pydantic model class.
        Flattens all fields into a single dictionary using dot notation for nested fields.

        Args:
            model_class: A Pydantic BaseModel class (not an instance)
            prefix: Prefix for nested field names (used internally for recursion)

        Returns:
            Dict containing flattened field information with types and enum values
        """
        info = {}

        # Use model_fields to get field information from Pydantic models
        for field_name, field_info in model_class.model_fields.items():
            field_type = field_info.annotation
            origin = get_origin(field_type)
            args = get_args(field_type)

            # Create the full field name with prefix
            full_field_name = f"{prefix}.{field_name}" if prefix else field_name

            # Handle Optional types (Union[T, None])
            if origin is Union:
                # Get the first non-None type from Union
                non_none_types = [arg for arg in args if arg is not type(None)]
                if non_none_types:
                    field_type = non_none_types[0]
                    origin = get_origin(field_type)
                    args = get_args(field_type)

            # Handle Enum types
            if inspect.isclass(field_type) and issubclass(field_type, Enum):
                info[full_field_name] = {
                    "type": "enum",
                    "values": [e.value for e in field_type],
                }
            # Handle nested BaseModel types - recursively flatten them
            elif inspect.isclass(field_type) and issubclass(field_type, BaseModel):
                # Recursively extract nested fields and merge them
                nested_info = self._extract_model_info(field_type, full_field_name)
                info.update(nested_info)
            # Handle List types
            elif origin is list or origin is List:
                if args:
                    list_item_type = args[0]
                    if inspect.isclass(list_item_type) and issubclass(
                        list_item_type, BaseModel
                    ):
                        # For arrays of objects, we'll flatten the object structure
                        # but indicate it's an array field
                        nested_info = self._extract_model_info(
                            list_item_type, full_field_name
                        )
                        # Mark each nested field as being part of an array
                        for nested_field, nested_field_info in nested_info.items():
                            nested_field_info["is_array_item"] = True
                        info.update(nested_info)
                    elif inspect.isclass(list_item_type) and issubclass(
                        list_item_type, Enum
                    ):
                        info[full_field_name] = {
                            "type": "array",
                            "item_type": "enum",
                            "values": [e.value for e in list_item_type],
                        }
                    else:
                        info[full_field_name] = {
                            "type": "array",
                            "item_type": self._get_simple_type_name(list_item_type),
                        }
                else:
                    info[full_field_name] = {"type": "array", "item_type": "unknown"}
            # Handle basic types
            elif field_type == str:
                info[full_field_name] = {"type": "string"}
            elif field_type in (int, float):
                info[full_field_name] = {"type": "number"}
            elif field_type == bool:
                info[full_field_name] = {"type": "boolean"}
            elif field_type in (date, datetime):
                info[full_field_name] = {"type": "date"}
            else:
                info[full_field_name] = {"type": self._get_simple_type_name(field_type)}

        return info

    def _get_simple_type_name(self, field_type) -> str:
        """Helper function to get a simple string representation of a type."""
        if hasattr(field_type, "__name__"):
            return field_type.__name__
        elif hasattr(field_type, "_name"):
            return field_type._name
        else:
            return str(field_type)

    def get_model_info(self) -> Dict[str, Any]:
        """
        Get flattened field information from the generated model.

        Returns:
            Dictionary containing field information with types and enum values
        """
        if self._model_info is None:
            model = self.generate_model()
            self._model_info = self._extract_model_info(model)
        return self._model_info

    def generate_filter_model(self) -> BaseModel:
        """
        Generate a Pydantic model for filter extraction based on the main model.

        Returns:
            Pydantic BaseModel class for query filters
        """
        if self._filter_model_class is None:
            model_info = self.get_model_info()

            # Create operator enum
            class OperatorEnum(str, Enum):
                lt = "<"
                gt = ">"
                isin = "isin"
                notin = "notin"
                eq = "is"
                ne = "different"
                be = "between"

            # Create field enum from model info
            field_enum_members = {k: k for k in model_info.keys()}
            FieldEnum = Enum("FieldEnum", field_enum_members)

            # Store model_info in a way that the validator can access it
            _model_info = model_info

            # Create Query class with validation
            class Query(BaseModel):
                field: FieldEnum
                operator: OperatorEnum
                value: Union[str, float, int, List[str], List[date]]

                @field_validator("value")
                def validate_value(cls, v, info: ValidationInfo):
                    if "field" not in info.data or "operator" not in info.data:
                        return v

                    field = info.data["field"].value
                    op = info.data["operator"].value
                    field_info = _model_info[field]
                    ftype = field_info["type"]

                    def fail(msg):
                        raise ValueError(
                            f"Invalid value for field '{field}' with type '{ftype}': {msg}"
                        )

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

            # Create QueryFilters class
            class QueryFilters(BaseModel):
                filters: list[list[Query]] = Field(
                    description="Filtering conditions for the query"
                )

            self._filter_model_class = QueryFilters
        return self._filter_model_class

    def generate_system_prompt(self) -> str:
        """
        Generate a system prompt for LLM-based filter extraction supporting
        single-slice queries **and** multi-slice comparisons.
        """
        model_info = self.get_model_info()

        return f"""
Today is {datetime.now().strftime("%Y-%m-%d")}

You are an expert assistant that converts **natural-language questions into JSON filters** for an analytics engine.

The user might ask for:
• a **single slice** of data – e.g. "show my restaurant purchases", or  
• a **comparison of multiple slices** – e.g. "compare hotel spend last year with food spend on my gold card".

### 1. Available Schema
{json.dumps(model_info, indent=2)}

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

### 3. Reasoning steps
1. Detect time references and convert to ISO dates.  
2. Extract entities (amounts, categories, card types …).  
3. Determine intent: single slice vs multi-slice comparison.  
4. Map entities to schema fields.  
5. Choose operators.  
6. Validate values (enum membership, numeric type, date format).

### 4. Output format  ❗️
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

### 5. Examples

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

### 6. Edge cases & rules

* If the query clearly says "compare A with B", output exactly two slices.
* For ≥3 comparisons, output one slice per dataset.
* If the query references unknown fields, reply with an empty `filters` list.
* Never produce invalid enum values or non-ISO dates.

Now read the user's question and output **only** the JSON object described above.
"""

    def populate_with_examples(self, model_instance: BaseModel = None) -> dict:
        """
        Populate a model instance with example values recursively.

        Args:
            model_instance: Optional model instance to populate, if None uses generated model

        Returns:
            Dictionary with example values for all fields
        """
        if model_instance is None:
            model_class = self.generate_model()
            model_instance = model_class

        result = {}
        for name, field_info in model_instance.model_fields.items():
            annotation = field_info.annotation

            # Handle Optional types by extracting the inner type
            origin = get_origin(annotation)
            if origin is Union:
                # Get the first non-None type from Union (Optional creates Union[T, None])
                inner_types = [
                    arg for arg in annotation.__args__ if arg is not type(None)
                ]
                if inner_types:
                    annotation = inner_types[0]

            # Check if it's a BaseModel subclass (nested model)
            if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                result[name] = self.populate_with_examples(annotation)
            else:
                result[name] = self.get_example_value(annotation)
        return result

    def print_model_summary(self):
        """Print a summary of the generated model and its fields."""
        model = self.generate_model()
        model_info = self.get_model_info()

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
                    print(
                        f"    Sample Values: {values}"
                    )  # ... (+{len(values)-5} more)")
            elif field_type == "array":
                item_type = field_info.get("item_type", "unknown")
                print(f"  {field_name}: {field_type}[{item_type}]")
                if "values" in field_info:
                    values = field_info["values"]
                    print(
                        f"    Enum Values: {values}"
                    )  # {'...' if len(values) > 5 else ''}")
            else:
                print(f"  {field_name}: {field_type}")
                if field_info.get("is_array_item"):
                    print(f"    (Part of array structure)")

    def generate_filters_from_query(self, query: str):
        """
        Generate filters from a natural language query using the LLM agent.

        Args:
            query: Natural language query string

        Returns:
            Dictionary containing the parsed filters
        """
        if self._llm_client is None:
            self._llm_client = LLM(
                model="ollama/qwen3:8b",
                result_type=self.generate_filter_model(),
                system_prompt=self.generate_system_prompt(),
                api_key="key",
            )

        # Use the synchronous parse method
        result = self._llm_client.llm_agent.run_sync([query])
        return result

    async def generate_filters_from_query_async(self, query: str):
        """
        Generate filters from a natural language query using the LLM agent asynchronously.

        Args:
            query: Natural language query string

        Returns:
            Dictionary containing the parsed filters
        """
        if self._llm_client is None:
            self._llm_client = LLM(
                model="gemini-2.0-flash",
                result_type=self.generate_filter_model(),
                system_prompt=self.generate_system_prompt(),
                api_key="AIzaSyDp8n_AmYsspADJBaNpkJvBdlch1-9vkhw",
            )

        # Use the asynchronous parse method
        result = await self._llm_client.llm_agent.run([query])
        return result

    def FilterToElasticQuery(self, query_filters: dict) -> List[Dict[str, Any]]:
        """
        Convert QueryFilters model output to Elasticsearch query DSL.
        
        Args:
            query_filters: Dictionary containing filters from the model output
            
        Returns:
            List of Elasticsearch queries, one for each filter slice
        """
        if not query_filters or "filters" not in query_filters:
            return []

        def _keyword_field(f: str) -> str:
            """Return field.keyword unless field already has a dot path at end or appears numeric/date."""
            if f.endswith(".keyword"):
                return f
            # heuristics: if last part of path is already 'keyword', skip
            last = f.split(".")[-1]
            # If last part likely numeric/date field names we still try keyword. We cannot know. We'll just add keyword.
            return f"{f}.keyword"

        elastic_queries: List[Dict[str, Any]] = []

        for filter_slice in query_filters["filters"]:
            if not filter_slice:
                continue

            must_clauses: List[Dict[str, Any]] = []

            for filter_condition in filter_slice:
                field = filter_condition["field"]
                operator = filter_condition["operator"]
                value = filter_condition["value"]

                # Determine if field likely string (heuristic: value is str and not ISO date pattern)
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
            elastic_queries.append(elastic_query)

        return elastic_queries

    def ExecuteElasticQueries(self, elastic_queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute Elasticsearch queries and return results.
        
        Args:
            elastic_queries: List of Elasticsearch query DSL dictionaries
            
        Returns:
            List of query results
        """
        if not elastic_queries:
            return []

        results = []
        for query in elastic_queries:
            try:
                response = self.es_client.search(index=self.index_name, body=query)
                
                # Extract relevant information from the response
                result = {
                    "total_hits": response["hits"]["total"]["value"],
                    "documents": [hit["_source"] for hit in response["hits"]["hits"]]
                }
                
                results.append(result)
                
            except Exception as e:
                results.append({
                    "error": str(e),
                    "total_hits": 0,
                    "documents": []
                })
        
        return results

    def QueryFromNaturalLanguage(self, query: str, execute: bool = True, size: int = 100) -> Dict[str, Any]:
        """
        Complete pipeline: Convert natural language to filters, then to Elasticsearch queries, and optionally execute.
        
        Args:
            query: Natural language query string
            execute: Whether to execute the queries against Elasticsearch
            size: Maximum number of results per query slice
            
        Returns:
            Dictionary containing filters, elastic queries, and optionally results
        """
        # Generate filters from natural language
        filters = self.generate_filters_from_query(query)
        
        # Convert to Elasticsearch queries
        elastic_queries = self.FilterToElasticQuery(filters)
        
        response = {
            "natural_language_query": query,
            "extracted_filters": filters,
            "elasticsearch_queries": elastic_queries
        }
        
        # Execute queries if requested
        if execute and elastic_queries:
            results = self.ExecuteElasticQueries(elastic_queries)
            response["results"] = results
            
        return response

    async def QueryFromNaturalLanguageAsync(self, query: str, execute: bool = True, size: int = 100) -> Dict[str, Any]:
        """
        Async version of complete pipeline.
        """
        # Generate filters from natural language
        filters = await self.generate_filters_from_query_async(query)
        
        # Convert to Elasticsearch queries
        elastic_queries = self.FilterToElasticQuery(filters)
        
        response = {
            "natural_language_query": query,
            "extracted_filters": filters,
            "elasticsearch_queries": elastic_queries
        }
        
        # Execute queries if requested
        if execute and elastic_queries:
            results = self.ExecuteElasticQueries(elastic_queries)
            response["results"] = results
            
        return response

    def debug_category_fields(self) -> Dict[str, Any]:
        """
        Debug method to show which category fields are being processed and their enum values.
        
        Returns:
            Dictionary containing debug information about category fields
        """
        debug_info = {
            "configured_category_fields": self.category_fields,
            "processed_fields": {},
            "errors": []
        }
        
        try:
            # Get the mapping to understand field structure
            properties = self._get_index_mapping()
            
            # Check each configured category field
            for category_field in self.category_fields:
                try:
                    # Determine the correct field path for querying
                    field_path = f"{category_field}.keyword" if "." in category_field else category_field
                    
                    # Try to get distinct values
                    distinct_values = self.get_distinct_field_values(field_path)
                    
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
            
            # Also check what fields exist in the mapping
            debug_info["available_fields"] = self._get_all_field_paths(properties)
            
        except Exception as e:
            debug_info["errors"].append(f"General error in debug_category_fields: {str(e)}")
            print(f"❌ General error: {str(e)}")
        
        return debug_info
    
    def _get_all_field_paths(self, mapping: Dict[str, Any], prefix: str = "") -> List[str]:
        """Helper method to get all available field paths from mapping."""
        paths = []
        
        for field_name, field_props in mapping.items():
            full_path = f"{prefix}.{field_name}" if prefix else field_name
            paths.append(full_path)
            
            # If field has properties (nested), recurse
            if "properties" in field_props:
                nested_paths = self._get_all_field_paths(field_props["properties"], full_path)
                paths.extend(nested_paths)
        
        return paths

    def run_raw_elastic_query(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        """
        Execute a raw Elasticsearch query directly.
        
        Args:
            query: Raw Elasticsearch query dictionary
            size: Maximum number of results to return
            
        Returns:
            Dictionary containing query results or error information
        """
        try:
            # Add size parameter if not already set
            if "size" not in query:
                query["size"] = size
            
            # Execute the query
            response = self.es_client.search(index=self.index_name, body=query)
            
            # Parse results
            result = {
                "total_hits": response["hits"]["total"]["value"] if isinstance(response["hits"]["total"], dict) else response["hits"]["total"],
                "documents": [hit["_source"] for hit in response["hits"]["hits"]],
                "query": query,
                "success": True
            }
            
            # Add aggregation results if present
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

        print(f"Completed processing all queries.")

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
