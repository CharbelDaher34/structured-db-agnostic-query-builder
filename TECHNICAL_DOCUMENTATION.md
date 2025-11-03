# Elasticsearch Model Generator - Technical Documentation

## Overview

This module provides a three-stage pipeline for converting natural language queries into Elasticsearch DSL queries:

1. **Schema Model Generation** (`ModelBuilder`) - Converts Elasticsearch mappings to Pydantic models
2. **Filter Model Generation** (`FilterModelBuilder`) - Creates structured filter models for LLM consumption
3. **DSL Conversion** (`FiltersToDsl`) - Transforms filter models into Elasticsearch queries

---

## Stage 1: Schema Model Generation (`ModelBuilder`)

### Purpose
Dynamically generates Pydantic models from Elasticsearch index mappings, enabling type-safe query construction and validation.

### Input Models

#### Constructor Parameters
```python
ModelBuilder(
    es_client: Optional[Elasticsearch] = None,      # ES client for live connection
    index_name: Optional[str] = None,               # Target index name
    category_fields: Optional[List[str]] = None,    # Fields to treat as enums
    fields_to_ignore: Optional[List[str]] = None,   # Fields to exclude
    mapping: Optional[Dict[str, Any]] = None,       # Pre-fetched mapping (offline mode)
    enum_fields: Optional[Dict[str, List[Any]]] = None,  # Pre-defined enum values
    es_host: Optional[str] = None                   # ES host URL
)
```

**Operating Modes:**
- **ES Mode**: Requires `es_client` + `index_name` - Fetches mapping from live cluster
- **Mapping Mode**: Requires `mapping` - Works with pre-fetched mapping (no DB connection)

#### Elasticsearch Mapping Structure (Input)
```json
{
  "properties": {
    "transaction": {
      "properties": {
        "amount": { "type": "float" },
        "timestamp": { "type": "date" },
        "receiver": {
          "properties": {
            "name": { "type": "text" },
            "category_type": { "type": "keyword" }
          }
        }
      }
    },
    "card_type": { "type": "keyword" }
  }
}
```

### Processing Logic

#### Type Mapping
The module maps Elasticsearch types to Python types:

```python
ES_TYPE_MAP = {
    "text": str, 
    "keyword": str, 
    "integer": int, 
    "long": int,
    "double": float, 
    "float": float,
    "boolean": bool, 
    "date": datetime,
    "object": Dict[str, Any], 
    "nested": List[Any]
}
```

#### Field Processing Rules

1. **Simple Fields**: Direct type mapping
   ```python
   "amount": float → (Optional[float], Field(default=None))
   ```

2. **Nested Objects**: Recursive model creation
   ```python
   "receiver": {...} → ReceiverModel (nested Pydantic model)
   ```

3. **Category Fields**: Convert to Enums
   - If field in `category_fields` list
   - Fetches distinct values from ES
   - Creates Python Enum dynamically
   ```python
   "card_type": ["GOLD", "PLATINUM"] → Enum('CardTypeEnum', {...})
   ```

4. **Nested Arrays**: List of nested models
   ```python
   "type": "nested" → List[NestedModel]
   ```

### Output Models

#### 1. Pydantic Model Class
```python
# Generated dynamically via create_model()
class ES_UserTransactions(BaseModel):
    card_type: Optional[CardTypeEnum] = None
    transaction: Transaction_Model = ...
    
class Transaction_Model(BaseModel):
    amount: Optional[float] = None
    timestamp: Optional[datetime] = None
    receiver: Receiver_Model = ...
```

#### 2. Model Info Dictionary
Flattened field metadata for downstream processing:

```python
{
    "card_type": {
        "type": "enum",
        "values": ["GOLD", "PLATINUM", "CLASSIC"]
    },
    "transaction.amount": {
        "type": "number"
    },
    "transaction.timestamp": {
        "type": "date"
    },
    "transaction.receiver.name": {
        "type": "string"
    },
    "transaction.receiver.category_type": {
        "type": "enum",
        "values": ["food", "travel", "shopping"]
    }
}
```

**Key Method:**
```python
model_info = model_builder.GetModelInfo()  # Returns flattened dict above
```

---

## Stage 2: Filter Model Generation (`FilterModelBuilder`)

### Purpose
Creates a structured Pydantic model that LLMs can output to represent query filters, aggregations, sorting, and grouping operations.

### Input Model

```python
FilterModelBuilder(
    model_info: Dict[str, Any]  # Output from ModelBuilder.GetModelInfo()
)
```

**Input Structure (model_info):**
```python
{
    "transaction.amount": {"type": "number"},
    "transaction.timestamp": {"type": "date"},
    "card_type": {"type": "enum", "values": ["GOLD", "PLATINUM"]},
    # ... more fields
}
```

### Output Models

The builder generates a comprehensive Pydantic model hierarchy:

#### 1. Core Enums

```python
class OperatorEnum(str, Enum):
    lt = "<"              # Less than (numbers, dates)
    gt = ">"              # Greater than (numbers, dates)
    isin = "isin"         # Value in list
    notin = "notin"       # Value not in list
    eq = "is"             # Equals
    ne = "different"      # Not equal
    be = "between"        # Range (dates, numbers)
    contains = "contains" # Partial string match
    exists = "exists"     # Field exists check

class SortOrderEnum(str, Enum):
    asc = "asc"
    desc = "desc"

class AggregationEnum(str, Enum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"

class TimeIntervalEnum(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
```

#### 2. Dynamic FieldEnum
Generated from `model_info` keys:

```python
FieldEnum = Enum("FieldEnum", {
    "transaction.amount": "transaction.amount",
    "transaction.timestamp": "transaction.timestamp",
    "card_type": "card_type",
    # ... all fields from model_info
})
```

#### 3. Query Condition Model

```python
class Query(BaseModel):
    field: FieldEnum              # Which field to filter
    operator: OperatorEnum        # How to filter
    value: Union[                 # Filter value(s)
        str, float, int, bool, date,
        List[Union[str, float, int, date]],
        None
    ]
    
    @field_validator("value")
    def validate_value(cls, v, info: ValidationInfo):
        # Validates value against field type and operator
        # Example: "between" requires list of 2 values
        # Example: enum fields validate against allowed values
        ...
```

**Example Query Instances:**
```python
# Simple equality
Query(
    field="card_type",
    operator="is",
    value="GOLD"
)

# Date range
Query(
    field="transaction.timestamp",
    operator="between",
    value=["2024-01-01", "2024-12-31"]
)

# Numeric comparison
Query(
    field="transaction.amount",
    operator=">",
    value=1000
)
```

#### 4. Aggregation Model

```python
class Aggregation(BaseModel):
    field: FieldEnum                          # Field to aggregate
    type: AggregationEnum                     # Aggregation type
    having_operator: Optional[OperatorEnum]   # Post-aggregation filter
    having_value: Optional[Union[str, int, float]]  # Having threshold
```

**Example:**
```python
Aggregation(
    field="transaction.amount",
    type="sum",
    having_operator=">",
    having_value=1000
)
# Translates to: "SUM(amount) > 1000" (HAVING clause)
```

#### 5. Sort Field Model

```python
class SortField(BaseModel):
    field: FieldEnum
    order: SortOrderEnum = SortOrderEnum.asc
```

#### 6. Query Slice Model
Represents a single query with all its parameters:

```python
class QuerySlice(BaseModel):
    conditions: List[Query]                    # AND-joined filters
    sort: Optional[List[SortField]] = None     # Multi-field sorting
    limit: Optional[int] = None                # Result limit
    group_by: Optional[List[FieldEnum]] = None # Grouping fields
    aggregations: Optional[List[Aggregation]] = None  # Metrics
    interval: Optional[TimeIntervalEnum] = TimeIntervalEnum.MONTH
    
    @model_validator(mode='after')
    def validate_slice(self) -> 'QuerySlice':
        # Enforces logical constraints:
        # - aggregations/interval require group_by
        # - interval only for date fields
        # - removes invalid combinations
        ...
```

**Example Query Slice:**
```python
QuerySlice(
    conditions=[
        Query(field="card_type", operator="is", value="GOLD"),
        Query(field="transaction.timestamp", operator="between", 
              value=["2024-01-01", "2024-12-31"])
    ],
    group_by=["transaction.timestamp"],
    interval="month",
    aggregations=[
        Aggregation(field="transaction.amount", type="sum")
    ],
    limit=100
)
```

#### 7. Root Filter Model

```python
class QueryFilters(BaseModel):
    filters: List[QuerySlice]  # Multiple slices for comparisons
```

**Example (Comparison Query):**
```python
QueryFilters(
    filters=[
        QuerySlice(  # Slice 1: GOLD cards
            conditions=[Query(field="card_type", operator="is", value="GOLD")],
            aggregations=[Aggregation(field="transaction.amount", type="sum")]
        ),
        QuerySlice(  # Slice 2: PLATINUM cards
            conditions=[Query(field="card_type", operator="is", value="PLATINUM")],
            aggregations=[Aggregation(field="transaction.amount", type="sum")]
        )
    ]
)
```

### System Prompt Generation

The builder also generates an LLM system prompt that:
1. Documents all available fields from `model_info`
2. Explains all operators and their constraints
3. Provides 10+ realistic examples
4. Includes validation rules and guardrails

**Output:**
```python
system_prompt = filter_builder.GenerateSystemPrompt()
# Returns multi-page string with schema, examples, rules
```

---

## Stage 3: DSL Conversion (`FiltersToDsl`)

### Purpose
Converts the structured filter model (LLM output) into executable Elasticsearch DSL queries.

### Input Model

```python
def FiltersToDsl(
    query_filters: dict,        # Serialized QueryFilters from LLM
    model_info: Dict[str, Any]  # Field metadata for type resolution
) -> List[Dict[str, Any]]
```

**Input Structure (query_filters):**
```python
{
    "filters": [
        {
            "conditions": [
                {"field": "card_type", "operator": "is", "value": "GOLD"},
                {"field": "transaction.timestamp", "operator": "between", 
                 "value": ["2024-01-01", "2024-12-31"]}
            ],
            "group_by": ["transaction.timestamp"],
            "interval": "month",
            "aggregations": [
                {"field": "transaction.amount", "type": "sum"}
            ],
            "sort": [{"field": "transaction.amount", "order": "desc"}],
            "limit": 10
        }
    ]
}
```

### Processing Logic

#### 1. Operator Translation

| Filter Operator | Elasticsearch DSL |
|----------------|-------------------|
| `">"` | `{"range": {field: {"gt": value}}}` |
| `"<"` | `{"range": {field: {"lt": value}}}` |
| `"is"` | `{"term": {field.keyword: value}}` |
| `"different"` | `{"bool": {"must_not": {"term": {...}}}}` |
| `"isin"` | `{"terms": {field.keyword: [values]}}` |
| `"notin"` | `{"bool": {"must_not": {"terms": {...}}}}` |
| `"between"` | `{"range": {field: {"gte": val1, "lte": val2}}}` |
| `"contains"` | `{"wildcard": {field.keyword: {"value": "*val*"}}}` |
| `"exists"` | `{"exists": {"field": field}}` |

#### 2. Field Type Resolution

```python
def _keyword_field(field: str) -> str:
    """Appends .keyword for string/enum fields to ensure exact matching"""
    return f"{field}.keyword" if not field.endswith(".keyword") else field
```

**Logic:**
- String/enum fields → Use `.keyword` for exact matching
- Numeric/date fields → Use base field name
- Check `model_info` for field type

#### 3. Conditions Processing

```python
# Input conditions (AND-joined)
conditions = [
    {"field": "card_type", "operator": "is", "value": "GOLD"},
    {"field": "transaction.amount", "operator": ">", "value": 1000}
]

# Output DSL
{
    "query": {
        "bool": {
            "must": [
                {"term": {"card_type.keyword": "GOLD"}},
                {"range": {"transaction.amount": {"gt": 1000}}}
            ]
        }
    }
}
```

#### 4. Sort Processing

```python
# Input
"sort": [
    {"field": "transaction.amount", "order": "desc"},
    {"field": "transaction.timestamp", "order": "asc"}
]

# Output DSL
{
    "sort": [
        {"transaction.amount": {"order": "desc"}},
        {"transaction.timestamp": {"order": "asc"}}
    ]
}
```

#### 5. Group By & Aggregations

This is the most complex transformation.

##### Simple Grouping (Non-Date Field)

```python
# Input
{
    "group_by": ["card_type"],
    "aggregations": [
        {"field": "transaction.amount", "type": "sum"}
    ]
}

# Output DSL
{
    "query": {...},
    "size": 0,  # Don't return documents, only aggregations
    "aggs": {
        "group_by_0": {
            "terms": {
                "field": "card_type.keyword",
                "size": 100
            },
            "aggs": {
                "sum_transaction_amount": {
                    "sum": {"field": "transaction.amount"}
                },
                "documents": {
                    "top_hits": {"size": 100}
                }
            }
        }
    }
}
```

##### Date Histogram Grouping

```python
# Input
{
    "group_by": ["transaction.timestamp"],
    "interval": "month",
    "aggregations": [
        {"field": "transaction.amount", "type": "sum"},
        {"field": "transaction.amount", "type": "count"}
    ]
}

# Output DSL
{
    "query": {...},
    "size": 0,
    "aggs": {
        "group_by_0": {
            "date_histogram": {
                "field": "transaction.timestamp",
                "calendar_interval": "month",
                "format": "yyyy-MM"  # Format varies by interval
            },
            "aggs": {
                "sum_transaction_amount": {
                    "sum": {"field": "transaction.amount"}
                },
                "count_transaction_amount": {
                    "value_count": {"field": "transaction.amount"}
                },
                "documents": {
                    "top_hits": {"size": 100}
                }
            }
        }
    }
}
```

**Interval Format Mapping:**
```python
format_map = {
    "day": "yyyy-MM-dd",
    "week": "yyyy-'W'ww",
    "month": "yyyy-MM",
    "year": "yyyy"
}
```

##### Multi-Level Grouping

```python
# Input
{
    "group_by": ["transaction.currency", "transaction.receiver.location"],
    "aggregations": [
        {"field": "transaction.amount", "type": "min"},
        {"field": "transaction.amount", "type": "max"}
    ]
}

# Output DSL (nested aggregations)
{
    "aggs": {
        "group_by_0": {  # First level: currency
            "terms": {
                "field": "transaction.currency.keyword",
                "size": 100
            },
            "aggs": {
                "group_by_1": {  # Second level: location
                    "terms": {
                        "field": "transaction.receiver.location.keyword",
                        "size": 100
                    },
                    "aggs": {  # Metrics at deepest level
                        "min_transaction_amount": {
                            "min": {"field": "transaction.amount"}
                        },
                        "max_transaction_amount": {
                            "max": {"field": "transaction.amount"}
                        },
                        "documents": {
                            "top_hits": {"size": 100}
                        }
                    }
                }
            }
        }
    }
}
```

##### HAVING Clause (Post-Aggregation Filtering)

```python
# Input
{
    "group_by": ["transaction.timestamp"],
    "interval": "day",
    "aggregations": [
        {
            "field": "transaction.id",
            "type": "count",
            "having_operator": ">",
            "having_value": 1
        }
    ]
}

# Output DSL (bucket_selector)
{
    "aggs": {
        "group_by_0": {
            "date_histogram": {...},
            "aggs": {
                "count_transaction_id": {
                    "value_count": {"field": "transaction.id.keyword"}
                },
                "having_filter": {
                    "bucket_selector": {
                        "buckets_path": {
                            "var_0": "count_transaction_id"
                        },
                        "script": "params.var_0 > 1"
                    }
                },
                "documents": {"top_hits": {"size": 100}}
            }
        }
    }
}
```

**Operator Mapping for Scripts:**
```python
op_map = {
    ">": ">",
    "<": "<",
    "is": "==",
    "different": "!=",
    ">=": ">=",
    "<=": "<="
}
```

#### 6. Aggregation Type Mapping

```python
aggregation_type_map = {
    "sum": {"sum": {"field": field}},
    "avg": {"avg": {"field": field}},
    "count": {"value_count": {"field": field}},
    "min": {"min": {"field": field}},
    "max": {"max": {"field": field}}
}
```

### Output Model

The function returns a **list** of Elasticsearch DSL queries (one per slice):

```python
[
    {
        "query": {
            "bool": {
                "must": [...]
            }
        },
        "sort": [...],
        "size": 10,
        "aggs": {...}
    },
    # ... more queries for comparison slices
]
```

**Complete Example:**

Input:
```python
{
    "filters": [
        {
            "conditions": [
                {"field": "card_type", "operator": "is", "value": "GOLD"},
                {"field": "transaction.timestamp", "operator": "between", 
                 "value": ["2024-01-01", "2024-12-31"]}
            ],
            "group_by": ["transaction.timestamp"],
            "interval": "month",
            "aggregations": [
                {"field": "transaction.amount", "type": "sum"}
            ]
        }
    ]
}
```

Output:
```python
[
    {
        "query": {
            "bool": {
                "must": [
                    {"term": {"card_type.keyword": "GOLD"}},
                    {"range": {"transaction.timestamp": {
                        "gte": "2024-01-01",
                        "lte": "2024-12-31"
                    }}}
                ]
            }
        },
        "size": 0,
        "aggs": {
            "group_by_0": {
                "date_histogram": {
                    "field": "transaction.timestamp",
                    "calendar_interval": "month",
                    "format": "yyyy-MM"
                },
                "aggs": {
                    "sum_transaction_amount": {
                        "sum": {"field": "transaction.amount"}
                    },
                    "documents": {
                        "top_hits": {"size": 100}
                    }
                }
            }
        }
    }
]
```

---

## Complete Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ Stage 1: Schema Model Generation (ModelBuilder)                    │
├─────────────────────────────────────────────────────────────────────┤
│ Input:  Elasticsearch Mapping (JSON)                                │
│ Output: Pydantic Model + model_info dict                            │
│                                                                      │
│ ES Mapping → Type Resolution → Enum Creation → Pydantic Model       │
│           └─→ GetModelInfo() → Flattened field metadata dict        │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│ Stage 2: Filter Model Generation (FilterModelBuilder)              │
├─────────────────────────────────────────────────────────────────────┤
│ Input:  model_info dict (from Stage 1)                              │
│ Output: QueryFilters Pydantic Model + System Prompt                 │
│                                                                      │
│ model_info → FieldEnum Creation → QueryFilters Model Structure      │
│           └─→ GenerateSystemPrompt() → LLM instructions             │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│ LLM Processing (External)                                           │
├─────────────────────────────────────────────────────────────────────┤
│ Input:  Natural Language Query + System Prompt                      │
│ Output: QueryFilters instance (dict)                                │
│                                                                      │
│ "Show spending by month" → LLM → QueryFilters JSON                  │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│ Stage 3: DSL Conversion (FiltersToDsl)                             │
├─────────────────────────────────────────────────────────────────────┤
│ Input:  QueryFilters dict + model_info                              │
│ Output: List of Elasticsearch DSL queries                           │
│                                                                      │
│ QueryFilters → Operator Translation → Field Type Resolution         │
│             → Aggregation Building → Elasticsearch DSL              │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│ Execution (Elasticsearch)                                            │
├─────────────────────────────────────────────────────────────────────┤
│ DSL Query → ES Search API → Results                                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Design Patterns

### 1. Dynamic Model Generation
Uses `pydantic.create_model()` to generate models at runtime based on ES schema.

### 2. Type Safety Through Validation
- Field validators ensure operator-value compatibility
- Model validators enforce logical query constraints
- Enum fields restrict values to schema-defined options

### 3. Separation of Concerns
- **ModelBuilder**: Schema → Types
- **FilterModelBuilder**: Types → Query Structure
- **FiltersToDsl**: Query Structure → Database Query

### 4. Nested Aggregation Building
Recursively builds nested aggregation structures for multi-level grouping.

### 5. Keyword Field Handling
Automatically appends `.keyword` to string/enum fields for exact matching in ES.

---

## Common Patterns

### Pattern 1: Simple Filter
```python
# Natural Language → Filter → DSL
"Show GOLD card transactions" 
→ {"field": "card_type", "operator": "is", "value": "GOLD"}
→ {"term": {"card_type.keyword": "GOLD"}}
```

### Pattern 2: Time-Based Aggregation
```python
"Monthly spending totals"
→ {"group_by": ["timestamp"], "interval": "month", 
    "aggregations": [{"field": "amount", "type": "sum"}]}
→ {"date_histogram": {"calendar_interval": "month"}, 
    "aggs": {"sum_amount": {"sum": {...}}}}
```

### Pattern 3: Comparison Query
```python
"Compare GOLD vs PLATINUM spending"
→ QueryFilters with 2 slices (one per card type)
→ 2 separate ES queries executed independently
```

### Pattern 4: Post-Aggregation Filter
```python
"Days with more than 1 transaction"
→ {"aggregations": [{"type": "count", "having_operator": ">", 
                      "having_value": 1}]}
→ {"bucket_selector": {"script": "params.var_0 > 1"}}
```

---

## Error Handling & Validation

### Stage 1 (ModelBuilder)
- Validates ES client availability for live mode
- Handles missing/malformed mappings
- Skips ignored field types (e.g., `alias`)

### Stage 2 (FilterModelBuilder)
- **Field Validator**: Checks value type against operator
  - `"between"` must have list of 2 values
  - `"contains"` only for strings
  - Enum values must be in allowed list
- **Model Validator**: Enforces structural constraints
  - Removes `aggregations` if no `group_by`
  - Removes `interval` if no date field in `group_by`

### Stage 3 (FiltersToDsl)
- Handles missing `filters` key (returns `match_all` query)
- Resolves field types from `model_info`
- Safely handles empty condition lists

---

## Performance Considerations

1. **Caching**: ModelBuilder caches mapping and model_info
2. **Lazy Generation**: Models only built when `Build()` called
3. **Batch Processing**: Multiple slices executed independently
4. **Top Hits Limitation**: Default 100 documents per bucket
5. **Terms Aggregation Size**: Default 100 unique values

---

## Extension Points

1. **Custom Operators**: Add to `OperatorEnum` and implement in `FiltersToDsl`
2. **Custom Aggregations**: Extend `AggregationEnum` and add DSL translation
3. **Custom Field Types**: Add to `ES_TYPE_MAP` in ModelBuilder
4. **Custom Validators**: Add field/model validators to Query models
5. **Alternative DSL Targets**: Adapt `FiltersToDsl` for other query languages

---

## Limitations

1. **No OR Logic**: All conditions within a slice are AND-joined
2. **Single Nested Level**: Doesn't handle deeply nested array queries
3. **Limited Date Math**: No relative date expressions (e.g., "now-7d")
4. **No Full-Text Search**: Designed for structured filtering, not text search
5. **No Geo Queries**: No geospatial operator support

---

## Summary

This module provides a **type-safe, validated pipeline** for converting natural language to Elasticsearch queries:

1. **ModelBuilder** transforms ES mappings into Pydantic schemas with proper types and enums
2. **FilterModelBuilder** creates a structured query format that LLMs can reliably output
3. **FiltersToDsl** converts that structured format into optimized ES DSL

The three-stage design ensures:
- ✅ Type safety at every stage
- ✅ Validation before execution
- ✅ Flexibility for complex queries (grouping, aggregations, having clauses)
- ✅ Clear separation between schema, filters, and execution

