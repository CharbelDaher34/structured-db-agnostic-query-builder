# FilterModelBuilder Class Description

The `FilterModelBuilder` class transforms field metadata from Elasticsearch mappings into structured Pydantic models that can validate and process natural language queries. It acts as a bridge between raw field information and type-safe query structures.

## Input: `model_info`

The primary input is `model_info` - a flattened dictionary containing field metadata from the `ModelBuilder` class.

### Example `model_info`:
```python
{
    "card_type": {
        "type": "enum", 
        "values": ["GOLD", "SILVER", "PLATINUM"]
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
        "values": ["food", "travel", "shopping", "entertainment"]
    },
    "transaction.receiver.location": {
        "type": "string"
    },
    "transaction.type": {
        "type": "enum",
        "values": ["Deposit", "Withdrawal", "Transfer"]
    },
    "transaction.currency": {
        "type": "enum",
        "values": ["USD", "EUR", "GBP"]
    },
    "transaction.items": {
        "type": "array",
        "item_type": "string"
    }
}
```

## Conversion to Filter Model

The `BuildFilterModel()` method creates a hierarchical Pydantic model structure:

### 1. **Dynamic Field Enum**
```python
# Creates enum from model_info keys
FieldEnum = Enum("FieldEnum", {
    "card_type": "card_type",
    "transaction.amount": "transaction.amount",
    "transaction.timestamp": "transaction.timestamp",
    # ... all other fields
})
```

### 2. **Supported Operators**
```python
class OperatorEnum(str, Enum):
    lt = "<"         # Less than (number, date)
    gt = ">"         # Greater than (number, date)
    isin = "isin"    # Value in list (any)
    notin = "notin"  # Value not in list (any)
    eq = "is"        # Equals (any)
    ne = "different" # Not equal (any)
    be = "between"   # Range (number, date)
    contains = "contains"  # Partial string match (string)
    exists = "exists"      # Field exists/not exists (any, bool value)
```

### 3. **Core Filter Components**
```python
class Query(BaseModel):
    field: FieldEnum                    # Must be from available fields
    operator: OperatorEnum              # <, >, is, isin, contains, etc.
    value: Union[str, int, float, bool, date, List[...], None]

class SortField(BaseModel):
    field: FieldEnum
    order: SortOrderEnum                # asc, desc

class Aggregation(BaseModel):
    field: FieldEnum
    type: AggregationEnum               # sum, avg, count, min, max
    having_operator: Optional[OperatorEnum]
    having_value: Optional[Union[str, int, float]]
```

### 4. **Complete Query Structure**
```python
class QuerySlice(BaseModel):
    conditions: List[Query]             # AND-joined filters
    sort: Optional[List[SortField]]     # Multi-field sorting
    limit: Optional[int]                # Result limit
    group_by: Optional[List[FieldEnum]] # Grouping fields
    aggregations: Optional[List[Aggregation]]  # Calculations
    interval: Optional[TimeIntervalEnum]       # day, week, month, year

class QueryFilters(BaseModel):
    filters: List[QuerySlice]           # Multiple slices for comparisons
```

## Filter Model Fields & Constraints

### **Field-Level Constraints**

1. **Field Validation**
   - Only fields from `model_info` are allowed
   - Dynamic enum prevents invalid field references

2. **Operator Constraints**
   ```python
   # Numeric/Date only operators
   if operator in ("<", ">", "between"):
       if field_type not in ("number", "date"):
           raise ValueError("Operator only for number/date fields")
   
   # String-specific operators
   if operator == "contains":
       if field_type != "string" or not isinstance(value, str):
           raise ValueError("Expected string for contains")
   
   # List operators
   if operator in ("isin", "notin"):
       if not isinstance(value, list):
           raise ValueError("Expected list for isin/notin")
   
   # Exists operator
   if operator == "exists":
       if not isinstance(value, bool):
           raise ValueError("Expected bool (True=exists, False=not exists)")
   ```

3. **Value Type Validation**
   ```python
   # Enum value checking for list operators
   if operator in ("isin", "notin") and field_type == "enum":
       if not all(x in field_info.get("values", []) for x in value):
           raise ValueError(f"Values must be in enum: {field_info['values']}")
   
   # Between operator validation
   if operator == "between":
       if not isinstance(value, list) or len(value) != 2:
           raise ValueError("Between requires list of 2 values")
   
   # Note: "is" and "different" operators currently have no specific validation
   # beyond basic type checking in the current implementation
   ```

### **Slice-Level Constraints**

1. **Aggregation Dependencies**
   ```python
   # Aggregations require group_by
   if not group_by and aggregations:
       aggregations = None  # Auto-remove invalid aggregations
   ```

2. **Interval Constraints**
   ```python
   # Interval only for date fields in group_by
   if interval and group_by:
       has_date_field = any(
           model_info.get(field.value, {}).get("type") == "date" 
           for field in group_by
       )
       if not has_date_field:
           interval = None  # Auto-remove invalid interval
   ```

3. **Logical Validation**
   ```python
   # Remove null field conditions
   for query in conditions:
       if query.field.value == "null":
           conditions.remove(query)
   ```

## Example Filter Model Output

Given the `model_info` above, a valid filter model instance might look like:

```python
{
    "filters": [
        {
            "conditions": [
                {
                    "field": "transaction.receiver.category_type",
                    "operator": "is",
                    "value": "food"
                },
                {
                    "field": "transaction.amount",
                    "operator": ">",
                    "value": 50.0
                }
            ],
            "sort": [
                {
                    "field": "transaction.timestamp",
                    "order": "desc"
                }
            ],
            "limit": 10,
            "group_by": ["transaction.timestamp"],
            "interval": "month",
            "aggregations": [
                {
                    "field": "transaction.amount",
                    "type": "sum",
                    "having_operator": ">",
                    "having_value": 1000
                }
            ]
        }
    ]
}
```

## Summary

The `FilterModelBuilder` takes raw field metadata and creates a comprehensive validation framework that:
- **Ensures type safety** through dynamic enums and strict validation
- **Prevents logical errors** via cross-field validation rules
- **Supports complex queries** with grouping, aggregation, and sorting
- **Auto-corrects invalid combinations** rather than failing
- **Provides clear error messages** for debugging

This enables robust natural language to database query conversion with minimal runtime errors.

---

# Natural Language Query Capabilities

The filter model can handle a wide variety of natural language queries and convert them into structured filters. Here are the main categories and examples:

## Query Types Supported

### 1. **Simple Filtering Queries**
Natural language queries that filter data based on specific criteria.

**Examples:**
- *"Show me all food transactions"*
- *"Find transactions over $100"*
- *"What are my gold card purchases?"*
- *"List all deposits in USD"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "food"
        }
      ]
    }
  ]
}
```

### 2. **Range and Date Queries**
Queries involving numerical ranges, date ranges, and time periods.

**Examples:**
- *"Show transactions between $50 and $200"*
- *"Find all purchases from last month"*
- *"What transactions happened in 2024?"*
- *"Show me withdrawals over €500"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.amount",
          "operator": "between",
          "value": [50, 200]
        }
      ]
    }
  ]
}
```

### 3. **Multiple Condition Queries**
Complex queries with multiple AND-joined conditions.

**Examples:**
- *"Show me food transactions over $50 from my gold card"*
- *"Find all travel expenses in London paid with EUR"*
- *"What are my online shopping purchases under $100?"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "food"
        },
        {
          "field": "transaction.amount",
          "operator": ">",
          "value": 50
        },
        {
          "field": "card_type",
          "operator": "is",
          "value": "GOLD"
        }
      ]
    }
  ]
}
```

### 4. **Aggregation and Grouping Queries**
Queries that require grouping data and performing calculations.

**Examples:**
- *"How much did I spend on food each month?"*
- *"Show me total spending by category"*
- *"What's my average transaction amount by location?"*
- *"Count my transactions by card type"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "food"
        }
      ],
      "group_by": ["transaction.timestamp"],
      "interval": "month",
      "aggregations": [
        {
          "field": "transaction.amount",
          "type": "sum"
        }
      ]
    }
  ]
}
```

### 5. **Sorting and Limiting Queries**
Queries that need specific ordering or result limits.

**Examples:**
- *"Show me my top 5 most expensive transactions"*
- *"List my recent deposits, newest first"*
- *"What are my smallest 10 withdrawals?"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [],
      "sort": [
        {
          "field": "transaction.amount",
          "order": "desc"
        }
      ],
      "limit": 5
    }
  ]
}
```

### 6. **Comparison Queries**
Queries that compare different slices of data side-by-side.

**Examples:**
- *"Compare my spending on food vs travel"*
- *"Show gold card vs silver card transaction totals"*
- *"Compare this year's deposits with last year's"*
- *"Using only the last 3 months, compare my spending on food vs travel"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "food"
        }
      ]
    },
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "travel"
        }
      ]
    }
  ]
}
```

**Real-World Example with Date Constraints:**
*"Using only the last 3 months, compare my spending on food vs travel"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "Restaurant"
        },
        {
          "field": "transaction.timestamp",
          "operator": "between",
          "value": ["2025-04-18", "2025-07-18"]
        }
      ]
    },
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "is",
          "value": "Travel"
        },
        {
          "field": "transaction.timestamp",
          "operator": "between",
          "value": ["2025-04-18", "2025-07-18"]
        }
      ]
    }
  ]
}
```

### 7. **Exclusion and Negative Queries**
Queries that exclude certain data or look for missing information.

**Examples:**
- *"Show me all transactions except food and travel"*
- *"Find transactions that don't have a location"*
- *"What purchases were not made with my gold card?"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "notin",
          "value": ["food", "travel"]
        }
      ]
    }
  ]
}
```

### 8. **Partial Match Queries**
Queries that search for partial text matches.

**Examples:**
- *"Find all transactions containing 'Starbucks'"*
- *"Show me payments to stores with 'Market' in the name"*
- *"What transactions have 'Online' in the description?"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.receiver.name",
          "operator": "contains",
          "value": "Starbucks"
        }
      ]
    }
  ]
}
```

## Query Complexity Handling

The filter model can handle increasingly complex queries by combining multiple features:

### **Complex Example:**
*"Show me my top 10 most expensive food and travel transactions from my gold card in London, grouped by month, where the total monthly spending was over $1000"*

**Generated Filter:**
```json
{
  "filters": [
    {
      "conditions": [
        {
          "field": "transaction.receiver.category_type",
          "operator": "isin",
          "value": ["food", "travel"]
        },
        {
          "field": "card_type",
          "operator": "is",
          "value": "GOLD"
        },
        {
          "field": "transaction.receiver.location",
          "operator": "is",
          "value": "London"
        }
      ],
      "sort": [
        {
          "field": "transaction.amount",
          "order": "desc"
        }
      ],
      "limit": 10,
      "group_by": ["transaction.timestamp"],
      "interval": "month",
      "aggregations": [
        {
          "field": "transaction.amount",
          "type": "sum",
          "having_operator": ">",
          "having_value": 1000
        }
      ]
    }
  ]
}
```

## Limitations

While powerful, the filter model has some limitations:

1. **Field Dependencies**: Can only query fields that exist in the `model_info`
2. **Operator Constraints**: Some operators only work with specific field types
3. **Validation Rules**: Enum values must match exactly what's in the schema
4. **Single Index**: Designed for querying one Elasticsearch index at a time

The system automatically handles edge cases and invalid combinations, making it robust for real-world natural language query processing.

---

# API Endpoints

The FastAPI application provides three main endpoints for converting natural language queries into Elasticsearch queries. All endpoints are built on top of the `FilterModelBuilder` and related classes.

## 1. `/query` - Database-Connected Endpoint

**Purpose**: Converts natural language to Elasticsearch queries using a live database connection (Db config are passed in the api code)

### Input (`QueryRequest`)
```python
{
    "user_input": str  # Natural language query from user
}
```

### Output (`QueryResponse`)
```python
{
    "natural_language_query": str,           # Original user query
    "extracted_filters": Dict[str, Any],     # Structured filter object
    "elasticsearch_queries": List[Dict[str, Any]]  # Ready-to-execute ES queries
}
```

### Process Flow
1. **Schema Fetching**: Connects to Elasticsearch to get index mapping and distinct values for category fields
2. **Model Generation**: Creates Pydantic models from the live schema
3. **LLM Processing**: Uses AI to convert natural language to structured filters
4. **DSL Conversion**: Transforms filters into Elasticsearch query DSL

---

## 2. `/query-from-mapping` - Mapping-Based Endpoint

**Purpose**: Converts natural language to Elasticsearch queries using provided mapping data (no database connection required).

### Input (`MappingQueryRequest`)
```python
{
    "user_input": str,                    # Natural language query
    "elasticsearch_mapping": Dict[str, Any],  # ES index mapping (properties section)
    "enum_fields": Dict[str, List[Any]],     # Field names -> possible values
    "fields_to_ignore": List[str]            # Fields to exclude from model
}
```

**Example Input**:
```json
{
    "user_input": "Compare my spending on travel vs food",
    "elasticsearch_mapping": {
        "transaction": {
            "properties": {
                "amount": {"type": "float"},
                "receiver": {
                    "properties": {
                        "category_type": {"type": "keyword"}
                    }
                }
            }
        }
    },
    "enum_fields": {
        "transaction.receiver.category_type": ["food", "travel", "shopping"]
    },
    "fields_to_ignore": ["user_id", "card_number"]
}
```

### Output (`QueryResponse`)
Same structure as `/query` endpoint - returns structured filters and Elasticsearch queries.

### Process Flow
1. **Schema Processing**: Uses provided mapping and enum values instead of fetching from database
2. **Model Generation**: Creates Pydantic models from provided schema
3. **LLM Processing**: Converts natural language to structured filters
4. **DSL Conversion**: Transforms filters into Elasticsearch query DSL
