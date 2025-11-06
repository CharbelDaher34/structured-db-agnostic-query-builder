# Database Query Builder

A database-agnostic natural language query builder that converts natural language queries into structured database queries (MongoDB aggregation pipelines and Elasticsearch queries) using LLM-powered structured output.

## Overview

This project provides a clean, extensible architecture for converting natural language queries into database-specific query languages. It uses OpenAI's language models with structured Pydantic outputs to ensure type-safe query generation.

**Key Features:**
- 🗣️ **Natural Language to Database Queries**: Convert plain English to MongoDB/Elasticsearch queries
- 🔌 **Database Agnostic**: Clean adapter pattern supporting multiple databases
- 🎯 **Type-Safe**: Full Pydantic validation for schema, filters, and queries
- 🧠 **LLM-Powered**: Uses OpenAI GPT models with structured outputs
- 🚀 **Production-Ready**: FastAPI REST API with async support
- 🔍 **Schema Inference**: Automatic schema extraction from live databases
- 📊 **Full Query Support**: Filtering, sorting, aggregations, grouping, having clauses, and time intervals

## Architecture

The project follows a **clean, layered architecture** with clear separation of concerns:

```
query_builder/
├── core/                    # Core models and interfaces
│   ├── models.py           # Shared data models (SchemaField, QueryResult, etc.)
│   └── interfaces.py       # Database adapter interfaces (ISchemaExtractor, IQueryTranslator, etc.)
│
├── schema/                  # Schema extraction and model building
│   ├── extractor.py        # Unified schema extraction layer
│   ├── model_builder.py    # Pydantic model generation from schema
│   └── type_mappings.py    # Database type to Python type mappings
│
├── query/                   # Query building and LLM interaction
│   ├── filter_builder.py   # Builds Pydantic filter models for LLM
│   ├── prompt_generator.py # Generates system prompts for LLM
│   └── translator.py       # Translates filters to database queries
│
├── llm/                     # LLM client management
│   └── client_factory.py   # LLM client factory (OpenAI, etc.)
│
├── execution/               # Query execution and result handling
│   ├── executor.py         # Query execution layer
│   └── result_formatter.py # Result formatting
│
├── adapters/                # Database-specific implementations
│   ├── mongodb/            # MongoDB adapter
│   │   ├── schema_extractor.py   # MongoDB schema inference
│   │   ├── query_translator.py   # Filter → Aggregation pipeline
│   │   └── executor.py           # MongoDB query execution
│   │
│   └── elasticsearch/      # Elasticsearch adapter
│       ├── schema_extractor.py   # ES mapping extraction
│       ├── query_translator.py   # Filter → ES query DSL
│       └── executor.py           # ES query execution
│
└── orchestrator.py          # Main entry point - coordinates all layers
```

### Design Patterns

1. **Adapter Pattern**: Database-specific implementations behind common interfaces
2. **Factory Pattern**: LLM client creation with different providers
3. **Builder Pattern**: Progressive model and filter construction
4. **Strategy Pattern**: Pluggable translators and executors per database

## Installation

### Prerequisites
- Python 3.13+
- MongoDB or Elasticsearch instance (optional, for live testing)
- OpenAI API key

### Using UV (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd elastic_query_builder

# Install dependencies with uv
uv sync

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration
```

### Using pip

```bash
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```env
# OpenAI Configuration
OPENAI_API_KEY=your-openai-api-key
LLM_MODEL=gpt-4.1

# MongoDB Configuration (if using MongoDB)
MONGO_URI=mongodb://user:password@host:port/?authSource=admin
MONGO_DATABASE=your_database
MONGO_COLLECTION=your_collection

# Elasticsearch Configuration (if using Elasticsearch)
ES_HOST=http://localhost:9200
ES_INDEX=your_index

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
```

## Quick Start

### 1. MongoDB Example

```python
import asyncio
from query_builder import QueryOrchestrator

async def main():
    # Initialize orchestrator for MongoDB
    orchestrator = QueryOrchestrator.from_mongodb(
        mongo_uri="mongodb://localhost:27017",
        database_name="mydb",
        collection_name="transactions",
        category_fields=["merchant_category", "currency"],  # Fields to sample for enums
        fields_to_ignore=["internal_id"],  # Fields to exclude from queries
        llm_model="gpt-4.1",
        llm_api_key="your-api-key",
        sample_size=1000,  # Documents to sample for schema inference
    )
    
    # Natural language query
    result = await orchestrator.query(
        natural_language_query="Show me the top 10 most expensive transactions in France",
        execute=True  # Set to False to only generate query without executing
    )
    
    # Access results
    print(f"Natural Language: {result['natural_language_query']}")
    print(f"Generated Pipeline: {result['database_queries']}")
    print(f"Results: {result['results']}")

if __name__ == "__main__":
    asyncio.run(main())
```

### 2. Elasticsearch Example

```python
import asyncio
from query_builder import QueryOrchestrator

async def main():
    # Initialize orchestrator for Elasticsearch
    orchestrator = QueryOrchestrator.from_elasticsearch(
        es_host="http://localhost:9200",
        index_name="transactions",
        category_fields=["category", "status"],
        fields_to_ignore=["_internal"],
        llm_model="gpt-4.1",
        llm_api_key="your-api-key",
    )
    
    # Natural language query
    result = await orchestrator.query(
        natural_language_query="What's the average transaction amount by category?",
        execute=True
    )
    
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
```

## REST API

### Starting the API Server

```bash
# Using uv
uv run api.py

# Or with uvicorn directly
uvicorn api:app --host 0.0.0.0 --port 8000
```

### API Endpoints

#### POST `/query`

Convert natural language query to database query (MongoDB).

**Request:**
```json
{
  "query": "Show me top 5 expensive transactions in USD",
  "category_fields": ["merchant_name", "currency"],  // Optional
  "fields_to_ignore": ["internal_field"]  // Optional
}
```

**Response:**
```json
{
  "natural_language_query": "Show me top 5 expensive transactions in USD",
  "extracted_filters": {
    "filters": [
      {
        "conditions": [
          {
            "type": "EnumFilter",
            "field": "currency",
            "operator": "is",
            "value": "USD"
          }
        ],
        "sort": [{"field": "amount", "order": "desc"}],
        "limit": 5
      }
    ]
  },
  "database_queries": [
    {
      "pipeline": [
        {"$match": {"currency": {"$eq": "USD"}}},
        {"$sort": {"amount": -1}},
        {"$limit": 5}
      ]
    }
  ]
}
```

## Core Components

### 1. QueryOrchestrator

The main entry point that coordinates all components.

**Factory Methods:**
- `QueryOrchestrator.from_mongodb(...)` - Create MongoDB orchestrator
- `QueryOrchestrator.from_elasticsearch(...)` - Create Elasticsearch orchestrator

**Methods:**
- `async query(natural_language_query, execute=True)` - Convert and optionally execute query
- `generate_model(model_name)` - Generate Pydantic model from schema
- `get_model_info()` - Get flattened field information
- `print_model_summary()` - Print schema summary

### 2. Schema Components

#### SchemaExtractor
Unified layer that wraps database-specific schema extractors.

#### ModelBuilder
Generates Pydantic models from normalized schema:
- Handles nested objects and arrays
- Creates enums from category fields
- Supports type validation

**Schema Format:**
```python
{
    "transaction.amount": {
        "type": "number"
    },
    "transaction.merchant_name": {
        "type": "string"
    },
    "transaction.timestamp": {
        "type": "date"
    },
    "transaction.category": {
        "type": "enum",
        "values": ["food", "travel", "shopping"]
    }
}
```

### 3. Filter Components

#### FilterModelBuilder
Builds structured Pydantic models that LLM uses for output:

**Filter Types:**
- `StringFilter` - String field operators: `is`, `different`, `contains`, `isin`, `notin`, `exists`
- `NumberFilter` - Number field operators: `<`, `>`, `is`, `different`, `between`, `isin`, `notin`, `exists`
- `DateFilter` - Date field operators: `<`, `>`, `is`, `different`, `between`, `exists`
- `BooleanFilter` - Boolean field operators: `is`, `different`, `exists`
- `EnumFilter` - Enum field operators: `is`, `different`, `isin`, `notin`, `exists`

**Query Structure:**
```python
{
    "filters": [  # List of query slices (for comparisons)
        {
            "conditions": [  # AND-joined filters
                {
                    "type": "EnumFilter",
                    "field": "category",
                    "operator": "is",
                    "value": "food"
                }
            ],
            "sort": [{"field": "amount", "order": "desc"}],
            "limit": 10,
            "group_by": ["timestamp"],
            "interval": "month",  # day, week, month, year
            "aggregations": [
                {
                    "field": "amount",
                    "type": "sum",  # sum, avg, count, min, max
                    "having_operator": ">",
                    "having_value": 1000
                }
            ]
        }
    ]
}
```

#### PromptGenerator
Generates system prompts for the LLM with:
- Available fields and their types
- Valid operators per field type
- Enum values for category fields
- Query structure instructions

### 4. Query Translation

#### QueryTranslator
Wraps database-specific translators to convert structured filters to database queries.

**MongoDB Translation:**
- Filters → `$match` stages
- Aggregations → `$group` stages
- Time intervals → `$dateToString` with format
- Having clauses → Post-group `$match`
- Sort → `$sort` stage
- Limit → `$limit` stage

**Elasticsearch Translation:**
- Filters → Query DSL `must` clauses
- Aggregations → `terms`, `sum`, `avg`, `count` aggregations
- Date histograms → `date_histogram` with interval
- Sort → `sort` array
- Limit → `size` parameter

### 5. Query Execution

#### QueryExecutor
Executes database queries and formats results into standardized `QueryResult` format:

```python
{
    "total_hits": 100,
    "documents": [...],  # List of result documents
    "aggregations": {...},  # Aggregation results (if any)
    "success": True,
    "error": None,
    "metadata": {...}
}
```

## Query Capabilities

### Supported Query Types

#### 1. Simple Filtering
```
"Show me all food transactions"
"Find transactions over $100"
"What are transactions in France?"
```

#### 2. Range Queries
```
"Show transactions between $50 and $200"
"Find purchases from last month"
"Transactions from 2024"
```

#### 3. Multiple Conditions (AND-joined)
```
"Show me food transactions over $50 in France"
"Find travel expenses in London paid with EUR"
```

#### 4. Aggregations and Grouping
```
"How much did I spend on food each month?"
"Show me total spending by category"
"What's my average transaction amount by location?"
"Count transactions by merchant"
```

#### 5. Sorting and Limiting
```
"Show me my top 5 most expensive transactions"
"List recent deposits, newest first"
"What are my smallest 10 withdrawals?"
```

#### 6. Comparison Queries (Multiple Slices)
```
"Compare my spending on food vs travel"
"Show gold card vs silver card transaction totals"
"Compare this year's deposits with last year's"
```

#### 7. Having Clauses (Post-Aggregation Filters)
```
"Show categories where total spending exceeds $1000"
"Which merchants have more than 10 transactions?"
```

#### 8. Time-Based Grouping
```
"Show daily transaction volume for last week"
"Monthly spending trends for this year"
"Weekly average transaction amount"
```

#### 9. Exclusion Queries
```
"Show all transactions except food and travel"
"Transactions that don't have a location"
"Exclude USD currency"
```

#### 10. Partial Text Matching
```
"Find transactions containing 'Starbucks'"
"Show payments to stores with 'Market' in the name"
```

### Complex Query Example

**Natural Language:**
```
"Show me my top 10 most expensive food and travel transactions from 
France in USD, grouped by month, where monthly total exceeds $1000"
```

**Generated Filter:**
```json
{
  "filters": [{
    "conditions": [
      {
        "type": "EnumFilter",
        "field": "transaction.category",
        "operator": "isin",
        "value": ["food", "travel"]
      },
      {
        "type": "StringFilter",
        "field": "transaction.country",
        "operator": "is",
        "value": "France"
      },
      {
        "type": "EnumFilter",
        "field": "transaction.currency",
        "operator": "is",
        "value": "USD"
      }
    ],
    "sort": [{"field": "transaction.amount", "order": "desc"}],
    "limit": 10,
    "group_by": ["transaction.timestamp"],
    "interval": "month",
    "aggregations": [{
      "field": "transaction.amount",
      "type": "sum",
      "having_operator": ">",
      "having_value": 1000
    }]
  }]
}
```

## Database Adapters

### Adding a New Database

To add support for a new database:

1. **Create adapter directory**: `query_builder/adapters/newdb/`

2. **Implement interfaces**:
   ```python
   # schema_extractor.py
   from query_builder.core.interfaces import ISchemaExtractor
   
   class NewDBSchemaExtractor(ISchemaExtractor):
       def get_schema(self) -> Dict[str, Any]:
           # Return normalized schema
           pass
       
       def get_enum_fields(self) -> Dict[str, List[Any]]:
           # Return enum values for category fields
           pass
   ```

   ```python
   # query_translator.py
   from query_builder.core.interfaces import IQueryTranslator
   
   class NewDBQueryTranslator(IQueryTranslator):
       def translate(self, filters: Dict[str, Any], 
                    model_info: Dict[str, Any]) -> List[Dict[str, Any]]:
           # Convert structured filters to DB queries
           pass
   ```

   ```python
   # executor.py
   from query_builder.core.interfaces import IQueryExecutor
   
   class NewDBQueryExecutor(IQueryExecutor):
       def execute(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
           # Execute queries and return results
           pass
       
       def execute_raw(self, query: Dict[str, Any], size: int) -> Dict[str, Any]:
           # Execute raw query
           pass
   ```

3. **Add factory method** to `QueryOrchestrator`:
   ```python
   @classmethod
   def from_newdb(cls, ...):
       schema_extractor = NewDBSchemaExtractor(...)
       query_translator = NewDBQueryTranslator()
       query_executor = NewDBQueryExecutor(...)
       
       return cls(
           schema_extractor=schema_extractor,
           query_translator=query_translator,
           query_executor=query_executor,
           ...
       )
   ```

## Testing

### Run Example Scripts

**MongoDB Example:**
```bash
uv run example_mongodb_usage.py
```

This runs 3 comprehensive examples:
1. Filtering, sorting, and limiting
2. Aggregations with grouping and having clauses
3. Date range filtering with monthly aggregations

**Test Improvements:**
```bash
uv run test_improvements.py
```

### Manual Testing

```python
import asyncio
from query_builder import QueryOrchestrator

async def test():
    orchestrator = QueryOrchestrator.from_mongodb(
        mongo_uri="mongodb://localhost:27017",
        database_name="test_db",
        collection_name="test_collection",
        llm_model="gpt-4.1",
        llm_api_key="your-key"
    )
    
    # Test schema extraction
    orchestrator.print_model_summary()
    
    # Test query conversion
    result = await orchestrator.query(
        "Show me the top 10 transactions",
        execute=False  # Just generate query
    )
    print(result)

asyncio.run(test())
```

## Project Structure

```
.
├── api.py                          # FastAPI REST API
├── example_mongodb_usage.py        # MongoDB usage examples
├── test_improvements.py            # Test suite
├── requirements.txt                # Pip dependencies
├── pyproject.toml                  # UV/Poetry project config
├── query_builder/                  # Main package
│   ├── __init__.py                # Package exports
│   ├── orchestrator.py            # Main orchestrator
│   ├── core/                      # Core models and interfaces
│   ├── schema/                    # Schema extraction and model building
│   ├── query/                     # Query building and LLM interaction
│   ├── llm/                       # LLM client management
│   ├── execution/                 # Query execution
│   └── adapters/                  # Database-specific implementations
│       ├── mongodb/
│       └── elasticsearch/
└── README.md
```

## Dependencies

**Core:**
- `pydantic` (>=2.11.7) - Data validation and models
- `pydantic-ai` (>=0.4.0) - LLM structured outputs

**Database Drivers:**
- `pymongo` (>=4.13.2) - MongoDB client
- `elasticsearch` (==8.11.1) - Elasticsearch client

**API:**
- `fastapi` (>=0.116.0) - Web framework
- `uvicorn` - ASGI server

**Utilities:**
- `pandas` (>=2.3.1) - Data processing
- `openpyxl` (>=3.1.5) - Excel file handling

## Configuration

### Category Fields

Fields specified in `category_fields` will be:
1. Sampled from the database to extract distinct values
2. Converted to enum types in the Pydantic model
3. Validated against the enum values in queries

**Example:**
```python
category_fields=["merchant_category", "currency", "country"]
```

### Fields to Ignore

Fields in `fields_to_ignore` will be excluded from:
- Schema extraction
- Model generation
- Query filtering

**Example:**
```python
fields_to_ignore=["_id", "internal_id", "created_by"]
```

### Sample Size (MongoDB)

For MongoDB, `sample_size` controls how many documents are sampled for:
- Schema inference (field types)
- Enum value extraction

**Default:** 1000 documents

## Best Practices

1. **Category Fields**: Only specify fields with reasonable cardinality (<1000 unique values)
2. **Sample Size**: Increase for more accurate schema inference, decrease for faster startup
3. **Field Naming**: Use consistent dot notation for nested fields (e.g., `transaction.amount`)
4. **Error Handling**: Always wrap queries in try-except blocks
5. **Async Usage**: Always use `await` with `orchestrator.query()`
6. **Query Execution**: Set `execute=False` to validate queries without running them

## Limitations

1. **Single Index/Collection**: Designed for querying one data source at a time
2. **Enum Cardinality**: High-cardinality fields (>1000 values) should not be category fields
3. **Schema Changes**: Requires orchestrator recreation after schema changes
4. **OR Conditions**: Not directly supported (use multiple slices for comparisons)
5. **Nested Array Queries**: Limited support for complex array operations

## Troubleshooting

### "LLM not configured" Error
Ensure you provide both `llm_model` and `llm_api_key` when creating the orchestrator.

### Schema Extraction Fails
- **MongoDB**: Check connection URI and ensure collection has documents
- **Elasticsearch**: Verify index exists and mapping is accessible

### Invalid Enum Values
Increase `sample_size` to ensure all possible enum values are captured during schema inference.

### Query Translation Errors
Check that field names in generated filters match the schema exactly (including nested paths).

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes following the existing architecture
4. Add tests for new functionality
5. Submit a pull request

## License

[Add your license here]

## Roadmap

- [ ] PostgreSQL adapter
- [ ] Support for OR conditions
- [ ] Query result caching
- [ ] Streaming results for large datasets
- [ ] More LLM providers (Anthropic, local models)
- [ ] GraphQL API
- [ ] Query history and optimization
- [ ] Web UI for query building

## Contact

[Add contact information]
