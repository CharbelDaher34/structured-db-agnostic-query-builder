# Database Query Builder

A database-agnostic natural language query builder that converts plain English questions into structured database queries (MongoDB aggregation pipelines, Elasticsearch DSL, or pandas operations against a CSV) using LLM-powered structured output.

## Overview

Type-safe Pydantic schemas pin down what the LLM is allowed to emit, and adapters translate that structured output to the native query language of each backend.

**Key features**
- **Natural language → database queries** — plain English in, native query out
- **Three adapters out of the box** — MongoDB, Elasticsearch, CSV files
- **Type-safe** — Pydantic validates schema, filters, and the LLM response
- **LLM-powered** — uses any OpenAI-compatible API (OpenAI, Ollama, vLLM) via `pydantic-ai`
- **Async, paginated REST API** — FastAPI with lifespan-managed orchestrator caching
- **Automatic schema inference** — sample documents/rows to discover fields and enum values
- **Aggregation support** — filtering, sorting, grouping, having clauses, time intervals

## Architecture

```
query_builder/
├── core/                          # Core models and interfaces
│   ├── models.py                  # SchemaField, QueryResult, etc.
│   └── interfaces.py              # ISchemaExtractor / IQueryTranslator / IQueryExecutor
│
├── schema/                        # Schema extraction and Pydantic model generation
│   ├── extractor.py               # Caching wrapper around the adapter
│   ├── model_builder.py           # Schema → Pydantic model
│   └── type_mappings.py
│
├── query/                         # Filter building, prompt, translation
│   ├── filter_builder.py          # Builds the discriminated-union filter model the LLM outputs into
│   ├── prompt_generator.py        # System prompt; trims large enum value lists
│   └── translator.py              # Wraps the adapter translator
│
├── llm/
│   └── client_factory.py          # Reads LLM_MODEL / LLM_API_KEY / LLM_BASE_URL; wraps pydantic-ai Agent
│
├── execution/
│   └── executor.py                # Sync + async (asyncio.to_thread) execution
│
├── adapters/
│   ├── mongodb/                   # MongoSchemaExtractor / Translator / Executor
│   ├── elasticsearch/             # ES adapter
│   └── csv/                       # CSV adapter (pandas-backed)
│
└── orchestrator.py                # QueryOrchestrator — public entry point with from_mongodb/from_elasticsearch/from_csv
```

### Request flow

```
natural language query
  → LLMClientFactory.parse_query()    pydantic-ai Agent → structured QueryFilters
  → QueryTranslator.translate()        QueryFilters → adapter-specific query
  → QueryExecutor.execute_async()      runs on a worker thread so blocking drivers don't stall the loop
  → QueryResult                        normalised result dict
```

## Installation

### Prerequisites
- Python 3.12+
- An OpenAI-compatible LLM endpoint (OpenAI, Ollama, vLLM, etc.)
- MongoDB or Elasticsearch if you plan to use those adapters

### Using uv (recommended)

```bash
git clone <repository-url>
cd structured-db-agnostic-query-builder
uv sync
cp .env.example .env  # then edit
```

### Environment variables

```env
# LLM
LLM_MODEL=gpt-4.1
LLM_API_KEY=...                          # or OPENAI_API_KEY
LLM_BASE_URL=http://localhost:11434/v1   # optional, for Ollama / vLLM / etc.

# MongoDB (used by the API server)
MONGO_URI=mongodb://user:password@host:port/?authSource=admin
MONGO_DATABASE=your_database
MONGO_COLLECTION=your_collection
MONGO_SAMPLE_SIZE=1000

# Comma-separated overrides for the API server's default orchestrator
DEFAULT_CATEGORY_FIELDS=transaction_location,transaction_currency,merchant_name
DEFAULT_FIELDS_TO_IGNORE=converted_currency,merchant_category_description

# Elasticsearch
ES_HOST=http://localhost:9200
ES_INDEX=your_index

# API
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=INFO
```

## Quick start

### MongoDB

```python
import asyncio
from query_builder import QueryOrchestrator

async def main():
    orchestrator = QueryOrchestrator.from_mongodb(
        mongo_uri="mongodb://localhost:27017",
        database_name="mydb",
        collection_name="transactions",
        category_fields=["merchant_category", "currency"],
        fields_to_ignore=["internal_id"],
        sample_size=1000,
    )
    orchestrator.warm_up()  # pre-build schema/filter model/prompt (optional)

    result = await orchestrator.query(
        "Show me the top 10 most expensive transactions in France",
        execute=True,
        offset=0,
        limit=50,           # only injected if the LLM-generated query has no $limit
    )

    print(result["database_queries"])
    print(result["results"])

    orchestrator.close()

asyncio.run(main())
```

`from_mongodb` shares a single `MongoClient` between the schema extractor and the executor. Schema is inferred from a **random** `$sample` (not the first N inserted documents), and `distinct()` is replaced with a bounded `$group + $limit` pipeline so high-cardinality category fields can't OOM the process. The post-`$group` sort automatically remaps `transaction.amount` → `sum_transaction_amount` (or to `_id.<field>` for group-by fields) so sorts after a grouped aggregation actually work.

### Elasticsearch

```python
import asyncio
from query_builder import QueryOrchestrator

async def main():
    orchestrator = QueryOrchestrator.from_elasticsearch(
        es_host="http://localhost:9200",
        index_name="transactions",
        category_fields=["category", "status"],
        fields_to_ignore=["_internal"],
        include_bucket_documents=False,  # opt in to per-bucket top_hits if you need it
    )
    result = await orchestrator.query(
        "What's the average transaction amount by category?",
        execute=True,
    )
    print(result)

asyncio.run(main())
```

### CSV

```python
import asyncio
from query_builder import QueryOrchestrator

async def main():
    orchestrator = QueryOrchestrator.from_csv(
        csv_path="transactions.csv",
        category_fields=["category", "currency"],
        date_columns=["timestamp"],            # parsed as datetimes
        fields_to_ignore=["internal_notes"],
        read_csv_kwargs={"sep": ",", "encoding": "utf-8"},
    )
    result = await orchestrator.query(
        "How much did I spend on food each month?",
        execute=True,
    )
    print(result["results"])

asyncio.run(main())
```

The CSV is loaded once into a pandas DataFrame and shared between the schema extractor and the executor. Filters become boolean masks; `group_by` + date `interval` is implemented with `pd.Grouper(freq=...)`; aggregations use `pd.NamedAgg`; sort remaps to grouped column names just like the Mongo translator does.

## REST API

### Start the server

```bash
uv run api.py
# or
uvicorn api:app --host 0.0.0.0 --port 8000
```

The API uses a FastAPI **lifespan**: a default MongoDB orchestrator is built **once** at startup, warmed up, and cached. Requests with a matching configuration reuse it; requests with overrides spin up additional orchestrators that are cached by `(uri, db, collection, category_fields, fields_to_ignore)`. All cached orchestrators are closed on shutdown.

### `POST /query`

| Param | In | Type | Description |
|---|---|---|---|
| `query` | body | string | Natural-language query |
| `category_fields` | body | list[str] | Override default category fields |
| `fields_to_ignore` | body | list[str] | Override default ignored fields |
| `mongo_uri` / `database_name` / `collection_name` | query | string | Override default MongoDB target |
| `execute` | query | bool | Run the query and include `results` in the response |
| `offset` | query | int ≥ 0 | Pagination offset (executed queries only) |
| `limit` | query | int ≥ 1 | Pagination limit (only applied when the generated query has no `$limit`) |

**Request:**

```json
{
  "query": "Show me top 5 expensive transactions in USD",
  "category_fields": ["merchant_name", "currency"]
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
          { "type": "EnumFilter", "field": "currency", "operator": "is", "value": "USD" }
        ],
        "sort": [{ "field": "amount", "order": "desc" }],
        "limit": 5
      }
    ]
  },
  "database_queries": [
    {
      "pipeline": [
        { "$match": { "currency": { "$eq": "USD" } } },
        { "$sort": { "amount": -1 } },
        { "$limit": 5 }
      ]
    }
  ],
  "results": null
}
```

## Filter model

The LLM emits a `QueryFilters` object whose schema is built dynamically from the live data source. Each slice has:

```python
{
  "conditions": [        # AND-joined typed filters
    { "type": "StringFilter",  "field": "...", "operator": "...", "value": ... },
    { "type": "NumberFilter",  "field": "...", "operator": "...", "value": ... },
    { "type": "DateFilter",    "field": "...", "operator": "...", "value": ... },
    { "type": "BooleanFilter", "field": "...", "operator": "...", "value": ... },
    { "type": "EnumFilter",    "field": "...", "operator": "...", "value": ... }
  ],
  "sort":         [{"field": "...", "order": "asc|desc"}],
  "limit":        10,
  "group_by":     ["..."],
  "aggregations": [
    { "field": "...", "type": "sum|avg|count|min|max",
      "having_operator": ">", "having_value": 1000 }
  ],
  "interval": "day|week|month|year"
}
```

Multiple slices in the top-level `filters` list are how comparisons (`A vs B`) are represented.

**Operators by type:**

| Type | Operators |
|---|---|
| string | `is`, `different`, `contains`, `isin`, `notin`, `exists` |
| number | `<`, `>`, `is`, `different`, `between`, `isin`, `notin`, `exists` |
| date | `<`, `>`, `is`, `different`, `between`, `exists` |
| boolean | `is`, `different`, `exists` |
| enum | `is`, `different`, `isin`, `notin`, `exists` |

Pydantic validators enforce that the chosen filter type matches the field's schema type and that enum values come from the sampled set.

## Adding a new adapter

Implement the three `Protocol` interfaces in [core/interfaces.py](query_builder/core/interfaces.py):

```python
class NewDBSchemaExtractor:    # → ISchemaExtractor
    def extract_schema(self) -> Dict[str, Any]: ...
    def get_distinct_values(self, field_path: str, size: int = 1000) -> List[Any]: ...
    def get_field_type(self, field_path: str) -> str: ...

class NewDBQueryTranslator:    # → IQueryTranslator
    def translate(self, filters, model_info) -> List[Dict[str, Any]]: ...

class NewDBQueryExecutor:      # → IQueryExecutor
    def execute(self, queries, offset=0, limit=None) -> List[Dict[str, Any]]: ...
    def execute_raw(self, query, size=100) -> Dict[str, Any]: ...
```

Then add a `from_newdb(...)` classmethod on `QueryOrchestrator` and (if the adapter holds a connection or DataFrame) pass the same handle to both the extractor and the executor so they share state.

## Configuration notes

- **`category_fields`** — only mark fields with reasonable cardinality (< 1000 distinct values). The schema extractor caps the distinct-value lookup, and the prompt generator further trims any enum > 50 values when embedding it in the system prompt (with a `values_truncated`/`total_values` hint).
- **`fields_to_ignore`** — matched either as a full dotted path (e.g. `transaction.internal_id`) or as a leaf field name.
- **`sample_size`** (MongoDB) — larger = more accurate schema inference but slower startup.
- **`date_columns`** (CSV) — pass these to make the executor treat the column as datetime; without it, date filters and date-histogram grouping won't work.
- **`include_grouped_documents`** (MongoDB) / **`include_bucket_documents`** (Elasticsearch) — opt in if you want the raw documents pushed into each aggregation bucket; off by default to avoid `$push: $$ROOT` blowing past MongoDB's 16 MB BSON limit and to keep ES responses small for pure metric queries.

## Limitations

- **Single index/collection/file per orchestrator.** No multi-source joins.
- **AND-only within a slice.** Multi-slice comparisons cover the most common "A vs B" case; nested `(A OR B) AND C` is not yet supported.
- **CSV is in-memory.** The CSV adapter loads the whole file into a pandas DataFrame.
- **Schema changes require recreating the orchestrator** (or calling `invalidate_cache()` on the adapter's extractor).

## Dependencies

Core: `pydantic`, `pydantic-ai` · DB drivers: `pymongo`, `elasticsearch==8.11.1` · CSV: `pandas` · API: `fastapi`, `uvicorn` · Utilities: `python-dotenv`, `openpyxl`

## Roadmap

- [x] MongoDB adapter
- [x] Elasticsearch adapter
- [x] CSV adapter
- [x] Pagination + async execution
- [ ] PostgreSQL adapter
- [ ] Nested `OR`/`AND` within a single slice
- [ ] Streaming results for large datasets
