# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run example scripts
uv run examples/example_mongodb_usage.py
uv run examples/example_client_factory_usage.py
uv run examples/example_csv_usage.py
uv run examples/test_ollama.py
```

## Environment Variables

```env
LLM_MODEL=gpt-4.1
LLM_API_KEY=...          # or OPENAI_API_KEY
LLM_BASE_URL=...         # optional, for OpenAI-compatible APIs (e.g., Ollama at http://localhost:11434/v1)

MONGO_URI=mongodb://user:password@host:port/?authSource=admin
MONGO_DATABASE=your_database
MONGO_COLLECTION=your_collection
MONGO_SAMPLE_SIZE=1000

ES_HOST=http://localhost:9200
ES_INDEX=your_index

LOG_LEVEL=INFO
```

## Architecture

This is a **natural language → database query** system. The user provides a plain English query; the system extracts a schema from the live data source, builds a Pydantic model from that schema, sends it to an LLM for structured output, then translates the structured output into a native database query.

### Request flow

```
natural language query
    → LLMClientFactory.parse_query()      [pydantic-ai Agent → structured QueryFilters]
    → QueryTranslator.translate()          [QueryFilters → adapter-specific query]
    → QueryExecutor.execute_async()        [run via asyncio.to_thread so blocking DB drivers don't stall the event loop]
    → QueryResult
```

`QueryOrchestrator` ([query_builder/orchestrator.py](query_builder/orchestrator.py)) is the single public entry point. It lazy-initialises all components on first use and caches them. Call `orchestrator.warm_up()` after construction to pre-build the schema-derived model, filter model, and system prompt so the first request isn't penalised. Call `orchestrator.close()` to release database connections.

### Package layout

| Package | Responsibility |
|---|---|
| `query_builder/core/` | Shared models (`SchemaField`, `QueryResult`) and `Protocol` interfaces (`ISchemaExtractor`, `IQueryTranslator`, `IQueryExecutor`) |
| `query_builder/schema/` | `SchemaExtractor` (unified wrapper), `ModelBuilder` (schema → Pydantic model), `type_mappings.py` |
| `query_builder/query/` | `FilterModelBuilder` (builds the discriminated-union Pydantic model the LLM outputs into), `PromptGenerator` (trims large enum lists with a `values_truncated` hint), `QueryTranslator` (thin wrapper over adapter translator) |
| `query_builder/llm/` | `LLMClientFactory` — reads `LLM_MODEL`, `LLM_API_KEY`, `LLM_BASE_URL` from env; wraps `pydantic-ai` `Agent` |
| `query_builder/execution/` | `QueryExecutor` with both sync `execute()` and async `execute_async()` (uses `asyncio.to_thread`); pagination kwargs `offset`/`limit` are forwarded to adapters that support them |
| `query_builder/adapters/mongodb/` | `MongoSchemaExtractor` (uses `$sample` for random sampling, bounded `$group + $limit` for distinct values), `MongoQueryTranslator` (remaps post-`$group` sort to `_id.<field>` or aggregation result names; `include_grouped_documents=False` by default to avoid the 16 MB BSON limit), `MongoQueryExecutor` (accepts a shared `MongoClient`, injects `$skip`/`$limit` for pagination) |
| `query_builder/adapters/elasticsearch/` | `ESSchemaExtractor`, `ESQueryTranslator` (`include_bucket_documents=False` by default — `top_hits` sub-aggregation is opt-in), `ESQueryExecutor` (doesn't mutate caller's query dict) |
| `query_builder/adapters/csv/` | `CSVSchemaExtractor`, `CSVQueryTranslator` (normalises each slice into an execution plan), `CSVQueryExecutor` (runs the plan with pandas: boolean-mask filtering, `pd.Grouper(freq=...)` for date intervals, `pd.NamedAgg` for sum/avg/count/min/max, having-clause post-filtering, JSON-safe result serialisation) |

### LLM structured output

`FilterModelBuilder.build_filter_model()` dynamically creates a `QueryFilters` Pydantic class whose fields are derived from the live schema. The LLM returns a `QueryFilters` instance containing a list of `QuerySlice` objects. Each slice has:
- `conditions` — AND-joined list of typed filters (`StringFilter | NumberFilter | DateFilter | BooleanFilter | EnumFilter`), discriminated by the `type` literal field
- `sort`, `limit`, `group_by`, `aggregations`, `interval`

Pydantic validators in `filter_builder.py` enforce that e.g. `EnumFilter` is only used on enum fields and that enum values are in the sampled set.

### Factory methods on `QueryOrchestrator`

```python
QueryOrchestrator.from_mongodb(mongo_uri, database_name, collection_name, ...)
QueryOrchestrator.from_elasticsearch(es_host, index_name, ...)
QueryOrchestrator.from_csv(csv_path, category_fields=..., date_columns=..., ...)
```

`from_mongodb` shares a single `MongoClient` between the schema extractor and the executor. `from_csv` loads the file once into a pandas DataFrame and shares it the same way.

### Adding a new database adapter

1. Create `query_builder/adapters/newdb/` with `schema_extractor.py`, `query_translator.py`, `executor.py` implementing the three `Protocol` interfaces in `core/interfaces.py`.
2. Add a `from_newdb(...)` classmethod to `QueryOrchestrator`. If the adapter holds a connection/resource, pass the same handle to both the extractor and the executor so they share state.

