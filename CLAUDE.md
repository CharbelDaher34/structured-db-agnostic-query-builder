# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (uses the dev dependency group from pyproject.toml)
uv sync

# Run example scripts
uv run examples/example_mongodb_usage.py
uv run examples/example_client_factory_usage.py
uv run examples/example_csv_usage.py
uv run examples/test_ollama.py

# Tests
uv run pytest                        # full suite (~25s, makes real LLM calls)
uv run pytest -q -m 'not llm'        # offline-only (~2s, no API key needed)
uv run pytest tests/test_ai_eval_complex.py -v   # the hard AI eval against synthetic orders dataset

# Lint / format / type check
uv run ruff check .
uv run ruff format .
uv run ty check

# Pre-commit (runs ruff + ty + hygiene hooks on every commit)
uv run pre-commit install            # one-time install
uv run pre-commit run --all-files    # manual run across the repo
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

# SQL adapter takes a full SQLAlchemy URL — postgres, mysql, sqlite, etc.
DATABASE_URL=postgresql+psycopg://user:password@host/database
# Or: sqlite:///path/to.db, mysql+pymysql://user:password@host/database

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
| `query_builder/execution/` | `QueryExecutor` with both sync `execute()` and async `execute_async()` (uses `asyncio.to_thread`); pagination kwargs `offset`/`limit` are forwarded to adapters. The `IQueryExecutor` protocol declares `offset` / `limit` directly so adapter signatures match — no `try`/`except TypeError` fallback |
| `query_builder/adapters/mongodb/` | `MongoSchemaExtractor` (uses `$sample` for random sampling, bounded `$group + $limit` for distinct values), `MongoQueryTranslator` (remaps post-`$group` sort to `_id.<field>` or aggregation result names; uses `$convert` for date grouping so BSON `Date` and ISO-string columns both work; `include_grouped_documents=False` by default to avoid the 16 MB BSON limit), `MongoQueryExecutor` (accepts a shared `MongoClient`, injects `$skip`/`$limit` for pagination, runs a piggy-back `$count` pipeline so `total_hits` is the **pre-pagination** matched row count) |
| `query_builder/adapters/elasticsearch/` | `ESSchemaExtractor` (records `es_type` + `has_keyword_subfield` so the translator knows when to add `.keyword`; accepts a shared `Elasticsearch` client), `ESQueryTranslator` (`_exact_match_field` honours `es_type`: `text` → suffix `.keyword`, `keyword` → raw field; `include_bucket_documents=False` by default — `top_hits` sub-aggregation is opt-in), `ESQueryExecutor` (doesn't mutate caller's query dict, injects `from`/`size`, uses `_msearch` for multi-slice queries to save round-trips) |
| `query_builder/adapters/csv/` | `CSVSchemaExtractor`, `CSVQueryTranslator` (normalises each slice into an execution plan), `CSVQueryExecutor` (runs the plan with pandas: boolean-mask filtering, `pd.Grouper(freq=...)` for date intervals, `pd.NamedAgg` for sum/avg/count/min/max, having-clause post-filtering, JSON-safe result serialisation; `total_hits` is the pre-pagination row count) |
| `query_builder/adapters/sql/` | `SQLSchemaExtractor` (reads SQLAlchemy `Table` metadata; maps `column.type.python_type` via `TypeMapper.normalize_python_type` so dialect type systems stay inside SQLAlchemy; runs a `SELECT DISTINCT … LIMIT N` per category field), `SQLQueryTranslator` (emits SQLAlchemy `Select` statements; dialect-aware date truncation — PostgreSQL `date_trunc`, SQLite `strftime`, MySQL `date_format`, generic `cast(col, Date)` fallback; HAVING for post-aggregation filters; rewrites sort to the aggregation-alias column when grouped), `SQLQueryExecutor` (single engine shared with the schema extractor; pre-pagination `total_hits` via `SELECT COUNT(*) FROM (<stmt>) sub`; `Decimal`/`datetime` → JSON-safe; `execute_raw` accepts either a `Select` or `{"sql": "...", "params": {...}}`) |

### LLM structured output

`FilterModelBuilder.build_filter_model()` dynamically creates a `QueryFilters` Pydantic class whose fields are derived from the live schema. The LLM returns a `QueryFilters` instance containing a list of `QuerySlice` objects. Each slice has:
- `conditions` — AND-joined list of typed filters (`StringFilter | NumberFilter | DateFilter | BooleanFilter | EnumFilter`), discriminated by the `type` literal field
- `sort`, `limit`, `group_by`, `aggregations`, `interval`

Pydantic validators in `filter_builder.py` enforce that e.g. `EnumFilter` is only used on enum fields and that enum values are in the sampled set.

### Schema-grounded LLM prompt

`PromptGenerator` ([query_builder/query/prompt_generator.py](query_builder/query/prompt_generator.py)) constructs the system prompt from the user's actual schema rather than a fixed domain. On each call it picks one representative enum / numeric / date field and builds worked examples that use those real field names (so the LLM sees `region`, `amount`, `order_date` for the orders dataset instead of hard-coded `transaction.amount` / `merchant.name`). The numeric picker skips `*_id`-looking columns so example aggregations are semantically meaningful. If the schema is sparse, it falls back to a generic abstract example.

### Factory methods on `QueryOrchestrator`

```python
QueryOrchestrator.from_mongodb(mongo_uri, database_name, collection_name, ...)
QueryOrchestrator.from_elasticsearch(es_host, index_name, ...)
QueryOrchestrator.from_csv(csv_path, category_fields=..., date_columns=..., ...)
QueryOrchestrator.from_sqlmodel(database_url, table, category_fields=..., ...)
```

`from_mongodb` and `from_elasticsearch` share a single client between the schema extractor and the executor, stash it on `orch._shared_client`, and close it in `orch.close()`. `from_csv` loads the file once into a pandas DataFrame and shares it the same way. `from_sqlmodel` builds one SQLAlchemy engine, passes it to both the extractor and the executor, and stashes it as `orch._sql_engine` so `close()` calls `engine.dispose()` (not `.close()`). `table` accepts a SQLModel class, a `sqlalchemy.Table`, or a table-name string (reflected against the engine).

### Adding a new database adapter

1. Create `query_builder/adapters/newdb/` with `schema_extractor.py`, `query_translator.py`, `executor.py` implementing the three `Protocol` interfaces in `core/interfaces.py`. The executor's `execute()` must accept `offset` and `limit` kwargs (declared in the protocol).
2. Add a `from_newdb(...)` classmethod to `QueryOrchestrator`. If the adapter holds a connection/resource, build it once in the classmethod, pass the same handle to both the extractor and the executor, and stash it on `orch._shared_client` so `close()` releases it.

### Testing

The suite has **282 tests** in ~25s, broken into:

- **Offline unit tests** ([tests/](tests/), 261 tests): exercise every module with mocked pymongo / Elasticsearch clients, real pandas for CSV, and a real in-memory SQLite engine for the SQL adapter. Run with `uv run pytest -q -m 'not llm'`.
- **AI integration tests** ([tests/test_ai_integration.py](tests/test_ai_integration.py), 5 tests): hit a real LLM through the full pipeline against a small CSV.
- **Hard AI evaluation** ([tests/test_ai_eval_complex.py](tests/test_ai_eval_complex.py), 16 tests): real LLM against a 200-row synthetic orders dataset ([tests/fixtures/build_complex_dataset.py](tests/fixtures/build_complex_dataset.py)) with 18 columns covering enums of varying cardinality, two date columns, numerics, booleans, and free text. Covers simple filters, multi-condition AND, date ranges, invalid-enum dropping, top-N/sort, "recent" without limit, group+sum, two-field group, having clauses, date histograms, multi-agg, and two-slice comparisons.

LLM-marked tests require `LLM_API_KEY` (or `OPENAI_API_KEY`); CI skips them via `-m 'not llm'`.

### Tooling & CI

- **Ruff** for lint + format. Config in [pyproject.toml](pyproject.toml) under `[tool.ruff]`. Rules: `E F I UP B SIM C4 RUF`; per-file ignores for tests and examples.
- **ty** (Astral's static type checker) for type checking, scoped to `query_builder/` only. Pydantic-AI's dynamic Agent construction and pandas typing gaps are downgraded to warnings rather than littering `# type: ignore`.
- **Pre-commit** ([.pre-commit-config.yaml](.pre-commit-config.yaml)) wires `ruff check --fix`, `ruff format`, `ty check`, and standard hygiene hooks (trailing whitespace, EOF, large files cap). Uses `language: system` so it runs via the venv'd tools without needing GitHub access.
- **GitHub Actions** ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs `ruff check`, `ruff format --check`, `ty check`, and the offline pytest suite on push / PR to `main`.
