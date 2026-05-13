# structured-query-builder

Natural-language → database query builder. Plain English in, native query out — for MongoDB, Elasticsearch, or CSV files. Built on `pydantic-ai` for structured LLM output.

```python
from query_builder import QueryOrchestrator

orch = QueryOrchestrator.from_csv("transactions.csv", category_fields=["currency"])
result = await orch.query("top 5 most expensive transactions in USD", execute=True)
print(result["results"])
```

---

## Install

The package ships with the CSV adapter included. MongoDB and Elasticsearch drivers are optional **extras** so you only install what you need.

Install with [uv](https://docs.astral.sh/uv/) directly from GitHub (not yet on PyPI):

```bash
# CSV adapter only
uv add "structured-query-builder @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder"

# + MongoDB driver
uv add "structured-query-builder[mongodb] @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder"

# + Elasticsearch driver
uv add "structured-query-builder[elasticsearch] @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder"

# All adapters
uv add "structured-query-builder[all] @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder"
```

### Pinning a version

Append `@<tag-or-commit>` to lock to a specific revision:

```bash
uv add "structured-query-builder @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder@v0.1.0"
```

### Requirements

- Python ≥ 3.12
- An OpenAI-compatible LLM endpoint (OpenAI, Ollama, vLLM, Azure OpenAI, …)

---

## Configure the LLM

The library reads credentials from environment variables, so you typically set them once via `.env` / `os.environ` and forget about them.

```env
LLM_MODEL=gpt-4.1
LLM_API_KEY=sk-...
# Optional — point at any OpenAI-compatible API (Ollama, vLLM, Azure, etc.)
LLM_BASE_URL=http://localhost:11434/v1
```

`LLM_API_KEY` falls back to `OPENAI_API_KEY` if the former is unset.

You can also pass these explicitly when you don't want to use env vars:

```python
from query_builder.llm.client_factory import LLMClientFactory

LLMClientFactory(model_name="gpt-4.1", api_key="sk-...", base_url="https://api.openai.com/v1")
```

---

## Usage

The public entry point is `QueryOrchestrator`. Pick a factory that matches your data source, optionally warm it up, and call `query()`.

### CSV files

```python
import asyncio
from query_builder import QueryOrchestrator

async def main():
    orch = QueryOrchestrator.from_csv(
        csv_path="transactions.csv",
        category_fields=["category", "currency"],   # exposed as enums to the LLM
        date_columns=["timestamp"],                 # parsed as datetimes
        fields_to_ignore=["internal_notes"],
        read_csv_kwargs={"sep": ",", "encoding": "utf-8"},
    )
    orch.warm_up()  # pre-build schema/filter model/prompt (optional)

    result = await orch.query("How much did I spend on food each month?", execute=True)

    print(result["extracted_filters"])     # structured filter the LLM produced
    print(result["database_queries"])      # the pandas execution plan
    print(result["results"])               # the actual rows
    orch.close()

asyncio.run(main())
```

The CSV is loaded once into a pandas DataFrame and reused for every query.

### MongoDB

```python
import asyncio
from query_builder import QueryOrchestrator

async def main():
    orch = QueryOrchestrator.from_mongodb(
        mongo_uri="mongodb://localhost:27017",
        database_name="mydb",
        collection_name="transactions",
        category_fields=["merchant_category", "currency"],
        fields_to_ignore=["internal_id"],
        sample_size=1000,
    )

    result = await orch.query(
        "Show me the top 10 most expensive transactions in France",
        execute=True,
        offset=0,
        limit=50,   # only injected if the LLM-generated query has no $limit
    )
    print(result["database_queries"])   # MongoDB aggregation pipeline
    print(result["results"])
    orch.close()

asyncio.run(main())
```

Requires `pip install "structured-query-builder[mongodb]"`. The orchestrator shares a single `MongoClient` between schema extraction and query execution.

### Elasticsearch

```python
import asyncio
from query_builder import QueryOrchestrator

async def main():
    orch = QueryOrchestrator.from_elasticsearch(
        es_host="http://localhost:9200",
        index_name="transactions",
        category_fields=["category", "status"],
        include_bucket_documents=False,   # opt in to per-bucket top_hits if needed
    )

    result = await orch.query(
        "What's the average transaction amount by category?",
        execute=True,
    )
    print(result["database_queries"])   # ES query DSL
    orch.close()

asyncio.run(main())
```

Requires `pip install "structured-query-builder[elasticsearch]"`.

---

## What you get back

`orchestrator.query()` returns a dict:

```python
{
    "natural_language_query": "...",
    "extracted_filters": {                  # validated Pydantic output from the LLM
        "filters": [
            {
                "conditions": [
                    {"type": "EnumFilter", "field": "currency",
                     "operator": "is", "value": "USD"}
                ],
                "sort": [{"field": "amount", "order": "desc"}],
                "limit": 5
            }
        ]
    },
    "database_queries": [...],              # native query (MongoDB pipeline / ES DSL / pandas plan)
    "results": [                            # only present when execute=True
        {
            "total_hits": 5,
            "documents": [...],
            "success": True,
        }
    ]
}
```

If `execute=False`, you get the structured filter and the translated query without running it — useful for showing the user what would be executed, building a UI, or sending the query somewhere else.

---

## Common options

| `QueryOrchestrator.query()` parameter | Description |
|---|---|
| `natural_language_query` | The user's plain-English question |
| `execute` (default `True`) | Whether to actually run the query against the data source |
| `offset` (default `0`) | Pagination offset, applied to executed queries |
| `limit` (default `None`) | Pagination limit, applied only when the generated query doesn't already have one |

| Method | Purpose |
|---|---|
| `orch.warm_up()` | Pre-build the schema, filter model and system prompt so the first query isn't slow |
| `orch.close()` | Release DB connections (no-op for CSV) |
| `orch.print_model_summary()` | Pretty-print the inferred schema — useful for verifying your `category_fields` and `fields_to_ignore` are correct |
| `orch.query_raw(query, size)` | Execute a raw native query, bypassing the LLM |

---

## How it works

1. **Schema extraction** — the adapter samples the data source (random `$sample` for MongoDB, mapping for Elasticsearch, dtypes for CSV) to discover fields, types, and enum values.
2. **Pydantic filter model** — a `QueryFilters` model is built dynamically from that schema. Field names, types, enum values, and per-type operators are all type-checked.
3. **Structured LLM call** — `pydantic-ai` calls your configured model with the system prompt + user query and *validates* the output against `QueryFilters`. The LLM can't return a malformed query.
4. **Translation** — the adapter converts the structured filter into a native query: MongoDB aggregation pipeline, Elasticsearch DSL, or a pandas execution plan.
5. **Execution** — runs on a worker thread (`asyncio.to_thread`) so blocking DB drivers don't stall your event loop.

Each filter slice supports `conditions` (AND-joined), `sort`, `limit`, `group_by`, `aggregations` (sum/avg/count/min/max with optional `having_operator`/`having_value`), and a date `interval` for time-bucketed grouping. Multiple slices express comparisons like "A vs B".

---

## Examples

See [`examples/`](examples/) for full runnable scripts:

- [`example_csv_usage.py`](examples/example_csv_usage.py) — three scenarios on a CSV (filter+sort+limit, group-by + having, date-interval aggregation)
- [`example_mongodb_usage.py`](examples/example_mongodb_usage.py) — MongoDB equivalents
- [`example_client_factory_usage.py`](examples/example_client_factory_usage.py) — using the LLM client factory directly

Run any of them:

```bash
LLM_MODEL=gpt-4.1 LLM_API_KEY=sk-... uv run examples/example_csv_usage.py
```

---

## Optional REST API

The repo also includes [`api.py`](api.py) — a small FastAPI server exposing `POST /query` with lifespan-managed orchestrator caching. It's bundled as a runnable example, not as a library import; if you want to expose the query builder over HTTP, copy it into your project and adapt the defaults.

---

## Limitations

- One data source per orchestrator — no cross-source joins.
- AND-only within a single filter slice. Multi-slice comparisons cover the most common "A vs B" case; nested `(A OR B) AND C` is not yet supported.
- The CSV adapter loads the file into memory (pandas DataFrame).
- Schema changes require recreating the orchestrator (or calling `invalidate_cache()` on the adapter's extractor).

---

## License

MIT. See [pyproject.toml](pyproject.toml).
