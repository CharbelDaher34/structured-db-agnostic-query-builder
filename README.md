# structured-query-builder

Natural-language → database query builder. Plain English in, native query out — for MongoDB, Elasticsearch, SQL databases (PostgreSQL / MySQL / SQLite via SQLAlchemy + SQLModel), or CSV files. Built on `pydantic-ai` for structured LLM output.

```python
from query_builder import QueryOrchestrator

orch = QueryOrchestrator.from_csv("transactions.csv", category_fields=["currency"])
result = await orch.query("top 5 most expensive transactions in USD", execute=True)
print(result["results"])
```

---

## How it works

This library never lets the model emit a raw database query. It builds a *typed shape of valid queries against your actual schema*, asks the model to fill in that shape, then translates the validated result into a native query. Direct text-to-SQL hands the model a blank cheque — it can invent columns, write `DROP TABLE`, or emit dialect-specific SQL your database doesn't speak. This approach hands it a form instead.

The four stages are deterministic and inspectable:

### 1. Sample the live data source → uniform field map

Each backend adapter reads your data source once and produces the same dict shape:

```python
{
    "amount":     {"type": "number"},
    "currency":   {"type": "enum", "values": ["USD", "EUR", "GBP"]},
    "created_at": {"type": "date"},
    "is_paid":    {"type": "boolean"},
}
```

How each adapter gets there:

- **SQL** — reads `Table` columns via SQLAlchemy and maps each `column.type.python_type` to a normalized type (`str` → `string`, `int`/`float` → `number`, `datetime` → `date`, …).
- **MongoDB** — random-samples N documents with `$sample` and infers each field's type from the actual values seen (so a schemaless collection still produces a usable schema).
- **Elasticsearch** — reads the index mapping; remembers `text` vs `keyword` so the translator knows whether to suffix `.keyword` for exact-match aggregations.
- **CSV** — inspects pandas dtypes; sniffs ISO-date strings in object columns to recover dates from string-typed CSVs.

For any field marked as a `category_field`, the adapter also runs a bounded distinct-values query (`SELECT DISTINCT col LIMIT N`, `$group + $limit`, ES `terms` agg, or `df[col].unique()[:N]`) and stores the result under `"values"`. The model is later only allowed to pick from this set for that field.

### 2. Field map → Pydantic model of valid queries

This is the core of the library — [`FilterModelBuilder`](query_builder/query/filter_builder.py) dynamically constructs a class hierarchy that *describes every legal query against your schema*. The class is built once per schema and cached.

The building blocks:

- A `FieldEnum` containing one member per field name in the schema. Anywhere a filter, sort, group-by, or aggregation names a field, it must come from this enum — referencing a column that doesn't exist fails validation, not execution.
- One filter class per data type, each pinned to its own operator enum:

  | Filter | Allowed operators |
  |---|---|
  | `StringFilter` | `is`, `different`, `contains`, `isin`, `notin`, `exists` |
  | `NumberFilter` | `<`, `>`, `is`, `different`, `between`, `isin`, `notin`, `exists` |
  | `DateFilter` | `<`, `>`, `is`, `different`, `between`, `exists` |
  | `BooleanFilter` | `is`, `different`, `exists` |
  | `EnumFilter` | `is`, `different`, `isin`, `notin`, `exists` (+ values must come from the schema's `values` list) |

- A discriminated `Union[StringFilter | NumberFilter | DateFilter | BooleanFilter | EnumFilter]` keyed on a `type` literal, so every condition is exactly one filter type — and a `model_validator` on each filter class rejects the wrong type on the wrong field (e.g. `StringFilter` pointed at a date column raises immediately).
- A `QuerySlice` wrapping `conditions: list[FilterType]` plus optional `sort`, `limit`, `group_by`, `aggregations`, and a date `interval` (`day`/`week`/`month`/`year`). A slice-level validator drops conditions where the LLM emitted a null-field placeholder and clears `aggregations`/`interval` whenever there's no `group_by`.
- A top-level `QueryFilters` containing `filters: list[QuerySlice]` — multiple slices express "A vs B" comparisons in a single round-trip.

The resulting class would reject, for example: a `StringFilter` on a `date` column; a `NumberFilter` with `operator="contains"`; an `EnumFilter` with `value="DEM"` when the sampled data only contained `["USD", "EUR", "GBP"]`; a sort on a field name that isn't in the table. All of these are caught at parse time, before any query is constructed.

### 3. Fill in the model

The system prompt is built from the same field map: [`PromptGenerator`](query_builder/query/prompt_generator.py) picks representative enum / numeric / date fields from *your* schema and writes worked examples using *your* actual field names, so the model sees `region`, `amount`, `order_date` instead of a hard-coded financial example that doesn't match your data.

The model is invoked with the system prompt + the user's natural-language query and constrained to return a `QueryFilters` instance via structured output. Whatever it returns is validated against the model above; if anything fails (unknown field, wrong filter type for a field, invalid enum value, …), parsing rejects it before translation runs.

### 4. Structured filter → native query

The validated `QueryFilters` is then translated by the backend-specific translator:

- **SQL** ([`SQLQueryTranslator`](query_builder/adapters/sql/query_translator.py)) — emits SQLAlchemy `Select` statements. Operators map to SQLAlchemy expressions: `>` → `col > value`, `between` → `col.between(a, b)`, `isin` → `col.in_(...)`, `contains` → `col.ilike("%v%")`. Aggregations become `func.sum`/`avg`/`count`/`min`/`max` with labeled aliases; `having_operator`/`having_value` become a `HAVING` clause. `group_by` with a date `interval` becomes dialect-aware date truncation (`date_trunc` on PostgreSQL, `strftime` on SQLite, `date_format` on MySQL, generic `cast(col, Date)` fallback). Sort on an aggregated field is rewritten to the alias so it references the result column, not the raw input.
- **MongoDB** — emits an aggregation pipeline: `$match` for filters, `$group` for aggregations (using `$convert` for date binning so BSON `Date` and ISO-string columns both work), `$sort`, `$skip`/`$limit` for pagination.
- **Elasticsearch** — emits the query DSL: `bool` queries for conditions, `terms` / `date_histogram` / `sum`/`avg`/… aggs for grouping, `from`/`size` for pagination. Multi-slice queries go through `_msearch` to save round-trips.
- **CSV** — emits a pandas execution plan: boolean masks for filters, `pd.Grouper(freq=...)` for date intervals, `pd.NamedAgg` for aggregations, post-mask `having` filtering.

The executor runs the native query and returns a uniform `{total_hits, documents, aggregations?, success}` shape across all backends. `total_hits` is always the *pre-pagination* row count, so the same UI code can render result counts regardless of which adapter served the request.

### What this design gives you

- **Schema-grounded.** Field names, types, and enum values come from your live data — the model can't invent a column or use an enum value that doesn't exist.
- **Read-only by construction.** The output type only describes filters, sort, group-by, and aggregation. There is no syntactic path to `DROP`, `DELETE`, or `UPDATE`.
- **Backend-agnostic.** The same natural-language query produces the same `QueryFilters` regardless of backend — only step 4 changes. Switching MongoDB → PostgreSQL doesn't touch the prompt.
- **Auditable.** `result["extracted_filters"]` exposes the structured intent *before* translation, so you can log it, cache it, hand-edit it, or diff two runs deterministically.

---

## Install

The package ships with the CSV and SQL (SQLAlchemy / SQLModel) adapters included. MongoDB and Elasticsearch drivers are optional **extras** — you only pay for the driver wheels you actually use.

For specific SQL dialects you'll also need the matching DB-API driver (`psycopg` for PostgreSQL, `pymysql` for MySQL, etc.) — SQLite works out of the box.

Install with [uv](https://docs.astral.sh/uv/) directly from GitHub (not yet on PyPI):

```bash
# CSV + SQL adapters
uv add "structured-query-builder @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder"

# + MongoDB driver
uv add "structured-query-builder[mongodb] @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder"

# + Elasticsearch driver
uv add "structured-query-builder[elasticsearch] @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder"

# All adapters
uv add "structured-query-builder[all] @ git+https://github.com/CharbelDaher34/structured-db-agnostic-query-builder"
```

### Requirements

- Python ≥ 3.12
- An OpenAI-compatible LLM endpoint (OpenAI, Ollama, vLLM, Azure OpenAI, …)

---

## Configure the LLM

Pick a provider via `LLM_PROVIDER` and set the matching API key. The library reads everything from environment variables, so you typically set them once via `.env` / `os.environ` and forget about them.

| `LLM_PROVIDER` | Backing pydantic-ai model | Required env vars |
|---|---|---|
| `openai` (default) | `OpenAIChatModel` | `LLM_MODEL`, `OPENAI_API_KEY` |
| `anthropic` | `AnthropicModel` | `LLM_MODEL`, `ANTHROPIC_API_KEY` |
| `google` | `GoogleModel` | `LLM_MODEL`, `GOOGLE_API_KEY` (or `GEMINI_API_KEY`) |
| `openai-compatible` | `OpenAIChatModel` | `LLM_MODEL`, `LLM_BASE_URL` (e.g. Ollama, vLLM); `OPENAI_API_KEY` optional |

```env
# OpenAI
LLM_PROVIDER=openai
LLM_MODEL=gpt-4.1
OPENAI_API_KEY=sk-...

# — or — Anthropic
LLM_PROVIDER=anthropic
LLM_MODEL=claude-sonnet-4-5
ANTHROPIC_API_KEY=sk-ant-...

# — or — Google Gemini
LLM_PROVIDER=google
LLM_MODEL=gemini-1.5-pro
GOOGLE_API_KEY=...

# — or — any OpenAI-compatible endpoint (Ollama, vLLM, Azure, …)
LLM_PROVIDER=openai-compatible
LLM_MODEL=qwen3:8b
LLM_BASE_URL=http://localhost:11434       # /v1 suffix is added automatically
```

You can also pass these explicitly when you don't want to use env vars:

```python
from query_builder.llm.client_factory import LLMClientFactory

# OpenAI
LLMClientFactory(provider="openai", model_name="gpt-4.1", api_key="sk-...")

# Anthropic
LLMClientFactory(provider="anthropic", model_name="claude-sonnet-4-5", api_key="sk-ant-...")

# Local Ollama
LLMClientFactory(
    provider="openai-compatible",
    model_name="qwen3:8b",
    base_url="http://localhost:11434",
)
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

### SQL (PostgreSQL / MySQL / SQLite via SQLAlchemy + SQLModel)

```python
import asyncio
from datetime import datetime
from sqlmodel import Field, SQLModel
from query_builder import QueryOrchestrator


class Transaction(SQLModel, table=True):
    id: int = Field(primary_key=True)
    amount: float
    currency: str
    category: str
    created_at: datetime


async def main():
    orch = QueryOrchestrator.from_sqlmodel(
        database_url="postgresql+psycopg://user:pwd@host/mydb",
        table=Transaction,                       # SQLModel class, Table, or table name
        category_fields=["currency", "category"],
    )

    result = await orch.query(
        "What's the average transaction amount by category in the last 30 days?",
        execute=True,
    )
    print(result["database_queries"])   # SQLAlchemy Select statements
    print(result["results"])
    orch.close()                        # disposes the SQLAlchemy engine

asyncio.run(main())
```

Works against anything SQLAlchemy supports — the `database_url` decides the dialect:

| Dialect | URL format | Extra driver |
|---|---|---|
| PostgreSQL | `postgresql+psycopg://user:pwd@host/db` | `pip install psycopg` |
| MySQL | `mysql+pymysql://user:pwd@host/db` | `pip install pymysql` |
| SQLite | `sqlite:///path/to.db` | built-in |

The `table` argument accepts a SQLModel/SQLAlchemy class, a `sqlalchemy.Table` instance, or a table-name string (reflected from the live database). One SQLAlchemy engine is shared between schema extraction and query execution, then disposed by `orch.close()`. Date-bucket grouping is dialect-aware (`date_trunc` on PostgreSQL, `strftime` on SQLite, `date_format` on MySQL).

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
    "database_queries": [...],              # native query (MongoDB pipeline / ES DSL / SQLAlchemy Select / pandas plan)
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
| `orch.build_query(nl_query)` | Translate without executing — returns `extracted_filters` + `database_queries` only. Same as `query(nl, execute=False)` but more discoverable when the intent is "show me what would run" |
| `orch.print_model_summary()` | Pretty-print the inferred schema — useful for verifying your `category_fields` and `fields_to_ignore` are correct |
| `orch.query_raw(query, size)` | Execute a raw native query, bypassing the LLM |

---

## Logging

The package uses a single logger entry point — every module logs through `QueryBuilderLogger` so you can adjust verbosity from one place:

```python
from query_builder import QueryBuilderLogger

QueryBuilderLogger.configure(level="DEBUG")   # DEBUG | INFO | WARNING | ERROR
```

If you don't call `configure()`, the package self-configures on first use from the `LOG_LEVEL` env var (default `INFO`). The default handler writes to stderr and the package logger doesn't propagate to the root logger, so it won't double-log when your host app has its own handler.

At `INFO`, each query emits a lifecycle trace with millisecond timings:

```
[INFO] query_builder.orchestrator :: orchestrator initialised: extractor=SQLSchemaExtractor translator=SQLQueryTranslator executor=SQLQueryExecutor llm_provider=openai llm_model=gpt-4.1
[INFO] query_builder.orchestrator :: query.start: 'top 5 most expensive transactions in USD' (execute=True, offset=0, limit=None)
[INFO] query_builder.orchestrator :: query.llm_call: provider=openai model=gpt-4.1
[INFO] query_builder.orchestrator :: query.llm_done: 1 slice(s) in 920 ms
[INFO] query_builder.orchestrator :: query.translated: 1 native query/queries via SQLQueryTranslator in 1 ms
[INFO] query_builder.orchestrator :: query.execute: 1 query/queries via SQLQueryExecutor
[INFO] query_builder.orchestrator :: query.execute_done: total_hits=5 failures=0 in 12 ms
[INFO] query_builder.orchestrator :: query.done: total 936 ms (execute=True)
```

Bump to `DEBUG` to also see the extracted-filter payload and per-field distinct-values resolution.

---

## Examples

See [`examples/`](examples/) for full runnable scripts:

- [`example_csv_usage.py`](examples/example_csv_usage.py) — three scenarios on a CSV (filter+sort+limit, group-by + having, date-interval aggregation)
- [`example_mongodb_usage.py`](examples/example_mongodb_usage.py) — MongoDB equivalents
- [`example_client_factory_usage.py`](examples/example_client_factory_usage.py) — using the LLM client factory directly

Run any of them:

```bash
LLM_PROVIDER=openai LLM_MODEL=gpt-4.1 OPENAI_API_KEY=sk-... uv run examples/example_csv_usage.py
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
