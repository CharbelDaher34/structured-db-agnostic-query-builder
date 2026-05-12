"""
FastAPI REST API for MongoDB Query Builder.

Converts natural language queries to MongoDB aggregation pipelines.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from query_builder import QueryOrchestrator

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# Default configuration from environment.
# Domain-specific defaults are env-driven; the CATEGORY_FIELDS / FIELDS_TO_IGNORE lists
# accept comma-separated values.
DEFAULT_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DEFAULT_DATABASE = os.getenv("MONGO_DATABASE", "visa_adcb_poc")
DEFAULT_COLLECTION = os.getenv("MONGO_COLLECTION", "llm_transactions")


def _parse_csv_env(name: str, fallback: List[str]) -> List[str]:
    raw = os.getenv(name)
    if not raw:
        return fallback
    return [item.strip() for item in raw.split(",") if item.strip()]


DEFAULT_CATEGORY_FIELDS = _parse_csv_env(
    "DEFAULT_CATEGORY_FIELDS",
    [
        "transaction_location",
        "transaction_currency",
        "merchant_name",
        "merchant_country",
        "merchant_category_name",
        "payment_card_name",
    ],
)
DEFAULT_FIELDS_TO_IGNORE = _parse_csv_env(
    "DEFAULT_FIELDS_TO_IGNORE",
    ["converted_currency", "merchant_category_description"],
)
DEFAULT_SAMPLE_SIZE = int(os.getenv("MONGO_SAMPLE_SIZE", "1000"))


def _orchestrator_key(
    mongo_uri: str,
    database_name: str,
    collection_name: str,
    category_fields: tuple,
    fields_to_ignore: tuple,
) -> tuple:
    return (mongo_uri, database_name, collection_name, category_fields, fields_to_ignore)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Create the default orchestrator once at startup and tear it (and any
    overrides created at request time) down on shutdown.
    """
    app.state.orchestrators: Dict[tuple, QueryOrchestrator] = {}

    default_key = _orchestrator_key(
        DEFAULT_MONGO_URI,
        DEFAULT_DATABASE,
        DEFAULT_COLLECTION,
        tuple(DEFAULT_CATEGORY_FIELDS),
        tuple(DEFAULT_FIELDS_TO_IGNORE),
    )

    logger.info(
        "Warming up default orchestrator for %s.%s",
        DEFAULT_DATABASE,
        DEFAULT_COLLECTION,
    )
    default_orch = QueryOrchestrator.from_mongodb(
        mongo_uri=DEFAULT_MONGO_URI,
        database_name=DEFAULT_DATABASE,
        collection_name=DEFAULT_COLLECTION,
        category_fields=DEFAULT_CATEGORY_FIELDS,
        fields_to_ignore=DEFAULT_FIELDS_TO_IGNORE,
        sample_size=DEFAULT_SAMPLE_SIZE,
    )
    default_orch.warm_up()
    app.state.orchestrators[default_key] = default_orch

    try:
        yield
    finally:
        for orch in app.state.orchestrators.values():
            try:
                orch.close()
            except Exception:
                logger.warning("Failed to close orchestrator", exc_info=True)


app = FastAPI(
    title="MongoDB Query Builder API",
    description="Convert natural language queries to MongoDB aggregation pipelines",
    version="1.0.0",
    lifespan=lifespan,
)


class QueryRequest(BaseModel):
    """Request model for natural language query."""

    query: str = Field(..., description="Natural language query string")
    category_fields: Optional[List[str]] = Field(
        None, description="Fields to treat as categories (defaults to server config)"
    )
    fields_to_ignore: Optional[List[str]] = Field(
        None, description="Fields to ignore (defaults to server config)"
    )


class QueryResponse(BaseModel):
    """Response model for query conversion."""

    natural_language_query: str
    extracted_filters: Dict[str, Any]
    database_queries: List[Dict[str, Any]]
    results: Optional[List[Dict[str, Any]]] = None


def _get_or_create_orchestrator(
    request: Request,
    mongo_uri: str,
    database_name: str,
    collection_name: str,
    category_fields: List[str],
    fields_to_ignore: List[str],
) -> QueryOrchestrator:
    """Return a cached orchestrator for the given config, creating one on miss."""
    cache: Dict[tuple, QueryOrchestrator] = request.app.state.orchestrators
    key = _orchestrator_key(
        mongo_uri,
        database_name,
        collection_name,
        tuple(category_fields),
        tuple(fields_to_ignore),
    )

    orch = cache.get(key)
    if orch is not None:
        return orch

    logger.info(
        "Building new orchestrator for %s.%s (cache miss)",
        database_name,
        collection_name,
    )
    orch = QueryOrchestrator.from_mongodb(
        mongo_uri=mongo_uri,
        database_name=database_name,
        collection_name=collection_name,
        category_fields=category_fields,
        fields_to_ignore=fields_to_ignore,
        sample_size=DEFAULT_SAMPLE_SIZE,
    )
    orch.warm_up()
    cache[key] = orch
    return orch


@app.post("/query", response_model=QueryResponse)
async def convert_query(
    request: Request,
    body: QueryRequest,
    mongo_uri: Optional[str] = Query(None, description="MongoDB connection URI"),
    database_name: Optional[str] = Query(None, description="MongoDB database name"),
    collection_name: Optional[str] = Query(None, description="MongoDB collection name"),
    execute: bool = Query(False, description="Run the query and include results"),
    offset: int = Query(0, ge=0, description="Pagination offset (executed queries only)"),
    limit: Optional[int] = Query(
        None,
        ge=1,
        description="Pagination limit applied when the generated query has no $limit",
    ),
):
    """
    Convert natural language query to a MongoDB aggregation pipeline,
    optionally executing it with pagination.
    """
    try:
        orchestrator = _get_or_create_orchestrator(
            request=request,
            mongo_uri=mongo_uri or DEFAULT_MONGO_URI,
            database_name=database_name or DEFAULT_DATABASE,
            collection_name=collection_name or DEFAULT_COLLECTION,
            category_fields=body.category_fields or DEFAULT_CATEGORY_FIELDS,
            fields_to_ignore=body.fields_to_ignore or DEFAULT_FIELDS_TO_IGNORE,
        )

        result = await orchestrator.query(
            natural_language_query=body.query,
            execute=execute,
            offset=offset,
            limit=limit,
        )

        logger.info(
            "query handled query=%r extracted_filter_slices=%d",
            body.query,
            len(result.get("extracted_filters", {}).get("filters", [])),
        )

        return QueryResponse(
            natural_language_query=result["natural_language_query"],
            extracted_filters=result["extracted_filters"],
            database_queries=result["database_queries"],
            results=result.get("results"),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Query conversion failed")
        raise HTTPException(status_code=500, detail=f"Query conversion failed: {e}")


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("API_PORT", "8000"))
    host = os.getenv("API_HOST", "0.0.0.0")

    uvicorn.run(app, host=host, port=port)
