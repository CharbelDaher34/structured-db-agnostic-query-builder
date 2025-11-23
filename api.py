"""
FastAPI REST API for MongoDB Query Builder.

Converts natural language queries to MongoDB aggregation pipelines.
"""

import os
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from query_builder import QueryOrchestrator

load_dotenv()

app = FastAPI(
    title="MongoDB Query Builder API",
    description="Convert natural language queries to MongoDB aggregation pipelines",
    version="1.0.0",
)


class QueryRequest(BaseModel):
    """Request model for natural language query."""
    query: str = Field(..., description="Natural language query string")
    # database_name: Optional[str] = Field(None, description="MongoDB database name")
    # collection_name: Optional[str] = Field(None, description="MongoDB collection name")
    category_fields: Optional[List[str]] = Field(None, description="Fields to treat as categories")
    fields_to_ignore: Optional[List[str]] = Field(None, description="Fields to ignore")


class QueryResponse(BaseModel):
    """Response model for query conversion."""
    natural_language_query: str
    extracted_filters: Dict[str, Any]
    database_queries: List[Dict[str, Any]]


# Default configuration from environment
DEFAULT_MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:vAF4jUA8Iq4amZaNPnQq87X9@84.16.230.94:27017/?authSource=admin")
DEFAULT_DATABASE = os.getenv("MONGO_DATABASE", "visa_adcb")
DEFAULT_COLLECTION = os.getenv("MONGO_COLLECTION", "llm_transactions")
DEFAULT_CATEGORY_FIELDS = [
    "transaction_location",
    "transaction_currency",
    "merchant_name",
    "merchant_country",
    "merchant_category_name",
    "payment_card_name"
]
DEFAULT_FIELDS_TO_IGNORE = ["converted_currency","merchant_category_description"]


def get_orchestrator(
    mongo_uri: str = DEFAULT_MONGO_URI,
    database_name: str = DEFAULT_DATABASE,
    collection_name: str = DEFAULT_COLLECTION,
    category_fields: Optional[List[str]] = None,
    fields_to_ignore: Optional[List[str]] = None,
) -> QueryOrchestrator:
    """Create or get cached orchestrator instance."""
    return QueryOrchestrator.from_mongodb(
        mongo_uri=mongo_uri,
        database_name=database_name,
        collection_name=collection_name,
        category_fields=category_fields or DEFAULT_CATEGORY_FIELDS,
        fields_to_ignore=fields_to_ignore or DEFAULT_FIELDS_TO_IGNORE,
        sample_size=1000,
    )


@app.post("/query", response_model=QueryResponse)
async def convert_query(
    request: QueryRequest,
    mongo_uri: Optional[str] = Query(None, description="MongoDB connection URI"),
    database_name: Optional[str] = Query(None, description="MongoDB database name"),
    collection_name: Optional[str] = Query(None, description="MongoDB collection name"),
):
    """
    Convert natural language query to MongoDB aggregation pipeline.
    
    Returns the MongoDB query without executing it or returning documents.
    """
    try:
        orchestrator = get_orchestrator(
            mongo_uri=mongo_uri or DEFAULT_MONGO_URI,
            database_name=database_name or DEFAULT_DATABASE,
            collection_name=collection_name or DEFAULT_COLLECTION,
            category_fields=request.category_fields,
            fields_to_ignore=request.fields_to_ignore,
        )
        
        # Convert query without executing
        result = await orchestrator.query(natural_language_query=request.query, execute=False)
        print(f"natural_language_query: {result['natural_language_query']}")
        print(f"extracted_filters: {result['extracted_filters']}")
        # print(f"database_queries: {result['database_queries']}")
        return QueryResponse(
            natural_language_query=result["natural_language_query"],
            extracted_filters=result["extracted_filters"],
            database_queries=result["database_queries"],
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query conversion failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("API_PORT", "8000"))
    host = os.getenv("API_HOST", "0.0.0.0")
    
    uvicorn.run(app, host=host, port=port)

