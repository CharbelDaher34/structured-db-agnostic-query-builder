from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
import os
from elasticsearch_model_generator import ElasticsearchModelGenerator, ModelBuilder, FilterModelBuilder, LlmClientFactory, FiltersToDsl
import json

app = FastAPI(title="Elasticsearch Query Generator API", version="1.0.0")

# Request/Response Models
class QueryRequest(BaseModel):
    user_input: str = Field(..., description="Natural language query from user")

class QueryResponse(BaseModel):
    natural_language_query: str
    extracted_filters: Dict[str, Any]
    elasticsearch_queries: List[Dict[str, Any]]

class MappingQueryRequest(BaseModel):
    user_input: str = Field(..., description="Natural language query from user")
    elasticsearch_mapping: Dict[str, Any] = Field(..., description="Elasticsearch index mapping")
    enum_fields: Dict[str, List[Any]] = Field(default={}, description="Dictionary of enum fields with their possible values, e.g., {'status': ['active', 'inactive']}")
    fields_to_ignore: List[str] = Field(default=[], description="Fields to ignore in model generation")

class PydanticModelResponse(BaseModel):
    model_schema: Dict[str, Any] = Field(..., description="Generated Pydantic model schema")
    model_info: Dict[str, Any] = Field(..., description="Flattened field information")
    natural_language_query: str
    extracted_filters: Dict[str, Any]
    elasticsearch_queries: List[Dict[str, Any]]

# Helper function to get environment variables
def GetEnvConfig():
    """Get configuration from environment variables."""
    config = {
        "index_name": os.getenv("INDEX_NAME", "user_transactions"),
        "es_host": os.getenv("ES_HOST", "http://elastic:rvs59tB_VVANUy4rC-kd@84.16.230.94:9200"),
        "category_fields": os.getenv("CATEGORY_FIELDS", "").split(",") if os.getenv("CATEGORY_FIELDS") else [
            "card_kind",
            "card_type", 
            "transaction.receiver.category_type",
            "transaction.receiver.location",
            "transaction.type",
            "transaction.currency"],
        "fields_to_ignore": os.getenv("FIELDS_TO_IGNORE", "").split(",") if os.getenv("FIELDS_TO_IGNORE") else ["user_id", "card_number"],
        # "model_name": os.getenv("MODEL_NAME", "ollama/qwen3:8b"),
        "model_name": "gemini-2.0-flash",
        "api_key": os.getenv("API_KEY", "AIzaSyDp8n_AmYsspADJBaNpkJvBdlch1-9vkhw")
    }
    
    if not config["index_name"] and not os.getenv("INDEX_NAME"):
        print("Warning: INDEX_NAME environment variable not set, using default.")
    if not config["api_key"] and not os.getenv("API_KEY"):
        print("Warning: API_KEY environment variable not set, using default.")
    
    return config

@app.get("/health")
async def HealthCheck():
    """Health check endpoint."""
    return {"status": "healthy", "message": "Elasticsearch Query Generator API is running"}

@app.post("/query", response_model=QueryResponse)
async def GenerateQuery(request: QueryRequest):
    """
    Generate Elasticsearch queries from natural language using environment configuration.
    """
    try:
        config = GetEnvConfig()
        generator = ElasticsearchModelGenerator(
            index_name=config["index_name"],
            es_host=config["es_host"],
            fields_to_ignore=config["fields_to_ignore"],
            category_fields=config["category_fields"],
            model_name=config["model_name"],
            api_key=config["api_key"]
        )
        result = await generator.QueryAsync(request.user_input, execute=False)
        return QueryResponse(
            natural_language_query=result["natural_language_query"],
            extracted_filters=result["extracted_filters"],
            elasticsearch_queries=result["elasticsearch_queries"]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/query-from-mapping", response_model=QueryResponse)
async def GenerateQueryFromMapping(request: MappingQueryRequest):
    """
    Generate Pydantic models and Elasticsearch queries from a provided ES mapping.
    This endpoint does not require a database connection.
    """
    try:
        config = GetEnvConfig()
        
        model_builder = ModelBuilder(
            mapping=request.elasticsearch_mapping,
            enum_fields=request.enum_fields,
            fields_to_ignore=request.fields_to_ignore
        )
        
        model_info = model_builder.GetModelInfo()
        
        filter_builder = FilterModelBuilder(model_info)
        llm_factory = LlmClientFactory(config["model_name"], config["api_key"])
        
        filter_model = filter_builder.BuildFilterModel()
        system_prompt = filter_builder.GenerateSystemPrompt()
        
        filters = await llm_factory.ParseQueryAsync(
            request.user_input, 
            filter_model, 
            system_prompt
        )
        
        elastic_queries = FiltersToDsl(filters, model_info)
        
        return QueryResponse(
            natural_language_query=request.user_input,
            extracted_filters=filters,
            elasticsearch_queries=elastic_queries
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/config")
async def GetConfig():
    """Get current environment configuration (for debugging)."""
    try:
        config = GetEnvConfig()
        safe_config = {
            "index_name": config["index_name"],
            "es_host": config["es_host"],
            "category_fields": config["category_fields"],
            "fields_to_ignore": config["fields_to_ignore"],
            "model_name": config["model_name"],
            "api_key_configured": bool(config["api_key"])
        }
        return safe_config
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8510) 