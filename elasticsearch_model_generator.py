"""
Backwards compatibility wrapper for ElasticsearchModelGenerator.

This module provides the same API as the original elasticsearch_model_generator.py
but uses the new refactored architecture underneath.

DEPRECATED: Use query_builder.QueryOrchestrator.from_elasticsearch() instead.
"""

import json
import sys
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from query_builder.orchestrator import QueryOrchestrator


class ElasticsearchModelGenerator:
    """
    Backwards compatibility wrapper for the original ElasticsearchModelGenerator.
    
    All methods delegate to the new QueryOrchestrator implementation.
    
    DEPRECATED: Use query_builder.QueryOrchestrator.from_elasticsearch() instead.
    """
    
    def __init__(
        self,
        index_name: str,
        es_host: str,
        fields_to_ignore: List[str] = [],
        category_fields: List[str] = [],
        model_name: str = "",
        api_key: str = "",
    ):
        """
        Initialize generator (delegates to QueryOrchestrator).
        
        Args:
            index_name: Elasticsearch index name
            es_host: Elasticsearch host URL
            fields_to_ignore: List of fields to ignore
            category_fields: List of fields to treat as categories
            model_name: LLM model name
            api_key: LLM API key
        """
        if not index_name:
            raise ValueError("index_name is required")
        
        self.index_name = index_name
        self.es_host = es_host
        
        # Create orchestrator with new architecture
        self._orchestrator = QueryOrchestrator.from_elasticsearch(
            es_host=es_host,
            index_name=index_name,
            category_fields=category_fields,
            fields_to_ignore=fields_to_ignore,
            llm_model=model_name if model_name else None,
            llm_api_key=api_key if api_key else None,
        )
    
    def GenerateModel(self, model_name: Optional[str] = None) -> type[BaseModel]:
        """Generate Pydantic model from ES mapping."""
        model_name = model_name or f"ES_{self.index_name.capitalize()}"
        return self._orchestrator.generate_model(model_name)
    
    def GetModelInfo(self) -> Dict[str, Any]:
        """Get flattened field information."""
        return self._orchestrator.get_model_info()
    
    def PrintModelSummary(self):
        """Print summary of generated model."""
        self._orchestrator.print_model_summary()
    
    def Query(self, query: str, execute: bool = True) -> Dict[str, Any]:
        """Complete pipeline: natural language to ES results."""
        response = self._orchestrator.query(query, execute)
        
        # Rename for backwards compatibility
        if "database_queries" in response:
            response["elasticsearch_queries"] = response.pop("database_queries")
        
        return response
    
    async def QueryAsync(self, query: str, execute: bool = True) -> Dict[str, Any]:
        """Async version of complete pipeline."""
        response = await self._orchestrator.query_async(query, execute)
        
        # Rename for backwards compatibility
        if "database_queries" in response:
            response["elasticsearch_queries"] = response.pop("database_queries")
        
        return response
    
    def RunRawQuery(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        """Execute raw Elasticsearch query."""
        return self._orchestrator.query_raw(query, size)
    
    # Legacy method aliases
    def generate_model(self, model_name: Optional[str] = None) -> type[BaseModel]:
        return self.GenerateModel(model_name)
    
    def get_model_info(self) -> Dict[str, Any]:
        return self.GetModelInfo()
    
    def print_model_summary(self):
        return self.PrintModelSummary()
    
    def generate_filters_from_query(self, query: str):
        return self.Query(query, execute=False)["extracted_filters"]
    
    async def generate_filters_from_query_async(self, query: str):
        result = await self.QueryAsync(query, execute=False)
        return result["extracted_filters"]
    
    def FilterToElasticQuery(self, query_filters: dict) -> List[Dict[str, Any]]:
        model_info = self.GetModelInfo()
        return self._orchestrator.query_translator.translator.translate(
            query_filters, model_info
        )
    
    def ExecuteElasticQueries(
        self, elastic_queries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        return self._orchestrator.query_executor.execute(elastic_queries)
    
    def QueryFromNaturalLanguage(
        self, query: str, execute: bool = True, size: int = 100
    ) -> Dict[str, Any]:
        return self.Query(query, execute)
    
    async def QueryFromNaturalLanguageAsync(
        self, query: str, execute: bool = True, size: int = 100
    ) -> Dict[str, Any]:
        return await self.QueryAsync(query, execute)
    
    def run_raw_elastic_query(
        self, query: Dict[str, Any], size: int = 100
    ) -> Dict[str, Any]:
        return self.RunRawQuery(query, size)
    
    def debug_category_fields(self) -> Dict[str, Any]:
        """Debug method for category fields (limited support in new architecture)."""
        print("Warning: debug_category_fields has limited support in refactored version")
        return {
            "configured_category_fields": self._orchestrator.category_fields,
            "message": "Use orchestrator.get_model_info() for field information",
        }
    
    def populate_with_examples(self, model_class: type[BaseModel]) -> dict:
        """Populate model with example values (limited support)."""
        print("Warning: populate_with_examples not fully supported in refactored version")
        return {}
    
    def get_example_value(self, annotation):
        """Get example value for annotation (limited support)."""
        if annotation is str:
            return "example_string"
        elif annotation in (int, float):
            return 42
        elif annotation is bool:
            return True
        return None


def process_single_query(
    natural_language_query: str,
    es_host: str,
    index_name: str,
    category_fields: Optional[List[str]] = None,
    fields_to_ignore: Optional[List[str]] = None,
    llm_model: str = "gpt-4o",
    llm_api_key: Optional[str] = None,
    execute: bool = True,
) -> Dict[str, Any]:
    """
    Process a single natural language query and return the result.
    
    Args:
        natural_language_query: Natural language query string
        es_host: Elasticsearch host URL
        index_name: Elasticsearch index name
        category_fields: List of fields to treat as categories
        fields_to_ignore: List of fields to ignore
        llm_model: LLM model name
        llm_api_key: LLM API key
        execute: If True, execute the query and return results
        
    Returns:
        Dictionary with query, filters, elasticsearch queries, and optionally results
    """
    client = ElasticsearchModelGenerator(
        es_host=es_host,
        index_name=index_name,
        category_fields=category_fields or [],
        fields_to_ignore=fields_to_ignore or [],
        model_name=llm_model,
        api_key=llm_api_key or "",
    )
    
    return client.Query(natural_language_query, execute=execute)


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Example usage
    if len(sys.argv) > 1:
        query = sys.argv[1]
    else:
        query = "Show me all my transactions from April 2024"
    
    result = process_single_query(
        natural_language_query=query,
        es_host=os.getenv("elastic_host", "http://localhost:9200"),
        index_name="user_transactions",
        category_fields=[
            "card_kind",
            "card_type",
            "transaction.receiver.category_type",
            "transaction.receiver.location",
            "transaction.type",
            "transaction.currency",
        ],
        fields_to_ignore=["user_id", "card_number"],
        llm_model="gpt-4o",
        llm_api_key=os.getenv("OPENAI_API_KEY"),
        execute=True,
    )
    
    print("\n" + "=" * 80)
    print("QUERY RESULT")
    print("=" * 80)
    print(f"\nNatural Language Query: {result['natural_language_query']}")
    print(f"\nExtracted Filters:\n{json.dumps(result['extracted_filters'], indent=2)}")
    print(f"\nElasticsearch Queries:\n{json.dumps(result['elasticsearch_queries'], indent=2)}")
    
    if "results" in result:
        for i, res in enumerate(result["results"]):
            print(f"\n--- Result {i+1} ---")
            print(f"Total Hits: {res.get('total_hits', 0)}")
            print(f"Documents Returned: {len(res.get('documents', []))}")
            if res.get('error'):
                print(f"Error: {res['error']}")
