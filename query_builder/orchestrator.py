"""
Query orchestrator - main entry point.

Coordinates all components to provide a unified query interface.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from query_builder.core.interfaces import (
    ISchemaExtractor,
    IQueryTranslator,
    IQueryExecutor,
)
from query_builder.schema.extractor import SchemaExtractor
from query_builder.schema.model_builder import ModelBuilder
from query_builder.query.filter_builder import FilterModelBuilder
from query_builder.query.prompt_generator import PromptGenerator
from query_builder.query.translator import QueryTranslator
from query_builder.execution.executor import QueryExecutor
from query_builder.llm.client_factory import LLMClientFactory


class QueryOrchestrator:
    """
    Main orchestrator for database-agnostic query building.
    
    Coordinates schema extraction, model building, LLM parsing,
    query translation, and execution.
    """
    
    def __init__(
        self,
        schema_extractor: ISchemaExtractor,
        query_translator: IQueryTranslator,
        query_executor: IQueryExecutor,
        category_fields: Optional[List[str]] = None,
        fields_to_ignore: Optional[List[str]] = None,
        llm_model: Optional[str] = None,
        llm_api_key: Optional[str] = None,
    ):
        """
        Initialize query orchestrator with database adapters.
        
        Args:
            schema_extractor: Database-specific schema extractor
            query_translator: Database-specific query translator
            query_executor: Database-specific query executor
            category_fields: List of fields to treat as categories
            fields_to_ignore: List of fields to ignore
            llm_model: LLM model name (for natural language queries)
            llm_api_key: LLM API key
        """
        # Store adapters
        self._schema_extractor_impl = schema_extractor
        self._query_translator_impl = query_translator
        self._query_executor_impl = query_executor
        
        # Initialize layers
        self.schema_extractor = SchemaExtractor(
            schema_extractor, category_fields=category_fields
        )
        self.query_translator = QueryTranslator(query_translator)
        self.query_executor = QueryExecutor(query_executor)
        
        # Configuration
        self.category_fields = category_fields or []
        self.fields_to_ignore = fields_to_ignore or []
        
        # LLM setup
        self.llm_factory: Optional[LLMClientFactory] = None
        if llm_model and llm_api_key:
            self.llm_factory = LLMClientFactory(llm_model, llm_api_key)
        
        # Cached components
        self._model_builder: Optional[ModelBuilder] = None
        self._filter_builder: Optional[FilterModelBuilder] = None
        self._prompt_generator: Optional[PromptGenerator] = None
    
    @classmethod
    def from_elasticsearch(
        cls,
        es_host: str,
        index_name: str,
        category_fields: Optional[List[str]] = None,
        fields_to_ignore: Optional[List[str]] = None,
        llm_model: Optional[str] = None,
        llm_api_key: Optional[str] = None,
    ) -> "QueryOrchestrator":
        """
        Create orchestrator for Elasticsearch.
        
        Args:
            es_host: Elasticsearch host URL
            index_name: Name of the index
            category_fields: List of fields to treat as categories
            fields_to_ignore: List of fields to ignore
            llm_model: LLM model name
            llm_api_key: LLM API key
            
        Returns:
            Configured QueryOrchestrator for Elasticsearch
        """
        from query_builder.adapters.elasticsearch import (
            ESSchemaExtractor,
            ESQueryTranslator,
            ESQueryExecutor,
        )
        
        schema_extractor = ESSchemaExtractor(
            es_host=es_host,
            index_name=index_name,
            category_fields=category_fields,
        )
        query_translator = ESQueryTranslator()
        query_executor = ESQueryExecutor(es_host=es_host, index_name=index_name)
        
        return cls(
            schema_extractor=schema_extractor,
            query_translator=query_translator,
            query_executor=query_executor,
            category_fields=category_fields,
            fields_to_ignore=fields_to_ignore,
            llm_model=llm_model,
            llm_api_key=llm_api_key,
        )
    
    @classmethod
    def from_mongodb(
        cls,
        mongo_uri: str,
        database_name: str,
        collection_name: str,
        category_fields: Optional[List[str]] = None,
        fields_to_ignore: Optional[List[str]] = None,
        llm_model: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        sample_size: int = 1000,
    ) -> "QueryOrchestrator":
        """
        Create orchestrator for MongoDB.
        
        Args:
            mongo_uri: MongoDB connection URI
            database_name: Name of the database
            collection_name: Name of the collection
            category_fields: List of fields to treat as categories
            fields_to_ignore: List of fields to ignore
            llm_model: LLM model name
            llm_api_key: LLM API key
            sample_size: Number of documents to sample for schema inference
            
        Returns:
            Configured QueryOrchestrator for MongoDB
        """
        from query_builder.adapters.mongodb import (
            MongoSchemaExtractor,
            MongoQueryTranslator,
            MongoQueryExecutor,
        )
        
        schema_extractor = MongoSchemaExtractor(
            mongo_uri=mongo_uri,
            database_name=database_name,
            collection_name=collection_name,
            category_fields=category_fields,
            sample_size=sample_size,
        )
        query_translator = MongoQueryTranslator()
        query_executor = MongoQueryExecutor(
            mongo_uri=mongo_uri,
            database_name=database_name,
            collection_name=collection_name,
        )
        
        return cls(
            schema_extractor=schema_extractor,
            query_translator=query_translator,
            query_executor=query_executor,
            category_fields=category_fields,
            fields_to_ignore=fields_to_ignore,
            llm_model=llm_model,
            llm_api_key=llm_api_key,
        )
    
    def _get_model_builder(self) -> ModelBuilder:
        """Get or create model builder."""
        if self._model_builder is None:
            schema = self.schema_extractor.get_schema()
            enum_fields = self.schema_extractor.get_enum_fields()
            
            self._model_builder = ModelBuilder(
                schema=schema,
                fields_to_ignore=self.fields_to_ignore,
                enum_fields=enum_fields,
            )
        return self._model_builder
    
    def _get_filter_builder(self) -> FilterModelBuilder:
        """Get or create filter builder."""
        if self._filter_builder is None:
            model_info = self.get_model_info()
            self._filter_builder = FilterModelBuilder(model_info)
        return self._filter_builder
    
    def _get_prompt_generator(self) -> PromptGenerator:
        """Get or create prompt generator."""
        if self._prompt_generator is None:
            model_info = self.get_model_info()
            self._prompt_generator = PromptGenerator(model_info)
        return self._prompt_generator
    
    def generate_model(self, model_name: str = "GeneratedModel") -> type[BaseModel]:
        """
        Generate Pydantic model from schema.
        
        Args:
            model_name: Name for the generated model
            
        Returns:
            Generated Pydantic model class
        """
        model_builder = self._get_model_builder()
        return model_builder.build(model_name)
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Get flattened field information.
        
        Returns:
            Dictionary mapping field paths to field metadata
        """
        model_builder = self._get_model_builder()
        return model_builder.get_model_info()
    
    def print_model_summary(self):
        """Print summary of the generated model."""
        model = self.generate_model()
        model_info = self.get_model_info()
        
        print(f"\n=== Model Summary ===")
        print(f"Model Class: {model.__name__}")
        print(f"Total Fields: {len(model_info)}")
        
        print("\n=== Field Details ===")
        for field_name, field_info in model_info.items():
            field_type = field_info["type"]
            if field_type == "enum":
                values = field_info.get("values", [])
                print(f"  {field_name}: {field_type} ({len(values)} values)")
                if len(values) <= 10:
                    print(f"    Values: {values}")
            elif field_type == "array":
                item_type = field_info.get("item_type", "unknown")
                print(f"  {field_name}: {field_type}[{item_type}]")
            else:
                print(f"  {field_name}: {field_type}")
    
    def query(self, natural_language_query: str, execute: bool = True) -> Dict[str, Any]:
        """
        Convert natural language query to database query and optionally execute.
        
        Args:
            natural_language_query: Natural language query string
            execute: If True, execute the query and return results
            
        Returns:
            Dictionary with query, filters, and optionally results
        """
        if not self.llm_factory:
            raise ValueError("LLM not configured. Provide llm_model and llm_api_key.")
        
        # Build filter model and prompt
        filter_builder = self._get_filter_builder()
        prompt_generator = self._get_prompt_generator()
        
        filter_model = filter_builder.build_filter_model()
        system_prompt = prompt_generator.generate_system_prompt()
        
        # Parse query with LLM
        filters = self.llm_factory.parse_query(
            natural_language_query, filter_model, system_prompt
        )
        
        # Translate to database queries
        model_info = self.get_model_info()
        db_queries = self.query_translator.translate(filters, model_info)
        
        response = {
            "natural_language_query": natural_language_query,
            "extracted_filters": filters,
            "database_queries": db_queries,
        }
        
        # Execute if requested
        if execute and db_queries:
            results = self.query_executor.execute(db_queries)
            response["results"] = results
        
        return response
    
    async def query_async(
        self, natural_language_query: str, execute: bool = True
    ) -> Dict[str, Any]:
        """
        Async version of query().
        
        Args:
            natural_language_query: Natural language query string
            execute: If True, execute the query and return results
            
        Returns:
            Dictionary with query, filters, and optionally results
        """
        if not self.llm_factory:
            raise ValueError("LLM not configured. Provide llm_model and llm_api_key.")
        
        # Build filter model and prompt
        filter_builder = self._get_filter_builder()
        prompt_generator = self._get_prompt_generator()
        
        filter_model = filter_builder.build_filter_model()
        system_prompt = prompt_generator.generate_system_prompt()
        
        # Parse query with LLM (async)
        filters = await self.llm_factory.parse_query_async(
            natural_language_query, filter_model, system_prompt
        )
        
        # Translate to database queries
        model_info = self.get_model_info()
        db_queries = self.query_translator.translate(filters, model_info)
        
        response = {
            "natural_language_query": natural_language_query,
            "extracted_filters": filters,
            "database_queries": db_queries,
        }
        
        # Execute if requested
        if execute and db_queries:
            results = self.query_executor.execute(db_queries)
            response["results"] = results
        
        return response
    
    def query_raw(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        """
        Execute a raw database query.
        
        Args:
            query: Raw database query object
            size: Number of results to return
            
        Returns:
            Query result
        """
        return self.query_executor.execute_raw(query, size)

