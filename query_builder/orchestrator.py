"""
Query orchestrator - main entry point.

Coordinates all components to provide a unified query interface.
"""

import logging
from typing import Any, Optional

from pydantic import BaseModel

from query_builder.core.interfaces import (
    IQueryExecutor,
    IQueryTranslator,
    ISchemaExtractor,
)
from query_builder.execution.executor import QueryExecutor
from query_builder.llm.client_factory import LLMClientFactory
from query_builder.query.filter_builder import FilterModelBuilder
from query_builder.query.prompt_generator import PromptGenerator
from query_builder.query.translator import QueryTranslator
from query_builder.schema.extractor import SchemaExtractor
from query_builder.schema.model_builder import ModelBuilder

logger = logging.getLogger(__name__)


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
        category_fields: Optional[list[str]] = None,
        fields_to_ignore: Optional[list[str]] = None,
    ):
        """
        Initialize query orchestrator with database adapters.

        Args:
            schema_extractor: Database-specific schema extractor
            query_translator: Database-specific query translator
            query_executor: Database-specific query executor
            category_fields: List of fields to treat as categories
            fields_to_ignore: List of fields to ignore
        """
        # Store adapters (so close() can reach them)
        self._schema_extractor_impl = schema_extractor
        self._query_translator_impl = query_translator
        self._query_executor_impl = query_executor

        # Initialize layers
        self.schema_extractor = SchemaExtractor(schema_extractor, category_fields=category_fields)
        self.query_translator = QueryTranslator(query_translator)
        self.query_executor = QueryExecutor(query_executor)

        # Configuration
        self.category_fields = category_fields or []
        self.fields_to_ignore = fields_to_ignore or []

        # LLM setup
        self.llm_factory: Optional[LLMClientFactory] = LLMClientFactory()

        # Cached components
        self._model_builder: Optional[ModelBuilder] = None
        self._filter_builder: Optional[FilterModelBuilder] = None
        self._prompt_generator: Optional[PromptGenerator] = None

    @classmethod
    def from_elasticsearch(
        cls,
        es_host: str,
        index_name: str,
        category_fields: Optional[list[str]] = None,
        fields_to_ignore: Optional[list[str]] = None,
        include_bucket_documents: bool = False,
    ) -> "QueryOrchestrator":
        """Create orchestrator for Elasticsearch with a single shared client."""
        from elasticsearch import Elasticsearch

        from query_builder.adapters.elasticsearch import (
            ESQueryExecutor,
            ESQueryTranslator,
            ESSchemaExtractor,
        )

        # One ES client shared between schema extraction and execution.
        client = Elasticsearch(hosts=[es_host])

        schema_extractor = ESSchemaExtractor(
            es_host=es_host,
            index_name=index_name,
            category_fields=category_fields,
            client=client,
        )
        query_translator = ESQueryTranslator(include_bucket_documents=include_bucket_documents)
        query_executor = ESQueryExecutor(es_host=es_host, index_name=index_name, client=client)

        orch = cls(
            schema_extractor=schema_extractor,
            query_translator=query_translator,
            query_executor=query_executor,
            category_fields=category_fields,
            fields_to_ignore=fields_to_ignore,
        )
        orch._shared_client = client
        return orch

    @classmethod
    def from_csv(
        cls,
        csv_path: str,
        category_fields: Optional[list[str]] = None,
        fields_to_ignore: Optional[list[str]] = None,
        date_columns: Optional[list[str]] = None,
        read_csv_kwargs: Optional[dict[str, Any]] = None,
    ) -> "QueryOrchestrator":
        """
        Create orchestrator backed by a CSV file.

        The file is loaded into a pandas DataFrame once and shared between the
        schema extractor and the executor.

        Args:
            csv_path: Path to the CSV file.
            category_fields: Columns to expose as enums (LLM picks values from a
                fixed list rather than free-form strings).
            fields_to_ignore: Columns to exclude from the generated schema.
            date_columns: Columns to parse as datetimes when loading. Forces the
                schema type to "date" so date filters and date_histogram-style
                grouping work.
            read_csv_kwargs: Extra keyword args forwarded to pd.read_csv (delimiter,
                encoding, nrows, etc.).
        """
        import pandas as pd

        from query_builder.adapters.csv import (
            CSVQueryExecutor,
            CSVQueryTranslator,
            CSVSchemaExtractor,
        )

        # Load the CSV once; both the schema extractor and the executor read from
        # the same in-memory DataFrame so we never round-trip to disk twice.
        kwargs = dict(read_csv_kwargs or {})
        if date_columns:
            kwargs.setdefault("parse_dates", date_columns)
        df = pd.read_csv(csv_path, **kwargs)

        schema_extractor = CSVSchemaExtractor(
            csv_path=csv_path,
            category_fields=category_fields,
            date_columns=date_columns,
            df=df,
        )
        query_translator = CSVQueryTranslator()
        query_executor = CSVQueryExecutor(
            csv_path=csv_path,
            df=df,
            date_columns=date_columns,
        )

        return cls(
            schema_extractor=schema_extractor,
            query_translator=query_translator,
            query_executor=query_executor,
            category_fields=category_fields,
            fields_to_ignore=fields_to_ignore,
        )

    @classmethod
    def from_mongodb(
        cls,
        mongo_uri: str,
        database_name: str,
        collection_name: str,
        category_fields: Optional[list[str]] = None,
        fields_to_ignore: Optional[list[str]] = None,
        sample_size: int = 1000,
        include_grouped_documents: bool = False,
    ) -> "QueryOrchestrator":
        """Create orchestrator for MongoDB with a single shared MongoClient."""
        from pymongo import MongoClient

        from query_builder.adapters.mongodb import (
            MongoQueryExecutor,
            MongoQueryTranslator,
            MongoSchemaExtractor,
        )

        # One MongoClient shared between schema extraction and execution.
        client = MongoClient(mongo_uri)

        schema_extractor = MongoSchemaExtractor(
            mongo_uri=mongo_uri,
            database_name=database_name,
            collection_name=collection_name,
            category_fields=category_fields,
            sample_size=sample_size,
            client=client,
        )
        query_translator = MongoQueryTranslator(
            include_grouped_documents=include_grouped_documents,
        )
        query_executor = MongoQueryExecutor(
            mongo_uri=mongo_uri,
            database_name=database_name,
            collection_name=collection_name,
            client=client,
        )

        orch = cls(
            schema_extractor=schema_extractor,
            query_translator=query_translator,
            query_executor=query_executor,
            category_fields=category_fields,
            fields_to_ignore=fields_to_ignore,
        )
        # Stash the shared client for clean shutdown.
        orch._shared_client = client
        return orch

    @classmethod
    def from_sqlmodel(
        cls,
        database_url: str,
        table: Any,
        category_fields: Optional[list[str]] = None,
        fields_to_ignore: Optional[list[str]] = None,
        engine_kwargs: Optional[dict[str, Any]] = None,
    ) -> "QueryOrchestrator":
        """
        Create orchestrator backed by any SQLAlchemy/SQLModel-compatible database.

        Works with PostgreSQL, MySQL, SQLite, etc. — anything SQLAlchemy supports.

        Args:
            database_url: SQLAlchemy connection URL, e.g.
                ``postgresql+psycopg://user:pwd@host/db``, ``mysql+pymysql://…``,
                ``sqlite:///path/to.db``.
            table: SQLModel class, ``sqlalchemy.Table``, or table name string.
                String names are reflected from the live database.
            category_fields: Columns to expose as enums.
            fields_to_ignore: Columns to exclude from the generated schema.
            engine_kwargs: Extra kwargs forwarded to ``sqlalchemy.create_engine``
                (e.g. ``{"pool_pre_ping": True, "echo": False}``).
        """
        from sqlalchemy import create_engine

        from query_builder.adapters.sql import (
            SQLQueryExecutor,
            SQLQueryTranslator,
            SQLSchemaExtractor,
            resolve_table,
        )

        engine = create_engine(database_url, **(engine_kwargs or {}))
        table_obj = resolve_table(engine, table)

        schema_extractor = SQLSchemaExtractor(
            engine=engine,
            table=table_obj,
            category_fields=category_fields,
        )
        query_translator = SQLQueryTranslator(table=table_obj, dialect_name=engine.dialect.name)
        query_executor = SQLQueryExecutor(engine=engine, owns_engine=True)

        orch = cls(
            schema_extractor=schema_extractor,
            query_translator=query_translator,
            query_executor=query_executor,
            category_fields=category_fields,
            fields_to_ignore=fields_to_ignore,
        )
        # SQLAlchemy engines use .dispose(), not .close() — stash the executor
        # itself so close() picks the right teardown path.
        orch._sql_engine = engine
        return orch

    def close(self) -> None:
        """Release database connections held by the orchestrator's adapters."""
        # SQLAlchemy engine uses dispose(), so it can't go through the
        # _shared_client (close()) path. Handle it explicitly first.
        engine = getattr(self, "_sql_engine", None)
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                logger.warning("Failed to dispose SQLAlchemy engine", exc_info=True)
            return

        shared = getattr(self, "_shared_client", None)
        if shared is not None:
            try:
                shared.close()
            except Exception:
                logger.warning("Failed to close shared MongoClient", exc_info=True)
            return

        # Fall back to closing adapters individually if no shared client was set up.
        for adapter in (self._schema_extractor_impl, self._query_executor_impl):
            close = getattr(adapter, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    logger.warning("Adapter close() failed", exc_info=True)

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

    def warm_up(self) -> None:
        """
        Pre-build the schema-derived model, filter model, and prompt.

        Useful at startup so the first request isn't penalised by lazy initialisation.
        """
        self._get_model_builder()
        self._get_filter_builder().build_filter_model()
        self._get_prompt_generator().generate_system_prompt()

    def generate_model(self, model_name: str = "GeneratedModel") -> type[BaseModel]:
        """Generate Pydantic model from schema."""
        return self._get_model_builder().build(model_name)

    def get_model_info(self) -> dict[str, Any]:
        """Get flattened field information."""
        return self._get_model_builder().get_model_info()

    def print_model_summary(self):
        """Print summary of the generated model."""
        model = self.generate_model()
        model_info = self.get_model_info()

        print("\n=== Model Summary ===")
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

    async def query(
        self,
        natural_language_query: str,
        execute: bool = True,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Convert natural language query to database query and optionally execute.

        Args:
            natural_language_query: Natural language query string
            execute: If True, execute the query and return results
            offset: Pagination offset to apply to executed queries
            limit: Optional pagination limit to apply when the LLM-generated query
                doesn't already include one
        """
        if not self.llm_factory:
            raise ValueError(
                "LLM not configured. Provide llm_model and llm_api_key in the constructor."
            )

        filter_builder = self._get_filter_builder()
        prompt_generator = self._get_prompt_generator()

        filter_model = filter_builder.build_filter_model()
        system_prompt = prompt_generator.generate_system_prompt()

        # Parse query with LLM (async)
        filters = await self.llm_factory.parse_query(
            inputs=natural_language_query,
            filter_model=filter_model,
            system_prompt=system_prompt,
        )

        filters = filters.model_dump(mode="json")
        logger.debug("extracted_filters=%s", filters)

        # Translate to database queries
        model_info = self.get_model_info()
        db_queries = self.query_translator.translate(filters, model_info)

        response: dict[str, Any] = {
            "natural_language_query": natural_language_query,
            "extracted_filters": filters,
            "database_queries": db_queries,
        }

        # Execute if requested, off the event loop so we don't block other requests
        if execute and db_queries:
            results = await self.query_executor.execute_async(
                db_queries, offset=offset, limit=limit
            )
            response["results"] = results

        return response

    def query_raw(self, query: dict[str, Any], size: int = 100) -> dict[str, Any]:
        """Execute a raw database query."""
        return self.query_executor.execute_raw(query, size)

    async def query_raw_async(self, query: dict[str, Any], size: int = 100) -> dict[str, Any]:
        """Async variant of query_raw."""
        return await self.query_executor.execute_raw_async(query, size)
