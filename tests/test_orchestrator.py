"""Tests for QueryOrchestrator using lightweight fake adapters."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from query_builder.orchestrator import QueryOrchestrator


class FakeSchemaExtractor:
    def __init__(self, schema=None, distinct=None):
        self.schema = schema or {
            "name": {"type": "string"},
            "status": {"type": "string"},
            "balance": {"type": "number"},
        }
        self._distinct = distinct or {"status": ["active", "closed"]}
        self.close_calls = 0

    def extract_schema(self):
        return self.schema

    def get_distinct_values(self, field_path, size=1000):
        return self._distinct.get(field_path, [])

    def get_field_type(self, field_path):
        return self.schema.get(field_path, {}).get("type", "unknown")

    def close(self):
        self.close_calls += 1


class FakeTranslator:
    def __init__(self):
        self.calls: list[dict[str, Any]] = []

    def translate(self, filters, model_info):
        self.calls.append({"filters": filters, "model_info": model_info})
        return [{"echo": filters}]


class FakeExecutor:
    def __init__(self, results=None):
        self.results = results or [
            {"total_hits": 1, "documents": [{"name": "alice"}], "success": True}
        ]
        self.exec_calls: list[dict[str, Any]] = []
        self.close_calls = 0

    def execute(self, queries, offset=0, limit=None):
        self.exec_calls.append({"queries": queries, "offset": offset, "limit": limit})
        return self.results

    def execute_raw(self, query, size=100):
        return {"total_hits": 0, "documents": [], "raw": query, "size": size}

    def close(self):
        self.close_calls += 1


# ---------------------------------------------------------------- fixtures


@pytest.fixture
def fake_orchestrator(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "gpt-4o")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    return QueryOrchestrator(
        schema_extractor=FakeSchemaExtractor(),
        query_translator=FakeTranslator(),
        query_executor=FakeExecutor(),
        category_fields=["status"],
    )


# ---------------------------------------------------------------- tests


class TestModelBuilding:
    def test_generate_model_returns_pydantic_model(self, fake_orchestrator):
        Model = fake_orchestrator.generate_model("M")
        assert Model.__name__ == "M"
        assert "name" in Model.model_fields
        assert "status" in Model.model_fields

    def test_get_model_info_marks_status_as_enum(self, fake_orchestrator):
        info = fake_orchestrator.get_model_info()
        assert info["status"]["type"] == "enum"
        assert set(info["status"]["values"]) == {"active", "closed"}

    def test_warm_up_caches_components(self, fake_orchestrator):
        fake_orchestrator.warm_up()
        assert fake_orchestrator._model_builder is not None
        assert fake_orchestrator._filter_builder is not None
        assert fake_orchestrator._prompt_generator is not None


class TestQuery:
    @pytest.mark.asyncio
    async def test_query_no_execute(self, fake_orchestrator):
        # Stub the LLM call so we don't hit a real API
        fake_filters = MagicMock()
        fake_filters.model_dump = MagicMock(return_value={"filters": [{"conditions": []}]})
        fake_orchestrator.llm_factory.parse_query = AsyncMock(return_value=fake_filters)

        out = await fake_orchestrator.query("show me rows", execute=False)
        assert out["natural_language_query"] == "show me rows"
        assert "database_queries" in out
        assert "results" not in out

    @pytest.mark.asyncio
    async def test_query_with_execute(self, fake_orchestrator):
        fake_filters = MagicMock()
        fake_filters.model_dump = MagicMock(return_value={"filters": [{"conditions": []}]})
        fake_orchestrator.llm_factory.parse_query = AsyncMock(return_value=fake_filters)

        out = await fake_orchestrator.query("show me rows", execute=True, offset=2, limit=5)
        assert "results" in out
        assert out["results"][0]["documents"] == [{"name": "alice"}]
        # pagination kwargs forwarded to fake executor
        executor = fake_orchestrator._query_executor_impl
        assert executor.exec_calls[0]["offset"] == 2
        assert executor.exec_calls[0]["limit"] == 5

    @pytest.mark.asyncio
    async def test_query_raises_without_llm(self, fake_orchestrator):
        fake_orchestrator.llm_factory = None
        with pytest.raises(ValueError, match="LLM not configured"):
            await fake_orchestrator.query("x")

    @pytest.mark.asyncio
    async def test_build_query_returns_queries_without_executing(self, fake_orchestrator):
        # Stub the LLM so we don't hit a real API
        fake_filters = MagicMock()
        fake_filters.model_dump = MagicMock(return_value={"filters": [{"conditions": []}]})
        fake_orchestrator.llm_factory.parse_query = AsyncMock(return_value=fake_filters)

        out = await fake_orchestrator.build_query("show me rows")

        assert out["natural_language_query"] == "show me rows"
        assert "extracted_filters" in out
        assert "database_queries" in out
        assert "results" not in out
        # Executor never called when building only
        assert fake_orchestrator._query_executor_impl.exec_calls == []


class TestClose:
    def test_close_closes_adapters_individually(self, fake_orchestrator):
        fake_orchestrator.close()
        assert fake_orchestrator._schema_extractor_impl.close_calls == 1
        assert fake_orchestrator._query_executor_impl.close_calls == 1

    def test_close_prefers_shared_client(self, fake_orchestrator):
        shared = MagicMock()
        fake_orchestrator._shared_client = shared
        fake_orchestrator.close()
        shared.close.assert_called_once()
        # individual adapter close was not called
        assert fake_orchestrator._schema_extractor_impl.close_calls == 0


class TestRawQuery:
    def test_query_raw(self, fake_orchestrator):
        out = fake_orchestrator.query_raw({"q": 1}, size=11)
        assert out["raw"] == {"q": 1}
        assert out["size"] == 11

    @pytest.mark.asyncio
    async def test_query_raw_async(self, fake_orchestrator):
        out = await fake_orchestrator.query_raw_async({"q": 1}, size=2)
        assert out["raw"] == {"q": 1}


class TestFactories:
    def test_from_csv(self, sample_csv, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        orch = QueryOrchestrator.from_csv(
            csv_path=str(sample_csv),
            category_fields=["status"],
            date_columns=["created_at"],
        )
        info = orch.get_model_info()
        assert info["status"]["type"] == "enum"
        # Shared DataFrame between extractor and executor
        assert orch._schema_extractor_impl.df is orch._query_executor_impl.df

    def test_from_mongodb_shares_client(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        with patch("pymongo.MongoClient") as MongoClientMock:
            client = MongoClientMock.return_value
            orch = QueryOrchestrator.from_mongodb("uri", "db", "coll", category_fields=["status"])
            # Same client used by both adapters and stashed for close()
            assert orch._shared_client is client
            assert orch._schema_extractor_impl.client is client
            assert orch._query_executor_impl.client is client

            orch.close()
            client.close.assert_called_once()

    def test_from_elasticsearch(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        with (
            patch("query_builder.adapters.elasticsearch.schema_extractor.Elasticsearch"),
            patch("query_builder.adapters.elasticsearch.executor.Elasticsearch"),
        ):
            orch = QueryOrchestrator.from_elasticsearch("http://localhost:9200", "test-index")
            assert orch._schema_extractor_impl.index_name == "test-index"
            assert orch._query_executor_impl.index_name == "test-index"
