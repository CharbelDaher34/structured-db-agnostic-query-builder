"""
Integration tests that exercise the full natural-language → query pipeline
against a real LLM, with all database access mocked out.

Marked with `@pytest.mark.llm` so they can be skipped (-m 'not llm').
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from dotenv import load_dotenv

from query_builder.orchestrator import QueryOrchestrator

# Load .env so provider API keys / LLM_MODEL are available when running locally.
load_dotenv()


def _any_provider_key_present() -> bool:
    return any(
        os.getenv(var)
        for var in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY", "GEMINI_API_KEY")
    )


pytestmark = [
    pytest.mark.llm,
    pytest.mark.skipif(
        not _any_provider_key_present(),
        reason="Requires a provider API key (OPENAI_API_KEY / ANTHROPIC_API_KEY / GOOGLE_API_KEY) for real LLM calls",
    ),
]


# --------------------------------------------------------- mocked-DB fixtures


class MockMongoCollection:
    """A mock pymongo Collection with a small in-memory corpus."""

    def __init__(self):
        self.documents = [
            {
                "_id": f"id_{i}",
                "name": f"customer_{i}",
                "status": ["active", "pending", "closed"][i % 3],
                "balance": (i + 1) * 1000.0,
                "created_at": f"2024-0{(i % 9) + 1}-15",
            }
            for i in range(12)
        ]
        self.aggregate = MagicMock(side_effect=self._aggregate)
        self.find = MagicMock(side_effect=self._find)
        self.last_pipeline: list[dict[str, Any]] = []

    def _aggregate(self, pipeline, **kwargs):
        self.last_pipeline = pipeline
        # Schema sampling
        if pipeline and "$sample" in pipeline[0]:
            return iter(self.documents)
        # Distinct values
        if pipeline and "$match" in pipeline[0] and "$group" in pipeline[1]:
            field = pipeline[1]["$group"]["_id"].lstrip("$")
            seen = []
            for d in self.documents:
                v = d.get(field)
                if v is not None and v not in seen:
                    seen.append({"_id": v})
            return iter(seen)
        # Execution pipeline — just return the docs (we mainly care that the
        # pipeline is well-formed Mongo aggregation syntax)
        return iter(self.documents[:3])

    def _find(self, *args, **kwargs):
        cursor = MagicMock()
        cursor.limit.return_value = iter(self.documents[:5])
        cursor.skip.return_value = cursor
        return cursor


@pytest.fixture
def mongo_orchestrator():
    """Orchestrator backed by a mocked pymongo client."""
    coll = MockMongoCollection()
    db = MagicMock()
    db.__getitem__.return_value = coll
    client = MagicMock()
    client.__getitem__.return_value = db

    with patch("pymongo.MongoClient", return_value=client):
        orch = QueryOrchestrator.from_mongodb(
            mongo_uri="mongodb://mocked",
            database_name="db",
            collection_name="coll",
            category_fields=["status"],
        )
        # Expose the collection for assertions
        orch._mock_collection = coll
        yield orch


# --------------------------------------------------------- CSV fixture (real I/O)


@pytest.fixture
def csv_orchestrator(sample_csv):
    return QueryOrchestrator.from_csv(
        csv_path=str(sample_csv),
        category_fields=["status", "segment"],
        date_columns=["created_at"],
    )


# --------------------------------------------------------- tests


class TestLLMEndToEndCSV:
    """Real LLM → translator → real pandas executor against the sample CSV."""

    @pytest.mark.asyncio
    async def test_filter_query_extracts_status_condition(self, csv_orchestrator):
        # Be explicit so the LLM picks the enum `status` field rather than the
        # boolean `active` flag.
        out = await csv_orchestrator.query(
            "Show me only the customers whose status is 'active'", execute=True
        )

        assert "extracted_filters" in out
        slices = out["extracted_filters"].get("filters", [])
        assert slices, "LLM produced no filter slices"

        found_status = False
        for s in slices:
            for cond in s.get("conditions", []) or []:
                if cond.get("field") == "status" and cond.get("value") == "active":
                    found_status = True
                    break

        assert found_status, f"LLM did not include status=active filter: {slices}"

        if out.get("results"):
            documents = out["results"][0].get("documents", [])
            for doc in documents:
                assert doc["status"] == "active"

    @pytest.mark.asyncio
    async def test_top_n_sort_query(self, csv_orchestrator):
        out = await csv_orchestrator.query(
            "Show me the top 3 customers by balance descending", execute=True
        )

        slices = out["extracted_filters"]["filters"]
        slice0 = slices[0]
        assert slice0.get("limit") == 3
        sort = slice0.get("sort") or []
        assert any(s.get("field") == "balance" and s.get("order") == "desc" for s in sort)

        documents = out["results"][0]["documents"]
        assert len(documents) == 3
        balances = [d["balance"] for d in documents]
        assert balances == sorted(balances, reverse=True)

    @pytest.mark.asyncio
    async def test_group_by_with_aggregation(self, csv_orchestrator):
        out = await csv_orchestrator.query(
            "Group by status and show the count of customers per status",
            execute=True,
        )
        slice0 = out["extracted_filters"]["filters"][0]
        assert slice0.get("group_by") == ["status"] or "status" in (slice0.get("group_by") or [])
        # Result documents should contain a count column for each status group.
        documents = out["results"][0]["documents"]
        assert len(documents) >= 1


class TestLLMEndToEndMongo:
    """Real LLM → Mongo translator → mocked pymongo aggregate."""

    @pytest.mark.asyncio
    async def test_translator_produces_valid_aggregate_pipeline(self, mongo_orchestrator):
        out = await mongo_orchestrator.query(
            "Find customers whose status is active and balance is above 5000",
            execute=True,
        )

        # database_queries should be a list of {"pipeline": [...]} dicts
        db_queries = out["database_queries"]
        assert len(db_queries) >= 1
        assert "pipeline" in db_queries[0]
        pipeline = db_queries[0]["pipeline"]

        # First stage should be a $match
        first_stage = pipeline[0]
        assert "$match" in first_stage

        # The mock collection should have been invoked with this pipeline
        coll = mongo_orchestrator._mock_collection
        # Find the call that used our query (the executor stage, not the
        # schema-sampling stage)
        called_pipelines = [args[0] for args, _ in coll.aggregate.call_args_list]
        # At least one call should not be a $sample/$group call
        non_schema = [p for p in called_pipelines if p and "$sample" not in p[0]]
        assert non_schema, "Executor never ran the query pipeline"

    @pytest.mark.asyncio
    async def test_aggregation_translates_to_group_stage(self, mongo_orchestrator):
        out = await mongo_orchestrator.query(
            "Show me the average balance grouped by status", execute=True
        )
        pipeline = out["database_queries"][0]["pipeline"]
        group_stages = [s for s in pipeline if "$group" in s]
        assert group_stages, f"No $group stage in pipeline: {pipeline}"
        group_spec = group_stages[0]["$group"]
        # Must group by status and have an avg_balance aggregation
        assert group_spec["_id"] == {"status": "$status"}
        assert "avg_balance" in group_spec
        assert group_spec["avg_balance"] == {"$avg": "$balance"}
