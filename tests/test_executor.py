"""Tests for the QueryExecutor coordinator."""

import pytest

from query_builder.execution.executor import QueryExecutor


class FakeAdapter:
    def __init__(self, accepts_pagination: bool = True, fail: bool = False):
        self.accepts_pagination = accepts_pagination
        self.fail = fail
        self.calls = []

    def execute(self, queries, offset=None, limit=None):
        if not self.accepts_pagination:
            # Simulate older adapter signature
            raise TypeError("unexpected keyword argument")
        if self.fail:
            raise RuntimeError("boom")
        self.calls.append({"queries": queries, "offset": offset, "limit": limit})
        return [{"total_hits": len(queries), "documents": [], "success": True}]

    def execute_raw(self, query, size=100):
        if self.fail:
            raise RuntimeError("boom-raw")
        return {"total_hits": 1, "documents": [{"q": query, "size": size}], "success": True}


class TestExecute:
    def test_empty_queries_returns_empty(self):
        exec_ = QueryExecutor(FakeAdapter())
        assert exec_.execute([]) == []

    def test_passes_pagination_when_supported(self):
        adapter = FakeAdapter()
        QueryExecutor(adapter).execute([{"q": 1}], offset=5, limit=10)
        assert adapter.calls[0] == {"queries": [{"q": 1}], "offset": 5, "limit": 10}

    def test_exception_returns_error_documents(self):
        adapter = FakeAdapter(fail=True)
        out = QueryExecutor(adapter).execute([{"q": 1}, {"q": 2}])
        assert len(out) == 2
        for r in out:
            assert r["success"] is False
            assert "boom" in r["error"]


class TestExecuteAsync:
    @pytest.mark.asyncio
    async def test_execute_async_dispatches_to_sync(self):
        adapter = FakeAdapter()
        result = await QueryExecutor(adapter).execute_async([{"q": 1}], offset=2, limit=3)
        assert result[0]["success"] is True
        assert adapter.calls[0]["offset"] == 2


class TestExecuteRaw:
    def test_raw_passthrough(self):
        adapter = FakeAdapter()
        out = QueryExecutor(adapter).execute_raw({"x": 1}, size=42)
        assert out["success"] is True
        assert out["documents"][0]["size"] == 42

    def test_raw_exception_wrapped(self):
        adapter = FakeAdapter(fail=True)
        out = QueryExecutor(adapter).execute_raw({"x": 1})
        assert out["success"] is False
        assert "boom-raw" in out["error"]

    @pytest.mark.asyncio
    async def test_raw_async(self):
        adapter = FakeAdapter()
        out = await QueryExecutor(adapter).execute_raw_async({"x": 1}, size=5)
        assert out["success"] is True
