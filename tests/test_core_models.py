"""Tests for query_builder.core.models."""

from query_builder.core.models import (
    LLMConfig,
    NormalizedSchema,
    QueryResult,
    SchemaField,
)


class TestSchemaField:
    def test_minimal(self):
        f = SchemaField(name="age", type="number")
        assert f.name == "age"
        assert f.type == "number"
        assert f.values is None
        assert f.is_array_item is False

    def test_enum_with_values(self):
        f = SchemaField(name="status", type="enum", values=["a", "b"])
        assert f.values == ["a", "b"]

    def test_array_with_item_type(self):
        f = SchemaField(name="tags", type="array", item_type="string")
        assert f.item_type == "string"

    def test_nested_properties(self):
        inner = SchemaField(name="city", type="string")
        outer = SchemaField(name="address", type="object", properties={"city": inner})
        assert outer.properties["city"].type == "string"


class TestQueryResult:
    def test_default(self):
        r = QueryResult()
        assert r.total_hits == 0
        assert r.documents == []
        assert r.success is True
        assert r.error is None
        assert r.aggregations is None
        assert r.metadata == {}

    def test_populated(self):
        r = QueryResult(
            total_hits=2,
            documents=[{"a": 1}, {"a": 2}],
            aggregations={"sum": 3},
        )
        assert r.total_hits == 2
        assert len(r.documents) == 2
        assert r.aggregations == {"sum": 3}

    def test_error_case(self):
        r = QueryResult(error="boom", success=False)
        assert r.success is False
        assert r.error == "boom"


class TestNormalizedSchema:
    def test_default_metadata(self):
        s = NormalizedSchema(fields={"id": {"type": "number"}}, source_type="mongodb")
        assert s.source_type == "mongodb"
        assert s.metadata == {}

    def test_with_metadata(self):
        s = NormalizedSchema(fields={}, source_type="elasticsearch", metadata={"version": 1})
        assert s.metadata == {"version": 1}


class TestLLMConfig:
    def test_defaults(self):
        c = LLMConfig(model="gpt-4", api_key="x")
        assert c.temperature == 0
        assert c.max_tokens is None
