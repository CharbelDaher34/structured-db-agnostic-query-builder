"""Tests for the Elasticsearch adapter using a mocked Elasticsearch client."""

from unittest.mock import MagicMock, patch

import pytest

from query_builder.adapters.elasticsearch.executor import ESQueryExecutor
from query_builder.adapters.elasticsearch.query_translator import ESQueryTranslator
from query_builder.adapters.elasticsearch.schema_extractor import ESSchemaExtractor

# ----------------------------------------------------------- schema extractor


SAMPLE_MAPPING = {
    "test-index": {
        "mappings": {
            "properties": {
                "name": {"type": "text"},
                "status": {"type": "keyword"},
                "age": {"type": "long"},
                "score": {"type": "double"},
                "active": {"type": "boolean"},
                "created_at": {"type": "date"},
                "internal_id": {"type": "alias", "path": "id"},
                "tags": {
                    "type": "nested",
                    "properties": {"name": {"type": "keyword"}},
                },
            }
        }
    }
}


class TestESSchemaExtractor:
    def _make_extractor(self, mapping=SAMPLE_MAPPING, distinct_buckets=None, category_fields=None):
        with patch("query_builder.adapters.elasticsearch.schema_extractor.Elasticsearch") as ES:
            client = ES.return_value
            client.indices.get_mapping.return_value = mapping
            client.search.return_value = {
                "aggregations": {"distinct_values": {"buckets": distinct_buckets or []}}
            }
            ext = ESSchemaExtractor(
                "http://localhost:9200",
                "test-index",
                category_fields=category_fields,
            )
            ext._es_client_mock = client  # for assertions in the test
            return ext

    def test_extract_schema_normalizes_types(self):
        ext = self._make_extractor()
        schema = ext.extract_schema()
        assert schema["name"]["type"] == "string"
        assert schema["status"]["type"] == "string"
        assert schema["age"]["type"] == "number"
        assert schema["active"]["type"] == "boolean"
        assert schema["created_at"]["type"] == "date"

    def test_alias_field_ignored(self):
        ext = self._make_extractor()
        schema = ext.extract_schema()
        assert "internal_id" not in schema

    def test_nested_field_marked_as_array(self):
        ext = self._make_extractor()
        schema = ext.extract_schema()
        # The nested parent and the inner field both surface
        assert schema["tags"]["type"] == "array"
        assert "tags.name" in schema

    def test_category_field_marked_as_enum(self):
        ext = self._make_extractor(
            distinct_buckets=[{"key": "a"}, {"key": "b"}],
            category_fields=["status"],
        )
        schema = ext.extract_schema()
        assert schema["status"]["type"] == "enum"
        assert schema["status"]["values"] == ["a", "b"]

    def test_get_distinct_values_uses_keyword_for_text(self):
        ext = self._make_extractor(distinct_buckets=[{"key": "v1"}, {"key": "v2"}])
        values = ext.get_distinct_values("name")
        assert values == ["v1", "v2"]
        # called with name.keyword in the agg
        called_with = ext._es_client_mock.search.call_args
        body = called_with.kwargs
        assert body["aggs"]["distinct_values"]["terms"]["field"] == "name.keyword"

    def test_extract_schema_cached(self):
        ext = self._make_extractor()
        ext.extract_schema()
        ext.extract_schema()
        # called once during extract_schema (only mapping fetch)
        assert ext._es_client_mock.indices.get_mapping.call_count == 1

    def test_get_field_type(self):
        ext = self._make_extractor()
        assert ext.get_field_type("age") == "number"
        assert ext.get_field_type("missing") == "unknown"


# ---------------------------------------------------------------- translator


class TestESQueryTranslator:
    def test_empty_filters_yields_match_all(self, basic_model_info):
        out = ESQueryTranslator().translate({}, basic_model_info)
        assert out == [{"query": {"match_all": {}}}]

    def test_no_conditions_yields_match_all(self, basic_model_info):
        filters = {"filters": [{"conditions": []}]}
        out = ESQueryTranslator().translate(filters, basic_model_info)
        assert out[0]["query"] == {"match_all": {}}

    def test_string_is_uses_keyword_suffix(self, basic_model_info):
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "is", "value": "alice"}]}]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        must = clause["query"]["bool"]["must"]
        assert must[0]["term"] == {"name.keyword": "alice"}

    def test_number_range(self, basic_model_info):
        filters = {
            "filters": [{"conditions": [{"field": "balance", "operator": ">", "value": 100}]}]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        assert clause["query"]["bool"]["must"][0] == {"range": {"balance": {"gt": 100}}}

    def test_between(self, basic_model_info):
        filters = {
            "filters": [
                {"conditions": [{"field": "balance", "operator": "between", "value": [1, 10]}]}
            ]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        assert clause["query"]["bool"]["must"][0] == {"range": {"balance": {"gte": 1, "lte": 10}}}

    def test_isin_date_range(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [
                        {
                            "field": "created_at",
                            "operator": "isin",
                            "value": ["2024-01-01", "2024-12-31"],
                        }
                    ]
                }
            ]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        must = clause["query"]["bool"]["must"][0]
        assert must == {"range": {"created_at": {"gte": "2024-01-01", "lte": "2024-12-31"}}}

    def test_contains_wildcard(self, basic_model_info):
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "contains", "value": "ali"}]}]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        wc = clause["query"]["bool"]["must"][0]["wildcard"]
        assert wc["name.keyword"]["value"] == "*ali*"
        assert wc["name.keyword"]["case_insensitive"] is True

    def test_exists(self, basic_model_info):
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "exists", "value": True}]}]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        assert clause["query"]["bool"]["must"][0] == {"exists": {"field": "name"}}

    def test_sort(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "sort": [{"field": "balance", "order": "desc"}],
                }
            ]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        assert clause["sort"] == [{"balance": {"order": "desc"}}]

    def test_limit_becomes_size(self, basic_model_info):
        filters = {"filters": [{"conditions": [], "limit": 7}]}
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        assert clause["size"] == 7

    def test_group_by_terms_aggregation_uses_keyword(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [{"field": "balance", "type": "sum"}],
                }
            ]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        assert clause["size"] == 0
        agg = clause["aggs"]["group_by_0"]
        # enum/string field — must use .keyword
        assert agg["terms"]["field"] == "status.keyword"
        sub = agg["aggs"]
        assert sub["sum_balance"] == {"sum": {"field": "balance"}}

    def test_group_by_date_uses_date_histogram(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["created_at"],
                    "interval": "month",
                    "aggregations": [{"field": "balance", "type": "avg"}],
                }
            ]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        dh = clause["aggs"]["group_by_0"]["date_histogram"]
        assert dh["field"] == "created_at"
        assert dh["calendar_interval"] == "month"
        assert dh["format"] == "yyyy-MM"

    def test_count_on_string_uses_keyword(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [{"field": "name", "type": "count"}],
                }
            ]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        sub = clause["aggs"]["group_by_0"]["aggs"]
        assert sub["count_name"] == {"value_count": {"field": "name.keyword"}}

    def test_having_adds_bucket_selector(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [
                        {
                            "field": "balance",
                            "type": "sum",
                            "having_operator": ">",
                            "having_value": 100,
                        }
                    ],
                }
            ]
        }
        clause = ESQueryTranslator().translate(filters, basic_model_info)[0]
        sub = clause["aggs"]["group_by_0"]["aggs"]
        assert "having_filter" in sub
        sel = sub["having_filter"]["bucket_selector"]
        assert sel["buckets_path"] == {"var_0": "sum_balance"}
        assert "params.var_0 > 100" in sel["script"]

    def test_top_hits_only_when_requested(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [{"field": "balance", "type": "sum"}],
                }
            ]
        }
        # Default: off
        default = ESQueryTranslator().translate(filters, basic_model_info)[0]
        assert "documents" not in default["aggs"]["group_by_0"]["aggs"]
        # Opt-in
        with_docs = ESQueryTranslator(include_bucket_documents=True).translate(
            filters, basic_model_info
        )[0]
        assert "top_hits" in with_docs["aggs"]["group_by_0"]["aggs"]["documents"]


# ---------------------------------------------------------------- executor


class TestESQueryExecutor:
    def _executor(self):
        with patch("query_builder.adapters.elasticsearch.executor.Elasticsearch") as ES:
            client = ES.return_value
            ex = ESQueryExecutor("http://localhost:9200", "test-index")
            return ex, client

    def test_execute_returns_normalized(self):
        ex, client = self._executor()
        client.search.return_value = {
            "hits": {
                "total": {"value": 2},
                "hits": [
                    {"_source": {"a": 1}},
                    {"_source": {"a": 2}},
                ],
            }
        }
        out = ex.execute([{"query": {"match_all": {}}}])
        assert out[0]["total_hits"] == 2
        assert out[0]["documents"] == [{"a": 1}, {"a": 2}]
        assert out[0]["success"] is True

    def test_execute_includes_aggregations(self):
        ex, client = self._executor()
        client.search.return_value = {
            "hits": {"total": {"value": 0}, "hits": []},
            "aggregations": {"group_by_0": {"buckets": [{"key": "a", "doc_count": 1}]}},
        }
        out = ex.execute([{"query": {"match_all": {}}, "aggs": {}}])
        assert "aggregations" in out[0]
        assert out[0]["aggregations"]["group_by_0"]["buckets"][0]["key"] == "a"

    def test_execute_failure_returns_error(self):
        ex, client = self._executor()
        client.search.side_effect = RuntimeError("network down")
        out = ex.execute([{"query": {"match_all": {}}}])
        assert out[0]["success"] is False
        assert "network down" in out[0]["error"]

    def test_execute_empty_queries(self):
        ex, _ = self._executor()
        assert ex.execute([]) == []

    def test_execute_raw_passes_size_default(self):
        ex, client = self._executor()
        client.search.return_value = {"hits": {"total": 1, "hits": [{"_source": {"x": 1}}]}}
        out = ex.execute_raw({"query": {"match_all": {}}})
        client.search.assert_called_once()
        # size default is 100
        assert client.search.call_args.kwargs["size"] == 100
        assert out["total_hits"] == 1
        assert out["success"] is True

    def test_execute_raw_does_not_mutate_caller_dict(self):
        ex, client = self._executor()
        client.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}
        original = {"query": {"match_all": {}}}
        ex.execute_raw(original, size=5)
        assert "size" not in original

    def test_execute_raw_respects_explicit_size(self):
        ex, client = self._executor()
        client.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}
        ex.execute_raw({"query": {"match_all": {}}, "size": 3}, size=99)
        assert client.search.call_args.kwargs["size"] == 3
