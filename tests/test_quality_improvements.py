"""
Regression tests for the quality improvements in this round:

- CSV / Mongo executors return pre-pagination total_hits
- QueryTranslator delegates instead of returning an ES-shaped default
- ES translator uses es_type to decide on .keyword suffixing
- ModelBuilder auto-creates implicit parents for dotted field paths
- ES executor accepts a shared client and injects from/size pagination
- Prompt renders single-brace JSON in every example
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from query_builder.adapters.csv import CSVQueryExecutor
from query_builder.adapters.elasticsearch.executor import ESQueryExecutor
from query_builder.adapters.elasticsearch.query_translator import ESQueryTranslator
from query_builder.adapters.elasticsearch.schema_extractor import ESSchemaExtractor
from query_builder.adapters.mongodb.executor import MongoQueryExecutor
from query_builder.query.prompt_generator import PromptGenerator
from query_builder.query.translator import QueryTranslator
from query_builder.schema.model_builder import ModelBuilder

# ---------------------------------------------------------- total_hits semantics


class TestCSVTotalHitsIsPrePagination:
    def test_total_hits_counts_matched_not_returned(self, sample_df):
        ex = CSVQueryExecutor(csv_path="x.csv", df=sample_df)
        plan = {"plan": {"sort": [{"field": "id", "order": "asc"}], "limit": 3}}
        out = ex.execute([plan])[0]
        assert out["total_hits"] == 12  # full dataset, since no conditions
        assert len(out["documents"]) == 3  # but only 3 returned

    def test_total_hits_with_filter_and_pagination(self, sample_df):
        ex = CSVQueryExecutor(csv_path="x.csv", df=sample_df)
        plan = {
            "plan": {
                "conditions": [{"field": "status", "operator": "is", "value": "active"}],
                "limit": 1,
            }
        }
        out = ex.execute([plan])[0]
        # 4 active rows in sample_df (every 3rd of 12) — adjust to whatever
        # _exactly_ is in the fixture: count them explicitly.
        expected = int((sample_df["status"] == "active").sum())
        assert out["total_hits"] == expected
        assert len(out["documents"]) == 1


class TestMongoTotalHitsIsPrePagination:
    def test_runs_count_pipeline_when_paginated(self):
        collection = MagicMock()
        # First call: data (returns 1 doc). Second call: $count returns total=42.
        collection.aggregate.side_effect = [
            iter([{"_id": "abc", "x": 1}]),
            iter([{"n": 42}]),
        ]
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db

        ex = MongoQueryExecutor("uri", "db", "coll", client=client)
        out = ex.execute([{"pipeline": [{"$match": {"x": 1}}]}], offset=0, limit=5)[0]
        assert out["total_hits"] == 42
        assert len(out["documents"]) == 1
        # The second call had a $count stage appended
        count_pipeline = collection.aggregate.call_args_list[1].args[0]
        assert count_pipeline[-1] == {"$count": "n"}

    def test_skips_count_pipeline_when_no_pagination(self):
        collection = MagicMock()
        collection.aggregate.return_value = iter([{"_id": "abc"}, {"_id": "def"}, {"_id": "ghi"}])
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db

        ex = MongoQueryExecutor("uri", "db", "coll", client=client)
        out = ex.execute([{"pipeline": [{"$match": {}}]}])[0]
        # Only one aggregate call — no $count second roundtrip
        assert collection.aggregate.call_count == 1
        assert out["total_hits"] == 3


# ---------------------------------------------------------- QueryTranslator fix


class _Recorder:
    def __init__(self):
        self.calls = []

    def translate(self, filters, model_info):
        self.calls.append((filters, model_info))
        return [{"adapter_native": filters}]


class TestQueryTranslatorAlwaysDelegates:
    def test_empty_filters_delegated(self):
        rec = _Recorder()
        out = QueryTranslator(rec).translate({}, {})
        assert out == [{"adapter_native": {}}]
        assert rec.calls == [({}, {})]

    def test_none_normalised_to_empty_dict(self):
        rec = _Recorder()
        QueryTranslator(rec).translate(None, {})
        assert rec.calls == [({}, {})]


# ---------------------------------------------------------- ES keyword suffix


class TestESKeywordSuffixUsesEsType:
    def test_text_field_gets_keyword_suffix(self):
        model_info = {"name": {"type": "string", "es_type": "text", "has_keyword_subfield": True}}
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "is", "value": "alice"}]}]
        }
        clause = ESQueryTranslator().translate(filters, model_info)[0]
        assert clause["query"]["bool"]["must"][0] == {"term": {"name.keyword": "alice"}}

    def test_keyword_field_does_not_get_suffix(self):
        model_info = {"status": {"type": "string", "es_type": "keyword"}}
        filters = {
            "filters": [{"conditions": [{"field": "status", "operator": "is", "value": "active"}]}]
        }
        clause = ESQueryTranslator().translate(filters, model_info)[0]
        assert clause["query"]["bool"]["must"][0] == {"term": {"status": "active"}}

    def test_text_without_keyword_subfield_uses_raw_field(self):
        model_info = {"body": {"type": "string", "es_type": "text", "has_keyword_subfield": False}}
        filters = {
            "filters": [{"conditions": [{"field": "body", "operator": "is", "value": "hello"}]}]
        }
        clause = ESQueryTranslator().translate(filters, model_info)[0]
        assert clause["query"]["bool"]["must"][0] == {"term": {"body": "hello"}}

    def test_falls_back_to_keyword_when_metadata_missing(self):
        model_info = {"name": {"type": "string"}}  # no es_type recorded
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "is", "value": "alice"}]}]
        }
        clause = ESQueryTranslator().translate(filters, model_info)[0]
        assert clause["query"]["bool"]["must"][0] == {"term": {"name.keyword": "alice"}}

    def test_group_by_keyword_field_no_double_suffix(self):
        model_info = {
            "status": {"type": "string", "es_type": "keyword"},
            "balance": {"type": "number"},
        }
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [{"field": "balance", "type": "sum"}],
                }
            ]
        }
        clause = ESQueryTranslator().translate(filters, model_info)[0]
        # group_by_0 must use raw `status`, not `status.keyword`
        assert clause["aggs"]["group_by_0"]["terms"]["field"] == "status"


class TestESSchemaRecordsEsType:
    def test_es_type_preserved_in_field_info(self):
        mapping = {
            "idx": {
                "mappings": {
                    "properties": {
                        "name": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}},
                        },
                        "status": {"type": "keyword"},
                    }
                }
            }
        }
        with patch("query_builder.adapters.elasticsearch.schema_extractor.Elasticsearch") as ES:
            ES.return_value.indices.get_mapping.return_value = mapping
            ext = ESSchemaExtractor("http://h", "idx")
            schema = ext.extract_schema()
            assert schema["name"]["es_type"] == "text"
            assert schema["name"].get("has_keyword_subfield") is True
            assert schema["status"]["es_type"] == "keyword"


# ---------------------------------------------------------- ModelBuilder parents


class TestModelBuilderAutoParents:
    def test_implicit_parent_built(self):
        # Only the dotted children are declared; parent `user` is not.
        schema = {
            "user.name": {"type": "string"},
            "user.age": {"type": "number"},
            "id": {"type": "number"},
        }
        Model = ModelBuilder(schema).build("Root")
        assert "user" in Model.model_fields
        assert "id" in Model.model_fields

    def test_explicit_parent_still_works(self):
        schema = {
            "user": {"type": "object"},
            "user.name": {"type": "string"},
        }
        Model = ModelBuilder(schema).build("Root")
        assert "user" in Model.model_fields


# ---------------------------------------------------------- ES executor


class TestESExecutorImprovements:
    def _executor(self):
        client = MagicMock()
        return ESQueryExecutor("http://h", "idx", client=client), client

    def test_borrowed_client_not_closed(self):
        ex, client = self._executor()
        ex.close()
        client.close.assert_not_called()

    def test_pagination_injects_from_and_size(self):
        ex, client = self._executor()
        client.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}
        ex.execute([{"query": {"match_all": {}}}], offset=20, limit=10)
        kwargs = client.search.call_args.kwargs
        assert kwargs["from"] == 20
        assert kwargs["size"] == 10

    def test_pagination_does_not_overwrite_explicit_size(self):
        ex, client = self._executor()
        client.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}
        ex.execute([{"query": {"match_all": {}}, "size": 7}], limit=99)
        assert client.search.call_args.kwargs["size"] == 7

    def test_does_not_mutate_caller_query(self):
        ex, client = self._executor()
        client.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}
        original = {"query": {"match_all": {}}}
        ex.execute([original], offset=5, limit=10)
        assert "from" not in original
        assert "size" not in original


# ---------------------------------------------------------- Prompt rendering


class TestPromptRendersCleanly:
    def test_no_literal_double_braces(self):
        prompt = PromptGenerator({"foo": {"type": "string"}}).generate_system_prompt()
        # Every JSON example should be valid single-brace JSON.
        assert "{{" not in prompt
        assert "}}" not in prompt

    def test_examples_are_valid_json(self):
        import json
        import re

        prompt = PromptGenerator({"foo": {"type": "string"}}).generate_system_prompt()
        # Pull every fenced ```json``` block and try to parse it
        blocks = re.findall(r"```json\n(.*?)\n```", prompt, re.DOTALL)
        assert blocks, "no ```json blocks found in prompt"
        for block in blocks:
            json.loads(block)  # raises if any example is malformed
