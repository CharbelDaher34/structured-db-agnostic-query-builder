"""Tests for the MongoDB adapter using mocked pymongo collection."""

from unittest.mock import MagicMock, patch

import pytest

from query_builder.adapters.mongodb.executor import MongoQueryExecutor
from query_builder.adapters.mongodb.query_translator import MongoQueryTranslator
from query_builder.adapters.mongodb.schema_extractor import MongoSchemaExtractor

# ----------------------------------------------------------- schema extractor


def _make_mock_client(sample_docs=None, distinct_docs=None):
    """Return (client_mock, collection_mock) pre-wired for schema extraction."""
    collection = MagicMock(name="collection")

    def aggregate_side_effect(pipeline, **kwargs):
        # First stage tells us which call this is.
        if pipeline and "$sample" in pipeline[0]:
            return iter(sample_docs or [])
        if pipeline and "$match" in pipeline[0]:
            return iter(distinct_docs or [])
        return iter([])

    collection.aggregate.side_effect = aggregate_side_effect
    collection.find.return_value = iter(sample_docs or [])

    db = MagicMock(name="db")
    db.__getitem__.return_value = collection
    client = MagicMock(name="client")
    client.__getitem__.return_value = db
    return client, collection


class TestMongoSchemaExtractor:
    def test_extract_schema_from_sampled_docs(self):
        client, _collection = _make_mock_client(
            sample_docs=[
                {
                    "_id": 1,
                    "name": "alice",
                    "age": 30,
                    "active": True,
                    "created": "2024-01-01",
                    "tags": ["a", "b"],
                    "meta": {"x": 1},
                },
                {
                    "_id": 2,
                    "name": "bob",
                    "age": 31,
                    "active": False,
                    "created": "2024-02-01",
                    "tags": ["a"],
                    "meta": {"x": 2},
                },
            ]
        )
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        schema = ext.extract_schema()
        assert schema["name"]["type"] == "string"
        assert schema["age"]["type"] == "number"
        assert schema["active"]["type"] == "boolean"
        assert schema["created"]["type"] == "date"
        assert schema["tags"]["type"] == "array"
        assert schema["meta"]["type"] == "object"
        # _id field should be skipped
        assert "_id" not in schema

    def test_extract_schema_marks_category_fields_as_enum(self):
        client, _ = _make_mock_client(sample_docs=[{"name": "alice", "status": "active"}])
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client, category_fields=["status"])
        schema = ext.extract_schema()
        assert schema["status"]["type"] == "enum"

    def test_extract_schema_falls_back_when_sampling_fails(self):
        collection = MagicMock()
        collection.aggregate.side_effect = RuntimeError("no $sample")
        find_cursor = MagicMock()
        find_cursor.limit.return_value = iter([{"name": "alice", "age": 1}])
        collection.find.return_value = find_cursor
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db

        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        schema = ext.extract_schema()
        assert "name" in schema

    def test_extract_schema_empty_collection(self):
        client, _ = _make_mock_client(sample_docs=[])
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        assert ext.extract_schema() == {}

    def test_extract_schema_is_cached(self):
        client, collection = _make_mock_client(sample_docs=[{"name": "alice"}])
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        ext.extract_schema()
        ext.extract_schema()
        assert collection.aggregate.call_count == 1

    def test_get_distinct_values(self):
        distinct_docs = [{"_id": "active"}, {"_id": "closed"}, {"_id": None}]
        client, _ = _make_mock_client(distinct_docs=distinct_docs)
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        values = ext.get_distinct_values("status")
        assert values == ["active", "closed"]

    def test_get_distinct_values_cached(self):
        distinct_docs = [{"_id": "a"}]
        client, collection = _make_mock_client(distinct_docs=distinct_docs)
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        ext.get_distinct_values("status")
        ext.get_distinct_values("status")
        # only one aggregation call for distinct (the schema call didn't run)
        assert collection.aggregate.call_count == 1

    def test_get_distinct_values_swallows_error(self):
        collection = MagicMock()
        collection.aggregate.side_effect = RuntimeError("boom")
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        assert ext.get_distinct_values("status") == []

    def test_get_field_type(self):
        client, _ = _make_mock_client(sample_docs=[{"name": "alice", "age": 1}])
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        assert ext.get_field_type("name") == "string"
        assert ext.get_field_type("missing") == "unknown"

    def test_close_only_closes_owned_client(self):
        client, _ = _make_mock_client(sample_docs=[])
        ext = MongoSchemaExtractor("uri", "db", "coll", client=client)
        ext.close()
        client.close.assert_not_called()


# ---------------------------------------------------------------- translator


class TestMongoQueryTranslator:
    def test_empty_filters(self, basic_model_info):
        out = MongoQueryTranslator().translate({}, basic_model_info)
        assert out == [{"pipeline": []}]

    def test_simple_match(self, basic_model_info):
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "is", "value": "alice"}]}]
        }
        out = MongoQueryTranslator().translate(filters, basic_model_info)
        pipeline = out[0]["pipeline"]
        assert pipeline[0] == {"$match": {"name": {"$eq": "alice"}}}

    def test_multiple_conditions_use_and(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [
                        {"field": "balance", "operator": ">", "value": 100},
                        {"field": "status", "operator": "is", "value": "active"},
                    ]
                }
            ]
        }
        out = MongoQueryTranslator().translate(filters, basic_model_info)
        match_stage = out[0]["pipeline"][0]["$match"]
        assert "$and" in match_stage
        assert len(match_stage["$and"]) == 2

    def test_isin_with_date_range(self, basic_model_info):
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
        out = MongoQueryTranslator().translate(filters, basic_model_info)
        match = out[0]["pipeline"][0]["$match"]
        assert match == {"created_at": {"$gte": "2024-01-01", "$lte": "2024-12-31"}}

    def test_contains_becomes_regex(self, basic_model_info):
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "contains", "value": "ali"}]}]
        }
        out = MongoQueryTranslator().translate(filters, basic_model_info)
        match = out[0]["pipeline"][0]["$match"]
        assert match["name"]["$regex"] == "ali"
        assert match["name"]["$options"] == "i"

    def test_exists_true(self, basic_model_info):
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "exists", "value": True}]}]
        }
        match = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"][0][
            "$match"
        ]
        assert match == {"name": {"$exists": True, "$ne": None}}

    def test_exists_false(self, basic_model_info):
        filters = {
            "filters": [{"conditions": [{"field": "name", "operator": "exists", "value": False}]}]
        }
        match = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"][0][
            "$match"
        ]
        assert "$or" in match

    def test_group_by_string_field(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [{"field": "balance", "type": "sum"}],
                }
            ]
        }
        pipeline = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"]
        group = next(s for s in pipeline if "$group" in s)["$group"]
        assert group["_id"] == {"status": "$status"}
        assert group["sum_balance"] == {"$sum": "$balance"}

    def test_group_by_date_field_uses_datetostring(self, basic_model_info):
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
        pipeline = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"]
        group = next(s for s in pipeline if "$group" in s)["$group"]
        assert "$dateToString" in group["_id"]["created_at"]

    def test_count_aggregation(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [{"field": "id", "type": "count"}],
                }
            ]
        }
        pipeline = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"]
        group = next(s for s in pipeline if "$group" in s)["$group"]
        assert group["count_id"] == {"$sum": 1}

    def test_having_adds_followup_match(self, basic_model_info):
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
        pipeline = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"]
        # Last $match is the having
        having = pipeline[-1]["$match"]
        assert having == {"sum_balance": {"$gt": 100}}

    def test_sort_remaps_to_id_path(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [{"field": "balance", "type": "sum"}],
                    "sort": [
                        {"field": "status", "order": "asc"},
                        {"field": "balance", "order": "desc"},
                    ],
                }
            ]
        }
        pipeline = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"]
        sort = next(s for s in pipeline if "$sort" in s)["$sort"]
        assert sort == {"_id.status": 1, "sum_balance": -1}

    def test_sort_without_group_passes_raw_field(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "sort": [{"field": "balance", "order": "desc"}],
                }
            ]
        }
        pipeline = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"]
        sort = pipeline[-1]["$sort"]
        assert sort == {"balance": -1}

    def test_limit_appended_at_end(self, basic_model_info):
        filters = {"filters": [{"conditions": [], "limit": 5}]}
        pipeline = MongoQueryTranslator().translate(filters, basic_model_info)[0]["pipeline"]
        assert pipeline[-1] == {"$limit": 5}


# ---------------------------------------------------------------- executor


class TestMongoQueryExecutor:
    def test_execute_empty(self):
        client, _ = _make_mock_client(sample_docs=[])
        ex = MongoQueryExecutor("uri", "db", "coll", client=client)
        assert ex.execute([]) == []

    def test_execute_runs_pipeline(self):
        collection = MagicMock()
        collection.aggregate.return_value = [
            {"_id": "abc123", "name": "alice"},
            {"_id": "def456", "name": "bob"},
        ]
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db

        ex = MongoQueryExecutor("uri", "db", "coll", client=client)
        out = ex.execute([{"pipeline": [{"$match": {"x": 1}}]}])
        assert out[0]["success"] is True
        assert out[0]["total_hits"] == 2
        # _id is stringified
        for doc in out[0]["documents"]:
            assert isinstance(doc["_id"], str)

    def test_execute_injects_pagination(self):
        collection = MagicMock()
        collection.aggregate.return_value = []
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db

        ex = MongoQueryExecutor("uri", "db", "coll", client=client)
        ex.execute([{"pipeline": [{"$match": {}}]}], offset=10, limit=5)
        # The first aggregate call is the paginated data fetch; the second is
        # the pre-pagination $count for total_hits.
        data_pipeline = collection.aggregate.call_args_list[0].args[0]
        count_pipeline = collection.aggregate.call_args_list[1].args[0]
        assert {"$skip": 10} in data_pipeline
        assert {"$limit": 5} in data_pipeline
        assert count_pipeline[-1] == {"$count": "n"}

    def test_execute_does_not_inject_limit_when_pipeline_has_one(self):
        collection = MagicMock()
        collection.aggregate.return_value = []
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db
        ex = MongoQueryExecutor("uri", "db", "coll", client=client)

        ex.execute([{"pipeline": [{"$limit": 100}]}], limit=5)
        pipeline = collection.aggregate.call_args.args[0]
        # only the pipeline-defined $limit
        assert sum(1 for stage in pipeline if "$limit" in stage) == 1

    def test_execute_empty_pipeline_and_no_limit_uses_find(self):
        # With neither limit nor offset, the executor takes the find() path with
        # a default cap of 100 documents.
        collection = MagicMock()
        cursor = MagicMock()
        cursor.skip.return_value.limit.return_value = [{"_id": "x", "name": "alice"}]
        collection.find.return_value = cursor
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db

        ex = MongoQueryExecutor("uri", "db", "coll", client=client)
        out = ex.execute([{"pipeline": []}])
        cursor.skip.assert_called_with(0)
        cursor.skip.return_value.limit.assert_called_with(100)
        assert out[0]["success"] is True

    def test_execute_handles_failure(self):
        collection = MagicMock()
        collection.aggregate.side_effect = RuntimeError("agg failed")
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db
        ex = MongoQueryExecutor("uri", "db", "coll", client=client)

        out = ex.execute([{"pipeline": [{"$match": {}}]}])
        assert out[0]["success"] is False
        assert "agg failed" in out[0]["error"]

    def test_execute_raw_pipeline(self):
        collection = MagicMock()
        collection.aggregate.return_value = [{"_id": "x"}]
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db
        ex = MongoQueryExecutor("uri", "db", "coll", client=client)

        out = ex.execute_raw({"pipeline": [{"$match": {}}]})
        assert out["success"] is True
        assert out["total_hits"] == 1

    def test_execute_raw_with_filter(self):
        collection = MagicMock()
        cursor = MagicMock()
        cursor.limit.return_value = [{"_id": "x"}]
        collection.find.return_value = cursor
        db = MagicMock()
        db.__getitem__.return_value = collection
        client = MagicMock()
        client.__getitem__.return_value = db
        ex = MongoQueryExecutor("uri", "db", "coll", client=client)

        out = ex.execute_raw({"filter": {"x": 1}}, size=42)
        cursor.limit.assert_called_with(42)
        assert out["success"] is True

    def test_close_does_not_close_borrowed_client(self):
        client, _ = _make_mock_client(sample_docs=[])
        ex = MongoQueryExecutor("uri", "db", "coll", client=client)
        ex.close()
        client.close.assert_not_called()
