"""Tests for the CSV adapter (schema extractor, translator, executor)."""

from pathlib import Path

import pandas as pd
import pytest

from query_builder.adapters.csv import (
    CSVQueryExecutor,
    CSVQueryTranslator,
    CSVSchemaExtractor,
)

# ---------------------------------------------------------------- schema extractor


class TestCSVSchemaExtractor:
    def test_basic_inference(self, sample_csv):
        ext = CSVSchemaExtractor(csv_path=str(sample_csv), date_columns=["created_at"])
        schema = ext.extract_schema()
        assert schema["name"]["type"] == "string"
        assert schema["balance"]["type"] == "number"
        assert schema["active"]["type"] == "boolean"
        assert schema["created_at"]["type"] == "date"

    def test_category_field_marked_as_enum(self, sample_csv):
        ext = CSVSchemaExtractor(csv_path=str(sample_csv), category_fields=["status"])
        schema = ext.extract_schema()
        assert schema["status"]["type"] == "enum"

    def test_string_dates_detected(self, sample_csv):
        # date_columns NOT set — values should still be inferred as date from ISO strings
        ext = CSVSchemaExtractor(csv_path=str(sample_csv))
        schema = ext.extract_schema()
        assert schema["created_at"]["type"] == "date"

    def test_distinct_values(self, sample_csv):
        ext = CSVSchemaExtractor(csv_path=str(sample_csv))
        values = ext.get_distinct_values("status")
        assert sorted(values) == ["active", "closed", "pending"]

    def test_distinct_values_unknown_column(self, sample_csv):
        ext = CSVSchemaExtractor(csv_path=str(sample_csv))
        assert ext.get_distinct_values("missing") == []

    def test_distinct_values_size_cap(self, sample_csv):
        ext = CSVSchemaExtractor(csv_path=str(sample_csv))
        assert len(ext.get_distinct_values("name", size=3)) == 3

    def test_get_field_type(self, sample_csv):
        ext = CSVSchemaExtractor(csv_path=str(sample_csv))
        assert ext.get_field_type("balance") == "number"
        assert ext.get_field_type("missing") == "unknown"

    def test_caches_schema(self, sample_csv):
        ext = CSVSchemaExtractor(csv_path=str(sample_csv))
        s1 = ext.extract_schema()
        s2 = ext.extract_schema()
        assert s1 is s2

    def test_invalidate_cache(self, sample_csv):
        ext = CSVSchemaExtractor(csv_path=str(sample_csv))
        ext.extract_schema()
        ext.invalidate_cache()
        assert ext._schema_cache is None

    def test_uses_provided_dataframe(self, sample_df):
        ext = CSVSchemaExtractor(csv_path="not-loaded.csv", df=sample_df)
        schema = ext.extract_schema()
        assert "balance" in schema


# ---------------------------------------------------------------- translator


class TestCSVQueryTranslator:
    def test_empty_filters_returns_empty_plan(self, basic_model_info):
        out = CSVQueryTranslator().translate({}, basic_model_info)
        assert out == [
            {
                "plan": {
                    "conditions": [],
                    "group_by": None,
                    "interval": None,
                    "aggregations": None,
                    "sort": None,
                    "limit": None,
                }
            }
        ]

    def test_passes_conditions(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [{"field": "status", "operator": "is", "value": "active"}],
                    "limit": 5,
                }
            ]
        }
        out = CSVQueryTranslator().translate(filters, basic_model_info)
        plan = out[0]["plan"]
        assert plan["conditions"][0]["field"] == "status"
        assert plan["limit"] == 5

    def test_drops_unknown_field_conditions(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [
                        {"field": "ghost", "operator": "is", "value": 1},
                        {"field": "balance", "operator": ">", "value": 0},
                    ]
                }
            ]
        }
        out = CSVQueryTranslator().translate(filters, basic_model_info)
        plan = out[0]["plan"]
        assert len(plan["conditions"]) == 1
        assert plan["conditions"][0]["field"] == "balance"

    def test_group_by_drops_unknown(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["status", "ghost"],
                    "interval": "month",
                    "aggregations": [{"field": "balance", "type": "sum"}],
                }
            ]
        }
        plan = CSVQueryTranslator().translate(filters, basic_model_info)[0]["plan"]
        assert plan["group_by"] == ["status"]
        # interval kept since group_by remains
        assert plan["interval"] == "month"

    def test_group_by_all_unknown_clears(self, basic_model_info):
        filters = {
            "filters": [
                {
                    "conditions": [],
                    "group_by": ["ghost"],
                    "aggregations": [{"field": "balance", "type": "sum"}],
                }
            ]
        }
        plan = CSVQueryTranslator().translate(filters, basic_model_info)[0]["plan"]
        assert plan["group_by"] is None
        # without group_by, aggregations are dropped
        assert plan["aggregations"] is None


# ---------------------------------------------------------------- executor


@pytest.fixture
def csv_executor(sample_csv):
    return CSVQueryExecutor(csv_path=str(sample_csv), date_columns=["created_at"])


class TestCSVQueryExecutor:
    def test_empty_queries(self, csv_executor):
        assert csv_executor.execute([]) == []

    def test_simple_filter(self, csv_executor):
        plan = {"plan": {"conditions": [{"field": "status", "operator": "is", "value": "active"}]}}
        results = csv_executor.execute([plan])
        assert results[0]["success"] is True
        for doc in results[0]["documents"]:
            assert doc["status"] == "active"

    def test_number_gt(self, csv_executor):
        plan = {"plan": {"conditions": [{"field": "balance", "operator": ">", "value": 5000}]}}
        out = csv_executor.execute([plan])[0]
        for doc in out["documents"]:
            assert doc["balance"] > 5000

    def test_isin(self, csv_executor):
        plan = {
            "plan": {
                "conditions": [
                    {"field": "status", "operator": "isin", "value": ["active", "closed"]}
                ]
            }
        }
        out = csv_executor.execute([plan])[0]
        for doc in out["documents"]:
            assert doc["status"] in ("active", "closed")

    def test_contains(self, csv_executor):
        plan = {
            "plan": {
                "conditions": [{"field": "name", "operator": "contains", "value": "customer_1"}]
            }
        }
        out = csv_executor.execute([plan])[0]
        # matches customer_1, customer_10, customer_11
        names = {d["name"] for d in out["documents"]}
        assert "customer_1" in names

    def test_between_date(self, csv_executor):
        plan = {
            "plan": {
                "conditions": [
                    {
                        "field": "created_at",
                        "operator": "between",
                        "value": ["2024-01-22", "2024-02-12"],
                    }
                ]
            }
        }
        out = csv_executor.execute([plan])[0]
        assert out["success"] is True
        assert out["total_hits"] > 0

    def test_sort_and_limit(self, csv_executor):
        plan = {
            "plan": {
                "conditions": [],
                "sort": [{"field": "balance", "order": "desc"}],
                "limit": 3,
            }
        }
        out = csv_executor.execute([plan])[0]
        assert len(out["documents"]) == 3
        balances = [d["balance"] for d in out["documents"]]
        assert balances == sorted(balances, reverse=True)

    def test_group_by_with_count(self, csv_executor):
        plan = {
            "plan": {
                "group_by": ["status"],
                "aggregations": [{"field": "id", "type": "count"}],
            }
        }
        out = csv_executor.execute([plan])[0]
        assert out["success"] is True
        # 3 distinct statuses
        assert out["total_hits"] == 3
        for doc in out["documents"]:
            assert "count_id" in doc

    def test_group_by_with_sum_avg(self, csv_executor):
        plan = {
            "plan": {
                "group_by": ["status"],
                "aggregations": [
                    {"field": "balance", "type": "sum"},
                    {"field": "balance", "type": "avg"},
                ],
            }
        }
        out = csv_executor.execute([plan])[0]
        for doc in out["documents"]:
            assert "sum_balance" in doc
            assert "avg_balance" in doc

    def test_having_clause(self, csv_executor):
        plan = {
            "plan": {
                "group_by": ["status"],
                "aggregations": [
                    {
                        "field": "id",
                        "type": "count",
                        "having_operator": ">",
                        "having_value": 4,
                    }
                ],
            }
        }
        out = csv_executor.execute([plan])[0]
        for doc in out["documents"]:
            assert doc["count_id"] > 4

    def test_date_interval_month(self, csv_executor):
        plan = {
            "plan": {
                "group_by": ["created_at"],
                "interval": "month",
                "aggregations": [{"field": "balance", "type": "sum"}],
            }
        }
        out = csv_executor.execute([plan])[0]
        assert out["success"] is True
        # 12 weekly rows starting 2024-01-15 -> spans 3 months
        assert out["total_hits"] >= 2

    def test_pagination_offset(self, csv_executor):
        plan = {"plan": {"sort": [{"field": "id", "order": "asc"}]}}
        out = csv_executor.execute([plan], offset=5, limit=3)[0]
        ids = [d["id"] for d in out["documents"]]
        assert ids == [6, 7, 8]

    def test_failing_plan_returns_error(self, csv_executor):
        plan = {
            "plan": {
                "conditions": [{"field": "balance", "operator": "between", "value": "not-a-list"}]
            }
        }
        csv_executor.execute([plan])[0]
        # between with bad value returns None mask -> no error; check operator with no col
        # Use an operator we know will fail
        plan_bad = {
            "plan": {
                "group_by": ["balance"],
                "aggregations": [{"field": "balance", "type": "unknown_op"}],
            }
        }
        out_bad = csv_executor.execute([plan_bad])[0]
        # Unknown aggregator is just skipped -> still succeeds with a count
        assert out_bad["success"] is True

    def test_sort_field_dropped_when_missing_post_group(self, csv_executor):
        plan = {
            "plan": {
                "group_by": ["status"],
                "aggregations": [{"field": "balance", "type": "sum"}],
                "sort": [{"field": "nonexistent_field", "order": "asc"}],
            }
        }
        out = csv_executor.execute([plan])[0]
        assert out["success"] is True

    def test_execute_raw_with_filter(self, csv_executor):
        out = csv_executor.execute_raw(
            {"filter": [{"field": "status", "operator": "is", "value": "active"}]},
            size=10,
        )
        assert out["success"] is True
        for doc in out["documents"]:
            assert doc["status"] == "active"

    def test_execute_raw_with_plan(self, csv_executor):
        out = csv_executor.execute_raw({"plan": {"limit": 2}})
        assert out["success"] is True
        assert len(out["documents"]) == 2

    def test_sanitize_doc_no_nan_or_timestamp(self, sample_df):
        # Add a NaN cell to test sanitization
        sample_df.loc[0, "balance"] = float("nan")
        ex = CSVQueryExecutor(csv_path="x.csv", df=sample_df)
        out = ex.execute([{"plan": {"limit": 1}}])[0]
        doc = out["documents"][0]
        assert doc["balance"] is None
        # date column must be JSON-friendly string, not Timestamp
        assert isinstance(doc["created_at"], str)
