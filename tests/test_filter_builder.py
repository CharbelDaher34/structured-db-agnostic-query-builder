"""Tests for FilterModelBuilder."""

import pytest
from pydantic import ValidationError

from query_builder.query.filter_builder import FilterModelBuilder


@pytest.fixture
def filter_model(basic_model_info):
    return FilterModelBuilder(basic_model_info).build_filter_model()


class TestBuild:
    def test_caches(self, basic_model_info):
        builder = FilterModelBuilder(basic_model_info)
        m1 = builder.build_filter_model()
        m2 = builder.build_filter_model()
        assert m1 is m2

    def test_has_filters_field(self, filter_model):
        assert "filters" in filter_model.model_fields


class TestFilterValidation:
    def test_string_filter_on_string_field(self, filter_model):
        data = {
            "filters": [
                {
                    "conditions": [
                        {
                            "type": "StringFilter",
                            "field": "name",
                            "operator": "is",
                            "value": "alice",
                        }
                    ]
                }
            ]
        }
        instance = filter_model(**data)
        assert len(instance.filters) == 1

    def test_string_filter_on_number_field_rejected(self, filter_model):
        with pytest.raises(ValidationError) as exc:
            filter_model(
                filters=[
                    {
                        "conditions": [
                            {
                                "type": "StringFilter",
                                "field": "balance",
                                "operator": "is",
                                "value": "x",
                            }
                        ]
                    }
                ]
            )
        assert "StringFilter used for non-string" in str(exc.value)

    def test_number_filter_on_number_field(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [
                        {
                            "type": "NumberFilter",
                            "field": "balance",
                            "operator": ">",
                            "value": 100,
                        }
                    ]
                }
            ]
        )
        assert instance.filters[0].conditions[0].value == 100

    def test_enum_filter_valid_value(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [
                        {
                            "type": "EnumFilter",
                            "field": "status",
                            "operator": "is",
                            "value": "active",
                        }
                    ]
                }
            ]
        )
        assert instance.filters[0].conditions[0].value == "active"

    def test_enum_filter_invalid_value_rejected(self, filter_model):
        with pytest.raises(ValidationError) as exc:
            filter_model(
                filters=[
                    {
                        "conditions": [
                            {
                                "type": "EnumFilter",
                                "field": "status",
                                "operator": "is",
                                "value": "unknown",
                            }
                        ]
                    }
                ]
            )
        assert "Invalid enum value" in str(exc.value)

    def test_enum_filter_list_with_invalid_value_rejected(self, filter_model):
        with pytest.raises(ValidationError):
            filter_model(
                filters=[
                    {
                        "conditions": [
                            {
                                "type": "EnumFilter",
                                "field": "status",
                                "operator": "isin",
                                "value": ["active", "rogue"],
                            }
                        ]
                    }
                ]
            )

    def test_enum_filter_exists_skips_value_check(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [
                        {
                            "type": "EnumFilter",
                            "field": "status",
                            "operator": "exists",
                            "value": True,
                        }
                    ]
                }
            ]
        )
        assert instance.filters[0].conditions[0].value is True

    def test_date_filter_on_date_field(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [
                        {
                            "type": "DateFilter",
                            "field": "created_at",
                            "operator": ">",
                            "value": "2024-01-01",
                        }
                    ]
                }
            ]
        )
        assert instance.filters[0].conditions[0].field.value == "created_at"

    def test_boolean_filter_on_boolean_field(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [
                        {
                            "type": "BooleanFilter",
                            "field": "active",
                            "operator": "is",
                            "value": True,
                        }
                    ]
                }
            ]
        )
        assert instance.filters[0].conditions[0].value is True


class TestSliceValidation:
    def test_aggregations_dropped_without_group_by(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [],
                    "aggregations": [{"field": "balance", "type": "sum"}],
                    "interval": "month",
                }
            ]
        )
        assert instance.filters[0].aggregations is None
        assert instance.filters[0].interval is None

    def test_interval_dropped_when_group_by_is_not_date(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "interval": "month",
                    "aggregations": [{"field": "balance", "type": "sum"}],
                }
            ]
        )
        assert instance.filters[0].interval is None

    def test_interval_kept_for_date_group_by(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [],
                    "group_by": ["created_at"],
                    "interval": "month",
                    "aggregations": [{"field": "balance", "type": "sum"}],
                }
            ]
        )
        assert instance.filters[0].interval is not None

    def test_aggregation_having_operator(self, filter_model):
        instance = filter_model(
            filters=[
                {
                    "conditions": [],
                    "group_by": ["status"],
                    "aggregations": [
                        {
                            "field": "balance",
                            "type": "sum",
                            "having_operator": ">",
                            "having_value": 1000,
                        }
                    ],
                }
            ]
        )
        agg = instance.filters[0].aggregations[0]
        assert agg.having_operator.value == ">"
        assert agg.having_value == 1000
