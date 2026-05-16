"""
End-to-end AI evaluation against a complex synthetic orders dataset.

Hits the real LLM and checks that the extracted filters + executed plan
behave correctly. Each test case asserts on a structural property of the
LLM output (operators, group_by, sort, limit, agg type) rather than the
exact wording, to allow for benign LLM variation.

Run with:
    uv run pytest tests/test_ai_eval_complex.py -v
    uv run pytest tests/test_ai_eval_complex.py -v -k "filter or sort"

Skip with:
    uv run pytest -m 'not llm'
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Optional

import pytest
from dotenv import load_dotenv

from query_builder import QueryOrchestrator

from .fixtures.build_complex_dataset import (
    CATEGORY_FIELDS,
    DATE_COLUMNS,
    build_orders,
    write_csv,
)

load_dotenv()

pytestmark = [
    pytest.mark.llm,
    pytest.mark.skipif(
        not (os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")),
        reason="Requires LLM_API_KEY for real LLM calls",
    ),
]


# ---------------------------------------------------------------- fixture


@pytest.fixture(scope="module")
def orders_orchestrator(tmp_path_factory):
    """Build the complex orders CSV once and share across tests in this module."""
    path = tmp_path_factory.mktemp("ai_eval") / "orders.csv"
    write_csv(path, build_orders(n=200, seed=4242))
    orch = QueryOrchestrator.from_csv(
        csv_path=str(path),
        category_fields=CATEGORY_FIELDS,
        date_columns=DATE_COLUMNS,
    )
    orch.warm_up()
    yield orch
    orch.close()


# ---------------------------------------------------------------- helpers


def _all_conditions(slices: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for s in slices:
        yield from (s.get("conditions") or [])


def _has_condition(
    slices: list[dict[str, Any]],
    *,
    field: str,
    operator: Optional[str] = None,
    value: Any = ...,
    filter_type: Optional[str] = None,
) -> bool:
    """Return True iff any condition matches the given properties."""
    for c in _all_conditions(slices):
        if c.get("field") != field:
            continue
        if filter_type and c.get("type") != filter_type:
            continue
        if operator and c.get("operator") != operator:
            continue
        if value is not ... and c.get("value") != value:
            continue
        return True
    return False


def _slice0(filters: dict[str, Any]) -> dict[str, Any]:
    return filters["filters"][0]


async def _run(orch, q: str) -> dict[str, Any]:
    """Convenience wrapper — execute and return everything."""
    return await orch.query(q, execute=True)


# ============================================================ FILTER TESTS


class TestSimpleFilters:
    @pytest.mark.asyncio
    async def test_single_enum_filter(self, orders_orchestrator):
        out = await _run(orders_orchestrator, "Show me only the EMEA orders.")
        slices = out["extracted_filters"]["filters"]
        assert _has_condition(
            slices,
            field="region",
            filter_type="EnumFilter",
            operator="is",
            value="EMEA",
        )
        for doc in out["results"][0]["documents"][:20]:
            assert doc["region"] == "EMEA"

    @pytest.mark.asyncio
    async def test_numeric_greater_than(self, orders_orchestrator):
        out = await _run(orders_orchestrator, "Show orders where the amount is greater than 5000.")
        slices = out["extracted_filters"]["filters"]
        assert _has_condition(
            slices,
            field="amount",
            filter_type="NumberFilter",
            operator=">",
            value=5000,
        )
        for doc in out["results"][0]["documents"]:
            assert doc["amount"] > 5000

    @pytest.mark.asyncio
    async def test_boolean_filter(self, orders_orchestrator):
        out = await _run(orders_orchestrator, "Show me only the returned orders.")
        slices = out["extracted_filters"]["filters"]
        assert _has_condition(
            slices,
            field="is_returned",
            filter_type="BooleanFilter",
            value=True,
        )
        for doc in out["results"][0]["documents"]:
            assert doc["is_returned"] is True

    @pytest.mark.asyncio
    async def test_contains_string(self, orders_orchestrator):
        out = await _run(orders_orchestrator, "Find orders whose notes contain the word 'fragile'.")
        slices = out["extracted_filters"]["filters"]
        # Accept either StringFilter contains, or contains on notes
        assert _has_condition(slices, field="notes", operator="contains")


class TestMultiConditionFilters:
    @pytest.mark.asyncio
    async def test_two_enums_and(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Show APAC enterprise orders.",
        )
        slices = out["extracted_filters"]["filters"]
        assert _has_condition(slices, field="region", value="APAC")
        assert _has_condition(slices, field="customer_segment", value="Enterprise")
        for doc in out["results"][0]["documents"]:
            assert doc["region"] == "APAC"
            assert doc["customer_segment"] == "Enterprise"

    @pytest.mark.asyncio
    async def test_enum_plus_numeric_plus_boolean(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Show high-priority Electronics orders above $2000 that were not returned.",
        )
        slices = out["extracted_filters"]["filters"]
        assert _has_condition(slices, field="priority", value="high")
        assert _has_condition(slices, field="product_category", value="Electronics")
        assert _has_condition(slices, field="amount", operator=">")
        assert _has_condition(slices, field="is_returned", filter_type="BooleanFilter", value=False)


class TestDateFilters:
    @pytest.mark.asyncio
    async def test_date_range_between(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Show orders placed between 2024-03-01 and 2024-06-30.",
        )
        slices = out["extracted_filters"]["filters"]
        # Accept either `between` or `isin` with 2-date list — both translate
        # to the same range.
        found = False
        for c in _all_conditions(slices):
            if (
                c.get("field") == "order_date"
                and c.get("type") == "DateFilter"
                and c.get("operator") in ("between", "isin")
            ):
                v = c.get("value")
                if isinstance(v, list) and len(v) == 2:
                    found = True
                    break
        assert found, f"no date range condition found: {slices}"


# ============================================================ INVALID ENUM


class TestInvalidEnumDropped:
    @pytest.mark.asyncio
    async def test_invalid_region_dropped(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Show orders in Antarctica region.",
        )
        slices = out["extracted_filters"]["filters"]
        # Antarctica is not a valid region — the condition should be dropped
        for c in _all_conditions(slices):
            if c.get("field") == "region":
                # If the LLM kept the field, the value must NOT be 'Antarctica'
                assert "antarctica" not in str(c.get("value", "")).lower(), f"invented value: {c}"


# ============================================================ SORT + LIMIT


class TestSortAndLimit:
    @pytest.mark.asyncio
    async def test_top_n_desc(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Show me the top 5 orders by amount descending.",
        )
        s = _slice0(out["extracted_filters"])
        assert s.get("limit") == 5
        sort = s.get("sort") or []
        assert any(x.get("field") == "amount" and x.get("order") == "desc" for x in sort), (
            f"expected sort by amount desc, got {sort}"
        )
        docs = out["results"][0]["documents"]
        assert len(docs) == 5
        amounts = [d["amount"] for d in docs]
        assert amounts == sorted(amounts, reverse=True)

    @pytest.mark.asyncio
    async def test_recent_no_limit(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Show me the recent orders.",
        )
        s = _slice0(out["extracted_filters"])
        # "Recent" must NOT introduce a limit, only a desc sort
        assert s.get("limit") is None, f"unexpected limit: {s}"
        sort = s.get("sort") or []
        assert sort, "expected a sort by date desc for 'recent'"


# ============================================================ AGGREGATIONS


class TestGroupingAndAggregation:
    @pytest.mark.asyncio
    async def test_group_by_with_sum(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "What is the total amount per region?",
        )
        s = _slice0(out["extracted_filters"])
        assert s.get("group_by") == ["region"]
        aggs = s.get("aggregations") or []
        assert any(a.get("field") == "amount" and a.get("type") == "sum" for a in aggs), (
            f"expected sum(amount), got {aggs}"
        )
        docs = out["results"][0]["documents"]
        assert len(docs) >= 1
        for d in docs:
            assert "sum_amount" in d

    @pytest.mark.asyncio
    async def test_group_by_two_fields(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "For each region and product_category, show the average amount.",
        )
        s = _slice0(out["extracted_filters"])
        gb = s.get("group_by") or []
        assert set(gb) == {"region", "product_category"}, f"got {gb}"

    @pytest.mark.asyncio
    async def test_having_clause(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Show me regions with more than 30 orders, grouped by region with a count.",
        )
        s = _slice0(out["extracted_filters"])
        aggs = s.get("aggregations") or []
        having = [a for a in aggs if a.get("having_operator")]
        assert having, f"expected at least one having clause: {aggs}"
        h = having[0]
        assert h.get("having_operator") == ">"
        assert h.get("having_value") in (30, 30.0)

        # Verify executed results respect the having
        for doc in out["results"][0]["documents"]:
            # The count column name is "count_<field>"
            count_keys = [k for k in doc if k.startswith("count_")]
            assert count_keys, f"no count column in result: {doc}"
            assert doc[count_keys[0]] > 30

    @pytest.mark.asyncio
    async def test_date_histogram_month(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Show the total order amount per month based on order_date.",
        )
        s = _slice0(out["extracted_filters"])
        assert s.get("group_by") == ["order_date"]
        assert s.get("interval") == "month"
        docs = out["results"][0]["documents"]
        # 2024 spans 12 months → at most 12 buckets, at least 6
        assert 6 <= len(docs) <= 13

    @pytest.mark.asyncio
    async def test_multiple_aggregations(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Per customer_segment show the count of orders, "
            "the average amount, and the maximum amount.",
        )
        s = _slice0(out["extracted_filters"])
        assert s.get("group_by") == ["customer_segment"]
        agg_types = {a.get("type") for a in (s.get("aggregations") or [])}
        # Accept reasonable variation: at minimum these three must appear
        assert {"count", "avg", "max"}.issubset(agg_types), f"got {agg_types}"


# ============================================================ COMPARISONS


class TestComparisonsAndSlices:
    @pytest.mark.asyncio
    async def test_two_slice_comparison(self, orders_orchestrator):
        out = await _run(
            orders_orchestrator,
            "Compare orders from the AMER region versus the EMEA region.",
        )
        slices = out["extracted_filters"]["filters"]
        assert len(slices) == 2
        values = []
        for s in slices:
            for c in s.get("conditions") or []:
                if c.get("field") == "region":
                    values.append(c.get("value"))
        assert set(values) == {"AMER", "EMEA"}, f"slice values: {values}"
        # Each slice ran
        assert len(out["results"]) == 2
