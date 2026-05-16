"""
Tests for the SQL adapter.

Uses an in-memory SQLite database so the tests are real-DB integration
(no mocks of SQLAlchemy itself) but still offline.
"""

from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlmodel import Field, Session, SQLModel

from query_builder.adapters.sql import (
    SQLQueryExecutor,
    SQLQueryTranslator,
    SQLSchemaExtractor,
    resolve_table,
)
from query_builder.orchestrator import QueryOrchestrator


class Order(SQLModel, table=True):
    id: int = Field(primary_key=True)
    region: str
    status: str
    amount: float
    quantity: int
    is_paid: bool
    order_date: datetime


@pytest.fixture
def engine():
    eng = create_engine("sqlite://")  # in-memory
    SQLModel.metadata.create_all(eng)

    rows = [
        Order(
            id=1,
            region="north",
            status="shipped",
            amount=100.0,
            quantity=2,
            is_paid=True,
            order_date=datetime(2024, 1, 5),
        ),
        Order(
            id=2,
            region="north",
            status="pending",
            amount=50.0,
            quantity=1,
            is_paid=False,
            order_date=datetime(2024, 1, 15),
        ),
        Order(
            id=3,
            region="south",
            status="shipped",
            amount=200.0,
            quantity=4,
            is_paid=True,
            order_date=datetime(2024, 2, 10),
        ),
        Order(
            id=4,
            region="south",
            status="shipped",
            amount=75.0,
            quantity=3,
            is_paid=True,
            order_date=datetime(2024, 2, 20),
        ),
        Order(
            id=5,
            region="east",
            status="cancelled",
            amount=10.0,
            quantity=1,
            is_paid=False,
            order_date=datetime(2024, 3, 1),
        ),
    ]
    with Session(eng) as session:
        for r in rows:
            session.add(r)
        session.commit()

    yield eng
    eng.dispose()


@pytest.fixture
def table(engine):
    return Order.__table__


@pytest.fixture
def extractor(engine, table):
    return SQLSchemaExtractor(
        engine=engine,
        table=table,
        category_fields=["region", "status"],
    )


@pytest.fixture
def translator(table):
    return SQLQueryTranslator(table=table, dialect_name="sqlite")


@pytest.fixture
def executor(engine):
    return SQLQueryExecutor(engine=engine, owns_engine=False)


# ------------------------------------------------------------------ resolve


class TestResolveTable:
    def test_resolves_sqlmodel_class(self, engine):
        tbl = resolve_table(engine, Order)
        assert tbl.name == "order"

    def test_returns_table_instance_unchanged(self, engine, table):
        assert resolve_table(engine, table) is table

    def test_resolves_table_name_via_reflection(self, engine):
        tbl = resolve_table(engine, "order")
        assert tbl.name == "order"

    def test_rejects_invalid_input(self, engine):
        with pytest.raises(TypeError):
            resolve_table(engine, 42)

    def test_missing_table_name_raises(self):
        # Fresh engine (empty DB) so reflection of a non-existent name fails
        empty = create_engine("sqlite://")
        with pytest.raises(ValueError):
            resolve_table(empty, "no_such_table")


# ------------------------------------------------------------------ schema


class TestSchemaExtractor:
    def test_extract_schema_basic_types(self, extractor):
        schema = extractor.extract_schema()
        assert schema["id"]["type"] == "number"
        assert schema["region"]["type"] == "enum"  # in category_fields
        assert schema["status"]["type"] == "enum"
        assert schema["amount"]["type"] == "number"
        assert schema["is_paid"]["type"] == "boolean"
        assert schema["order_date"]["type"] == "date"

    def test_schema_records_sql_type(self, extractor):
        schema = extractor.extract_schema()
        # Stash the SQL type for debugging/dialect-aware decisions later.
        assert "sql_type" in schema["amount"]

    def test_extract_schema_cached(self, extractor):
        first = extractor.extract_schema()
        second = extractor.extract_schema()
        assert first is second

    def test_invalidate_cache_re_extracts(self, extractor):
        first = extractor.extract_schema()
        extractor.invalidate_cache()
        second = extractor.extract_schema()
        assert first is not second
        assert first == second

    def test_distinct_values(self, extractor):
        values = extractor.get_distinct_values("region")
        assert set(values) == {"north", "south", "east"}

    def test_distinct_values_cached(self, extractor):
        extractor.get_distinct_values("region")
        # second call should hit cache — same object identity
        assert extractor._enum_cache["region"] is extractor.get_distinct_values("region")

    def test_distinct_values_unknown_field(self, extractor):
        assert extractor.get_distinct_values("missing_field") == []

    def test_get_field_type(self, extractor):
        assert extractor.get_field_type("amount") == "number"
        assert extractor.get_field_type("missing") == "unknown"


# --------------------------------------------------------------- translator


class TestTranslator:
    def _model_info(self):
        return {
            "id": {"type": "number"},
            "region": {"type": "enum", "values": ["north", "south", "east"]},
            "status": {"type": "enum", "values": ["shipped", "pending", "cancelled"]},
            "amount": {"type": "number"},
            "quantity": {"type": "number"},
            "is_paid": {"type": "boolean"},
            "order_date": {"type": "date"},
        }

    def test_empty_filters_yields_select_all(self, translator):
        queries = translator.translate({}, self._model_info())
        assert len(queries) == 1
        assert queries[0]["is_aggregation"] is False
        assert queries[0]["statement"] is not None

    def test_simple_equality_filter(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [
                            {
                                "type": "EnumFilter",
                                "field": "region",
                                "operator": "is",
                                "value": "north",
                            }
                        ]
                    }
                ]
            },
            self._model_info(),
        )
        results = executor.execute(queries)
        assert results[0]["success"] is True
        assert results[0]["total_hits"] == 2
        assert all(doc["region"] == "north" for doc in results[0]["documents"])

    def test_number_range(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [
                            {
                                "type": "NumberFilter",
                                "field": "amount",
                                "operator": ">",
                                "value": 60,
                            }
                        ]
                    }
                ]
            },
            self._model_info(),
        )
        results = executor.execute(queries)
        # 100, 200, 75 are > 60
        assert results[0]["total_hits"] == 3

    def test_between_operator(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [
                            {
                                "type": "NumberFilter",
                                "field": "amount",
                                "operator": "between",
                                "value": [50, 100],
                            }
                        ]
                    }
                ]
            },
            self._model_info(),
        )
        results = executor.execute(queries)
        assert results[0]["total_hits"] == 3  # 100, 50, 75

    def test_isin_operator(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [
                            {
                                "type": "EnumFilter",
                                "field": "region",
                                "operator": "isin",
                                "value": ["north", "south"],
                            }
                        ]
                    }
                ]
            },
            self._model_info(),
        )
        results = executor.execute(queries)
        assert results[0]["total_hits"] == 4  # 2 north + 2 south

    def test_unknown_field_dropped(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [
                            {
                                "type": "StringFilter",
                                "field": "made_up_field",
                                "operator": "is",
                                "value": "x",
                            }
                        ]
                    }
                ]
            },
            self._model_info(),
        )
        # No conditions survive — should return everything
        results = executor.execute(queries)
        assert results[0]["total_hits"] == 5

    def test_group_by_with_sum(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [],
                        "group_by": ["region"],
                        "aggregations": [
                            {"field": "amount", "type": "sum"},
                        ],
                    }
                ]
            },
            self._model_info(),
        )
        results = executor.execute(queries)
        assert results[0]["success"] is True
        buckets = {row["region"]: row["sum_amount"] for row in results[0]["documents"]}
        assert buckets["north"] == 150.0
        assert buckets["south"] == 275.0
        assert buckets["east"] == 10.0

    def test_having_clause(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [],
                        "group_by": ["region"],
                        "aggregations": [
                            {
                                "field": "amount",
                                "type": "sum",
                                "having_operator": ">",
                                "having_value": 100,
                            }
                        ],
                    }
                ]
            },
            self._model_info(),
        )
        results = executor.execute(queries)
        regions = {row["region"] for row in results[0]["documents"]}
        # north=150, south=275 pass; east=10 doesn't
        assert regions == {"north", "south"}

    def test_sort_on_aggregation_alias(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [],
                        "group_by": ["region"],
                        "aggregations": [{"field": "amount", "type": "sum"}],
                        "sort": [{"field": "amount", "order": "desc"}],
                    }
                ]
            },
            self._model_info(),
        )
        results = executor.execute(queries)
        sums = [row["sum_amount"] for row in results[0]["documents"]]
        assert sums == sorted(sums, reverse=True)

    def test_sort_and_limit(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [],
                        "sort": [{"field": "amount", "order": "desc"}],
                        "limit": 2,
                    }
                ]
            },
            self._model_info(),
        )
        results = executor.execute(queries)
        assert len(results[0]["documents"]) == 2
        assert results[0]["documents"][0]["amount"] == 200.0
        assert results[0]["documents"][1]["amount"] == 100.0

    def test_multiple_slices_become_multiple_queries(self, translator, executor):
        queries = translator.translate(
            {
                "filters": [
                    {
                        "conditions": [
                            {
                                "type": "EnumFilter",
                                "field": "region",
                                "operator": "is",
                                "value": "north",
                            }
                        ]
                    },
                    {
                        "conditions": [
                            {
                                "type": "EnumFilter",
                                "field": "region",
                                "operator": "is",
                                "value": "south",
                            }
                        ]
                    },
                ]
            },
            self._model_info(),
        )
        assert len(queries) == 2
        results = executor.execute(queries)
        assert results[0]["total_hits"] == 2  # north
        assert results[1]["total_hits"] == 2  # south


# ---------------------------------------------------------------- executor


class TestExecutor:
    def test_total_hits_is_pre_pagination(self, translator, executor):
        queries = translator.translate({}, TestTranslator()._model_info())
        results = executor.execute(queries, offset=0, limit=2)
        # 5 rows total, 2 returned because of limit, but total_hits == 5
        assert len(results[0]["documents"]) == 2
        assert results[0]["total_hits"] == 5

    def test_offset(self, translator, executor):
        queries = translator.translate(
            {"filters": [{"conditions": [], "sort": [{"field": "id", "order": "asc"}]}]},
            TestTranslator()._model_info(),
        )
        results = executor.execute(queries, offset=2, limit=2)
        ids = [d["id"] for d in results[0]["documents"]]
        assert ids == [3, 4]

    def test_dates_are_json_safe(self, translator, executor):
        queries = translator.translate({}, TestTranslator()._model_info())
        results = executor.execute(queries, limit=1)
        # ISO string, not a datetime object
        assert isinstance(results[0]["documents"][0]["order_date"], str)
        assert "T" in results[0]["documents"][0]["order_date"]

    def test_empty_queries(self, executor):
        assert executor.execute([]) == []

    def test_missing_statement_returns_error(self, executor):
        out = executor.execute([{"is_aggregation": False}])
        assert out[0]["success"] is False
        assert "missing 'statement'" in out[0]["error"]

    def test_execute_raw_with_statement(self, translator, executor):
        queries = translator.translate({}, TestTranslator()._model_info())
        out = executor.execute_raw(queries[0], size=2)
        assert out["success"] is True
        assert len(out["documents"]) == 2

    def test_execute_raw_with_sql(self, executor):
        out = executor.execute_raw(
            {
                "sql": 'SELECT region, amount FROM "order" WHERE region = :r',
                "params": {"r": "north"},
            },
            size=10,
        )
        assert out["success"] is True
        assert all(d["region"] == "north" for d in out["documents"])

    def test_execute_raw_missing_input(self, executor):
        out = executor.execute_raw({}, size=10)
        assert out["success"] is False
        assert "either 'statement' or 'sql'" in out["error"]


# -------------------------------------------------------------- orchestrator


class TestOrchestratorFactory:
    def test_from_sqlmodel_with_class(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

        # File-based SQLite so the factory's engine and our setup share state.
        # In-memory ``sqlite://`` opens a new isolated DB per connection.
        db_path = tmp_path / "orders.db"
        setup_engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(setup_engine)
        with Session(setup_engine) as s:
            s.add(
                Order(
                    id=1,
                    region="north",
                    status="shipped",
                    amount=10.0,
                    quantity=1,
                    is_paid=True,
                    order_date=datetime(2024, 1, 1),
                )
            )
            s.commit()
        setup_engine.dispose()

        orch = QueryOrchestrator.from_sqlmodel(
            database_url=f"sqlite:///{db_path}",
            table=Order,
            category_fields=["region"],
        )
        schema = orch.get_model_info()
        assert "region" in schema
        assert schema["region"]["type"] == "enum"
        assert schema["region"]["values"] == ["north"]
        orch.close()

    def test_from_sqlmodel_close_disposes_engine(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

        orch = QueryOrchestrator.from_sqlmodel(
            database_url="sqlite://",
            table=Order,
        )
        # close() should dispose without raising; calling it twice is also safe.
        orch.close()
        orch.close()
