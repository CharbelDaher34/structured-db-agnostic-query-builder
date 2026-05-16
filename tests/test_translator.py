"""Tests for the QueryTranslator coordinator."""

from query_builder.query.translator import QueryTranslator


class FakeTranslator:
    def __init__(self):
        self.calls = []

    def translate(self, filters, model_info):
        self.calls.append((filters, model_info))
        return [{"echo": filters}]


class TestQueryTranslator:
    def test_empty_filters_delegates_with_empty_dict(self):
        # Each adapter knows its own "match all" shape — emitting an ES-flavoured
        # `match_all` here would break Mongo and CSV.
        fake = FakeTranslator()
        out = QueryTranslator(fake).translate({}, {})
        assert out == [{"echo": {}}]
        assert fake.calls == [({}, {})]

    def test_none_filters_normalised_to_empty_dict(self):
        fake = FakeTranslator()
        QueryTranslator(fake).translate(None, {})
        assert fake.calls == [({}, {})]

    def test_delegates_when_filters_present(self):
        fake = FakeTranslator()
        filters = {"filters": [{"conditions": []}]}
        out = QueryTranslator(fake).translate(filters, {"a": {"type": "string"}})
        assert out == [{"echo": filters}]
        assert len(fake.calls) == 1
