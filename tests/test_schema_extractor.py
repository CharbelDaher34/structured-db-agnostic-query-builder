"""Tests for the SchemaExtractor coordinator wrapper."""

from query_builder.schema.extractor import SchemaExtractor


class FakeExtractor:
    def __init__(self):
        self.extract_calls = 0
        self.distinct_calls: list = []

    def extract_schema(self):
        self.extract_calls += 1
        return {"a": {"type": "string"}, "b": {"type": "number"}}

    def get_distinct_values(self, field_path, size=1000):
        self.distinct_calls.append((field_path, size))
        if field_path == "a":
            return ["x", "y", "z"]
        if field_path == "missing":
            raise RuntimeError("nope")
        return []

    def get_field_type(self, field_path):
        return self.extract_schema().get(field_path, {}).get("type", "unknown")


class TestSchemaExtractor:
    def test_get_schema_caches(self):
        fake = FakeExtractor()
        ext = SchemaExtractor(fake)
        s1 = ext.get_schema()
        s2 = ext.get_schema()
        assert s1 == s2
        assert fake.extract_calls == 1

    def test_get_schema_force_refresh(self):
        fake = FakeExtractor()
        ext = SchemaExtractor(fake)
        ext.get_schema()
        ext.get_schema(force_refresh=True)
        assert fake.extract_calls == 2

    def test_get_enum_fields_only_category_fields(self):
        fake = FakeExtractor()
        ext = SchemaExtractor(fake, category_fields=["a"])
        enums = ext.get_enum_fields()
        assert enums == {"a": ["x", "y", "z"]}

    def test_get_enum_fields_caches(self):
        fake = FakeExtractor()
        ext = SchemaExtractor(fake, category_fields=["a"])
        ext.get_enum_fields()
        ext.get_enum_fields()
        # only called once for "a"
        assert len(fake.distinct_calls) == 1

    def test_get_enum_fields_skips_empty(self):
        fake = FakeExtractor()
        ext = SchemaExtractor(fake, category_fields=["empty_field"])
        enums = ext.get_enum_fields()
        assert enums == {}

    def test_get_enum_fields_swallows_extractor_error(self):
        fake = FakeExtractor()
        ext = SchemaExtractor(fake, category_fields=["missing"])
        enums = ext.get_enum_fields()
        # error is swallowed -> empty enums
        assert enums == {}

    def test_get_field_type_passthrough(self):
        fake = FakeExtractor()
        ext = SchemaExtractor(fake)
        assert ext.get_field_type("a") == "string"
