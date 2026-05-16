"""Tests for query_builder.schema.type_mappings."""

from datetime import date, datetime
from typing import Any

from query_builder.schema.type_mappings import TypeMapper


class TestGetPythonType:
    def test_common_string(self):
        assert TypeMapper.get_python_type("string") is str

    def test_common_number(self):
        assert TypeMapper.get_python_type("number") is float

    def test_common_integer(self):
        assert TypeMapper.get_python_type("integer") is int

    def test_common_boolean(self):
        assert TypeMapper.get_python_type("boolean") is bool

    def test_common_date(self):
        assert TypeMapper.get_python_type("date") is datetime

    def test_elasticsearch_text(self):
        assert TypeMapper.get_python_type("text", "elasticsearch") is str

    def test_elasticsearch_long(self):
        assert TypeMapper.get_python_type("long", "elasticsearch") is int

    def test_mongodb_int(self):
        assert TypeMapper.get_python_type("int", "mongodb") is int

    def test_postgres_varchar(self):
        assert TypeMapper.get_python_type("varchar", "postgresql") is str

    def test_postgres_date_is_date_class(self):
        assert TypeMapper.get_python_type("date", "postgresql") is date

    def test_unknown_returns_any(self):
        assert TypeMapper.get_python_type("totally_made_up") is Any

    def test_case_insensitive(self):
        assert TypeMapper.get_python_type("STRING") is str


class TestNormalizeType:
    def test_string(self):
        assert TypeMapper.normalize_type("text", "elasticsearch") == "string"

    def test_number(self):
        assert TypeMapper.normalize_type("long", "elasticsearch") == "number"

    def test_boolean(self):
        assert TypeMapper.normalize_type("boolean") == "boolean"

    def test_date(self):
        assert TypeMapper.normalize_type("date") == "date"

    def test_object(self):
        assert TypeMapper.normalize_type("object") == "object"

    def test_array(self):
        assert TypeMapper.normalize_type("array") == "array"

    def test_unknown(self):
        assert TypeMapper.normalize_type("garbage") == "unknown"
