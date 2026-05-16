"""Tests for ModelBuilder."""

from enum import Enum

import pytest
from pydantic import BaseModel, ValidationError

from query_builder.schema.model_builder import ModelBuilder


class TestBuild:
    def test_simple_schema(self):
        schema = {
            "name": {"type": "string"},
            "age": {"type": "number"},
            "active": {"type": "boolean"},
        }
        mb = ModelBuilder(schema)
        Model = mb.build("UserModel")
        assert issubclass(Model, BaseModel)
        assert Model.__name__ == "UserModel"
        assert set(Model.model_fields.keys()) == {"name", "age", "active"}

    def test_field_types_optional_by_default(self):
        schema = {"name": {"type": "string"}}
        Model = ModelBuilder(schema).build()
        # All fields are Optional => no field is required
        m = Model()
        assert m.name is None

    def test_enum_field(self):
        schema = {"status": {"type": "string"}}
        enum_fields = {"status": ["active", "inactive"]}
        Model = ModelBuilder(schema, enum_fields=enum_fields).build()
        # The status field should be Optional[Enum]
        instance = Model(status="active")
        assert instance.status.value == "active"
        with pytest.raises(ValidationError):
            Model(status="unknown")

    def test_fields_to_ignore_skips_field(self):
        schema = {
            "name": {"type": "string"},
            "secret": {"type": "string"},
        }
        Model = ModelBuilder(schema, fields_to_ignore=["secret"]).build()
        assert "secret" not in Model.model_fields
        assert "name" in Model.model_fields

    def test_ignored_types_dropped(self):
        schema = {
            "good": {"type": "string"},
            "bad": {"type": "alias"},
            "ugly": {"type": "unknown"},
        }
        Model = ModelBuilder(schema).build()
        assert set(Model.model_fields.keys()) == {"good"}

    def test_underscore_fields_dropped(self):
        schema = {"_id": {"type": "string"}, "name": {"type": "string"}}
        Model = ModelBuilder(schema).build()
        assert "_id" not in Model.model_fields
        assert "name" in Model.model_fields

    def test_caches_built_model(self):
        schema = {"a": {"type": "string"}}
        mb = ModelBuilder(schema)
        m1 = mb.build()
        m2 = mb.build()
        assert m1 is m2

    def test_nested_object(self):
        # ModelBuilder only nests when the parent has its own schema entry.
        schema = {
            "user": {"type": "object"},
            "user.name": {"type": "string"},
            "user.age": {"type": "number"},
            "id": {"type": "number"},
        }
        Model = ModelBuilder(schema).build("Root")
        assert "user" in Model.model_fields
        assert "id" in Model.model_fields

    def test_enum_sanitizes_member_names(self):
        schema = {"region": {"type": "string"}}
        Model = ModelBuilder(schema, enum_fields={"region": ["New York", "São Paulo"]}).build()
        # member values must be reachable through the Enum class
        instance = Model(region="New York")
        assert instance.region.value == "New York"


class TestGetModelInfo:
    def test_string(self):
        schema = {"name": {"type": "string"}}
        info = ModelBuilder(schema).get_model_info()
        assert info["name"]["type"] == "string"

    def test_number(self):
        schema = {"age": {"type": "number"}}
        info = ModelBuilder(schema).get_model_info()
        assert info["age"]["type"] == "number"

    def test_boolean(self):
        schema = {"active": {"type": "boolean"}}
        info = ModelBuilder(schema).get_model_info()
        assert info["active"]["type"] == "boolean"

    def test_date(self):
        schema = {"created": {"type": "date"}}
        info = ModelBuilder(schema).get_model_info()
        assert info["created"]["type"] == "date"

    def test_enum_records_values(self):
        schema = {"status": {"type": "string"}}
        info = ModelBuilder(schema, enum_fields={"status": ["a", "b"]}).get_model_info()
        assert info["status"]["type"] == "enum"
        assert info["status"]["values"] == ["a", "b"]

    def test_caches_info(self):
        mb = ModelBuilder({"a": {"type": "string"}})
        i1 = mb.get_model_info()
        i2 = mb.get_model_info()
        assert i1 is i2
