"""Tests for PromptGenerator."""

import json

from query_builder.query.prompt_generator import PromptGenerator


class TestPromptGenerator:
    def test_includes_schema(self, basic_model_info):
        prompt = PromptGenerator(basic_model_info).generate_system_prompt()
        assert "name" in prompt
        assert "balance" in prompt
        assert "StringFilter" in prompt

    def test_enum_values_inlined_when_small(self, basic_model_info):
        # Inspect the summarised structure (the rendered prompt mentions
        # "values_truncated" in its documentation so a string search is unreliable).
        summary = PromptGenerator(basic_model_info)._summarise_model_info()
        assert summary["status"]["values"] == ["active", "pending", "closed"]
        assert "values_truncated" not in summary["status"]

    def test_enum_values_truncated_when_large(self):
        large_enum = [f"val{i}" for i in range(75)]
        model_info = {"big_enum": {"type": "enum", "values": large_enum}}
        gen = PromptGenerator(model_info, max_enum_values_inline=10)
        summary = gen._summarise_model_info()
        assert summary["big_enum"]["values_truncated"] is True
        assert summary["big_enum"]["total_values"] == 75
        assert len(summary["big_enum"]["values"]) == 10

    def test_no_truncation_when_disabled(self):
        large_enum = [f"v{i}" for i in range(100)]
        model_info = {"e": {"type": "enum", "values": large_enum}}
        gen = PromptGenerator(model_info, max_enum_values_inline=0)
        summary = gen._summarise_model_info()
        assert summary == model_info  # unchanged

    def test_summary_preserves_non_enum_fields(self, basic_model_info):
        summary = PromptGenerator(basic_model_info)._summarise_model_info()
        assert summary["id"] == {"type": "number"}

    def test_prompt_is_non_empty_string(self, basic_model_info):
        prompt = PromptGenerator(basic_model_info).generate_system_prompt()
        assert isinstance(prompt, str)
        assert len(prompt) > 500
