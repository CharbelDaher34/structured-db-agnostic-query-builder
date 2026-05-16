"""Tests for LLMClientFactory (mocked pydantic-ai Agent)."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from query_builder.llm.client_factory import LLMClientFactory


class FakeResult(BaseModel):
    answer: str


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Don't leak the project's real LLM_API_KEY into tests."""
    for var in ("LLM_MODEL", "LLM_API_KEY", "OPENAI_API_KEY", "LLM_BASE_URL"):
        monkeypatch.delenv(var, raising=False)


class TestConfig:
    def test_requires_model_name(self):
        with pytest.raises(ValueError, match="model_name is required"):
            LLMClientFactory()

    def test_requires_api_key_when_no_base_url(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        with pytest.raises(ValueError, match="api_key is required"):
            LLMClientFactory()

    def test_picks_up_env_vars(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        monkeypatch.setenv("LLM_API_KEY", "sk-test")
        f = LLMClientFactory()
        assert f.api_key == "sk-test"
        # gpt-prefixed model gets passed through verbatim
        assert f.model == "gpt-4o"

    def test_default_model_settings(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        f = LLMClientFactory(api_key="x")
        assert f.model_settings == {"temperature": 0, "top_p": 1.0}

    def test_anthropic_model_prefix(self, monkeypatch):
        f = LLMClientFactory(model_name="claude-sonnet-4-5", api_key="ak-test")
        assert os.environ.get("ANTHROPIC_API_KEY") == "ak-test"
        assert f.model == "claude-sonnet-4-5"

    def test_unknown_provider_defaults_to_openai_prefix(self, monkeypatch):
        f = LLMClientFactory(model_name="custom-model", api_key="x")
        assert f.model == "openai:custom-model"

    def test_base_url_normalises_to_v1(self, monkeypatch):
        f = LLMClientFactory(
            model_name="qwen3:8b",
            api_key="not-set",
            base_url="http://localhost:11434",
        )
        assert f.base_url == "http://localhost:11434/v1"

    def test_base_url_already_v1_preserved(self):
        f = LLMClientFactory(
            model_name="qwen3:8b",
            api_key="x",
            base_url="http://localhost:11434/v1",
        )
        assert f.base_url == "http://localhost:11434/v1"

    def test_base_url_without_api_key(self):
        f = LLMClientFactory(
            model_name="qwen3:8b",
            base_url="http://localhost:11434",
        )
        assert f.base_url == "http://localhost:11434/v1"


class TestParseQuery:
    @pytest.mark.asyncio
    async def test_parse_query_with_filter_model(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        f = LLMClientFactory(api_key="x")

        fake_agent = MagicMock()
        fake_agent.run = AsyncMock(return_value=MagicMock(output=FakeResult(answer="hi")))

        with patch.object(f, "_create_agent", return_value=fake_agent) as mk:
            result = await f.parse_query(
                inputs="hello", filter_model=FakeResult, system_prompt="sp"
            )

        mk.assert_called_once_with(FakeResult, "sp")
        fake_agent.run.assert_awaited_once_with(["hello"])
        assert isinstance(result, FakeResult)
        assert result.answer == "hi"

    @pytest.mark.asyncio
    async def test_parse_query_with_list_input(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        f = LLMClientFactory(api_key="x")
        fake_agent = MagicMock()
        fake_agent.run = AsyncMock(return_value=MagicMock(output="ok"))

        with patch.object(f, "_create_agent", return_value=fake_agent):
            await f.parse_query(inputs=["a", "b"], filter_model=None, system_prompt="")
        fake_agent.run.assert_awaited_once_with(["a", "b"])

    @pytest.mark.asyncio
    async def test_parse_query_passes_none_output_type(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        f = LLMClientFactory(api_key="x")
        fake_agent = MagicMock()
        fake_agent.run = AsyncMock(return_value=MagicMock(output="raw"))

        with patch.object(f, "_create_agent", return_value=fake_agent) as mk:
            await f.parse_query(inputs="hi", system_prompt="sp")
        mk.assert_called_once_with(None, "sp")
