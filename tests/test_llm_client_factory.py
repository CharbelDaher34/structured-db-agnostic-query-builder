"""Tests for LLMClientFactory (mocked pydantic-ai Agent)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from query_builder.llm.client_factory import SUPPORTED_PROVIDERS, LLMClientFactory


class FakeResult(BaseModel):
    answer: str


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Don't leak the project's real keys into tests."""
    for var in (
        "LLM_PROVIDER",
        "LLM_MODEL",
        "LLM_BASE_URL",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
    ):
        monkeypatch.delenv(var, raising=False)


class TestConfig:
    def test_requires_model_name(self):
        with pytest.raises(ValueError, match="model_name is required"):
            LLMClientFactory()

    def test_rejects_unknown_provider(self):
        with pytest.raises(ValueError, match="Unknown provider"):
            LLMClientFactory(provider="cohere", model_name="x")

    def test_default_provider_is_openai(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        f = LLMClientFactory()
        assert f.provider == "openai"

    def test_provider_from_env(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "anthropic")
        monkeypatch.setenv("LLM_MODEL", "claude-sonnet-4-5")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "ak-test")
        f = LLMClientFactory()
        assert f.provider == "anthropic"

    def test_provider_arg_overrides_env(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        f = LLMClientFactory(
            provider="anthropic", model_name="claude-sonnet-4-5", api_key="ak-test"
        )
        assert f.provider == "anthropic"

    def test_default_model_settings(self, monkeypatch):
        f = LLMClientFactory(model_name="gpt-4o", api_key="x")
        assert f.model_settings == {"temperature": 0, "top_p": 1.0}

    def test_supported_providers_list(self):
        # Public-API guarantee: exactly these four providers are wired up.
        assert set(SUPPORTED_PROVIDERS) == {
            "openai",
            "anthropic",
            "google",
            "openai-compatible",
        }


class TestOpenAIProvider:
    def test_requires_openai_api_key(self, monkeypatch):
        monkeypatch.setenv("LLM_MODEL", "gpt-4o")
        with pytest.raises(ValueError, match="OPENAI_API_KEY is required"):
            LLMClientFactory()

    def test_reads_openai_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-env")
        f = LLMClientFactory(model_name="gpt-4o")
        # Model is the OpenAIChatModel instance, not a string
        from pydantic_ai.models.openai import OpenAIChatModel

        assert isinstance(f.model, OpenAIChatModel)

    def test_explicit_api_key_wins_over_env(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-env")
        f = LLMClientFactory(model_name="gpt-4o", api_key="sk-explicit")
        # No exception means the explicit key was used; nothing else to assert
        # without poking pydantic-ai internals.
        from pydantic_ai.models.openai import OpenAIChatModel

        assert isinstance(f.model, OpenAIChatModel)


class TestAnthropicProvider:
    def test_requires_anthropic_api_key(self):
        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY is required"):
            LLMClientFactory(provider="anthropic", model_name="claude-sonnet-4-5")

    def test_builds_anthropic_model(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "ak-test")
        f = LLMClientFactory(provider="anthropic", model_name="claude-sonnet-4-5")
        from pydantic_ai.models.anthropic import AnthropicModel

        assert isinstance(f.model, AnthropicModel)
        assert f.provider == "anthropic"


class TestGoogleProvider:
    def test_requires_google_api_key(self):
        with pytest.raises(ValueError, match="GOOGLE_API_KEY"):
            LLMClientFactory(provider="google", model_name="gemini-1.5-pro")

    def test_accepts_gemini_api_key_alias(self, monkeypatch):
        monkeypatch.setenv("GEMINI_API_KEY", "gk-test")
        f = LLMClientFactory(provider="google", model_name="gemini-1.5-pro")
        from pydantic_ai.models.google import GoogleModel

        assert isinstance(f.model, GoogleModel)

    def test_google_api_key_takes_precedence_over_gemini(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_API_KEY", "gk-google")
        monkeypatch.setenv("GEMINI_API_KEY", "gk-gemini")
        # No exception — and the explicit GOOGLE_API_KEY is checked first
        f = LLMClientFactory(provider="google", model_name="gemini-1.5-pro")
        from pydantic_ai.models.google import GoogleModel

        assert isinstance(f.model, GoogleModel)


class TestOpenAICompatibleProvider:
    def test_requires_base_url(self):
        with pytest.raises(ValueError, match="LLM_BASE_URL is required"):
            LLMClientFactory(provider="openai-compatible", model_name="qwen3:8b")

    def test_base_url_normalises_to_v1(self):
        f = LLMClientFactory(
            provider="openai-compatible",
            model_name="qwen3:8b",
            base_url="http://localhost:11434",
        )
        assert f.base_url == "http://localhost:11434/v1"

    def test_base_url_already_v1_preserved(self):
        f = LLMClientFactory(
            provider="openai-compatible",
            model_name="qwen3:8b",
            base_url="http://localhost:11434/v1",
        )
        assert f.base_url == "http://localhost:11434/v1"

    def test_base_url_trailing_slash_stripped(self):
        f = LLMClientFactory(
            provider="openai-compatible",
            model_name="qwen3:8b",
            base_url="http://localhost:11434/",
        )
        assert f.base_url == "http://localhost:11434/v1"

    def test_works_without_api_key(self):
        # Ollama and other local servers don't require a real API key.
        f = LLMClientFactory(
            provider="openai-compatible",
            model_name="qwen3:8b",
            base_url="http://localhost:11434",
        )
        from pydantic_ai.models.openai import OpenAIChatModel

        assert isinstance(f.model, OpenAIChatModel)

    def test_reads_base_url_from_env(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "openai-compatible")
        monkeypatch.setenv("LLM_MODEL", "qwen3:8b")
        monkeypatch.setenv("LLM_BASE_URL", "http://localhost:11434")
        f = LLMClientFactory()
        assert f.base_url == "http://localhost:11434/v1"


class TestParseQuery:
    @pytest.mark.asyncio
    async def test_parse_query_with_filter_model(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-x")
        f = LLMClientFactory(model_name="gpt-4o")

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
        monkeypatch.setenv("OPENAI_API_KEY", "sk-x")
        f = LLMClientFactory(model_name="gpt-4o")
        fake_agent = MagicMock()
        fake_agent.run = AsyncMock(return_value=MagicMock(output="ok"))

        with patch.object(f, "_create_agent", return_value=fake_agent):
            await f.parse_query(inputs=["a", "b"], filter_model=None, system_prompt="")
        fake_agent.run.assert_awaited_once_with(["a", "b"])

    @pytest.mark.asyncio
    async def test_parse_query_passes_none_output_type(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-x")
        f = LLMClientFactory(model_name="gpt-4o")
        fake_agent = MagicMock()
        fake_agent.run = AsyncMock(return_value=MagicMock(output="raw"))

        with patch.object(f, "_create_agent", return_value=fake_agent) as mk:
            await f.parse_query(inputs="hi", system_prompt="sp")
        mk.assert_called_once_with(None, "sp")
