"""
LLM client factory and management.

Handles creation and lifecycle of LLM clients for query parsing.
Supports text, images, audio, video, and document inputs.

Provider selection
------------------

The factory builds a pydantic-ai ``Model`` from one of four supported providers:

==================== ======================== =============================
``LLM_PROVIDER``     pydantic-ai model class  Required env var
==================== ======================== =============================
``openai``           ``OpenAIChatModel``      ``OPENAI_API_KEY``
``anthropic``        ``AnthropicModel``       ``ANTHROPIC_API_KEY``
``google``           ``GoogleModel``          ``GOOGLE_API_KEY`` (or ``GEMINI_API_KEY``)
``openai-compatible`` ``OpenAIChatModel``     ``LLM_BASE_URL`` (e.g. Ollama, vLLM); ``OPENAI_API_KEY`` optional
==================== ======================== =============================

``LLM_PROVIDER`` defaults to ``openai`` when unset.
"""

import os
from typing import Any, Optional, Union

from pydantic import BaseModel
from pydantic_ai import Agent, AudioUrl, BinaryContent, DocumentUrl, ImageUrl, VideoUrl

# Type alias for supported input types
InputType = Union[str, ImageUrl, AudioUrl, VideoUrl, DocumentUrl, BinaryContent]

# Supported providers — kept as a module-level constant so callers can introspect
# the allowed values (e.g. for a CLI ``--provider`` choices list).
SUPPORTED_PROVIDERS = ("openai", "anthropic", "google", "openai-compatible")


class LLMClientFactory:
    """
    Creates and manages LLM clients for query processing.

    Builds a pydantic-ai ``Model`` instance for one of the supported providers
    (OpenAI, Anthropic, Google Gemini, or any OpenAI-compatible endpoint such
    as Ollama or vLLM) and exposes a single :meth:`parse_query` entry point
    that returns structured Pydantic output.

    Configuration is read from the environment by default:

    - ``LLM_PROVIDER``: one of ``openai``, ``anthropic``, ``google``,
      ``openai-compatible`` (default: ``openai``)
    - ``LLM_MODEL``: model name (e.g. ``gpt-4o``, ``claude-sonnet-4-5``,
      ``gemini-1.5-pro``, ``qwen3:8b``)
    - ``OPENAI_API_KEY`` / ``ANTHROPIC_API_KEY`` / ``GOOGLE_API_KEY``:
      provider-specific key (only the one matching ``LLM_PROVIDER`` is needed)
    - ``LLM_BASE_URL``: required for ``openai-compatible``
    """

    def __init__(
        self,
        provider: Optional[str] = None,
        model_name: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model_settings: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize LLM client factory.

        Args:
            provider: One of ``openai``, ``anthropic``, ``google``,
                ``openai-compatible``. Falls back to ``LLM_PROVIDER`` env var,
                then defaults to ``openai``.
            model_name: Model name. Falls back to ``LLM_MODEL`` env var.
            api_key: Provider API key. Falls back to the provider's standard
                env var (``OPENAI_API_KEY`` / ``ANTHROPIC_API_KEY`` /
                ``GOOGLE_API_KEY``). Optional for ``openai-compatible``.
            base_url: Required for ``openai-compatible``. Falls back to
                ``LLM_BASE_URL`` env var.
            model_settings: Pydantic-AI model settings (temperature, top_p, …).
                Defaults to ``{"temperature": 0, "top_p": 1.0}`` for stable
                structured output.

        Raises:
            ValueError: If required arguments for the chosen provider are
                missing, or if the provider name is not recognised.
        """
        provider = (provider or os.getenv("LLM_PROVIDER") or "openai").lower()
        model_name = model_name or os.getenv("LLM_MODEL")

        if not model_name:
            raise ValueError(
                "model_name is required (provide as parameter or set LLM_MODEL env var)"
            )
        if provider not in SUPPORTED_PROVIDERS:
            raise ValueError(
                f"Unknown provider {provider!r}. Choose from: {', '.join(SUPPORTED_PROVIDERS)}"
            )

        self.provider = provider
        self.model_name = model_name
        self.base_url: Optional[str] = None
        self.model_settings = model_settings or {"temperature": 0, "top_p": 1.0}

        self.model = self._build_model(provider, model_name, api_key, base_url)

    # ------------------------------------------------------------------ model

    def _build_model(
        self,
        provider: str,
        model_name: str,
        api_key: Optional[str],
        base_url: Optional[str],
    ) -> Any:
        """Construct the pydantic-ai Model for the chosen provider."""
        if provider == "openai":
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider

            key = api_key or os.getenv("OPENAI_API_KEY")
            if not key:
                raise ValueError(
                    "OPENAI_API_KEY is required for provider 'openai' "
                    "(provide as api_key parameter or set the env var)"
                )
            return OpenAIChatModel(model_name=model_name, provider=OpenAIProvider(api_key=key))

        if provider == "anthropic":
            from pydantic_ai.models.anthropic import AnthropicModel
            from pydantic_ai.providers.anthropic import AnthropicProvider

            key = api_key or os.getenv("ANTHROPIC_API_KEY")
            if not key:
                raise ValueError(
                    "ANTHROPIC_API_KEY is required for provider 'anthropic' "
                    "(provide as api_key parameter or set the env var)"
                )
            return AnthropicModel(model_name=model_name, provider=AnthropicProvider(api_key=key))

        if provider == "google":
            from pydantic_ai.models.google import GoogleModel
            from pydantic_ai.providers.google import GoogleProvider

            key = api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
            if not key:
                raise ValueError(
                    "GOOGLE_API_KEY (or GEMINI_API_KEY) is required for provider 'google'"
                )
            return GoogleModel(model_name=model_name, provider=GoogleProvider(api_key=key))

        # provider == "openai-compatible" — Ollama, vLLM, Azure OpenAI, etc.
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider

        base = base_url or os.getenv("LLM_BASE_URL")
        if not base:
            raise ValueError(
                "LLM_BASE_URL is required for provider 'openai-compatible' "
                "(e.g. http://localhost:11434 for Ollama)"
            )
        # Normalise to /v1 — most OpenAI-compatible servers expect this suffix.
        base = base.rstrip("/")
        if not base.endswith("/v1"):
            base = f"{base}/v1"
        self.base_url = base

        # api_key may legitimately be absent for local servers (Ollama accepts
        # any value); only forward it when one is available.
        kwargs: dict[str, Any] = {"base_url": base}
        key = api_key or os.getenv("OPENAI_API_KEY")
        if key:
            kwargs["api_key"] = key
        return OpenAIChatModel(model_name=model_name, provider=OpenAIProvider(**kwargs))

    # ------------------------------------------------------------------ agent

    def _create_agent(
        self,
        output_type: Optional[type[BaseModel]],
        system_prompt: str,
    ) -> Agent[None, Optional[BaseModel]]:
        """Create a Pydantic AI agent."""
        agent_kwargs: dict[str, Any] = {
            "model": self.model,
            "system_prompt": system_prompt,
            "model_settings": self.model_settings,
            "retries": 3,  # Recover from validation errors on the LLM's first attempt
        }
        if output_type is not None:
            agent_kwargs["output_type"] = output_type
        return Agent(**agent_kwargs)

    async def parse_query(
        self,
        inputs: Union[InputType, list[InputType]],
        filter_model: Optional[type[BaseModel]] = None,
        system_prompt: str = "",
    ) -> Union[dict[str, Any], BaseModel]:
        """
        Parse inputs asynchronously with multimodal support.

            # Structured output with filter_model
            result = await factory.parse_query(
                inputs="Find high priority items",
                filter_model=FilterModel,
                system_prompt="Parse the query",
            )

            # Image analysis with text
            result = await factory.parse_query(
                inputs=[
                    "What's in this image?",
                    ImageUrl(url="https://example.com/image.png"),
                ],
                filter_model=ImageAnalysisModel,
                system_prompt="Analyze and extract data",
            )
        """
        agent = self._create_agent(filter_model, system_prompt)
        input_data = [inputs] if not isinstance(inputs, list) else inputs
        result = await agent.run(input_data)
        return result.output
