"""
Example usage of LLMClientFactory with provider selection.

Configuration is loaded from environment variables:

- LLM_PROVIDER: one of openai, anthropic, google, openai-compatible
                (default: openai)
- LLM_MODEL:    model name for the chosen provider, e.g.
                gpt-4o, claude-sonnet-4-5, gemini-1.5-pro, qwen3:8b
- OPENAI_API_KEY / ANTHROPIC_API_KEY / GOOGLE_API_KEY:
                the provider-specific key (only the one for LLM_PROVIDER is read)
- LLM_BASE_URL: required when LLM_PROVIDER=openai-compatible
                (e.g. http://localhost:11434 for Ollama)
"""

import asyncio
import os

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from query_builder.llm.client_factory import LLMClientFactory

load_dotenv()


class FilterModel(BaseModel):
    """Example structured-output model."""

    response: str = Field(description="Response to the user's message")


async def main():
    provider = os.getenv("LLM_PROVIDER", "openai")
    print("Configuration:")
    print(f"  Provider: {provider}")
    print(f"  Model:    {os.getenv('LLM_MODEL', 'not set')}")
    if provider == "openai-compatible":
        print(f"  Base URL: {os.getenv('LLM_BASE_URL', 'not set')}")
    print()

    # Factory reads provider/model/key/base_url from the environment.
    factory = LLMClientFactory(model_settings={"temperature": 0, "top_p": 1.0})

    result = await factory.parse_query(
        inputs=["Hello, how are you?"],
        filter_model=FilterModel,
        system_prompt="Reply to the user's message.",
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
