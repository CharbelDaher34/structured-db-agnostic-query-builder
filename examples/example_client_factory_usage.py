"""
Example usage of LLMClientFactory with list of inputs.

Configuration is loaded from environment variables:
- LLM_MODEL: Model name (e.g., "gpt-4o", "llama3.1")
- LLM_API_KEY: API key for the model provider
- LLM_BASE_URL: Optional base URL for OpenAI-compatible APIs (e.g., Ollama, vLLM)
"""

import asyncio
import os
from pydantic import BaseModel, Field

from query_builder.llm.client_factory import LLMClientFactory
# laod dotenv
from dotenv import load_dotenv

load_dotenv()
class FilterModel(BaseModel):
    """Example filter model for query parsing."""
    # status: str = Field(description="Status filter (e.g., 'active', 'completed')")
    # priority: int = Field(description="Priority level (1-5)")
    # category: str = Field(description="Category name")
    response: str = Field(description="Response to the user's message")


async def test_with_list_inputs():
    """
    Test LLMClientFactory with list of inputs from environment configuration.
    
    The factory automatically reads configuration from:
    - LLM_MODEL: Model name
    - LLM_API_KEY or OPENAI_API_KEY: API key  
    - LLM_BASE_URL: Optional base URL for OpenAI-compatible APIs
    """
    
    # Display configuration (from environment)
    print("Configuration:")
    print(f"  Model: {os.getenv('LLM_MODEL', 'not set')}")
    api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY", "not-set")
    print(f"  API Key: {api_key[:10]}..." if len(api_key) > 10 else f"  API Key: {api_key}")
    print(f"  Base URL: {os.getenv('LLM_BASE_URL', 'None (using default)')}")
    print()
    
    # Create factory - reads from environment variables automatically
    factory = LLMClientFactory(
        model_settings={
            "temperature": 0,
            "top_p": 1.0
        }
    )
    
    # Test with list of text inputs
    inputs = [
        "Hello, how are you?",
    ]
    
    print("Processing inputs:")
    for i, inp in enumerate(inputs, 1):
        print(f"  {i}. {inp}")
    print()
    
    result = await factory.parse_query(
        inputs=inputs,
        filter_model=FilterModel,
        system_prompt="Reply to the user's message."
    )
    
    print(result)


if __name__ == "__main__":
    asyncio.run(test_with_list_inputs())

