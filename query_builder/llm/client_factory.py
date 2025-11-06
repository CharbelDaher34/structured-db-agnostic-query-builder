"""
LLM client factory and management.

Handles creation and lifecycle of LLM clients for query parsing.
Supports text, images, audio, video, and document inputs.
"""

import os
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel
from pydantic_ai import Agent, ImageUrl, AudioUrl, VideoUrl, DocumentUrl, BinaryContent
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

# Type alias for supported input types
InputType = Union[str, ImageUrl, AudioUrl, VideoUrl, DocumentUrl, BinaryContent]


class LLMClientFactory:
    """
    Creates and manages LLM clients for query processing.
    
    Handles client initialization, reuse, and query parsing with
    structured output. Supports OpenAI and OpenAI-compatible APIs.
    
    Reads configuration from environment variables by default:
    - LLM_MODEL: Model name
    - LLM_API_KEY or OPENAI_API_KEY: API key
    - LLM_BASE_URL: Optional base URL for OpenAI-compatible APIs
    """
    
    def __init__(
        self,
        model_name: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model_settings: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize LLM client factory.
        
        Args:
            model_name: Name of the LLM model (e.g., "gpt-4o", "openai:gpt-4o").
                       If not provided, reads from LLM_MODEL environment variable.
            api_key: API key for the LLM provider.
                    If not provided, reads from LLM_API_KEY or OPENAI_API_KEY.
            base_url: Optional base URL for OpenAI-compatible APIs.
                     If not provided, reads from LLM_BASE_URL environment variable.
            model_settings: Optional model settings (temperature, top_p, etc.)
            
        Raises:
            ValueError: If model_name or api_key is missing and not in environment
        """
        # Read from environment variables if not provided
        model_name = model_name or os.getenv("LLM_MODEL")
        api_key = api_key or os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
        base_url = base_url or os.getenv("LLM_BASE_URL")
        
        if not model_name:
            raise ValueError("model_name is required (provide as parameter or set LLM_MODEL env var)")
        if not api_key:
            raise ValueError("api_key is required (provide as parameter or set LLM_API_KEY/OPENAI_API_KEY env var)")
        
        self.api_key = api_key
        self.base_url = base_url
        self.model_settings = model_settings or {"temperature": 0, "top_p": 1.0}
        
        # Configure model based on base_url
        if base_url:
            # Use OpenAI-compatible API with custom base URL
            self.model = OpenAIModel(
                model_name=model_name,
                provider=OpenAIProvider(
                    base_url=base_url,
                    api_key=api_key,
                ),
            )
        else:
            # Use standard OpenAI model or other providers
            # Set API key in environment for Pydantic AI to use
            if model_name.startswith(("openai:", "gpt")):
                os.environ["OPENAI_API_KEY"] = api_key
                self.model = model_name
            elif model_name.startswith(("anthropic:", "claude")):
                os.environ["ANTHROPIC_API_KEY"] = api_key
                self.model = model_name
            elif model_name.startswith(("gemini:", "google:")):
                os.environ["GEMINI_API_KEY"] = api_key
                self.model = model_name
            else:
                # Default to OpenAI
                os.environ["OPENAI_API_KEY"] = api_key
                self.model = f"openai:{model_name}"
    
    def _create_agent(
        self,
        output_type: type[BaseModel],
        system_prompt: str,
    ) -> Agent[None, BaseModel]:
        """
        Create a Pydantic AI agent.
        
        Args:
            output_type: Pydantic model for structured output
            system_prompt: System prompt for the LLM
            
        Returns:
            Configured Pydantic AI Agent
        """
        return Agent(
            model=self.model,
            output_type=output_type,
            system_prompt=system_prompt,
            model_settings=self.model_settings,
            retries=3,  # Increase retries for validation errors
        )
    
    async def parse_query(
        self,
        inputs: Union[InputType, List[InputType]],
        filter_model: type[BaseModel],
        system_prompt: str,
    ) -> Dict[str, Any]:
        """
        Parse inputs asynchronously with multimodal support.
        
        Supports text, images, audio, video, and documents as inputs.
        
        Args:
            inputs: Single input or list of inputs. Can be:
                - str: Text query
                - ImageUrl: Image from URL
                - AudioUrl: Audio from URL
                - VideoUrl: Video from URL
                - DocumentUrl: Document from URL
                - BinaryContent: Binary data (images, audio, video, documents)
                - List of any of the above
            filter_model: Pydantic model for filter structure
            system_prompt: System prompt with instructions
            
        Returns:
            Parsed filter dictionary
            
        Examples:
            # Text query
            result = await factory.parse_query(
                inputs="Find high priority items",
                filter_model=FilterModel,
                system_prompt="Parse the query"
            )
            
            # Image analysis with text
            result = await factory.parse_query(
                inputs=[
                    "What's in this image?",
                    ImageUrl(url="https://example.com/image.png")
                ],
                filter_model=FilterModel,
                system_prompt="Analyze and extract data"
            )
            
            # Local file with BinaryContent
            from pathlib import Path
            result = await factory.parse_query(
                inputs=[
                    "Extract data from this document",
                    BinaryContent(
                        data=Path("doc.pdf").read_bytes(),
                        media_type="application/pdf"
                    )
                ],
                filter_model=FilterModel,
                system_prompt="Extract structured data"
            )
        """
        agent = self._create_agent(filter_model, system_prompt)
        
        # Convert single input to list
        input_data = [inputs] if not isinstance(inputs, list) else inputs
        
        result = await agent.run(input_data)
        return result.output.model_dump(mode="json")

