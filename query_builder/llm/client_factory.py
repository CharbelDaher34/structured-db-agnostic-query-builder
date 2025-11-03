"""
LLM client factory and management.

Handles creation and lifecycle of LLM clients for query parsing.
"""

from typing import Any, Dict, Optional
from pydantic import BaseModel

from llm.llm_agent import LLM


class LLMClientFactory:
    """
    Creates and manages LLM clients for query processing.
    
    Handles client initialization, reuse, and query parsing with
    structured output.
    """
    
    def __init__(self, model_name: str, api_key: str):
        """
        Initialize LLM client factory.
        
        Args:
            model_name: Name of the LLM model to use (e.g., "gpt-4o")
            api_key: API key for the LLM provider
            
        Raises:
            ValueError: If model_name or api_key is missing
        """
        if not model_name:
            raise ValueError("model_name is required")
        if not api_key:
            raise ValueError("api_key is required")
        
        self.model_name = model_name
        self.api_key = api_key
        self._client: Optional[LLM] = None
    
    def get_client(
        self, output_type: type[BaseModel], system_prompt: str
    ) -> LLM:
        """
        Get or create LLM client.
        
        Args:
            output_type: Pydantic model for structured output
            system_prompt: System prompt for the LLM
            
        Returns:
            Configured LLM client
        """
        if self._client is None:
            self._client = LLM(
                model=self.model_name,
                output_type=output_type,
                system_prompt=system_prompt,
                api_key=self.api_key,
            )
        return self._client
    
    def parse_query(
        self,
        query: str,
        filter_model: type[BaseModel],
        system_prompt: str,
    ) -> Dict[str, Any]:
        """
        Parse natural language query synchronously.
        
        Args:
            query: Natural language query string
            filter_model: Pydantic model for filter structure
            system_prompt: System prompt with instructions
            
        Returns:
            Parsed filter dictionary
        """
        client = self.get_client(filter_model, system_prompt)
        return client.llm_agent.run_sync([query])
    
    async def parse_query_async(
        self,
        query: str,
        filter_model: type[BaseModel],
        system_prompt: str,
    ) -> Dict[str, Any]:
        """
        Parse natural language query asynchronously.
        
        Args:
            query: Natural language query string
            filter_model: Pydantic model for filter structure
            system_prompt: System prompt with instructions
            
        Returns:
            Parsed filter dictionary
        """
        client = self.get_client(filter_model, system_prompt)
        return await client.llm_agent.run([query])

