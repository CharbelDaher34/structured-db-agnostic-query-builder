"""
Quick test script to debug Ollama connection issues.

Run this to verify your Ollama setup is working correctly.
"""

import asyncio
import os
from dotenv import load_dotenv
from query_builder.llm.client_factory import LLMClientFactory

load_dotenv()

async def test_ollama():
    """Test Ollama connection with current environment variables."""
    
    model_name = "qwen3:8b"
    base_url = "http://localhost:11434"
    api_key = "dummy-key"
    
    print("=" * 60)
    print("Ollama Connection Test")
    print("=" * 60)
    print(f"Model: {model_name}")
    print(f"Base URL: {base_url}")
    print(f"API Key: {api_key[:10]}..." if len(api_key) > 10 else f"API Key: {api_key}")
    print()
    
    try:
        # Create factory
        factory = LLMClientFactory(
            model_name=model_name,
            api_key=api_key,
            base_url=base_url,
        )
        
        print("✅ Factory created successfully")
        print(f"   Normalized Base URL: {factory.base_url}")
        print()
        
        # Test simple query
        print("Testing simple query...")
        result = await factory.parse_query(
            inputs="Say hello in one word",
            system_prompt="You are a helpful assistant. Respond briefly."
        )
        
        print("✅ Query successful!")
        print(f"Response: {result}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print()
        print("Troubleshooting:")
        print("1. Make sure Ollama is running: ollama serve")
        print("2. Verify model exists: ollama ls")
        print("3. Test Ollama API directly:")
        print(f"   curl http://localhost:11434/api/tags")
        print()
        print("4. Check your environment variables:")
        print(f"   LLM_MODEL={model_name}")
        print(f"   LLM_BASE_URL={base_url}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_ollama())

