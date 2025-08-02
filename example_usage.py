import requests
import json
import os

# API base URL
API_BASE = "http://localhost:8510"

def test_health():
    """Test the health endpoint."""
    response = requests.get(f"{API_BASE}/health")
    print("Health Check:", response.json())

def test_query_endpoint():
    """Test the /query endpoint (requires environment variables)."""
    print("\n=== Testing /query endpoint ===")
    
    # This endpoint requires environment variables to be set
    query_data = {
        "user_input": "Show me all transactions over $100 from last month"
    }
    
    try:
        response = requests.post(f"{API_BASE}/query", json=query_data)
        if response.status_code == 200:
            result = response.json()
            print("✅ Query successful!")
            print(f"Natural Language Query: {result['natural_language_query']}")
            print(f"Extracted Filters: {json.dumps(result['extracted_filters'], indent=2)}")
            print(f"Elasticsearch Queries: {json.dumps(result['elasticsearch_queries'], indent=2)}")
        else:
            print(f"❌ Query failed: {response.status_code}")
            print(f"Error: {response.json()}")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_mapping_endpoint():
    """Test the /query-from-mapping endpoint."""
    print("\n=== Testing /query-from-mapping endpoint ===")
    
    # Example Elasticsearch mapping
    sample_mapping = {
        "user_id": {"type": "keyword"},
        "transaction_date": {"type": "date"},
        "amount": {"type": "double"},
        "currency": {"type": "keyword"},
        "card_type": {"type": "keyword"},
        "transaction": {
            "type": "object",
            "properties": {
                "type": {"type": "keyword"},
                "receiver": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "text"},
                        "category_type": {"type": "keyword"},
                        "location": {"type": "keyword"}
                    }
                }
            }
        }
    }
    
    mapping_data = {
        "user_input": "Show me all food transactions over $50 on my GOLD card",
        "elasticsearch_mapping": sample_mapping,
        "enum_fields": {
            "card_type": ["GOLDEN", "SILVER", "PLATINUM"],
            "currency": ["USD", "GBP", "EUR"],
            "transaction.type": ["DEBIT", "CREDIT"],
            "transaction.receiver.category_type": ["food", "travel", "entertainment"]
        },
        "fields_to_ignore": ["user_id"]
    }
    
    try:
        response = requests.post(f"{API_BASE}/query-from-mapping", json=mapping_data)
        if response.status_code == 200:
            result = response.json()
            print("✅ Mapping query successful!")
            print(f"Natural Language Query: {result['natural_language_query']}")
            print(f"Extracted Filters: {json.dumps(result['extracted_filters'], indent=2)}")
            print(f"Elasticsearch Queries: {json.dumps(result['elasticsearch_queries'], indent=2)}")
        else:
            print(f"❌ Mapping query failed: {response.status_code}")
            print(f"Error: {response.json()}")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_config_endpoint():
    """Test the /config endpoint."""
    print("\n=== Testing /config endpoint ===")
    
    try:
        response = requests.get(f"{API_BASE}/config")
        if response.status_code == 200:
            config = response.json()
            print("✅ Config retrieved:")
            print(json.dumps(config, indent=2))
        else:
            print(f"❌ Config failed: {response.status_code}")
            print(f"Error: {response.json()}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🚀 Testing Elasticsearch Query Generator API")
    print("=" * 50)
    
    # Test health first
    test_health()
    
    # Test config
    test_config_endpoint()
    
    # Test mapping endpoint (doesn't require DB connection)
    test_mapping_endpoint()
    
    # Test query endpoint (requires environment variables)
    print("\n" + "=" * 50)
    print("📝 To test the /query endpoint, set these environment variables:")
    print("export INDEX_NAME=your_index_name")
    print("export ES_HOST=your_elasticsearch_host")
    print("export API_KEY=your_llm_api_key")
    print("export CATEGORY_FIELDS=field1,field2,field3")
    print("export FIELDS_TO_IGNORE=field1,field2")
    print("export MODEL_NAME=ollama/qwen3:8b")
    print("=" * 50)
    
    test_query_endpoint() 