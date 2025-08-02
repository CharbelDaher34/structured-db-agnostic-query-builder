import asyncio
import json
from typing import Dict, Any, Optional
import httpx
from datetime import datetime
from utils import get_es_schema_for_api

# API Configuration
API_BASE_URL = "http://localhost:8510"
TEST_QUERY = "give me a list of all my transactions per month over the last 2 years having an average amount greater than 100"

ES_HOST = "http://elastic:rvs59tB_VVANUy4rC-kd@84.16.230.94:9200"
INDEX_NAME = "user_transactions"

# Define the fields you want to treat as categorical (enums).
# The script will fetch all distinct values for these fields.
CATEGORY_FIELDS = [
    "card_kind",
    "card_type", 
    "transaction.receiver.category_type",
    "transaction.receiver.location",
    "transaction.type",
    "transaction.currency"
]
SCHEMA_DATA = get_es_schema_for_api(
    es_host=ES_HOST,
    index_name="user_transactions",
    category_fields=CATEGORY_FIELDS
)
REAL_MAPPING = SCHEMA_DATA["elasticsearch_mapping"]
REAL_ENUM_FIELDS = SCHEMA_DATA["enum_fields"]
async def test_database_endpoint():
    """Test the /query endpoint that fetches mapping from database."""
    print("=" * 60)
    print("🔍 TESTING DATABASE ENDPOINT (/query)")
    print("=" * 60)
    
    payload = {
        "user_input": TEST_QUERY
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(f"{API_BASE_URL}/query", json=payload)
            
            if response.status_code == 200:
                result = response.json()
                print("✅ SUCCESS - Database endpoint responded")
                print(f"📝 Query: {result['natural_language_query']}")
                print(f"🔧 Extracted Filters:")
                print(json.dumps(result['extracted_filters'], indent=2))
                print(f"📊 Elasticsearch Queries ({len(result['elasticsearch_queries'])} queries):")
                for i, query in enumerate(result['elasticsearch_queries'], 1):
                    print(f"  Query {i}:")
                    print(json.dumps(query, indent=4))
                return result
            else:
                print(f"❌ ERROR - Status: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)}")
        return None

async def test_mapping_endpoint():
    """Test the /query-from-mapping endpoint that uses provided mapping."""
    print("\n" + "=" * 60)
    print("🗺️  TESTING MAPPING ENDPOINT (/query-from-mapping)")
    print("=" * 60)
    
    payload = {
        "user_input": TEST_QUERY,
        "elasticsearch_mapping": REAL_MAPPING,
        "enum_fields": REAL_ENUM_FIELDS,
        "fields_to_ignore": ["user_id", "card_number", "user_name"]  # These match the real API config
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(f"{API_BASE_URL}/query-from-mapping", json=payload)
            
            if response.status_code == 200:
                result = response.json()
                print("✅ SUCCESS - Mapping endpoint responded")
                print(f"📝 Query: {result['natural_language_query']}")
                print(f"🔧 Extracted Filters:")
                print(json.dumps(result['extracted_filters'], indent=2))
                print(f"📊 Elasticsearch Queries ({len(result['elasticsearch_queries'])} queries):")
                for i, query in enumerate(result['elasticsearch_queries'], 1):
                    print(f"  Query {i}:")
                    print(json.dumps(query, indent=4))
                return result
            else:
                print(f"❌ ERROR - Status: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)}")
        return None

def compare_results(db_result: Optional[Dict[str, Any]], mapping_result: Optional[Dict[str, Any]]):
    """Compare results from both endpoints."""
    print("\n" + "=" * 60)
    print("🔍 COMPARISON ANALYSIS")
    print("=" * 60)
    
    if not db_result or not mapping_result:
        print("❌ Cannot compare - one or both endpoints failed")
        return
    
    print(f"📝 Same Query: {db_result['natural_language_query'] == mapping_result['natural_language_query']}")
    
    # Compare extracted filters
    db_filters = db_result.get('extracted_filters', {})
    mapping_filters = mapping_result.get('extracted_filters', {})
    
    print(f"🔧 Filters Structure Match: {type(db_filters) == type(mapping_filters)}")
    
    if isinstance(db_filters, dict) and isinstance(mapping_filters, dict):
        print(f"🔧 Filter Keys Match: {set(db_filters.keys()) == set(mapping_filters.keys())}")
    
    # Compare elasticsearch queries
    db_queries = db_result.get('elasticsearch_queries', [])
    mapping_queries = mapping_result.get('elasticsearch_queries', [])
    
    print(f"📊 Query Count: DB={len(db_queries)}, Mapping={len(mapping_queries)}")
    
    if len(db_queries) == len(mapping_queries):
        print("📊 Query structures:")
        for i, (db_q, map_q) in enumerate(zip(db_queries, mapping_queries), 1):
            print(f"  Query {i} - Same structure: {db_q.keys() == map_q.keys()}")

async def test_health_and_config():
    """Test health and config endpoints."""
    print("\n" + "=" * 60)
    print("🏥 TESTING HEALTH & CONFIG ENDPOINTS")
    print("=" * 60)
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Test health
            health_response = await client.get(f"{API_BASE_URL}/health")
            if health_response.status_code == 200:
                print("✅ Health check passed")
                print(f"   {health_response.json()}")
            else:
                print(f"❌ Health check failed: {health_response.status_code}")
            
            # Test config
            config_response = await client.get(f"{API_BASE_URL}/config")
            if config_response.status_code == 200:
                print("✅ Config endpoint working")
                config = config_response.json()
                print(f"   Index: {config.get('index_name')}")
                print(f"   Model: {config.get('model_name')}")
                print(f"   API Key Configured: {config.get('api_key_configured')}")
                print(f"   Category Fields: {len(config.get('category_fields', []))}")
            else:
                print(f"❌ Config endpoint failed: {config_response.status_code}")
                
    except Exception as e:
        print(f"❌ Health/Config test failed: {str(e)}")

async def run_comprehensive_test():
    """Run comprehensive test of both endpoints."""
    print("🚀 STARTING COMPREHENSIVE API TEST")
    print(f"⏰ Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Test Query: '{TEST_QUERY}'")
    
    # Test health and config first
    await test_health_and_config()
    
    # Test both main endpoints
    db_result = await test_database_endpoint()
    mapping_result = await test_mapping_endpoint()
    
    # Compare results
    compare_results(db_result, mapping_result)
    
    print("\n" + "=" * 60)
    print("✅ TEST COMPLETED")
    print("=" * 60)

# Additional test queries for more comprehensive testing
ADDITIONAL_TEST_QUERIES = [
    "What are my top 5 most expensive transactions?",
    "Show me all deposits in USD from last year",
    "Compare my spending on food vs travel",
    "Group my transactions by month and show totals",
    "Find all transactions in London over €50"
]

async def run_multiple_query_test():
    """Test multiple queries on both endpoints."""
    print("\n🔄 TESTING MULTIPLE QUERIES")
    print("=" * 60)
    
    for i, query in enumerate(ADDITIONAL_TEST_QUERIES, 1):
        print(f"\n📝 Test Query {i}: '{query}'")
        print("-" * 40)
        
        # Test database endpoint
        db_payload = {"user_input": query}
        mapping_payload = {
            "user_input": query,
            "elasticsearch_mapping": REAL_MAPPING,
            "enum_fields": REAL_ENUM_FIELDS,
            "fields_to_ignore": ["user_id", "card_number"]
        }
        
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                # Test both endpoints
                db_response = await client.post(f"{API_BASE_URL}/query", json=db_payload)
                mapping_response = await client.post(f"{API_BASE_URL}/query-from-mapping", json=mapping_payload)
                
                db_success = db_response.status_code == 200
                mapping_success = mapping_response.status_code == 200
                
                print(f"  DB Endpoint: {'✅' if db_success else '❌'}")
                print(f"  Mapping Endpoint: {'✅' if mapping_success else '❌'}")
                
                if db_success and mapping_success:
                    db_result = db_response.json()
                    mapping_result = mapping_response.json()
                    
                    db_queries = len(db_result.get('elasticsearch_queries', []))
                    mapping_queries = len(mapping_result.get('elasticsearch_queries', []))
                    
                    print(f"  Generated Queries: DB={db_queries}, Mapping={mapping_queries}")
                
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")

if __name__ == "__main__":
    print("🧪 ELASTICSEARCH QUERY GENERATOR API TESTER")
    print("=" * 60)
    
    # Run main test
    asyncio.run(run_comprehensive_test())
    
    # # Optionally run multiple query test
    # print("\n" + "🔄" * 20)
    # user_input = input("Run additional multi-query test? (y/n): ").lower().strip()
    # if user_input == 'y':
    #     asyncio.run(run_multiple_query_test())
    
    print("\n🎉 All tests completed!") 