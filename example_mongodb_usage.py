"""
Example usage of the query builder with MongoDB.

3 comprehensive examples testing all functionalities with actual transaction data.
"""

import os
import json
import asyncio
from dotenv import load_dotenv
from query_builder import QueryOrchestrator

load_dotenv()

DEFAULT_MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:vAF4jUA8Iq4amZaNPnQq87X9@84.16.230.94:27017/?authSource=admin")
DEFAULT_DATABASE = os.getenv("MONGO_DATABASE", "visa_adcb")
DEFAULT_COLLECTION = os.getenv("MONGO_COLLECTION", "llm_transactions")
DEFAULT_CATEGORY_FIELDS = [
    "transaction_location",
    "transaction_currency",
    "merchant_name",
    "merchant_country",
    "merchant_category_name",
    "payment_card_name"
]
DEFAULT_FIELDS_TO_IGNORE = ["converted_currency","merchant_category_description"]

def setup_orchestrator():
    """Setup MongoDB orchestrator with transaction collection."""
    return QueryOrchestrator.from_mongodb(
        mongo_uri=DEFAULT_MONGO_URI,
        database_name=DEFAULT_DATABASE,
        collection_name=DEFAULT_COLLECTION,
        category_fields=DEFAULT_CATEGORY_FIELDS,
        fields_to_ignore=DEFAULT_FIELDS_TO_IGNORE,
        llm_model="gpt-4.1",
        llm_api_key=os.getenv("OPENAI_API_KEY"),
        sample_size=1000,
    )


async def example_1_filtering_sorting_limiting(orchestrator):
    """
    Example 1: Filtering, Sorting, and Limiting
    
    Tests:
    - Filter by merchantCountry (is)
    - Filter by currency (is)
    - Filter by amount range (between)
    - Filter by merchantName (contains)
    - Sort by amount (descending)
    - Limit results (10)
    """
    
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Filtering, Sorting, and Limiting")
    print("=" * 80)
    
    query = "Show me the top 10 most expensive transactions in France with USD currency, where amount is between 5000 and 50000, and merchant name contains 'Five'"
    
    print(f"\nNatural Language Query: {query}\n")
    
    result = await orchestrator.query(natural_language_query=query, execute=True)
    
    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2))
    
    print("\n--- Generated MongoDB Pipeline ---")
    print(json.dumps(result["database_queries"][0], indent=2))
    
    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Documents Found: {res['total_hits']}")
        print(f"Documents Returned: {len(res['documents'])}")
        
        if res['documents']:
            print("\nTop Transactions:")
            for i, doc in enumerate(res['documents'][:10], 1):
                print(f"\n  {i}. {doc.get('merchantName', 'N/A')}")
                print(f"     Country: {doc.get('merchantCountry', 'N/A')}")
                print(f"     Amount: {doc.get('amount', 0):,} {doc.get('currency', 'N/A')}")
                print(f"     Amount After Discount: {doc.get('amountAfterDiscount', 0):,.2f}")
                print(f"     Channel: {doc.get('channel', 'N/A')}")
                print(f"     Location: {doc.get('transactionLocation', 'N/A')}")
                print(f"     Timestamp: {doc.get('timestamp', 'N/A')}")


async def example_2_aggregations_grouping_having(orchestrator):
    """
    Example 2: Aggregations with Grouping and Having Clause
    
    Tests:
    - Group by merchantCountry and channel
    - Sum aggregation (total amount)
    - Average aggregation (average amount)
    - Count aggregation (number of transactions)
    - Having clause (filter groups with count > 5)
    - Sort by total amount (descending)
    """
    
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Aggregations with Grouping and Having Clause")
    print("=" * 80)
    
    query = "What is the total transaction amount, average amount, and count grouped by merchant country and channel, for groups with more than 5 transactions, sorted by total amount descending"
    
    print(f"\nNatural Language Query: {query}\n")
    
    result = await orchestrator.query(natural_language_query=query, execute=True)
    
    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2))
    
    print("\n--- Generated MongoDB Pipeline ---")
    print(json.dumps(result["database_queries"][0], indent=2))
    
    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Groups Found: {res['total_hits']}")
        print(f"Groups Returned: {len(res['documents'])}")
        
        if res['documents']:
            print("\nAggregation Results (Groups with >5 transactions):")
            for i, doc in enumerate(res['documents'][:15], 1):
                group_id = doc.get('_id', {})
                
                # Handle both dict (multiple group fields) and string (single field)
                if isinstance(group_id, dict):
                    country = group_id.get('merchantCountry', 'N/A')
                    channel = group_id.get('channel', 'N/A')
                else:
                    # Single field grouping
                    country = group_id
                    channel = 'N/A'
                
                # Extract aggregation values
                total = doc.get('sum_amount', 0)
                avg = doc.get('avg_amount', 0)
                count = doc.get('count_amount', 0)
                
                print(f"\n  {i}. Country: {country}, Channel: {channel}")
                print(f"     Total Amount: {total:,.0f}")
                print(f"     Average Amount: {avg:,.2f}")
                print(f"     Transaction Count: {count}")


async def example_3_date_range_monthly_aggregations(orchestrator):
    """
    Example 3: Date Range Filtering with Monthly Grouping and Aggregations
    
    Tests:
    - Date range filtering (last 2 years)
    - Filter by merchantCountry (is)
    - Filter by currency (is)
    - Group by timestamp (monthly interval)
    - Multiple aggregations (sum, avg, count)
    - Sort by grouped date
    """
    
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Date Range with Monthly Grouping and Aggregations")
    print("=" * 80)
    
    query = "Show me the total transaction amount, average amount, and count grouped by month for transactions in France with USD currency from the last 2 years, sorted by month"
    
    print(f"\nNatural Language Query: {query}\n")
    
    result = await orchestrator.query(natural_language_query=query, execute=True)
    
    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2))
    
    print("\n--- Generated MongoDB Pipeline ---")
    print(json.dumps(result["database_queries"][0], indent=2))
    
    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Monthly Groups: {res['total_hits']}")
        print(f"Groups Returned: {len(res['documents'])}")
        
        if res['documents']:
            print("\nMonthly Aggregation Results (France, USD, Last 2 Years):")
            for i, doc in enumerate(res['documents'], 1):
                group_id = doc.get('_id', {})
                
                # Handle both dict (multiple group fields) and string (single field)
                if isinstance(group_id, dict):
                    month = group_id.get('timestamp', 'N/A')
                else:
                    # Single field grouping - _id is the value directly
                    month = group_id
                
                total = doc.get('sum_amount', 0)
                avg = doc.get('avg_amount', 0)
                count = doc.get('count_amount', 0)
                
                print(f"\n  {i}. Month: {month}")
                print(f"     Total Amount: {total:,.0f} USD")
                print(f"     Average Amount: {avg:,.2f} USD")
                print(f"     Transaction Count: {count}")


async def main():
    """Main async function to run all examples."""
    print("\n" + "=" * 80)
    print("MONGODB QUERY BUILDER - 3 COMPREHENSIVE EXAMPLES")
    print("=" * 80)
    
    try:
        orchestrator = setup_orchestrator()
        
        print("\n--- Schema Summary ---")
        orchestrator.print_model_summary()
        
        # Run all 3 examples
        await example_1_filtering_sorting_limiting(orchestrator)
        await example_2_aggregations_grouping_having(orchestrator)
        await example_3_date_range_monthly_aggregations(orchestrator)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure MongoDB is running and MONGO_URI is configured in .env")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("ALL EXAMPLES COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
