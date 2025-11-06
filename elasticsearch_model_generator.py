"""
Example usage of the query builder with Elasticsearch.

3 comprehensive examples testing all functionalities with actual transaction data.
"""

import os
import json
import asyncio
from dotenv import load_dotenv
from query_builder import QueryOrchestrator

load_dotenv()

DEFAULT_ES_HOST = os.getenv("elastic_host", "http://localhost:9200")
DEFAULT_INDEX_NAME = os.getenv("ELASTIC_INDEX", "user_transactions")
DEFAULT_CATEGORY_FIELDS = [
    "card_kind",
    "card_type",
    "transaction.receiver.category_type",
    "transaction.receiver.location",
    "transaction.type",
    "transaction.currency",
]
DEFAULT_FIELDS_TO_IGNORE = ["user_id", "card_number"]


def setup_orchestrator():
    """Setup Elasticsearch orchestrator with transaction index."""
    return QueryOrchestrator.from_elasticsearch(
        es_host=DEFAULT_ES_HOST,
        index_name=DEFAULT_INDEX_NAME,
        category_fields=DEFAULT_CATEGORY_FIELDS,
        fields_to_ignore=DEFAULT_FIELDS_TO_IGNORE,
        llm_model="gpt-4o",
        llm_api_key=os.getenv("OPENAI_API_KEY"),
    )


async def example_1_filtering_sorting_limiting(orchestrator):
    """
    Example 1: Filtering, Sorting, and Limiting
    
    Tests:
    - Filter by card_type (is)
    - Filter by currency (is)
    - Filter by amount range (between)
    - Filter by receiver location (contains)
    - Sort by amount (descending)
    - Limit results (10)
    """
    
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Filtering, Sorting, and Limiting")
    print("=" * 80)
    
    query = "Show me the top 10 most expensive transactions with GOLD card and USD currency, where amount is between 1000 and 10000, and location contains 'New York'"
    
    print(f"\nNatural Language Query: {query}\n")
    
    result = await orchestrator.query(natural_language_query=query, execute=True)
    
    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2))
    
    print("\n--- Generated Elasticsearch Query ---")
    print(json.dumps(result["database_queries"][0], indent=2))
    
    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Documents Found: {res['total_hits']}")
        print(f"Documents Returned: {len(res['documents'])}")
        
        if res['documents']:
            print("\nTop Transactions:")
            for i, doc in enumerate(res['documents'][:10], 1):
                print(f"\n  {i}. {doc.get('transaction', {}).get('receiver', {}).get('name', 'N/A')}")
                print(f"     Card Type: {doc.get('card_type', 'N/A')}")
                print(f"     Amount: {doc.get('transaction', {}).get('amount', 0):,} {doc.get('transaction', {}).get('currency', 'N/A')}")
                print(f"     Location: {doc.get('transaction', {}).get('receiver', {}).get('location', 'N/A')}")
                print(f"     Timestamp: {doc.get('transaction', {}).get('timestamp', 'N/A')}")


async def example_2_aggregations_grouping_having(orchestrator):
    """
    Example 2: Aggregations with Grouping and Having Clause
    
    Tests:
    - Group by card_type and transaction.type
    - Sum aggregation (total amount)
    - Average aggregation (average amount)
    - Count aggregation (number of transactions)
    - Having clause (filter groups with count > 5)
    - Sort by total amount (descending)
    """
    
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Aggregations with Grouping and Having Clause")
    print("=" * 80)
    
    query = "What is the total transaction amount, average amount, and count grouped by card type and transaction type, for groups with more than 5 transactions, sorted by total amount descending"
    
    print(f"\nNatural Language Query: {query}\n")
    
    result = await orchestrator.query(natural_language_query=query, execute=True)
    
    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2))
    
    print("\n--- Generated Elasticsearch Query ---")
    print(json.dumps(result["database_queries"][0], indent=2))
    
    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Groups Found: {res['total_hits']}")
        print(f"Groups Returned: {len(res['documents'])}")
        
        if res['documents']:
            print("\nAggregation Results (Groups with >5 transactions):")
            for i, doc in enumerate(res['documents'][:15], 1):
                # Elasticsearch aggregation results structure
                card_type = doc.get('key', {}).get('card_type', 'N/A') if isinstance(doc.get('key'), dict) else doc.get('card_type', 'N/A')
                txn_type = doc.get('key', {}).get('transaction.type', 'N/A') if isinstance(doc.get('key'), dict) else doc.get('transaction.type', 'N/A')
                
                total = doc.get('total_amount', {}).get('value', 0) if isinstance(doc.get('total_amount'), dict) else doc.get('total_amount', 0)
                avg = doc.get('avg_amount', {}).get('value', 0) if isinstance(doc.get('avg_amount'), dict) else doc.get('avg_amount', 0)
                count = doc.get('doc_count', 0)
                
                print(f"\n  {i}. Card: {card_type}, Type: {txn_type}")
                print(f"     Total Amount: {total:,.0f}")
                print(f"     Average Amount: {avg:,.2f}")
                print(f"     Transaction Count: {count}")


async def example_3_date_range_monthly_aggregations(orchestrator):
    """
    Example 3: Date Range Filtering with Monthly Grouping and Aggregations
    
    Tests:
    - Date range filtering (last year)
    - Filter by card_kind (is)
    - Filter by currency (is)
    - Group by timestamp (monthly interval)
    - Multiple aggregations (sum, avg, count)
    - Sort by grouped date
    """
    
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Date Range with Monthly Grouping and Aggregations")
    print("=" * 80)
    
    query = "Show me the total transaction amount, average amount, and count grouped by month for CREDIT card transactions with USD currency from last year, sorted by month"
    
    print(f"\nNatural Language Query: {query}\n")
    
    result = await orchestrator.query(natural_language_query=query, execute=True)
    
    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2))
    
    print("\n--- Generated Elasticsearch Query ---")
    print(json.dumps(result["database_queries"][0], indent=2))
    
    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Monthly Groups: {res['total_hits']}")
        print(f"Groups Returned: {len(res['documents'])}")
        
        if res['documents']:
            print("\nMonthly Aggregation Results (CREDIT, USD, Last Year):")
            for i, doc in enumerate(res['documents'], 1):
                # Elasticsearch date histogram aggregation structure
                month = doc.get('key_as_string', doc.get('key', 'N/A'))
                
                total = doc.get('total_amount', {}).get('value', 0) if isinstance(doc.get('total_amount'), dict) else doc.get('total_amount', 0)
                avg = doc.get('avg_amount', {}).get('value', 0) if isinstance(doc.get('avg_amount'), dict) else doc.get('avg_amount', 0)
                count = doc.get('doc_count', 0)
                
                print(f"\n  {i}. Month: {month}")
                print(f"     Total Amount: {total:,.0f} USD")
                print(f"     Average Amount: {avg:,.2f} USD")
                print(f"     Transaction Count: {count}")


async def main():
    """Main async function to run all examples."""
    print("\n" + "=" * 80)
    print("ELASTICSEARCH QUERY BUILDER - 3 COMPREHENSIVE EXAMPLES")
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
        print("\nMake sure Elasticsearch is running and ELASTIC_HOST is configured in .env")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("ALL EXAMPLES COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
