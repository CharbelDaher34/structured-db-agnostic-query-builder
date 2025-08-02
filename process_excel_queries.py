import pandas as pd
import requests
import json
import time
from typing import Dict, Any, Optional

def call_query_api(query: str, api_base: str = "http://localhost:8510") -> Dict[str, Any]:
    """
    Call the /query API endpoint with a natural language query.
    
    Args:
        query: Natural language query string
        api_base: Base URL for the API
        
    Returns:
        Dictionary containing API response or error information
    """
    try:
        response = requests.post(
            f"{api_base}/query",
            json={"user_input": query},
            timeout=400
        )
        
        if response.status_code == 200:
            return {
                "status": "success",
                "response": response.json()
            }
        else:
            return {
                "status": "error",
                "error_code": response.status_code,
                "error_message": response.text
            }
    except requests.exceptions.RequestException as e:
        return {
            "status": "error",
            "error_code": "request_failed",
            "error_message": str(e)
        }

def process_excel_queries(
    excel_file: str = "user_queries_ollama.xlsx",
    api_base: str = "http://localhost:8510",
    delay_seconds: float = 1.0
) -> None:
    """
    Process queries from Excel file, call API, and save results.
    
    Args:
        excel_file: Path to Excel file containing queries
        api_base: Base URL for the API
        delay_seconds: Delay between API calls to avoid rate limiting
    """
    print(f"📖 Reading queries from {excel_file}")
    
    # Read Excel file
    try:
        df = pd.read_excel(excel_file)
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return
    
    print(f"📊 Found {len(df)} queries to process")
    
    # Initialize new columns if they don't exist
    if 'API_Status' not in df.columns:
        df['API_Status'] = ''
    if 'Natural_Language_Query' not in df.columns:
        df['Natural_Language_Query'] = ''
    if 'Extracted_Filters' not in df.columns:
        df['Extracted_Filters'] = ''
    if 'Elasticsearch_Queries' not in df.columns:
        df['Elasticsearch_Queries'] = ''
    if 'Error_Message' not in df.columns:
        df['Error_Message'] = ''
    if 'Processing_Time' not in df.columns:
        df['Processing_Time'] = ''
    
    # Process each query
    for idx, row in df.iterrows():
        query = row['Query']
        
        # Skip if already processed successfully
        if row.get('API_Status') == 'success':
            print(f"⏭️  Skipping row {idx + 1}: Already processed")
            continue
        
        print(f"🔄 Processing row {idx + 1}/{len(df)}: {query[:60]}...")
        
        start_time = time.time()
        result = call_query_api(query, api_base)            
        
        processing_time = time.time() - start_time
        time.sleep(2)
        
        # Update DataFrame with results
        df.at[idx, 'API_Status'] = result['status']
        df.at[idx, 'Processing_Time'] = f"{processing_time:.2f}s"
        
        if result['status'] == 'success':
            response_data = result['response']
            df.at[idx, 'Natural_Language_Query'] = response_data.get('natural_language_query', '')
            df.at[idx, 'Extracted_Filters'] = json.dumps(response_data.get('extracted_filters', {}), indent=2)
            df.at[idx, 'Elasticsearch_Queries'] = json.dumps(response_data.get('elasticsearch_queries', {}), indent=2)
            df.at[idx, 'Error_Message'] = ''
            print(f"✅ Success: {processing_time:.2f}s")
        else:
            df.at[idx, 'Natural_Language_Query'] = ''
            df.at[idx, 'Extracted_Filters'] = ''
            df.at[idx, 'Elasticsearch_Queries'] = ''
            df.at[idx, 'Error_Message'] = f"Code: {result.get('error_code', 'unknown')}, Message: {result.get('error_message', 'unknown error')}"
            print(f"❌ Error: {result.get('error_message', 'unknown error')}")
        
        # Save progress after each query
        try:
            df.to_excel(excel_file, index=False)
            print(f"💾 Progress saved to {excel_file}")
        except Exception as e:
            print(f"⚠️  Warning: Could not save progress: {e}")
        
        # Add delay between requests
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    
    print(f"\n🎉 Processing complete! Results saved to {excel_file}")
    
    # Print summary
    success_count = len(df[df['API_Status'] == 'success'])
    error_count = len(df[df['API_Status'] == 'error'])
    
    print(f"\n📈 Summary:")
    print(f"   ✅ Successful queries: {success_count}")
    print(f"   ❌ Failed queries: {error_count}")
    print(f"   📊 Total queries: {len(df)}")
    
    if error_count > 0:
        print(f"\n❌ Errors encountered:")
        error_df = df[df['API_Status'] == 'error'][['Query', 'Error_Message']]
        for idx, row in error_df.iterrows():
            print(f"   Row {idx + 1}: {row['Query'][:50]}... -> {row['Error_Message']}")

def main():
    """Main function to run the Excel query processor."""
    print("🚀 Starting Excel Query Processor")
    print("=" * 50)
    
    # Check if API is running
    try:
        response = requests.get("http://localhost:8510/health", timeout=5)
        if response.status_code == 200:
            print("✅ API is running and healthy")
        else:
            print("⚠️  API is running but may have issues")
    except requests.exceptions.RequestException:
        print("❌ API is not running or not accessible at http://localhost:8510")
        print("   Please start the API server first using: python api.py")
        return
    
    # Process the queries
    process_excel_queries(
        excel_file="user_queries_gemini.xlsx",
        api_base="http://localhost:8510",
        delay_seconds=1.0  # 1 second delay between requests
    )

if __name__ == "__main__":
    main() 