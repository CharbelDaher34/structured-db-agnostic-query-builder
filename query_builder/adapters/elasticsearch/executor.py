"""
Elasticsearch query executor.

Executes Elasticsearch DSL queries and returns normalized results.
"""

from typing import Any, Dict, List

from elasticsearch import Elasticsearch


class ESQueryExecutor:
    """
    Executes Elasticsearch queries.
    
    Implements the IQueryExecutor interface for Elasticsearch.
    """
    
    def __init__(self, es_host: str, index_name: str):
        """
        Initialize Elasticsearch query executor.
        
        Args:
            es_host: Elasticsearch host URL
            index_name: Name of the index to query
        """
        self.es_host = es_host
        self.index_name = index_name
        self.es_client = Elasticsearch(hosts=[es_host])
    
    def execute(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute multiple Elasticsearch queries.
        
        Args:
            queries: List of Elasticsearch DSL query objects
            
        Returns:
            List of result dictionaries
        """
        if not queries:
            return []
        
        results = []
        for query in queries:
            result = self._execute_single(query)
            results.append(result)
        
        return results
    
    def _execute_single(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single query."""
        try:
            response = self.es_client.search(index=self.index_name, **query)
            
            result = {
                "total_hits": response["hits"]["total"]["value"],
                "documents": [hit["_source"] for hit in response["hits"]["hits"]],
                "success": True,
            }
            
            # Handle aggregations
            if "aggregations" in response:
                result["aggregations"] = response["aggregations"]
            
            return result
        
        except Exception as e:
            return {
                "total_hits": 0,
                "documents": [],
                "error": str(e),
                "success": False,
            }
    
    def execute_raw(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        """
        Execute a raw Elasticsearch query.
        
        Args:
            query: Raw Elasticsearch DSL query
            size: Number of results to return
            
        Returns:
            Result dictionary
        """
        try:
            if "size" not in query:
                query["size"] = size
            
            response = self.es_client.search(index=self.index_name, **query)
            
            result = {
                "total_hits": (
                    response["hits"]["total"]["value"]
                    if isinstance(response["hits"]["total"], dict)
                    else response["hits"]["total"]
                ),
                "documents": [hit["_source"] for hit in response["hits"]["hits"]],
                "query": query,
                "success": True,
            }
            
            if "aggregations" in response:
                result["aggregations"] = response["aggregations"]
            
            return result
        
        except Exception as e:
            return {
                "error": str(e),
                "query": query,
                "success": False,
                "total_hits": 0,
                "documents": [],
            }

