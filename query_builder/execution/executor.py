"""
Query execution coordinator.

Handles execution of queries through database-specific executors.
"""

from typing import Any, Dict, List

from query_builder.core.interfaces import IQueryExecutor


class QueryExecutor:
    """
    Coordinates query execution.
    
    Wraps a database-specific query executor and provides common
    execution logic like error handling and retries.
    """
    
    def __init__(self, executor: IQueryExecutor):
        """
        Initialize query executor.
        
        Args:
            executor: Database-specific query executor implementation
        """
        self.executor = executor
    
    def execute(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute multiple queries.
        
        Args:
            queries: List of database-specific query objects
            
        Returns:
            List of result dictionaries
        """
        if not queries:
            return []
        
        try:
            return self.executor.execute(queries)
        except Exception as e:
            # Return error result for all queries
            return [
                {
                    "total_hits": 0,
                    "documents": [],
                    "error": str(e),
                    "success": False,
                }
                for _ in queries
            ]
    
    def execute_raw(self, query: Dict[str, Any], size: int = 100) -> Dict[str, Any]:
        """
        Execute a raw database query.
        
        Args:
            query: Raw database query object
            size: Number of results to return
            
        Returns:
            Result dictionary
        """
        try:
            return self.executor.execute_raw(query, size)
        except Exception as e:
            return {
                "total_hits": 0,
                "documents": [],
                "error": str(e),
                "success": False,
                "query": query,
            }

