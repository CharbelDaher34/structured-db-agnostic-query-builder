"""
MongoDB query executor.

Executes MongoDB aggregation pipelines and returns normalized results.
"""

from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


class MongoQueryExecutor:
    """
    Executes MongoDB queries.
    
    Implements the IQueryExecutor interface for MongoDB.
    """
    
    def __init__(self, mongo_uri: str, database_name: str, collection_name: str):
        """
        Initialize MongoDB query executor.
        
        Args:
            mongo_uri: MongoDB connection URI
            database_name: Name of the database
            collection_name: Name of the collection
        """
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name
        
        self.client: MongoClient = MongoClient(mongo_uri)
        self.db: Database = self.client[database_name]
        self.collection: Collection = self.db[collection_name]
    
    def execute(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute multiple MongoDB aggregation pipelines.
        
        Args:
            queries: List of MongoDB query objects (each with "pipeline" key)
            
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
        """Execute a single MongoDB aggregation pipeline."""
        try:
            pipeline = query.get("pipeline", [])
            
            if not pipeline:
                # No pipeline - return all documents
                documents = list(self.collection.find().limit(100))
            else:
                # Execute aggregation pipeline
                documents = list(self.collection.aggregate(pipeline))
            
            # Convert ObjectId to string for JSON serialization
            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
            
            result = {
                "total_hits": len(documents),
                "documents": documents,
                "success": True,
            }
            
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
        Execute a raw MongoDB query or aggregation.
        
        Args:
            query: Raw MongoDB query or aggregation pipeline
                   Format: {"pipeline": [...]} for aggregation
                   or {"filter": {...}} for find()
            size: Number of results to return (for find() queries)
            
        Returns:
            Result dictionary
        """
        try:
            if "pipeline" in query:
                # Aggregation pipeline
                documents = list(self.collection.aggregate(query["pipeline"]))
            elif "filter" in query:
                # Simple find query
                documents = list(self.collection.find(query["filter"]).limit(size))
            else:
                # Default: return all documents
                documents = list(self.collection.find().limit(size))
            
            # Convert ObjectId to string
            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
            
            result = {
                "total_hits": len(documents),
                "documents": documents,
                "query": query,
                "success": True,
            }
            
            return result
        
        except Exception as e:
            return {
                "error": str(e),
                "query": query,
                "success": False,
                "total_hits": 0,
                "documents": [],
            }

