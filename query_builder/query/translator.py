"""
Query translation coordinator.

Delegates translation to database-specific translators.
"""

from typing import Any, Dict, List

from query_builder.core.interfaces import IQueryTranslator


class QueryTranslator:
    """
    Coordinates query translation from filters to database queries.
    
    This class wraps a database-specific query translator and provides
    common pre/post-processing logic.
    """
    
    def __init__(self, translator: IQueryTranslator):
        """
        Initialize query translator.
        
        Args:
            translator: Database-specific query translator implementation
        """
        self.translator = translator
    
    def translate(
        self, filters: Dict[str, Any], model_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Translate filters to database-specific queries.
        
        Args:
            filters: Structured filters from LLM (QueryFilters format)
            model_info: Field information for validation and query building
            
        Returns:
            List of database-specific query objects
        """
        # Pre-processing: validate filters
        if not filters or "filters" not in filters:
            return [{"query": {"match_all": {}}}]
        
        # Delegate to database-specific translator
        queries = self.translator.translate(filters, model_info)
        
        # Post-processing: could add logging, metrics, etc.
        return queries

