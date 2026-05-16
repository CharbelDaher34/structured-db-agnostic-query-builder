"""
Query translation coordinator.

Delegates translation to database-specific translators.
"""

from typing import Any

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
        self, filters: dict[str, Any], model_info: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Translate filters to database-specific queries.

        Args:
            filters: Structured filters from LLM (QueryFilters format)
            model_info: Field information for validation and query building

        Returns:
            List of database-specific query objects
        """
        # Always delegate. Each adapter knows how to emit a "match-all" default
        # in its native shape — emitting an ES `match_all` here would break Mongo
        # and CSV adapters which don't speak DSL.
        return self.translator.translate(filters or {}, model_info)
