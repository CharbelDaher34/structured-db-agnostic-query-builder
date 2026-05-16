"""
Result formatting utilities.

Normalizes results from different databases into a consistent format.
"""

from typing import Any


class ResultFormatter:
    """
    Formats query results into a consistent structure.

    Can normalize results from different databases to provide a
    unified result format for the application layer.
    """

    @staticmethod
    def format_result(result: dict[str, Any]) -> dict[str, Any]:
        """
        Format a single query result.

        Args:
            result: Raw result from database executor

        Returns:
            Formatted result dictionary
        """
        formatted = {
            "total_hits": result.get("total_hits", 0),
            "documents": result.get("documents", []),
            "success": result.get("success", True),
        }

        if "aggregations" in result:
            formatted["aggregations"] = result["aggregations"]

        if "error" in result:
            formatted["error"] = result["error"]
            formatted["success"] = False

        return formatted

    @staticmethod
    def format_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Format multiple query results.

        Args:
            results: List of raw results from database executor

        Returns:
            List of formatted result dictionaries
        """
        return [ResultFormatter.format_result(result) for result in results]
