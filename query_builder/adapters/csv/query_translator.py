"""
CSV query translator.

Converts normalized filters into an execution plan the CSV executor can run
against a pandas DataFrame.

Unlike MongoDB or Elasticsearch, there is no native query language to emit — so
the "translation" here is mostly normalization: each filter slice becomes a
self-contained execution plan with the keys the executor consumes.
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class CSVQueryTranslator:
    """
    Translates structured filters to a pandas execution plan.

    Implements the IQueryTranslator interface for CSV files.

    The output is a list of `{"plan": {...}}` dicts. Each plan contains:
        - conditions:   list of filter dicts (applied as a chained boolean mask)
        - group_by:     list of column names (or None)
        - interval:     date-binning interval for date group_by fields
        - aggregations: list of {field, type, having_operator?, having_value?}
        - sort:         list of {field, order}
        - limit:        int or None
    """

    SUPPORTED_OPERATORS = {
        "string": {"is", "different", "contains", "isin", "notin", "exists"},
        "number": {"<", ">", "is", "different", "between", "isin", "notin", "exists"},
        "date": {"<", ">", "is", "different", "between", "exists"},
        "boolean": {"is", "different", "exists"},
        "enum": {"is", "different", "isin", "notin", "exists"},
    }

    def translate(
        self, filters: Dict[str, Any], model_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        if not filters or "filters" not in filters:
            return [{"plan": self._empty_plan()}]

        plans: List[Dict[str, Any]] = []
        for filter_slice in filters["filters"]:
            plans.append({"plan": self._build_plan(filter_slice, model_info)})
        return plans

    def _empty_plan(self) -> Dict[str, Any]:
        return {
            "conditions": [],
            "group_by": None,
            "interval": None,
            "aggregations": None,
            "sort": None,
            "limit": None,
        }

    def _build_plan(
        self, filter_slice: Dict[str, Any], model_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        plan = self._empty_plan()

        # Conditions — drop any whose field is not in model_info (LLM hallucinated)
        conditions = []
        for cond in filter_slice.get("conditions", []) or []:
            field = cond.get("field")
            if field not in model_info:
                logger.warning("Dropping condition on unknown field %r", field)
                continue
            conditions.append(cond)
        plan["conditions"] = conditions

        group_by = filter_slice.get("group_by") or None
        if group_by:
            # Drop unknown columns
            group_by = [g for g in group_by if g in model_info]
            plan["group_by"] = group_by or None

        plan["interval"] = filter_slice.get("interval") if plan["group_by"] else None
        plan["aggregations"] = (
            filter_slice.get("aggregations") or None if plan["group_by"] else None
        )
        plan["sort"] = filter_slice.get("sort") or None
        plan["limit"] = filter_slice.get("limit")

        return plan
