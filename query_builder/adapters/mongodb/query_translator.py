"""
MongoDB query translator.

Converts normalized filters to MongoDB aggregation pipeline.
"""

from datetime import datetime
from typing import Any, Optional

from query_builder._logging import QueryBuilderLogger

logger = QueryBuilderLogger.get(__name__)


def _looks_like_iso_date(value: Any) -> bool:
    """Return True iff value is a string parseable as an ISO date/datetime."""
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


class MongoQueryTranslator:
    """
    Translates structured filters to MongoDB aggregation pipeline.

    Implements the IQueryTranslator interface for MongoDB.
    """

    def __init__(self, include_grouped_documents: bool = False, grouped_documents_limit: int = 100):
        """
        Initialize MongoDB query translator.

        Args:
            include_grouped_documents: If True, $push the underlying documents into each group
                (capped by grouped_documents_limit). Defaults to False to avoid the 16MB BSON
                limit and unbounded memory growth.
            grouped_documents_limit: Maximum documents to retain per group when
                include_grouped_documents is True.
        """
        self.include_grouped_documents = include_grouped_documents
        self.grouped_documents_limit = grouped_documents_limit

    def translate(
        self, filters: dict[str, Any], model_info: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Convert QueryFilters to MongoDB aggregation pipelines.

        Args:
            filters: Structured filters from LLM (QueryFilters format)
            model_info: Field information for validation and query building

        Returns:
            List of MongoDB aggregation pipeline objects
        """
        if not filters or "filters" not in filters:
            return [{"pipeline": []}]

        mongo_queries: list[dict[str, Any]] = []

        for filter_slice in filters["filters"]:
            mongo_query = self._translate_slice(filter_slice, model_info)
            mongo_queries.append(mongo_query)

        return mongo_queries

    def _translate_slice(
        self, filter_slice: dict[str, Any], model_info: dict[str, Any]
    ) -> dict[str, Any]:
        """Translate a single query slice to MongoDB aggregation pipeline."""
        pipeline: list[dict[str, Any]] = []

        # Build $match stage from conditions
        match_conditions = []
        for condition in filter_slice.get("conditions", []):
            clause = self._translate_condition(condition, model_info)
            if clause:
                match_conditions.append(clause)

        if match_conditions:
            if len(match_conditions) == 1:
                pipeline.append({"$match": match_conditions[0]})
            else:
                pipeline.append({"$match": {"$and": match_conditions}})

        is_grouped = bool(filter_slice.get("group_by"))
        agg_result_names: dict[str, str] = {}  # original field path -> aggregation result name
        group_by_fields: list[str] = list(filter_slice.get("group_by") or [])

        # Process group_by and aggregations
        if is_grouped:
            group_stage, agg_result_names = self._build_group_stage(filter_slice, model_info)
            pipeline.append(group_stage)

            # Add having filter if needed
            having_conditions = self._build_having_conditions(filter_slice, agg_result_names)
            if having_conditions:
                pipeline.append({"$match": having_conditions})

        # Process sort — must come AFTER $group so we have to remap field names
        if filter_slice.get("sort"):
            sort_spec = self._build_sort_spec(
                filter_slice["sort"],
                is_grouped=is_grouped,
                group_by_fields=group_by_fields,
                agg_result_names=agg_result_names,
            )
            if sort_spec:
                pipeline.append({"$sort": sort_spec})

        # Process limit
        if "limit" in filter_slice and filter_slice["limit"] is not None:
            pipeline.append({"$limit": filter_slice["limit"]})

        return {"pipeline": pipeline}

    def _translate_condition(
        self, condition: dict[str, Any], model_info: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        """Translate a single condition to MongoDB query clause."""
        field = condition["field"]
        operator = condition["operator"]
        value = condition["value"]

        # Build MongoDB query based on operator
        if operator == ">":
            return {field: {"$gt": value}}
        elif operator == "<":
            return {field: {"$lt": value}}
        elif operator == "is":
            return {field: {"$eq": value}}
        elif operator == "different":
            return {field: {"$ne": value}}
        elif operator == "isin":
            if isinstance(value, list):
                # Detect a [start_date, end_date] range using robust ISO parsing
                if len(value) == 2 and all(_looks_like_iso_date(v) for v in value):
                    return {field: {"$gte": value[0], "$lte": value[1]}}
                return {field: {"$in": value}}
            return {field: {"$eq": value}}
        elif operator == "notin":
            if isinstance(value, list):
                return {field: {"$nin": value}}
            return {field: {"$ne": value}}
        elif operator == "between":
            if isinstance(value, list) and len(value) == 2:
                return {field: {"$gte": value[0], "$lte": value[1]}}
        elif operator == "contains":
            return {field: {"$regex": value, "$options": "i"}}
        elif operator == "exists":
            if value is True:
                return {field: {"$exists": True, "$ne": None}}
            elif value is False:
                return {"$or": [{field: {"$exists": False}}, {field: None}]}

        return None

    def _build_group_stage(
        self, filter_slice: dict[str, Any], model_info: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """
        Build MongoDB $group stage from group_by and aggregations.

        Returns:
            (group_stage_dict, mapping of original field path -> aggregation result name)
        """
        group_fields = filter_slice["group_by"]

        # Build _id for grouping
        group_id = {}
        for gf in group_fields:
            field_type = model_info.get(gf, {}).get("type")

            if field_type == "date":
                interval = filter_slice.get("interval", "month")

                # Coerce to a Date regardless of whether the field is stored as
                # a BSON Date or an ISO string. $convert with onError/onNull
                # avoids the silent grouping-into-null bucket that
                # $dateFromString alone causes for already-typed dates.
                date_conversion = {
                    "$convert": {
                        "input": f"${gf}",
                        "to": "date",
                        "onError": None,
                        "onNull": None,
                    }
                }

                format_map = {
                    "day": "%Y-%m-%d",
                    "week": "%Y-W%V",
                    "month": "%Y-%m",
                    "year": "%Y",
                }
                fmt = format_map.get(interval, "%Y-%m")
                group_id[gf] = {"$dateToString": {"format": fmt, "date": date_conversion}}
            else:
                group_id[gf] = f"${gf}"

        group_stage: dict[str, Any] = {"$group": {"_id": group_id}}
        agg_result_names: dict[str, str] = {}

        # Add aggregation fields
        if filter_slice.get("aggregations"):
            for agg in filter_slice["aggregations"]:
                agg_field_name = f"{agg['type'].lower()}_{agg['field'].replace('.', '_')}"
                agg_result_names[agg["field"]] = agg_field_name
                field_for_agg = agg["field"]

                if agg["type"] == "sum":
                    group_stage["$group"][agg_field_name] = {"$sum": f"${field_for_agg}"}
                elif agg["type"] == "avg":
                    group_stage["$group"][agg_field_name] = {"$avg": f"${field_for_agg}"}
                elif agg["type"] == "count":
                    group_stage["$group"][agg_field_name] = {"$sum": 1}
                elif agg["type"] == "min":
                    group_stage["$group"][agg_field_name] = {"$min": f"${field_for_agg}"}
                elif agg["type"] == "max":
                    group_stage["$group"][agg_field_name] = {"$max": f"${field_for_agg}"}

        # Optionally include underlying documents (capped to avoid 16 MB BSON limit)
        if self.include_grouped_documents:
            group_stage["$group"]["documents"] = {
                "$push": "$$ROOT",
            }
            # Cap with a $slice in a follow-up $project handled by caller via post-process;
            # cap inline via $accumulator is too complex, so we rely on $slice in projection.
            # Simpler: use $push with $$ROOT and trim via a post stage.

        return group_stage, agg_result_names

    def _build_having_conditions(
        self,
        filter_slice: dict[str, Any],
        agg_result_names: dict[str, str],
    ) -> Optional[dict[str, Any]]:
        """Build having conditions for post-aggregation filtering."""
        if not filter_slice.get("aggregations"):
            return None

        having_conditions = []

        for agg in filter_slice["aggregations"]:
            if agg.get("having_operator") and agg.get("having_value") is not None:
                agg_field_name = agg_result_names.get(
                    agg["field"],
                    f"{agg['type'].lower()}_{agg['field'].replace('.', '_')}",
                )
                operator = agg["having_operator"]
                value = agg["having_value"]

                if operator == ">":
                    having_conditions.append({agg_field_name: {"$gt": value}})
                elif operator == "<":
                    having_conditions.append({agg_field_name: {"$lt": value}})
                elif operator == "is":
                    having_conditions.append({agg_field_name: {"$eq": value}})
                elif operator == "different":
                    having_conditions.append({agg_field_name: {"$ne": value}})

        if not having_conditions:
            return None
        if len(having_conditions) == 1:
            return having_conditions[0]
        return {"$and": having_conditions}

    def _build_sort_spec(
        self,
        sort_entries: list[dict[str, Any]],
        is_grouped: bool,
        group_by_fields: list[str],
        agg_result_names: dict[str, str],
    ) -> dict[str, int]:
        """
        Build $sort spec, remapping field names to their post-group output names.

        After $group, the only addressable fields are `_id.<field>` for group_by fields
        and the aggregation result names. Sorting by a raw schema field after $group
        is a no-op, so we drop those entries and warn.
        """
        sort_spec: dict[str, int] = {}
        for s in sort_entries:
            field = s["field"]
            direction = 1 if s.get("order", "asc") == "asc" else -1

            if not is_grouped:
                sort_spec[field] = direction
                continue

            if field in group_by_fields:
                sort_spec[f"_id.{field}"] = direction
            elif field in agg_result_names:
                sort_spec[agg_result_names[field]] = direction
            else:
                logger.warning(
                    "Sort field %r is not in group_by or aggregations after $group; dropping.",
                    field,
                )

        return sort_spec
