"""
MongoDB query translator.

Converts normalized filters to MongoDB aggregation pipeline.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime


class MongoQueryTranslator:
    """
    Translates structured filters to MongoDB aggregation pipeline.
    
    Implements the IQueryTranslator interface for MongoDB.
    """
    
    def __init__(self):
        """Initialize MongoDB query translator."""
        pass
    
    def translate(
        self, filters: Dict[str, Any], model_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
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
        
        mongo_queries: List[Dict[str, Any]] = []
        
        for filter_slice in filters["filters"]:
            mongo_query = self._translate_slice(filter_slice, model_info)
            mongo_queries.append(mongo_query)
        
        return mongo_queries
    
    def _translate_slice(
        self, filter_slice: Dict[str, Any], model_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Translate a single query slice to MongoDB aggregation pipeline."""
        pipeline: List[Dict[str, Any]] = []
        
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
        
        # Process group_by and aggregations
        if "group_by" in filter_slice and filter_slice["group_by"]:
            group_stage = self._build_group_stage(filter_slice, model_info)
            pipeline.append(group_stage)
            
            # Add having filter if needed
            having_conditions = self._build_having_conditions(filter_slice)
            if having_conditions:
                pipeline.append({"$match": having_conditions})
        
        # Process sort
        if "sort" in filter_slice and filter_slice["sort"]:
            sort_spec = {}
            for s in filter_slice["sort"]:
                sort_spec[s["field"]] = 1 if s.get("order", "asc") == "asc" else -1
            pipeline.append({"$sort": sort_spec})
        
        # Process limit
        if "limit" in filter_slice:
            pipeline.append({"$limit": filter_slice["limit"]})
        
        return {"pipeline": pipeline}
    
    def _translate_condition(
        self, condition: Dict[str, Any], model_info: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
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
                # Check if it's a date range
                if len(value) == 2 and all(
                    isinstance(v, str) and len(v) == 10 for v in value
                ):
                    return {field: {"$gte": value[0], "$lte": value[1]}}
                else:
                    return {field: {"$in": value}}
            else:
                return {field: {"$eq": value}}
        elif operator == "notin":
            if isinstance(value, list):
                return {field: {"$nin": value}}
            else:
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
        self, filter_slice: Dict[str, Any], model_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build MongoDB $group stage from group_by and aggregations."""
        group_fields = filter_slice["group_by"]
        
        # Build _id for grouping
        group_id = {}
        for gf in group_fields:
            field_type = model_info.get(gf, {}).get("type")
            
            if field_type == "date":
                interval = filter_slice.get("interval", "month")
                
                # Convert string date to Date object, then format
                # Using $dateFromString for ISO string dates
                date_conversion = {
                    "$dateFromString": {
                        "dateString": f"${gf}",
                        "onError": None,
                        "onNull": None
                    }
                }
                
                # Date truncation based on interval
                if interval == "day":
                    group_id[gf] = {
                        "$dateToString": {"format": "%Y-%m-%d", "date": date_conversion}
                    }
                elif interval == "week":
                    group_id[gf] = {
                        "$dateToString": {"format": "%Y-W%V", "date": date_conversion}
                    }
                elif interval == "month":
                    group_id[gf] = {
                        "$dateToString": {"format": "%Y-%m", "date": date_conversion}
                    }
                elif interval == "year":
                    group_id[gf] = {
                        "$dateToString": {"format": "%Y", "date": date_conversion}
                    }
            else:
                group_id[gf] = f"${gf}"
        
        group_stage: Dict[str, Any] = {"$group": {"_id": group_id}}
        
        # Add aggregation fields
        if "aggregations" in filter_slice and filter_slice["aggregations"]:
            for agg in filter_slice["aggregations"]:
                agg_field_name = f"{agg['type'].lower()}_{agg['field'].replace('.', '_')}"
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
        
        # Always add documents array with $push
        group_stage["$group"]["documents"] = {"$push": "$$ROOT"}
        
        return group_stage
    
    def _build_having_conditions(self, filter_slice: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build having conditions for post-aggregation filtering."""
        if "aggregations" not in filter_slice or not filter_slice["aggregations"]:
            return None
        
        having_conditions = []
        
        for agg in filter_slice["aggregations"]:
            if agg.get("having_operator") and agg.get("having_value") is not None:
                agg_field_name = f"{agg['type'].lower()}_{agg['field'].replace('.', '_')}"
                operator = agg["having_operator"]
                value = agg["having_value"]
                
                # Build condition based on operator
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
        else:
            return {"$and": having_conditions}

