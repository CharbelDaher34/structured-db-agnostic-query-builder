"""
Elasticsearch query translator.

Converts normalized filters to Elasticsearch DSL queries.
"""

from typing import Any, Dict, List, Optional


class ESQueryTranslator:
    """
    Translates structured filters to Elasticsearch DSL.
    
    Implements the IQueryTranslator interface for Elasticsearch.
    """
    
    def __init__(self):
        """Initialize Elasticsearch query translator."""
        pass
    
    def translate(
        self, filters: Dict[str, Any], model_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Convert QueryFilters to Elasticsearch DSL queries.
        
        Args:
            filters: Structured filters from LLM (QueryFilters format)
            model_info: Field information for validation and query building
            
        Returns:
            List of Elasticsearch DSL query objects
        """
        if not filters or "filters" not in filters:
            return [{"query": {"match_all": {}}}]
        
        elastic_queries: List[Dict[str, Any]] = []
        
        for filter_slice in filters["filters"]:
            elastic_query = self._translate_slice(filter_slice, model_info)
            elastic_queries.append(elastic_query)
        
        return elastic_queries
    
    def _translate_slice(
        self, filter_slice: Dict[str, Any], model_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Translate a single query slice to ES DSL."""
        must_clauses: List[Dict[str, Any]] = []
        
        # Process conditions
        for condition in filter_slice.get("conditions", []):
            clause = self._translate_condition(condition, model_info)
            if clause:
                must_clauses.append(clause)
        
        # Build base query
        if must_clauses:
            elastic_query: Dict[str, Any] = {
                "query": {"bool": {"must": must_clauses}}
            }
        else:
            elastic_query = {"query": {"match_all": {}}}
        
        # Process sort
        if "sort" in filter_slice and filter_slice["sort"]:
            sort_configs = []
            for s in filter_slice["sort"]:
                sort_configs.append({s["field"]: {"order": s.get("order", "asc")}})
            elastic_query["sort"] = sort_configs
        
        # Process limit
        if "limit" in filter_slice:
            elastic_query["size"] = filter_slice["limit"]
        
        # Process group_by and aggregations
        if "group_by" in filter_slice and filter_slice["group_by"]:
            aggs = self._build_aggregations(filter_slice, model_info)
            elastic_query["aggs"] = aggs
            elastic_query["size"] = 0  # Don't return documents for aggregations
        
        return elastic_query
    
    def _translate_condition(
        self, condition: Dict[str, Any], model_info: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Translate a single condition to ES clause."""
        field = condition["field"]
        operator = condition["operator"]
        value = condition["value"]
        
        # Determine if we need .keyword suffix
        is_string = isinstance(value, str) and not self._is_date_string(value)
        exact_field = self._keyword_field(field) if is_string else field
        
        # Build clause based on operator
        if operator == ">":
            return {"range": {field: {"gt": value}}}
        elif operator == "<":
            return {"range": {field: {"lt": value}}}
        elif operator == "is":
            return {"term": {exact_field: value}}
        elif operator == "different":
            return {"bool": {"must_not": {"term": {exact_field: value}}}}
        elif operator == "isin":
            if isinstance(value, list):
                # Check if it's a date range (2 dates)
                if (
                    len(value) == 2
                    and all(self._is_date_string(str(v)) for v in value)
                ):
                    return {"range": {field: {"gte": value[0], "lte": value[1]}}}
                else:
                    return {"terms": {exact_field: value}}
            else:
                return {"term": {exact_field: value}}
        elif operator == "notin":
            if isinstance(value, list):
                return {"bool": {"must_not": {"terms": {exact_field: value}}}}
            else:
                return {"bool": {"must_not": {"term": {exact_field: value}}}}
        elif operator == "between":
            if isinstance(value, list) and len(value) == 2:
                return {"range": {field: {"gte": value[0], "lte": value[1]}}}
        elif operator == "contains":
            return {
                "wildcard": {
                    exact_field: {"value": f"*{value}*", "case_insensitive": True}
                }
            }
        elif operator == "exists":
            if value is True:
                return {"exists": {"field": field}}
            elif value is False:
                return {"bool": {"must_not": {"exists": {"field": field}}}}
        
        return None
    
    def _build_aggregations(
        self, filter_slice: Dict[str, Any], model_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build ES aggregations from group_by and aggregations."""
        group_fields = filter_slice["group_by"]
        limit_config = filter_slice.get("limit", 100)
        
        aggs: Dict[str, Any] = {}
        current_agg = aggs
        
        # Build nested aggregations for each group_by field
        for i, gf in enumerate(group_fields):
            agg_name = f"group_by_{i}"
            field_type = model_info.get(gf, {}).get("type")
            
            if field_type == "date":
                interval = filter_slice.get("interval", "month")
                
                # Set format based on interval
                format_map = {
                    "day": "yyyy-MM-dd",
                    "week": "yyyy-'W'ww",
                    "month": "yyyy-MM",
                    "year": "yyyy",
                }
                format_str = format_map.get(interval, "yyyy-MM")
                
                current_agg[agg_name] = {
                    "date_histogram": {
                        "field": gf,
                        "calendar_interval": interval,
                        "format": format_str,
                    }
                }
            else:
                agg_field = (
                    self._keyword_field(gf)
                    if field_type in ("string", "enum", "text")
                    else gf
                )
                current_agg[agg_name] = {
                    "terms": {"field": agg_field, "size": limit_config}
                }
            
            # Prepare for next level
            if i < len(group_fields) - 1:
                current_agg[agg_name]["aggs"] = {}
                current_agg = current_agg[agg_name]["aggs"]
        
        # Navigate to the deepest aggregation level
        target_for_sub_aggs = aggs
        for i in range(len(group_fields)):
            group_agg_name = f"group_by_{i}"
            if "aggs" in target_for_sub_aggs[group_agg_name]:
                target_for_sub_aggs = target_for_sub_aggs[group_agg_name]["aggs"]
            else:
                target_for_sub_aggs = target_for_sub_aggs[group_agg_name]
                break
        
        sub_aggs = target_for_sub_aggs.setdefault("aggs", {})
        
        # Always add top_hits to get documents per bucket
        sub_aggs["documents"] = {"top_hits": {"size": 100}}
        
        # Process aggregation metrics
        having_clauses = []
        if "aggregations" in filter_slice and filter_slice["aggregations"]:
            for agg in filter_slice["aggregations"]:
                agg_metric_name = f"{agg['type'].lower()}_{agg['field'].replace('.', '_')}"
                
                field_for_agg = agg["field"]
                field_info = model_info.get(field_for_agg, {})
                field_type = field_info.get("type")
                
                # Use .keyword for count on string fields
                if agg["type"] == "count" and field_type in ("string", "enum", "text"):
                    field_for_agg = self._keyword_field(field_for_agg)
                
                # Build metric aggregation
                if agg["type"] == "sum":
                    sub_aggs[agg_metric_name] = {"sum": {"field": field_for_agg}}
                elif agg["type"] == "avg":
                    sub_aggs[agg_metric_name] = {"avg": {"field": field_for_agg}}
                elif agg["type"] == "count":
                    sub_aggs[agg_metric_name] = {
                        "value_count": {"field": field_for_agg}
                    }
                elif agg["type"] == "min":
                    sub_aggs[agg_metric_name] = {"min": {"field": field_for_agg}}
                elif agg["type"] == "max":
                    sub_aggs[agg_metric_name] = {"max": {"field": field_for_agg}}
                
                # Check for having clause
                if agg.get("having_operator") and agg.get("having_value") is not None:
                    having_clauses.append(
                        {
                            "metric_name": agg_metric_name,
                            "operator": agg["having_operator"],
                            "value": agg["having_value"],
                        }
                    )
        
        # Add bucket_selector for having clauses
        if having_clauses:
            buckets_path = {}
            script_parts = []
            op_map = {">": ">", "<": "<", "is": "==", "different": "!=", ">=": ">=", "<=": "<="}
            
            for i, clause in enumerate(having_clauses):
                script_var = f"var_{i}"
                buckets_path[script_var] = clause["metric_name"]
                op_symbol = op_map.get(clause["operator"], "==")
                value = clause["value"]
                script_value = f"'{value}'" if isinstance(value, str) else value
                script_parts.append(f"params.{script_var} {op_symbol} {script_value}")
            
            script = " && ".join(script_parts)
            
            sub_aggs["having_filter"] = {
                "bucket_selector": {"buckets_path": buckets_path, "script": script}
            }
        
        return aggs
    
    @staticmethod
    def _keyword_field(field: str) -> str:
        """Add .keyword suffix if not already present."""
        if field.endswith(".keyword"):
            return field
        return f"{field}.keyword"
    
    @staticmethod
    def _is_date_string(s: str) -> bool:
        """Check if string looks like a date (YYYY-MM-DD format)."""
        return len(s) == 10 and s[4] == "-" and s[7] == "-"
