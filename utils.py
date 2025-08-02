from typing import Any, Dict, List, Optional
from elasticsearch import Elasticsearch


def get_es_schema_for_api(
    es_host: str, 
    index_name: str, 
    category_fields: List[str]
) -> Dict[str, Any]:
    """
    Fetches the Elasticsearch mapping and distinct values for specified category fields.

    This function connects to an Elasticsearch instance to retrieve the necessary schema
    information that can then be used to make requests to the `/query-from-mapping`
    API endpoint, which operates without a direct database connection.

    Args:
        es_host: The URL of the Elasticsearch host.
        index_name: The name of the index to get the schema from.
        category_fields: A list of field names to be treated as enums, for which
                         distinct values will be fetched.

    Returns:
        A dictionary containing the `elasticsearch_mapping` (the 'properties' block)
        and `enum_fields` (a mapping of category fields to their distinct values).
    """
    # Import here to avoid circular import
    from elasticsearch_model_generator import ModelBuilder
    
    es_client = Elasticsearch(hosts=[es_host])
    
    # 1. Get the full index mapping and extract the 'properties' section.
    full_mapping = es_client.indices.get_mapping(index=index_name)
    mapping_properties = full_mapping.get(index_name, {}).get("mappings", {}).get("properties", {})

    # Instantiate a ModelBuilder to reuse its GetDistinctValues logic.
    # We pass the client and index so it can perform the necessary queries.
    model_builder = ModelBuilder(
        es_client=es_client,
        index_name=index_name,
        category_fields=category_fields
    )

    def _get_field_type(path: str, properties: Dict[str, Any]) -> Optional[str]:
        """A helper to recursively find a field's type in the mapping."""
        keys = path.split('.')
        data = properties
        for i, key in enumerate(keys):
            if key in data:
                if 'properties' in data[key] and i < len(keys) - 1:
                    data = data[key]['properties']
                else:
                    return data[key].get('type')
        return None

    # 2. For each category field, determine its type and fetch its distinct values.
    enum_fields = {}
    for field in category_fields:
        # For 'text' fields, Elasticsearch performs aggregations on the '.keyword' sub-field.
        es_type = _get_field_type(field, mapping_properties)
        field_path_for_es = f"{field}.keyword" if es_type == "text" else field
        
        # Use the ModelBuilder's method to run the aggregation query.
        distinct_values = model_builder.GetDistinctValues(field_path_for_es)
        if distinct_values:
            enum_fields[field] = distinct_values
            
    return {
        "elasticsearch_mapping": mapping_properties,
        "enum_fields": enum_fields,
    } 