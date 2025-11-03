# MongoDB Adapter

This adapter provides MongoDB support for the query builder system.

## Features

- ✅ **Schema Inference**: Automatically infers schema by sampling documents
- ✅ **Aggregation Pipeline**: Converts filters to MongoDB aggregation pipelines
- ✅ **Natural Language**: Query MongoDB using natural language
- ✅ **Flexible**: Works with any MongoDB collection structure

## Installation

```bash
pip install pymongo
```

## Usage

### Basic Setup

```python
from query_builder import QueryOrchestrator

orchestrator = QueryOrchestrator.from_mongodb(
    mongo_uri="mongodb://localhost:27017",
    database_name="mydb",
    collection_name="mycollection",
    category_fields=["status", "type"],
    llm_model="gpt-4o",
    llm_api_key="sk-..."
)
```

### Natural Language Query

```python
result = orchestrator.query("Show me all transactions over $100")

print(result["database_queries"])  # MongoDB aggregation pipeline
print(result["results"])            # Query results
```

### Raw MongoDB Query

```python
# Using aggregation pipeline
result = orchestrator.query_raw({
    "pipeline": [
        {"$match": {"amount": {"$gt": 100}}},
        {"$sort": {"amount": -1}},
        {"$limit": 10}
    ]
})

# Using simple find()
result = orchestrator.query_raw({
    "filter": {"status": "completed"}
}, size=50)
```

## How It Works

### Schema Extraction

Since MongoDB is schemaless, the adapter samples documents to infer the schema:

1. Samples N documents (default: 1000)
2. Analyzes field types across all samples
3. Builds normalized schema
4. Caches for performance

### Query Translation

Converts structured filters to MongoDB aggregation pipelines:

- **Conditions** → `$match` stage
- **Sorting** → `$sort` stage
- **Limiting** → `$limit` stage
- **Grouping** → `$group` stage
- **Aggregations** → `$sum`, `$avg`, `$count`, etc.
- **Having** → `$match` after `$group`

### Example Translation

**Input (Natural Language):**
```
"What's the average order amount by customer, for customers with more than 5 orders?"
```

**Output (MongoDB Pipeline):**
```json
[
  {
    "$group": {
      "_id": {"customer_id": "$customer_id"},
      "avg_order_amount": {"$avg": "$order_amount"},
      "count_order_id": {"$sum": 1},
      "documents": {"$push": "$$ROOT"}
    }
  },
  {
    "$match": {
      "count_order_id": {"$gt": 5}
    }
  }
]
```

## Supported Operators

| Operator | MongoDB Equivalent | Example |
|----------|-------------------|---------|
| `>` | `$gt` | `{"field": {"$gt": 100}}` |
| `<` | `$lt` | `{"field": {"$lt": 50}}` |
| `is` | `$eq` | `{"field": {"$eq": "value"}}` |
| `different` | `$ne` | `{"field": {"$ne": "value"}}` |
| `isin` | `$in` | `{"field": {"$in": ["a", "b"]}}` |
| `notin` | `$nin` | `{"field": {"$nin": ["a", "b"]}}` |
| `between` | `$gte` + `$lte` | `{"field": {"$gte": 1, "$lte": 10}}` |
| `contains` | `$regex` | `{"field": {"$regex": "text", "$options": "i"}}` |
| `exists` | `$exists` | `{"field": {"$exists": true}}` |

## Supported Aggregations

- **sum**: `$sum`
- **avg**: `$avg`
- **count**: `$sum: 1`
- **min**: `$min`
- **max**: `$max`

## Date Grouping

Date fields can be grouped by interval:

- **day**: Groups by day (YYYY-MM-DD)
- **week**: Groups by week (YYYY-Www)
- **month**: Groups by month (YYYY-MM)
- **year**: Groups by year (YYYY)

## Configuration

### Sample Size

Control how many documents to sample for schema inference:

```python
orchestrator = QueryOrchestrator.from_mongodb(
    mongo_uri="...",
    database_name="mydb",
    collection_name="mycollection",
    sample_size=2000,  # Sample more documents for better schema
)
```

### Category Fields

Specify fields that should be treated as enums:

```python
orchestrator = QueryOrchestrator.from_mongodb(
    mongo_uri="...",
    database_name="mydb",
    collection_name="mycollection",
    category_fields=["status", "type", "category"],
)
```

The adapter will fetch distinct values for these fields.

## Limitations

1. **Schema Inference**: Accuracy depends on sample size
2. **Nested Arrays**: Complex nested array structures may not be fully captured
3. **Mixed Types**: If a field has multiple types, the most common is used
4. **Performance**: Large collections may require tuning sample_size

## Best Practices

1. **Sample Size**: Start with 1000, increase if schema is incomplete
2. **Category Fields**: Specify fields with limited distinct values
3. **Indexes**: Create indexes on frequently queried fields
4. **Connection Pooling**: Reuse orchestrator instance for multiple queries

## Example: Full Pipeline

```python
import os
from query_builder import QueryOrchestrator

# Setup
orchestrator = QueryOrchestrator.from_mongodb(
    mongo_uri=os.getenv("MONGO_URI"),
    database_name="ecommerce",
    collection_name="orders",
    category_fields=["status", "payment_method", "shipping_method"],
    fields_to_ignore=["_id", "internal_tracking"],
    llm_model="gpt-4o",
    llm_api_key=os.getenv("OPENAI_API_KEY"),
    sample_size=1000,
)

# Natural language query
result = orchestrator.query(
    "What's the total revenue by month for completed orders in 2024?"
)

# Access results
pipeline = result["database_queries"][0]["pipeline"]
documents = result["results"][0]["documents"]

print(f"Found {len(documents)} results")
for doc in documents:
    print(f"Month: {doc['_id']}, Revenue: {doc['total_revenue']}")
```

## Troubleshooting

### "Connection refused"
- Ensure MongoDB is running
- Check `mongo_uri` is correct
- Verify network access

### "Schema is empty"
- Collection has no documents
- Increase `sample_size`
- Check collection name

### "Field not found"
- Field may not exist in sampled documents
- Increase `sample_size`
- Check field name spelling

## Architecture

```
MongoSchemaExtractor
    ↓ (samples documents)
Normalized Schema
    ↓
ModelBuilder → Pydantic Model
    ↓
LLM → Structured Filters
    ↓
MongoQueryTranslator → Aggregation Pipeline
    ↓
MongoQueryExecutor → Results
```

## Comparison with Elasticsearch Adapter

| Feature | Elasticsearch | MongoDB |
|---------|--------------|---------|
| Schema Source | Mapping API | Document Sampling |
| Query Format | DSL | Aggregation Pipeline |
| Full-Text Search | Native | `$regex` |
| Nested Objects | Native | Native |
| Arrays | Nested Type | Native |
| Aggregations | Native | Native |

## Contributing

To improve the MongoDB adapter:

1. Enhance schema inference for complex types
2. Add support for more operators
3. Optimize aggregation pipeline generation
4. Add connection pooling
5. Improve error handling

## License

Part of the elastic_query_builder project.

