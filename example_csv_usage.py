"""
Example usage of the query builder with the CSV adapter.

Three comprehensive examples that exercise filtering, aggregation, grouping,
having clauses, and date intervals against the local examples.csv dataset
(customer risk-model output).
"""

import os
import json
import asyncio
from dotenv import load_dotenv
from query_builder import QueryOrchestrator

load_dotenv()

CSV_PATH = os.getenv("CSV_PATH", "examples.csv")

# Columns exposed to the LLM as enums (it picks from a fixed sampled set
# rather than emitting free-form strings).
DEFAULT_CATEGORY_FIELDS = [
    "CORP_SEG",
    "PRIME_RM_AS_OF_MONTH",
    "M1_RISK_BAND",
    "M2_RISK_BAND",
    "M3_RISK_BAND",
    "M4_RISK_BAND",
]

# The driver-name/value/shap columns explode the schema with hundreds of
# repetitive enum-like fields that the LLM never needs to filter on, so we
# strip them out. Same for IDs that aren't useful for analytic queries.
DEFAULT_FIELDS_TO_IGNORE = [
    f"M{m}_DRIVER_{i}_{suffix}"
    for m in (1, 2, 3, 4)
    for i in (1, 2, 3, 4, 5)
    for suffix in ("NAME", "VALUE", "SHAP", "DIRECTION")
]

# Columns to parse as datetimes so date filters and date_histogram-style
# grouping work end-to-end.
DEFAULT_DATE_COLUMNS = ["INSERT_DTTS", "AS_OF_DT"]


def setup_orchestrator() -> QueryOrchestrator:
    """Build a CSV-backed orchestrator and warm it up."""
    orchestrator = QueryOrchestrator.from_csv(
        csv_path=CSV_PATH,
        category_fields=DEFAULT_CATEGORY_FIELDS,
        fields_to_ignore=DEFAULT_FIELDS_TO_IGNORE,
        date_columns=DEFAULT_DATE_COLUMNS,
    )
    orchestrator.warm_up()
    return orchestrator


async def example_1_filtering_sorting_limiting(orchestrator: QueryOrchestrator) -> None:
    """
    Example 1: Filter + sort + limit.

    Tests:
    - Filter on enum field (CORP_SEG is 'SME')
    - Filter on enum field (M1_RISK_BAND is 'High')
    - Filter on numeric field (PORTFOLIO_CUR_BAL > 1,000,000)
    - Sort by PORTFOLIO_CUR_BAL descending
    - Limit to 10 results
    """

    print("\n" + "=" * 80)
    print("EXAMPLE 1: Filtering, Sorting, and Limiting")
    print("=" * 80)

    query = (
        "Show me the top 10 SME customers with a High M1 risk band and a "
        "portfolio balance above 1,000,000, sorted by portfolio balance "
        "descending."
    )

    print(f"\nNatural Language Query: {query}\n")

    result = await orchestrator.query(natural_language_query=query, execute=True)

    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2, default=str))

    print("\n--- Generated CSV Execution Plan ---")
    print(json.dumps(result["database_queries"][0], indent=2, default=str))

    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Rows Matched: {res['total_hits']}")
        print(f"Rows Returned: {len(res['documents'])}")

        for i, doc in enumerate(res["documents"][:10], 1):
            print(
                f"\n  {i}. CIF={doc.get('CSTMR_CIF')} "
                f"Seg={doc.get('CORP_SEG')} "
                f"Balance={doc.get('PORTFOLIO_CUR_BAL'):,.0f} "
                f"M1_Risk={doc.get('M1_RISK_BAND')} "
                f"M1_Prob={doc.get('M1_PROBABILITY')}"
            )


async def example_2_aggregations_grouping_having(orchestrator: QueryOrchestrator) -> None:
    """
    Example 2: Group by + multiple aggregations + having.

    Tests:
    - Group by two enum fields (CORP_SEG, M1_RISK_BAND)
    - Aggregations: avg(M1_PROBABILITY), sum(PORTFOLIO_CUR_BAL), count
    - Having: keep groups with count > 1
    - Sort by sum(PORTFOLIO_CUR_BAL) descending
    """

    print("\n" + "=" * 80)
    print("EXAMPLE 2: Aggregations with Grouping and Having Clause")
    print("=" * 80)

    query = (
        "Group customers by corporate segment and M1 risk band; show the "
        "average M1 probability, the total portfolio current balance, and "
        "the customer count for each group. Only include groups with more "
        "than 1 customer, sorted by total portfolio balance descending."
    )

    print(f"\nNatural Language Query: {query}\n")

    result = await orchestrator.query(natural_language_query=query, execute=True)

    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2, default=str))

    print("\n--- Generated CSV Execution Plan ---")
    print(json.dumps(result["database_queries"][0], indent=2, default=str))

    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Groups: {res['total_hits']}")
        print(f"Groups Returned: {len(res['documents'])}")

        for i, doc in enumerate(res["documents"], 1):
            group_id = doc.get("_id", {})
            if isinstance(group_id, dict):
                seg = group_id.get("CORP_SEG", "N/A")
                band = group_id.get("M1_RISK_BAND", "N/A")
            else:
                seg, band = group_id, "N/A"

            print(f"\n  {i}. Segment={seg}, M1_Risk_Band={band}")
            for k, v in doc.items():
                if k == "_id":
                    continue
                if isinstance(v, (int, float)):
                    print(f"     {k}: {v:,.2f}" if isinstance(v, float) else f"     {k}: {v}")
                else:
                    print(f"     {k}: {v}")


async def example_3_date_interval_aggregations(orchestrator: QueryOrchestrator) -> None:
    """
    Example 3: Date-interval grouping with filter + aggregations.

    Tests:
    - Filter on enum field (CORP_SEG is 'SME')
    - Group by AS_OF_DT with monthly interval
    - Aggregations: count, avg(M1_PROBABILITY), sum(PORTFOLIO_CUR_BAL)
    - Sort by month ascending
    """

    print("\n" + "=" * 80)
    print("EXAMPLE 3: Date Interval Grouping with Aggregations")
    print("=" * 80)

    query = (
        "For SME customers, show the customer count, average M1 probability, "
        "and total portfolio current balance grouped by month of AS_OF_DT, "
        "sorted by month ascending."
    )

    print(f"\nNatural Language Query: {query}\n")

    result = await orchestrator.query(natural_language_query=query, execute=True)

    print("--- Extracted Filters ---")
    print(json.dumps(result["extracted_filters"], indent=2, default=str))

    print("\n--- Generated CSV Execution Plan ---")
    print(json.dumps(result["database_queries"][0], indent=2, default=str))

    if "results" in result:
        res = result["results"][0]
        print("\n--- Results ---")
        print(f"Total Monthly Groups: {res['total_hits']}")
        print(f"Groups Returned: {len(res['documents'])}")

        for i, doc in enumerate(res["documents"], 1):
            group_id = doc.get("_id", {})
            month = group_id.get("AS_OF_DT") if isinstance(group_id, dict) else group_id
            print(f"\n  {i}. Month={month}")
            for k, v in doc.items():
                if k == "_id":
                    continue
                if isinstance(v, float):
                    print(f"     {k}: {v:,.2f}")
                else:
                    print(f"     {k}: {v}")


async def main() -> None:
    print("\n" + "=" * 80)
    print(f"CSV QUERY BUILDER - 3 COMPREHENSIVE EXAMPLES ({CSV_PATH})")
    print("=" * 80)

    orchestrator = None
    try:
        orchestrator = setup_orchestrator()

        print("\n--- Schema Summary ---")
        orchestrator.print_model_summary()

        await example_1_filtering_sorting_limiting(orchestrator)
        await example_2_aggregations_grouping_having(orchestrator)
        await example_3_date_interval_aggregations(orchestrator)

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if orchestrator is not None:
            orchestrator.close()

    print("\n" + "=" * 80)
    print("ALL EXAMPLES COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
