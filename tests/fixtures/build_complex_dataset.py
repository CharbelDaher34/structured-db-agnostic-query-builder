"""
Build a complex synthetic dataset for AI evaluation.

The dataset is a fictional e-commerce orders table with rich structure:
 - 18 columns spanning every supported type (number / string / enum / date / boolean)
 - Enum cardinalities of 3, 4, 5, 6, 8 so the LLM has to pick the right one
 - Two date columns (order_date, ship_date) so date-range / interval queries
   have an unambiguous target
 - Discount and quantity create realistic having-clause scenarios
 - is_returned + priority enable boolean + multi-condition filtering
 - notes is free-text so `contains` is exercised
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

REGIONS = ["EMEA", "AMER", "APAC", "LATAM"]
SEGMENTS = ["B2C", "B2B", "Enterprise"]
CATEGORIES = ["Electronics", "Apparel", "Home", "Books", "Beauty"]
SHIPPING = ["Standard", "Express", "Overnight", "Pickup"]
PRIORITIES = ["low", "medium", "high", "critical"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD"]
SALES_REPS = [f"rep_{i:02d}" for i in range(1, 9)]


def _rng_amount(rng: random.Random, category: str, segment: str) -> float:
    """Realistic amount distribution by category and segment."""
    base = {
        "Electronics": 450,
        "Apparel": 80,
        "Home": 120,
        "Books": 25,
        "Beauty": 60,
    }[category]
    if segment == "Enterprise":
        base *= 6
    elif segment == "B2B":
        base *= 2.5
    return round(base * rng.uniform(0.4, 2.4), 2)


def build_orders(n: int = 200, seed: int = 1234) -> list[dict]:
    rng = random.Random(seed)
    start = date(2024, 1, 1)
    rows: list[dict] = []

    for i in range(n):
        order_date = start + timedelta(days=rng.randint(0, 365))
        ship_date = order_date + timedelta(days=rng.randint(1, 14))
        region = rng.choice(REGIONS)
        segment = rng.choice(SEGMENTS)
        category = rng.choice(CATEGORIES)
        currency = rng.choice(CURRENCIES)
        # Skew priority by segment: Enterprise tends to be higher priority
        if segment == "Enterprise":
            priority = rng.choices(PRIORITIES, weights=[1, 3, 5, 4])[0]
        else:
            priority = rng.choices(PRIORITIES, weights=[5, 4, 2, 1])[0]
        is_returned = rng.random() < 0.12
        quantity = rng.randint(1, 25)
        discount = round(rng.choices([0.0, 0.05, 0.10, 0.20, 0.35], weights=[6, 3, 3, 2, 1])[0], 2)
        amount = _rng_amount(rng, category, segment) * quantity * (1 - discount)
        notes = ""
        if rng.random() < 0.15:
            notes = rng.choice(
                [
                    "gift wrap requested",
                    "fragile - handle with care",
                    "customer requested expedited shipping",
                    "loyalty member discount applied",
                    "bulk order - corporate purchase",
                    "complaint resolved with credit",
                ]
            )

        rows.append(
            {
                "order_id": 1000 + i,
                "customer_id": 100 + rng.randint(0, 79),  # ~80 distinct customers
                "customer_segment": segment,
                "region": region,
                "product_category": category,
                "shipping_method": rng.choice(SHIPPING),
                "priority": priority,
                "currency": currency,
                "sales_rep": rng.choice(SALES_REPS),
                "order_date": order_date.isoformat(),
                "ship_date": ship_date.isoformat(),
                "quantity": quantity,
                "unit_price": round(_rng_amount(rng, category, "B2C") / 2, 2),
                "discount_pct": discount,
                "amount": round(amount, 2),
                "is_returned": is_returned,
                "is_gift": rng.random() < 0.08,
                "notes": notes,
            }
        )

    return rows


def write_csv(path: Path, rows: list[dict]) -> Path:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path


# Columns the schema extractor should treat as enums
CATEGORY_FIELDS = [
    "customer_segment",
    "region",
    "product_category",
    "shipping_method",
    "priority",
    "currency",
]

DATE_COLUMNS = ["order_date", "ship_date"]


if __name__ == "__main__":
    out = Path(__file__).parent / "orders.csv"
    write_csv(out, build_orders())
    print(f"wrote {out}")
