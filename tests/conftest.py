"""Shared fixtures for the query_builder test suite."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest


@pytest.fixture
def sample_rows() -> list[dict[str, Any]]:
    """A handful of records with mixed types (string, number, enum, date, bool)."""
    base = datetime(2024, 1, 15)
    rows: list[dict[str, Any]] = []
    statuses = ["active", "pending", "closed"]
    segments = ["SME", "ENT", "RETAIL"]
    for i in range(12):
        rows.append(
            {
                "id": i + 1,
                "name": f"customer_{i}",
                "status": statuses[i % len(statuses)],
                "segment": segments[i % len(segments)],
                "balance": float((i + 1) * 1000),
                "active": i % 2 == 0,
                "created_at": (base + timedelta(days=i * 7)).strftime("%Y-%m-%d"),
            }
        )
    return rows


@pytest.fixture
def sample_csv(tmp_path: Path, sample_rows) -> Path:
    """Write sample_rows out to a CSV in tmp_path and return its path."""
    csv_path = tmp_path / "sample.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(sample_rows[0].keys()))
        writer.writeheader()
        writer.writerows(sample_rows)
    return csv_path


@pytest.fixture
def sample_df(sample_rows) -> pd.DataFrame:
    df = pd.DataFrame(sample_rows)
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df


@pytest.fixture
def basic_model_info() -> dict[str, Any]:
    """A minimal `model_info` dict that matches sample_rows."""
    return {
        "id": {"type": "number"},
        "name": {"type": "string"},
        "status": {"type": "enum", "values": ["active", "pending", "closed"]},
        "segment": {"type": "enum", "values": ["SME", "ENT", "RETAIL"]},
        "balance": {"type": "number"},
        "active": {"type": "boolean"},
        "created_at": {"type": "date"},
    }
