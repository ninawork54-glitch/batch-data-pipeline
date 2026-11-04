"""Shared fixtures. Tests write to a temp lake, never to the repo's data/ directory."""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.config import Settings


@pytest.fixture
def settings(tmp_path) -> Settings:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    return Settings(lake_root=str(tmp_path / "lake"), raw_dir=raw_dir)


@pytest.fixture
def bronze_frame() -> pd.DataFrame:
    """Three valid orders, every column as text, exactly as bronze would hold them."""
    return pd.DataFrame(
        {
            "order_id": ["ORD-1", "ORD-2", "ORD-3"],
            "order_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "store_id": ["S01", "S01", "S02"],
            "store_name": ["Austin Central", "Austin Central", "Denver North"],
            "region": ["South", "South", "West"],
            "product_id": ["P001", "P002", "P001"],
            "product_name": ["Trail Runner", "City Walker", "Trail Runner"],
            "category": ["Footwear", "Footwear", "Footwear"],
            "quantity": ["2", "1", "3"],
            "unit_price": ["120.00", "89.50", "120.00"],
            "currency": ["USD", "USD", "USD"],
            "_source_file": ["orders_2024_01.csv"] * 3,
            "_ingested_at": ["2024-02-01T00:00:00+00:00"] * 3,
        }
    )
