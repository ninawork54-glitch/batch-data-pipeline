"""Gold builds a star schema from silver. Grain: one row per order."""

from __future__ import annotations

import pandas as pd

from pipeline.gold import build_dim_date, build_dim_product, build_dim_store, build_fact
from pipeline.silver import prepare, split


def _valid(bronze_frame) -> pd.DataFrame:
    valid, _ = split(prepare(bronze_frame))
    return valid


def test_build_fact_computes_gross_amount(bronze_frame):
    fact = build_fact(_valid(bronze_frame))

    assert fact["gross_amount"].tolist() == [240.0, 89.5, 360.0]
    assert fact["date_key"].tolist() == [20240101, 20240102, 20240103]


def test_build_fact_keeps_one_row_per_order(bronze_frame):
    fact = build_fact(_valid(bronze_frame))

    assert len(fact) == 3
    assert fact["order_id"].is_unique


def test_dim_product_is_unique_per_product(bronze_frame):
    dim = build_dim_product(_valid(bronze_frame))

    assert len(dim) == 2
    assert dim["product_id"].is_unique
    assert list(dim.columns) == ["product_id", "product_name", "category"]


def test_dim_store_is_unique_per_store(bronze_frame):
    dim = build_dim_store(_valid(bronze_frame))

    assert len(dim) == 2
    assert dim["store_id"].is_unique


def test_dim_date_covers_every_day_in_range(bronze_frame):
    valid = _valid(bronze_frame)
    dim = build_dim_date(valid["order_date"].min(), valid["order_date"].max())

    assert len(dim) == 3
    assert dim["date_key"].tolist() == [20240101, 20240102, 20240103]
    assert dim["day_of_week"].iloc[0] == "Monday"
    assert not dim["is_weekend"].any()


def test_dim_date_marks_weekends():
    dim = build_dim_date(pd.Timestamp("2024-01-06"), pd.Timestamp("2024-01-07"))

    assert dim["is_weekend"].all()
