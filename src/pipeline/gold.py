"""Gold: conform silver into a star schema.

Grain is one row per order in `fact_sales`. Dimensions are built from the fact
itself, so a product that never sold does not appear - acceptable here because
the source only contains sales, not a product master.
"""

from __future__ import annotations

import logging

import pandas as pd

from .config import Settings
from .storage import read_table, write_table

log = logging.getLogger(__name__)

FACT_TABLE = "fact_sales"
DIM_TABLES = ("dim_product", "dim_store", "dim_date")


def build_dim_product(silver: pd.DataFrame) -> pd.DataFrame:
    dim = (
        silver[["product_id", "product_name", "category"]]
        .drop_duplicates(subset=["product_id"])
        .sort_values("product_id")
        .reset_index(drop=True)
    )
    return dim


def build_dim_store(silver: pd.DataFrame) -> pd.DataFrame:
    dim = (
        silver[["store_id", "store_name", "region"]]
        .drop_duplicates(subset=["store_id"])
        .sort_values("store_id")
        .reset_index(drop=True)
    )
    return dim


def build_dim_date(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    days = pd.date_range(start.normalize(), end.normalize(), freq="D")
    return pd.DataFrame(
        {
            "date_key": days.strftime("%Y%m%d").astype(int),
            "date": days,
            "year": days.year,
            "quarter": days.quarter,
            "month": days.month,
            "day": days.day,
            "day_of_week": days.day_name(),
            "is_weekend": days.day_of_week >= 5,
        }
    )


def build_fact(silver: pd.DataFrame) -> pd.DataFrame:
    fact = silver.copy()
    fact["date_key"] = fact["order_date"].dt.strftime("%Y%m%d").astype(int)
    fact["gross_amount"] = (fact["quantity"] * fact["unit_price"]).round(2)

    columns = [
        "order_id",
        "date_key",
        "product_id",
        "store_id",
        "quantity",
        "unit_price",
        "gross_amount",
        "currency",
    ]
    return fact[[c for c in columns if c in fact.columns]].reset_index(drop=True)


def run(settings: Settings) -> dict:
    silver = read_table(settings.table_uri("silver", "orders"), settings)
    if silver.empty:
        raise ValueError("silver is empty - refusing to build an empty star schema")

    dim_product = build_dim_product(silver)
    dim_store = build_dim_store(silver)
    dim_date = build_dim_date(silver["order_date"].min(), silver["order_date"].max())
    fact = build_fact(silver)

    write_table(dim_product, settings.table_uri("gold", "dim_product"), settings)
    write_table(dim_store, settings.table_uri("gold", "dim_store"), settings)
    write_table(dim_date, settings.table_uri("gold", "dim_date"), settings)
    write_table(fact, settings.table_uri("gold", FACT_TABLE), settings, partition_by=["date_key"])

    log.info(
        "gold: fact=%s products=%s stores=%s days=%s",
        len(fact),
        len(dim_product),
        len(dim_store),
        len(dim_date),
    )
    return {
        "layer": "gold",
        "fact_rows": len(fact),
        "products": len(dim_product),
        "stores": len(dim_store),
        "days": len(dim_date),
    }
