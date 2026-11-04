"""Silver: cast types, quarantine untrustworthy rows, and dedupe on the natural key.

The split is deliberate. A row that fails validation is not deleted - it is written
to a quarantine table with a reason, so the row count difference is explainable.
"""

from __future__ import annotations

import logging

import pandas as pd

from .config import Settings
from .storage import read_table, write_table

log = logging.getLogger(__name__)

SILVER_TABLE = "orders"
QUARANTINE_TABLE = "orders_quarantine"

KEY = "order_id"
NUMERIC_COLUMNS = ("quantity", "unit_price")
DATE_COLUMNS = ("order_date",)
REQUIRED_COLUMNS = (
    "order_id",
    "order_date",
    "store_id",
    "product_id",
    "quantity",
    "unit_price",
)


def prepare(raw: pd.DataFrame) -> pd.DataFrame:
    """Cast the columns silver owns. Everything else stays as text."""
    df = raw.copy()

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"source is missing required columns: {missing}")

    for column in DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column], errors="coerce")
    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def quarantine_reason(df: pd.DataFrame) -> pd.Series:
    """First failing rule per row, or NA when the row is valid.

    Order matters: the first match is the reason reported, so the checks run from
    structural (missing key) to semantic (negative price).
    """
    reason = pd.Series(pd.NA, index=df.index, dtype="object")

    checks: list[tuple[pd.Series, str]] = [
        (df[KEY].isna() | (df[KEY].astype(str).str.strip() == ""), "missing_order_id"),
        (df["order_date"].isna(), "unparseable_order_date"),
        (df["quantity"].isna(), "non_numeric_quantity"),
        (df["unit_price"].isna(), "non_numeric_unit_price"),
        (df["quantity"] <= 0, "non_positive_quantity"),
        (df["unit_price"] < 0, "negative_unit_price"),
    ]

    for mask, label in checks:
        mask = mask.fillna(False).astype(bool)
        reason = reason.mask(reason.isna() & mask, label)

    return reason


def split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (valid, rejected). Rejected rows carry `_quarantine_reason`."""
    reason = quarantine_reason(df)

    rejected = df[reason.notna()].copy()
    rejected["_quarantine_reason"] = reason[reason.notna()]

    valid = df[reason.isna()].copy()
    return valid, rejected


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the most recently ingested version of each order."""
    ordered = df.sort_values("_ingested_at", kind="stable")
    return ordered.drop_duplicates(subset=[KEY], keep="last")


def run(settings: Settings) -> dict:
    raw = read_table(settings.table_uri("bronze", "orders"), settings)
    prepared = prepare(raw)
    valid, rejected = split(prepared)
    valid = dedupe(valid)
    valid["order_date"] = valid["order_date"].dt.normalize()

    valid_uri = settings.table_uri("silver", SILVER_TABLE)
    rejected_uri = settings.table_uri("silver", QUARANTINE_TABLE)

    write_table(valid, valid_uri, settings, mode="overwrite")
    write_table(rejected, rejected_uri, settings, mode="overwrite", allow_empty=True)

    log.info(
        "silver: %s valid (from %s bronze), %s quarantined -> %s",
        len(valid),
        len(raw),
        len(rejected),
        valid_uri,
    )
    return {
        "layer": "silver",
        "rows_in": len(raw),
        "rows_out": len(valid),
        "quarantined": len(rejected),
        "uri": valid_uri,
    }
