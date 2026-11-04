"""Data-quality gate. Runs after gold, before publish.

A failed check raises `QualityError`, which fails the Airflow task and leaves the
previous good version in Postgres untouched.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from .config import Settings
from .gold import FACT_TABLE
from .storage import read_table

log = logging.getLogger(__name__)


class QualityError(RuntimeError):
    """Raised when one or more checks fail."""


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}: {self.detail}"


def not_null(df: pd.DataFrame, column: str, table: str) -> Check:
    nulls = int(df[column].isna().sum())
    return Check(
        name=f"{table}.{column} not_null",
        passed=nulls == 0,
        detail=f"{nulls} nulls",
    )


def unique(df: pd.DataFrame, column: str, table: str) -> Check:
    duplicates = int(df[column].duplicated().sum())
    return Check(
        name=f"{table}.{column} unique",
        passed=duplicates == 0,
        detail=f"{duplicates} duplicate values",
    )


def foreign_key(
    fact: pd.DataFrame, dim: pd.DataFrame, fact_column: str, dim_column: str, dim_name: str
) -> Check:
    orphans = int((~fact[fact_column].isin(dim[dim_column])).sum())
    return Check(
        name=f"{FACT_TABLE}.{fact_column} -> {dim_name}.{dim_column}",
        passed=orphans == 0,
        detail=f"{orphans} orphan rows",
    )


def non_negative(df: pd.DataFrame, column: str, table: str) -> Check:
    negative = int((df[column] < 0).sum())
    return Check(
        name=f"{table}.{column} non_negative",
        passed=negative == 0,
        detail=f"{negative} negative values",
    )


def min_rows(df: pd.DataFrame, minimum: int, table: str) -> Check:
    return Check(
        name=f"{table} min_rows",
        passed=len(df) >= minimum,
        detail=f"{len(df)} rows, minimum {minimum}",
    )


def run_all(settings: Settings) -> list[Check]:
    fact = read_table(settings.table_uri("gold", FACT_TABLE), settings)
    dim_product = read_table(settings.table_uri("gold", "dim_product"), settings)
    dim_store = read_table(settings.table_uri("gold", "dim_store"), settings)
    dim_date = read_table(settings.table_uri("gold", "dim_date"), settings)

    checks = [
        min_rows(fact, 1, FACT_TABLE),
        not_null(fact, "order_id", FACT_TABLE),
        not_null(fact, "gross_amount", FACT_TABLE),
        unique(fact, "order_id", FACT_TABLE),
        non_negative(fact, "quantity", FACT_TABLE),
        non_negative(fact, "unit_price", FACT_TABLE),
        non_negative(fact, "gross_amount", FACT_TABLE),
        unique(dim_product, "product_id", "dim_product"),
        unique(dim_store, "store_id", "dim_store"),
        unique(dim_date, "date_key", "dim_date"),
        foreign_key(fact, dim_product, "product_id", "product_id", "dim_product"),
        foreign_key(fact, dim_store, "store_id", "store_id", "dim_store"),
        foreign_key(fact, dim_date, "date_key", "date_key", "dim_date"),
    ]
    return checks


def assert_all(settings: Settings) -> list[Check]:
    checks = run_all(settings)
    for check in checks:
        log.info("%s", check)

    failures = [c for c in checks if not c.passed]
    if failures:
        summary = "; ".join(str(c) for c in failures)
        raise QualityError(f"{len(failures)} quality check(s) failed: {summary}")

    log.info("quality: %s checks passed", len(checks))
    return checks


def run(settings: Settings) -> dict:
    checks = assert_all(settings)
    return {"layer": "quality", "checks": len(checks), "passed": len(checks)}
