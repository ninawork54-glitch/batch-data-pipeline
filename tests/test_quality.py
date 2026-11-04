"""The quality gate is the last thing between a bad load and a dashboard."""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.gold import build_dim_date, build_dim_product, build_dim_store, build_fact
from pipeline.quality import (
    QualityError,
    assert_all,
    foreign_key,
    min_rows,
    non_negative,
    not_null,
    run_all,
    unique,
)
from pipeline.silver import prepare, split
from pipeline.storage import read_table, write_table


def _seed_gold(settings, frame) -> None:
    valid, _ = split(prepare(frame))

    write_table(build_dim_product(valid), settings.table_uri("gold", "dim_product"), settings)
    write_table(build_dim_store(valid), settings.table_uri("gold", "dim_store"), settings)
    write_table(
        build_dim_date(valid["order_date"].min(), valid["order_date"].max()),
        settings.table_uri("gold", "dim_date"),
        settings,
    )
    write_table(build_fact(valid), settings.table_uri("gold", "fact_sales"), settings)


def test_all_checks_pass_on_clean_data(settings, bronze_frame):
    _seed_gold(settings, bronze_frame)

    checks = assert_all(settings)

    assert len(checks) == 13
    assert all(check.passed for check in checks)


def test_duplicate_order_fails_the_gate(settings, bronze_frame):
    _seed_gold(settings, bronze_frame)
    uri = settings.table_uri("gold", "fact_sales")

    fact = read_table(uri, settings)
    write_table(pd.concat([fact, fact.iloc[[0]]], ignore_index=True), uri, settings)

    with pytest.raises(QualityError, match="unique"):
        assert_all(settings)


def test_orphan_product_fails_referential_integrity(settings, bronze_frame):
    _seed_gold(settings, bronze_frame)
    uri = settings.table_uri("gold", "fact_sales")

    fact = read_table(uri, settings)
    fact.loc[0, "product_id"] = "P999"
    write_table(fact, uri, settings)

    with pytest.raises(QualityError, match="orphan"):
        assert_all(settings)


def test_negative_amount_fails_the_gate(settings, bronze_frame):
    _seed_gold(settings, bronze_frame)
    uri = settings.table_uri("gold", "fact_sales")

    fact = read_table(uri, settings)
    fact.loc[0, "gross_amount"] = -10.0
    write_table(fact, uri, settings)

    with pytest.raises(QualityError, match="non_negative"):
        assert_all(settings)


def test_run_all_reports_one_check_per_rule(settings, bronze_frame):
    _seed_gold(settings, bronze_frame)

    names = [check.name for check in run_all(settings)]

    assert "fact_sales.order_id unique" in names
    assert "fact_sales.product_id -> dim_product.product_id" in names


def test_check_helpers_are_pure():
    df = pd.DataFrame({"k": ["a", "a", None], "v": [1, -2, 3]})

    assert not not_null(df, "k", "t").passed
    assert not unique(df, "k", "t").passed
    assert not non_negative(df, "v", "t").passed
    assert min_rows(df, 2, "t").passed
    assert not min_rows(df, 10, "t").passed


def test_foreign_key_detects_orphans():
    fact = pd.DataFrame({"pid": ["P1", "P9"]})
    dim = pd.DataFrame({"pid": ["P1", "P2"]})

    assert not foreign_key(fact, dim, "pid", "pid", "dim_product").passed
