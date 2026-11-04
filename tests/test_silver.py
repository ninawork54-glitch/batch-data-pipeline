"""Silver owns casting, quarantine, and dedupe."""

from __future__ import annotations

import pandas as pd

from pipeline.silver import dedupe, prepare, quarantine_reason, split


def test_prepare_casts_dates_and_numbers(bronze_frame):
    out = prepare(bronze_frame)

    assert str(out["order_date"].dtype).startswith("datetime64")
    assert out["quantity"].dtype.kind in "if"
    assert out["unit_price"].dtype.kind in "if"


def test_prepare_rejects_missing_columns(bronze_frame):
    import pytest

    broken = bronze_frame.drop(columns=["unit_price"])
    with pytest.raises(ValueError, match="missing required columns"):
        prepare(broken)


def test_quarantine_reports_first_failing_rule(bronze_frame):
    df = bronze_frame.copy()
    df.loc[0, "quantity"] = "-1"
    df.loc[1, "order_date"] = "not-a-date"
    df.loc[2, "order_id"] = ""

    reasons = quarantine_reason(prepare(df))

    assert reasons.iloc[0] == "non_positive_quantity"
    assert reasons.iloc[1] == "unparseable_order_date"
    assert reasons.iloc[2] == "missing_order_id"


def test_quarantine_flags_non_numeric_price(bronze_frame):
    df = bronze_frame.copy()
    df.loc[0, "unit_price"] = ""

    reasons = quarantine_reason(prepare(df))

    assert reasons.iloc[0] == "non_numeric_unit_price"
    assert reasons.iloc[1:].isna().all()


def test_split_keeps_valid_rows_and_labels_rejects(bronze_frame):
    df = bronze_frame.copy()
    df.loc[0, "unit_price"] = "-5"

    valid, rejected = split(prepare(df))

    assert len(valid) == 2
    assert len(rejected) == 1
    assert rejected["_quarantine_reason"].iloc[0] == "negative_unit_price"


def test_dedupe_keeps_the_latest_ingest(bronze_frame):
    duplicate = bronze_frame.iloc[[0]].assign(
        quantity="9",
        _ingested_at="2024-02-02T00:00:00+00:00",
    )
    df = pd.concat([bronze_frame, duplicate], ignore_index=True)

    valid, _ = split(prepare(df))
    out = dedupe(valid)

    assert len(out) == 3
    assert out.loc[out["order_id"] == "ORD-1", "quantity"].iloc[0] == 9


def test_dedupe_is_stable_when_ingest_times_match(bronze_frame):
    df = pd.concat([bronze_frame, bronze_frame], ignore_index=True)

    valid, _ = split(prepare(df))
    out = dedupe(valid)

    assert len(out) == 3
