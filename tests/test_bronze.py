"""Bronze keeps the source bytes and adds lineage. It must not clean anything."""

from __future__ import annotations

import pandas as pd

from pipeline.bronze import read_raw_csv, run
from pipeline.storage import read_table, table_exists


def _write_csv(settings, name: str = "orders_2024_01.csv") -> None:
    pd.DataFrame(
        {
            "order_id": ["ORD-1", "ORD-2"],
            "order_date": ["2024-01-01", "2024-01-02"],
            "store_id": ["S01", "S01"],
            "store_name": ["Austin Central", "Austin Central"],
            "region": ["South", "South"],
            "product_id": ["P001", "P002"],
            "product_name": ["Trail Runner", "City Walker"],
            "category": ["Footwear", "Footwear"],
            "quantity": ["2", "1"],
            "unit_price": ["120.00", "89.50"],
            "currency": ["USD", "USD"],
        }
    ).to_csv(settings.raw_dir / name, index=False)


def test_read_raw_csv_keeps_values_as_text(settings):
    _write_csv(settings)
    df = read_raw_csv(settings.raw_dir / "orders_2024_01.csv")

    assert df["quantity"].tolist() == ["2", "1"]
    assert df["_source_file"].iloc[0] == "orders_2024_01.csv"
    assert df["_ingested_at"].notna().all()


def test_read_raw_csv_preserves_dirty_values(settings):
    """A malformed price must survive bronze untouched."""
    _write_csv(settings)
    path = settings.raw_dir / "orders_2024_01.csv"
    text = path.read_text().replace("89.50", "")
    path.write_text(text)

    df = read_raw_csv(path)
    assert "" in df["unit_price"].tolist()


def test_run_writes_bronze_table(settings):
    _write_csv(settings)
    result = run(settings)

    assert result["rows"] == 2
    assert result["files"] == 1

    uri = settings.table_uri("bronze", "orders")
    assert table_exists(uri, settings)
    assert len(read_table(uri, settings)) == 2


def test_run_fails_loudly_with_no_source_files(settings):
    import pytest

    with pytest.raises(FileNotFoundError):
        run(settings)
