"""Bronze: land raw CSVs exactly as received, plus lineage columns.

Nothing is cast, cleaned, or rejected here. If the upstream file is wrong, bronze
should preserve the evidence of how it was wrong.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import Settings
from .storage import write_table

log = logging.getLogger(__name__)

BRONZE_TABLE = "orders"


def read_raw_csv(path: Path) -> pd.DataFrame:
    """Read a source file without inferring types, and record where it came from."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df["_source_file"] = path.name
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return df


def run(settings: Settings) -> dict:
    files = sorted(settings.raw_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(
            f"no CSV files in {settings.raw_dir} - run scripts/generate_sample_data.py first"
        )

    frames = [read_raw_csv(path) for path in files]
    bronze = pd.concat(frames, ignore_index=True)

    uri = settings.table_uri("bronze", BRONZE_TABLE)
    write_table(bronze, uri, settings, mode="overwrite")

    log.info("bronze: %s rows from %s files -> %s", len(bronze), len(files), uri)
    return {"layer": "bronze", "rows": len(bronze), "files": len(files), "uri": uri}
