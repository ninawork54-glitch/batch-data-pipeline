"""Runtime configuration, resolved from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_LAKE_ROOT = PROJECT_ROOT / "data" / "lake"


@dataclass(frozen=True)
class Settings:
    """Everything the pipeline needs to know about where data lives."""

    lake_root: str = str(DEFAULT_LAKE_ROOT)
    raw_dir: Path = DEFAULT_RAW_DIR
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            lake_root=os.getenv("LAKE_ROOT", str(DEFAULT_LAKE_ROOT)),
            raw_dir=Path(os.getenv("RAW_DIR", str(DEFAULT_RAW_DIR))),
            s3_endpoint=os.getenv("S3_ENDPOINT"),
            s3_access_key=os.getenv("S3_ACCESS_KEY"),
            s3_secret_key=os.getenv("S3_SECRET_KEY"),
            s3_region=os.getenv("S3_REGION", "us-east-1"),
        )

    @property
    def on_s3(self) -> bool:
        return self.lake_root.startswith("s3://")

    def table_uri(self, layer: str, name: str) -> str:
        """Resolve a logical table name to a concrete URI.

        Local paths are made absolute so the pipeline behaves the same whether it
        runs from the repo root, from Airflow, or from a test's temp directory.
        """
        if self.on_s3:
            return f"{self.lake_root.rstrip('/')}/{layer}/{name}"
        return str((Path(self.lake_root) / layer / name).resolve())
