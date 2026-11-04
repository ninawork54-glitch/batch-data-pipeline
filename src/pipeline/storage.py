"""Thin wrapper around delta-rs so the layers never touch storage details."""

from __future__ import annotations

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from .config import Settings


def storage_options(settings: Settings) -> dict[str, str]:
    """Translate pipeline settings into delta-rs object-store options."""
    if not settings.on_s3:
        return {}

    options: dict[str, str] = {
        "AWS_REGION": settings.s3_region,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if settings.s3_endpoint:
        options["AWS_ENDPOINT_URL"] = settings.s3_endpoint
        # MinIO speaks plain HTTP by default.
        options["AWS_ALLOW_HTTP"] = "true"
    if settings.s3_access_key:
        options["AWS_ACCESS_KEY_ID"] = settings.s3_access_key
    if settings.s3_secret_key:
        options["AWS_SECRET_ACCESS_KEY"] = settings.s3_secret_key
    return options


def write_table(
    df: pd.DataFrame,
    uri: str,
    settings: Settings,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    allow_empty: bool = False,
) -> str:
    """Write a dataframe as a Delta table and return the URI."""
    if df.empty and not allow_empty:
        raise ValueError(f"refusing to overwrite {uri} with an empty dataframe")

    write_deltalake(
        uri,
        df,
        mode=mode,
        partition_by=partition_by,
        storage_options=storage_options(settings),
    )
    return uri


def read_table(uri: str, settings: Settings) -> pd.DataFrame:
    """Read a Delta table into a pandas dataframe."""
    return DeltaTable(uri, storage_options=storage_options(settings)).to_pandas()


def table_exists(uri: str, settings: Settings) -> bool:
    try:
        DeltaTable(uri, storage_options=storage_options(settings))
    except TableNotFoundError:
        return False
    return True
