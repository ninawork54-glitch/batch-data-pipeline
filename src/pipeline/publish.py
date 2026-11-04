"""Publish gold tables to Postgres so a BI tool can read them.

Deliberately optional: with no WAREHOUSE_DSN the task is a no-op. That keeps local
runs and CI dependency-free while the Docker stack gets real marts.
"""

from __future__ import annotations

import logging
import os

from .config import Settings
from .gold import DIM_TABLES, FACT_TABLE
from .storage import read_table

log = logging.getLogger(__name__)

SCHEMA = "analytics"


def run(settings: Settings) -> dict:
    dsn = os.getenv("WAREHOUSE_DSN")
    if not dsn:
        log.info("publish: WAREHOUSE_DSN not set, skipping")
        return {"layer": "publish", "skipped": True}

    from sqlalchemy import create_engine, text

    engine = create_engine(dsn)
    tables = (*DIM_TABLES, FACT_TABLE)

    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

    for table in tables:
        df = read_table(settings.table_uri("gold", table), settings)
        df.to_sql(table, engine, schema=SCHEMA, if_exists="replace", index=False)
        log.info("publish: %s.%s <- %s rows", SCHEMA, table, len(df))

    return {"layer": "publish", "skipped": False, "tables": len(tables)}
