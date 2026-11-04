"""Airflow DAG: sales medallion pipeline.

Task bodies import the pipeline package inside the callable rather than at module
level. That keeps DAG parsing alive even when a dependency is missing, so the UI
shows an import error on the task instead of an empty DAG list.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

DOC = """
### Sales medallion

CSV exports land in `data/raw`, then move through four layers:

1. **bronze** - raw strings plus `_source_file` and `_ingested_at`
2. **silver** - typed, deduped on `order_id`, invalid rows quarantined with a reason
3. **gold** - `fact_sales` plus `dim_product`, `dim_store`, `dim_date`
4. **quality** - not-null, unique, foreign-key, non-negative, row-count checks

A failed quality check stops the run before anything is published, so BI keeps
reading the last good version.

Rerunning any date is safe: every layer overwrites from its upstream.
"""


@dag(
    dag_id="sales_medallion",
    description="CSV -> Delta bronze -> silver -> gold star schema -> quality gate -> Postgres",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["sales", "medallion", "delta"],
    doc_md=DOC,
)
def sales_medallion():
    @task
    def bronze_layer() -> dict:
        from pipeline.bronze import run
        from pipeline.config import Settings

        return run(Settings.from_env())

    @task
    def silver_layer() -> dict:
        from pipeline.config import Settings
        from pipeline.silver import run

        return run(Settings.from_env())

    @task
    def gold_layer() -> dict:
        from pipeline.config import Settings
        from pipeline.gold import run

        return run(Settings.from_env())

    @task
    def quality_gate() -> dict:
        from pipeline.config import Settings
        from pipeline.quality import run

        return run(Settings.from_env())

    @task
    def publish_marts() -> dict:
        from pipeline.config import Settings
        from pipeline.publish import run

        return run(Settings.from_env())

    bronze_layer() >> silver_layer() >> gold_layer() >> quality_gate() >> publish_marts()


sales_medallion()
