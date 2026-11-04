# Batch Data Pipeline — Sales Medallion

An end-to-end batch pipeline that lands raw point-of-sale exports, conforms them into a
star schema, gates every run on data-quality checks, and publishes marts for BI.

Orchestrated by Airflow, stored as Delta Lake, queried with DuckDB, published to Postgres
and read by Metabase.

The point of this repo is not the tools. It is the layering: raw data is never overwritten,
bad rows are quarantined instead of dropped silently, and no run is allowed to publish a
table that fails its contract.

```
        +--------------+
        |  data/raw    |  CSV exports (or any S3 prefix)
        +------+-------+
               |  ingest, add lineage columns
        +------v-------+
        |    BRONZE    |  Delta - raw strings, nothing rejected
        +------+-------+
               |  cast, quarantine, dedupe
        +------v-------+
        |    SILVER    |  Delta - typed, one row per order
        +------+-------+
               |  conform to star schema
        +------v-------+
        |     GOLD     |  Delta - dim_product, dim_store, dim_date, fact_sales
        +------+-------+
               |  quality gate -> publish
        +------v-------+
        |   Postgres   |  analytics schema -> Metabase
        +--------------+
```

## Stack and why

| Layer | Choice | Reason |
|---|---|---|
| Orchestration | Airflow 2.9 | Retries, backfills, and dependency handling are the actual job. A cron wrapper would not show anything. |
| Table format | Delta Lake | ACID commits on object storage, schema enforcement, time travel for reprocessing. |
| Compute | DuckDB | Reads Delta/Parquet in-process. No cluster to babysit for a dataset this size. |
| Object store | MinIO | S3 API locally, so the code path is identical to production. |
| Mart serving | Postgres | Metabase does not speak Delta. A small publish step keeps BI decoupled from the lake. |
| Quality | Custom checks | A ~150-line module that says exactly what it validates. Heavier frameworks add a dependency for six checks. |

## Quickstart

Local, no Docker, no cloud account:

```bash
pip install -e ".[dev]"
python scripts/generate_sample_data.py     # writes ~90 days of synthetic orders
python -m pipeline.run all                 # bronze -> silver -> gold -> quality
pytest -q
```

Full stack with Airflow and MinIO:

```bash
docker compose up -d
# Airflow  -> http://localhost:8080  (admin / admin)
# MinIO    -> http://localhost:9001  (minioadmin / minioadmin)
# Metabase -> http://localhost:3000
```

Then trigger the `sales_medallion` DAG.

## Data model

```
            dim_date
               |
  dim_store -- fact_sales -- dim_product
```

| Table | Grain | Notes |
|---|---|---|
| `fact_sales` | one row per order | `gross_amount = quantity * unit_price` |
| `dim_product` | one row per product | SCD-free: latest attributes win |
| `dim_store` | one row per store | region used for rollups |
| `dim_date` | one row per calendar day | generated from the fact date range |

## Quality gate

`pipeline/quality.py` runs before publish. A failed check raises and the DAG stops with
the failing check named in the log.

- `not_null` on every key and measure
- `unique` on every dimension key and on `fact_sales.order_id`
- `foreign_key` from each fact column to its dimension
- `non_negative` on `quantity`, `unit_price`, `gross_amount`
- `min_rows` so a silent empty load cannot pass

## Design decisions

**Strings all the way through bronze.** Bronze stores every column as text and adds
`_source_file` and `_ingested_at`. If the upstream sends `"1,200"` or a blank, bronze keeps
it. Casting in bronze destroys the evidence you need when silver rejects a row.

**Quarantine instead of filter.** Rows that fail validation go to `silver/orders_quarantine`
with a `_quarantine_reason`. Filtering them out makes the row count drop with no trace, and
nobody notices until a stakeholder asks why January is short.

**Dedupe on the natural key, latest ingest wins.** `order_id` is the key; duplicates are
resolved by `_ingested_at`. This makes reruns idempotent, which matters because Airflow
will retry a task and you should be able to rerun a month without double-counting.

**Overwrite, not append, at this scale.** Full refresh of ~100k rows takes seconds and
keeps the model simple. At 100M rows this becomes an incremental merge on `order_date`
and the overwrite disappears.

**Publish is a separate task.** If the quality gate fails, Postgres still holds the last
good version. BI keeps working while you fix the pipeline, which is the behaviour you
want at 2am.

## What I would change at scale

- Incremental silver with a merge on `(order_id, order_date)` instead of full overwrite
- Partition gold `fact_sales` by `date_key` and let Trino/Iceberg handle the serving layer
  instead of copying to Postgres
- Replace the custom checks with dbt tests once the transformation layer moves to dbt
- Add OpenLineage emission so column-level lineage is visible without reading code
- Alert on freshness (`max(_ingested_at)` age) rather than only on task failure

## Layout

```
src/pipeline/     bronze, silver, gold, quality, publish, storage, config
dags/             Airflow DAG
tests/            unit tests for each layer
scripts/          deterministic sample-data generator
docker/           Postgres init for the analytics database
```
