---
date: 2026-05-30
categories:
  - Architecture
authors:
  - datacoolie
description: "DataCoolie vs dbt — how a metadata-driven ETL framework compares to a SQL-first transformation tool. When to use each, key differences, and how they work together."
---

# DataCoolie vs dbt — ETL Framework vs SQL Transforms

DataCoolie and dbt solve different problems in the data stack. dbt transforms
data that is already in your warehouse using SQL models. DataCoolie handles the
full ETL lifecycle — extracting data from sources, transforming it with
Python-native engines, and loading it into lakehouses or warehouses.

This post compares the two fairly, explains when each tool fits best, and shows
how they complement each other.

<!-- more -->

## What dbt Does

[dbt](https://www.getdbt.com/) (data build tool) is a SQL-first transformation
framework. You write SELECT statements as models, and dbt materializes them as
tables or views in your warehouse (Snowflake, BigQuery, Redshift, Databricks
SQL). dbt handles dependency ordering, testing, documentation, and incremental
materializations.

dbt assumes data is already loaded into the warehouse. It does not extract data
from external sources or write to file-based storage. Its strength is turning
raw warehouse tables into clean, tested, documented analytics models.

## What DataCoolie Does

[DataCoolie](../../index.md) is a metadata-driven ETL framework for Python. You
define [connections](../../concepts/metadata-model.md),
[dataflows](../../concepts/metadata-model.md), and
[transforms](../../concepts/transformers-and-pipeline.md) as JSON, YAML, or
Excel metadata, and the framework executes the full read → transform → write →
[watermark](../../concepts/watermarks.md) cycle.

DataCoolie runs on [Polars or Spark](../../concepts/engines.md) and deploys to
local, AWS, Microsoft Fabric, or Databricks
[platforms](../../concepts/platforms.md) without code changes. It handles
[load strategies](../../concepts/load-strategies.md) like append, merge/upsert,
and SCD Type 2 natively.

## Key Differences

| Aspect | dbt | DataCoolie |
|--------|-----|------------|
| **Focus** | Transform layer (T in ELT) | Full ETL lifecycle (E + T + L) |
| **Language** | SQL | Python (metadata-driven) |
| **Execution** | Runs SQL in the warehouse engine | Runs DataFrames on Polars or Spark |
| **Data sources** | Warehouse tables only | Files, databases, APIs, lakehouses |
| **Load strategies** | Incremental models, snapshots | append, full_load, merge_upsert, merge_overwrite, scd2 |
| **Orchestration** | External (Airflow, dbt Cloud) | External (Airflow, Fabric, cron) |
| **Schema management** | Tests + contracts | Schema hints + type casting |
| **Multi-engine** | Single warehouse per project | Same metadata runs on Polars and Spark |

## When to Use dbt

dbt is the right choice when:

- Your data is **already in a warehouse** (Snowflake, BigQuery, Redshift)
- Your team is **SQL-first** and prefers writing transformations as SELECT
  statements
- You need **model documentation and testing** integrated into the transform
  layer
- Your warehouse handles compute, and you want to push all processing to it

## When to Use DataCoolie

DataCoolie is the right choice when:

- You need to **extract data** from files, databases, or APIs — not just
  transform what is already loaded
- You want **engine portability** — develop locally on Polars, deploy to Spark
  in production
- You need **platform portability** — the same pipeline runs on local, AWS,
  Fabric, or Databricks
- Your pipelines use **merge/upsert or SCD2** load strategies that go beyond
  simple INSERT/UPDATE
- You prefer **Python-native** DataFrame processing over SQL-only transforms

## Using Them Together

DataCoolie and dbt are complementary, not competing. A common architecture:

1. **DataCoolie** ingests raw data from sources (files, databases, APIs) into a
   bronze or silver lakehouse layer
2. **dbt** transforms the silver/gold layer inside a SQL warehouse, building
   analytics models, aggregations, and business logic

This gives you the best of both: DataCoolie's multi-source ingestion and
engine portability for the ETL layer, and dbt's SQL modeling, testing, and
documentation for the analytics layer.

## Summary

| Question | Answer |
|----------|--------|
| Do they compete? | No — different layers of the stack |
| Can I use both? | Yes — DataCoolie for ingestion, dbt for SQL transforms |
| Which is simpler? | dbt if you are SQL-only; DataCoolie if you need full ETL |
| Which scales better? | Depends on the layer — dbt scales with your warehouse; DataCoolie scales with Polars/Spark |
