---
date: 2026-05-30
categories:
  - Architecture
authors:
  - datacoolie
description: "DataCoolie vs Airflow and Prefect — how an ETL execution framework differs from workflow orchestrators. When to use each, and how to run DataCoolie inside an Airflow DAG."
---

# DataCoolie vs Airflow / Prefect — ETL Framework vs Orchestrator

DataCoolie and Airflow (or Prefect) operate at different levels of the data
stack. Airflow and Prefect are **workflow orchestrators** — they schedule tasks,
manage dependencies, and handle retries across an entire pipeline graph.
DataCoolie is an **ETL execution framework** — it handles the read → transform
→ write → watermark lifecycle inside each individual task.

This post explains the difference, when to use each, and how they work together.

<!-- more -->

## What Airflow and Prefect Do

[Apache Airflow](https://airflow.apache.org/) and
[Prefect](https://www.prefect.io/) are workflow orchestration platforms. You
define tasks as a directed acyclic graph (DAG), and the orchestrator:

- **Schedules** runs on a cron or event trigger
- **Orders** tasks based on declared dependencies
- **Retries** failed tasks with configurable backoff
- **Monitors** run history, SLAs, and alerting

Orchestrators do not care *what* each task does. A task might run a SQL query,
call an API, train a model, or execute an ETL pipeline. The orchestrator
manages *when* and *in what order* tasks run.

## What DataCoolie Does

[DataCoolie](../../index.md) handles what happens *inside* a single ETL task.
Given a [dataflow](../../concepts/metadata-model.md) defined in metadata, it:

1. **Reads** from a source ([connection](../../concepts/metadata-model.md))
2. **Applies** [transforms](../../concepts/transformers-and-pipeline.md) —
   schema hints, deduplication, computed columns, filters
3. **Writes** to a destination using a
   [load strategy](../../concepts/load-strategies.md) — append, merge, SCD2
4. **Updates** the [watermark](../../concepts/watermarks.md) so the next run
   picks up only new data

DataCoolie does not schedule jobs, manage cross-task dependencies, or send
alerts. It is the engine that runs inside a scheduled task.

## Key Differences

| Aspect | Airflow / Prefect | DataCoolie |
|--------|-------------------|------------|
| **Scope** | Workflow orchestration (DAG scheduling) | ETL execution (read → transform → write) |
| **Scheduling** | Built-in cron, sensors, event triggers | None — runs when called |
| **Task dependencies** | DAG-based ordering across tasks | Single-task execution |
| **Data processing** | Delegates to external tools | Native DataFrame processing (Polars / Spark) |
| **Retry model** | Task-level retry with backoff | Idempotent re-execution from watermark |
| **Load strategies** | Not applicable | append, full_load, merge_upsert, merge_overwrite, scd2 |
| **Multi-engine** | Not applicable | Same metadata on Polars and Spark |
| **Alerting** | Built-in | External (relies on orchestrator or logging) |

## Using Them Together

DataCoolie is designed to run *inside* an orchestrator. The most common pattern:

1. **Airflow** (or Prefect) schedules the DAG and manages dependencies
2. **DataCoolie** executes each ETL stage as a task within the DAG
3. DataCoolie's [watermarks](../../concepts/watermarks.md) handle incremental
   state; Airflow handles scheduling and alerting

### Example: DataCoolie Inside an Airflow DAG

```python
# dags/datacoolie_pipeline.py  (pseudocode — adapt to your environment)
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_datacoolie_stage(stage: str) -> None:
    """Execute a DataCoolie stage."""
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.platforms.local_platform import LocalPlatform
    from datacoolie.metadata.file_provider import FileProvider
    from datacoolie.orchestration.driver import DataCoolieDriver

    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    provider = FileProvider(
        config_path="/opt/pipelines/metadata.json",
        platform=platform,
    )

    with DataCoolieDriver(engine=engine, metadata_provider=provider) as driver:
        result = driver.run(stage=stage)
        if result.failed > 0:
            raise RuntimeError(
                f"Stage {stage}: {result.failed}/{result.total} dataflows failed"
            )


with DAG(
    "datacoolie_pipeline",
    schedule="0 6 * * *",  # daily at 06:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    bronze = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_datacoolie_stage,
        op_args=["bronze2silver"],
    )

    silver = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_datacoolie_stage,
        op_args=["silver2gold"],
    )

    bronze >> silver  # silver runs after bronze completes
```

This DAG runs two DataCoolie stages in sequence. Airflow handles scheduling,
retries, and alerting. DataCoolie handles the actual data processing —
reading, transforming, writing, and tracking watermarks.

!!! note "Pseudocode"
    The example above is simplified. In production, you would configure
    the platform and engine based on your deployment environment (Fabric,
    Databricks, AWS) and pass metadata paths via Airflow Variables or
    connections.

## When to Use Airflow / Prefect Alone

An orchestrator without DataCoolie is fine when:

- Your tasks are **simple SQL queries** or **API calls** that do not need
  DataFrame processing
- You use **dbt** for transformations and just need scheduling
- Each task is a self-contained script with no shared metadata model

## When to Add DataCoolie

Add DataCoolie when:

- You need **multi-source ingestion** (files, databases, APIs) with consistent
  load strategies
- You want **engine portability** — same pipeline on Polars for dev, Spark for
  prod
- You need **merge/upsert or SCD2** load strategies with automatic watermark
  tracking
- You want a **declarative metadata model** instead of imperative task code

## Summary

| Question | Answer |
|----------|--------|
| Do they compete? | No — different layers (orchestration vs execution) |
| Can I use both? | Yes — Airflow schedules, DataCoolie executes |
| Do I need Airflow? | Not required — DataCoolie runs standalone or inside any scheduler |
| Do I need DataCoolie? | Not required — Airflow works with any task code |
