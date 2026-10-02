---
title: DataCoolie data pipeline framework
description: DataCoolie is a metadata-driven, multi-engine and multi-platform Python data pipeline framework for SQL and Python dataflows.
---

# DataCoolie data pipeline framework

DataCoolie is a **multi-engine, multi-platform data pipeline framework** for
Python. It keeps pipeline intent in metadata, lets a dataflow use SQL or a
custom Python function, and runs compatible work with Polars or Spark across
Local, AWS, Microsoft Fabric, and Databricks environments.

Metadata-driven ETL is the central mechanism, not the whole product boundary:
the framework also owns preparation, file/API/database metadata providers,
watermarks, replay, maintenance, logging, and execution orchestration. A
project still owns its runner scripts, engine setup, custom table registration,
secrets, and external stage scheduler.

## What it is good at

- Reusing a declarative dataflow contract across a local development engine and
  a production engine.
- Combining multiple source operations into one engine-compatible DataFrame
  through SQL or a Python function.
- Keeping state, logs, query-file resolution, retries, replay and load
  strategies consistent across independently launched jobs.
- Scaling a stage by launching multiple Driver sessions and assigning each one
  a deterministic shard of the dataflows.

Built-in transformers operate on the current DataFrame. That is an intentional
component boundary; it does not limit the complexity of the source query or
the overall pipeline. SQL dialect behavior and supported load operations still
depend on the selected engine, platform, addressing mode and optional extras.

## What it is not

DataCoolie is not a workflow scheduler, a SQL-only transformation compiler, or
a drop-in replacement for a managed declarative pipeline service. An external
orchestrator can start Driver sessions, pass `job_num`/`job_index`, wait for a
stage barrier, and manage credentials. DataCoolie executes the dataflow work
inside each session.

## How it relates to declarative pipeline systems

Apache Spark Declarative Pipelines and Databricks Lakeflow are useful
comparators because they describe graph-oriented datasets and let a managed
runtime handle dependency planning. DataCoolie instead centers an explicit
Driver session and lets an external scheduler decide how many jobs to launch.
Those are different orchestration units, not mutually exclusive scaling
claims: a DataCoolie stage can use several jobs, and each job can use Polars
threads or a Spark cluster. The right choice depends on whether the project
needs DataCoolie's portable runner/provider boundary or a managed declarative
pipeline service.

See the current vendor descriptions for [Spark Declarative
Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
and [Databricks Lakeflow Declarative
Pipelines](https://docs.databricks.com/aws/en/ldp/) before making a platform
decision. These links are comparison context, not a compatibility claim.

## Ecosystem

| Component | Responsibility |
|---|---|
| Framework | Metadata preparation and dataflow execution. |
| CLI | Project initialization, validation, inspection, conversion and builds; it does not run a dataflow or upload a release. |
| Studio | Local-first exploration of project metadata, assets, lineage and run health. |
| AI Skills | Agent workflow, approvals and evidence handling; shared framework guidance should come from these public docs. |

Continue with the [user guide](../guide/index.md), browse the
[examples and templates](../examples/index.md), explore [DataCoolie
Studio](../studio/index.md), or read the [Reference](../reference/index.md).
