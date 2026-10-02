---
title: Platforms and deployment | DataCoolie
description: Choose an engine and deploy DataCoolie runners on Microsoft Fabric, Databricks, or AWS Glue.
---

# Platforms and deployment

Platform pages explain the host-specific decisions around engines, storage
paths, secrets, notebooks, jobs, and deployment packaging. They complement the
portable [runtime and operations guide](../operations/index.md), which owns the
runner lifecycle.

| Platform | Start here when | Guide |
|---|---|---|
| Microsoft Fabric | You run in OneLake notebooks and need to choose Spark or Polars by layer | [Microsoft Fabric](fabric.md) |
| Databricks | You use managed Spark, Unity Catalog, or UC Volumes | [Databricks](databricks.md) |
| AWS Glue | You run managed Spark ETL with S3, Glue Catalog, or Secrets Manager | [AWS Glue](aws-glue.md) |

Start with the [platform smoke download/build/local rehearsal](../../examples/runners.md#platform-smoke),
then follow one leaf guide to upload input and built metadata, choose the
execution identity and pass host parameters. The fixture's overwrite output
must be a fresh sandbox. Generic job success is insufficient: check required
selection, result counts, exact rows/types and any downstream catalog entry.

## Coverage and verification boundary

| Case | Route and boundary |
|---|---|
| Native Spark + file metadata | First-run recipe in all three guides; host adapters tested locally |
| Native Python/Polars | Fabric mount and Databricks file-only branches; single-node and connector limits |
| External SDK + Polars | Canonical Azure, Databricks SDK and AWS S3 runners; control auth differs from engine auth |
| Named table vs path files | Databricks UC alternatives; Glue Iceberg catalog vs S3 Delta; Fabric Files smoke |
| Secrets/database metadata | Optional per-host secrets; [provider setup](../providers/index.md) owns bootstrap |
| Dependencies/runtime | Host Spark reused; classic/serverless and bundled/custom Glue recipes separated |
| Scheduler and failure | Parameter tables, required-selection guards, output/log checks; generic runners allow valid empty shards |
| Functions/replay/maintenance/sharding | [Operations](../operations/index.md) and [canonical runner catalog](../../examples/index.md#runners) |

Local evidence includes metadata validation/build, downloaded-project execution
and exact output checks, simulated host adapters, and generated source/raw/ZIP
pages. It does **not** qualify live cloud access or every serverless/Spark
Connect operation. A branch not executed on its cloud host is not thereby
unsupported. Explicit exclusions include Glue Python Shell's incompatible
Python version, UC table registration on Volume files, and portable Workspace
Files support. EMR provisioning and Jobs API payloads have no repo-owned recipe.

Keep [runtime configuration](../operations/runtime-configuration.md) explicit.
For larger multi-stage scenarios, use the linked simulator/WWI handoffs after
preparing their additional assets and dependencies.
