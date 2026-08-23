---
title: ADR-0009 — Use-Case-Oriented Optional Dependencies | DataCoolie
description: Why DataCoolie publishes composable engine, platform, source, and metadata extras instead of platform matrix bundles.
---

# ADR-0009 — Use-Case-Oriented optional dependencies

**Status** · Accepted

## Context

DataCoolie supports local files, AWS and S3-compatible storage, Fabric,
Databricks, Polars, Spark, multiple lakehouse formats, and several metadata or
source connectors. A platform × engine × source matrix would duplicate
dependency lists and make each new connector a packaging breaking change.
Native Fabric and Databricks runtimes also provide `notebookutils`, `dbutils`,
Spark, and cloud connectors; installing Python stand-ins for those runtime
objects is both misleading and unreliable.

## Decision

- Publish extras by capability: engine (`polars`, `spark`), engine-format
  profiles (`polars-delta`, `spark-delta`, `polars-iceberg`), platform runtime
  (`aws`, `fabric-external`, `databricks-external`), source, and metadata.
- Keep profiles composable. A complete pipeline selects the engine/format,
  platform SDK, and source or metadata connector it actually uses.
- Keep one `aws` profile for AWS and S3-compatible endpoints such as MinIO;
  boto3 is the same client boundary in both cases.
- Keep native Fabric and Databricks on the base package plus host-provided
  runtime libraries. External Python processes install the corresponding
  `*-external` profile.
- Define `all` as the union of every published Python dependency and do not
  retain aliases for removed extras.

## Consequences

Consumers install only what their process needs and can migrate a pipeline by
changing a platform profile without changing its engine or source profile.
The old matrix extra names are intentionally removed, so release notes and
installation docs must use the new contract. Spark Iceberg remains a runtime,
JAR, and catalog configuration concern rather than a Python-only extra.

`pyproject.toml` is the source of truth for both PEP 621 metadata and Poetry's
optional dependency declarations; the packaging contract test verifies that
each profile is declared and that `all` has no omissions or duplicates.

## Related

- [Installation](../getting-started/installation.md)
- [Platforms](../concepts/platforms.md)
- Implementation plan: `plans/260821-packaging-usecase-extras/plan.md`
