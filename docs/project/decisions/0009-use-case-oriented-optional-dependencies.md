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
  These examples are representative. The complete profile inventory, including
  `cli`, `polars-sql` and `polars-hash`, is owned by the
  [package manifest](https://github.com/datacoolie/datacoolie/blob/main/pyproject.toml).
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

## Verification

The [packaging contract test](https://github.com/datacoolie/datacoolie/blob/main/scripts/tests/test_optional_dependencies.py)
checks profile membership, PEP 621/Poetry parity and the `all` union. Run it
from the contributor checkout as described in
[choosing contributor checks](../contributing.md#choose-checks-for-your-change).
These are declaration checks; installing a profile does not prove that its
backend, JVM runtime or service is qualified.

## Related

- [Installation](../../guide/getting-started/installation.md)
- [Platforms](../../reference/concepts/platforms.md)
- [Manifest and complete profiles](https://github.com/datacoolie/datacoolie/blob/main/pyproject.toml)
