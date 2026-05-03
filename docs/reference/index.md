---
title: Reference — DataCoolie Contracts & API
description: Precise field-level contracts for the DataCoolie metadata schema, plugin entry points, CLI, environment variables, and the full Python API reference.
---

# Reference

Precise, mechanical contracts. Use this section when you need exact field names,
types, defaults, or Python signatures. Prose explanations live under
[Concepts](../concepts/index.md); task recipes under [How-to](../how-to/index.md).

Some reference pages are generated only at docs-build time, so you will not see
their source `.md` files checked into `docs/reference/`.

## Configuration contracts

- [Metadata schema](metadata-schema.md) — **generated** from `datacoolie.core.models`.
  Covers the full field-level schema for `Connection`, `Dataflow`, `Transform`,
  schema hints, load strategies, watermark config, and partition config.
- [Plugin entry points](plugin-entry-points.md) — **generated** from `pyproject.toml`.
  Lists all registered entry-point groups for sources, destinations, transformers,
  engines, and secret resolvers.
- [Environment variables](environment-variables.md) — runtime overrides that
  DataCoolie reads from the process environment.
- [CLI](cli.md) — runner scripts available under `usecase-sim/runner/`.

## Python API reference

All packages are rendered directly from docstrings via `mkdocstrings`.

- [Core](api/core.md) — `DataCoolie`, `create_platform`, registry helpers.
- [Engines](api/engines.md) — `BaseEngine[DF]`, `PolarsEngine`, `SparkEngine`.
- [Platforms](api/platforms.md) — `BasePlatform`, `LocalPlatform`, `AWSPlatform`.
- [Sources](api/sources.md) — `BaseSourceReader`, `FileReader`, `APIReader`.
- [Destinations](api/destinations.md) — `BaseDestinationWriter`, `FileWriter`.
- [Transformers](api/transformers.md) — built-in transformer classes and `Pipeline`.
- [Orchestration](api/orchestration.md) — `DataCoolieDriver`, `JobDistributor`, `ParallelExecutor`.
- [Metadata](api/metadata.md) — provider classes and `BaseMetadataProvider`.
- [Watermark](api/watermark.md) — `WatermarkManager` and the raw-JSON contract.
- [Logging](api/logging.md) — `ETLLogger`, `LogPurpose`, `create_etl_logger`.
