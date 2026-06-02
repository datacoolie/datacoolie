---
title: Concepts — How DataCoolie Works
description: Understand the architecture, metadata model, engine contract, orchestration model, watermarks, logging, and secrets that underpin the DataCoolie ETL framework.
---

# Concepts

Explanation-tier documentation. Each page answers **"why does DataCoolie work
this way?"** rather than "how do I do X?". For task recipes see
[How-to](../how-to/index.md); for precise field-level contracts see
[Reference](../reference/index.md).

## How these concepts connect

DataCoolie's architecture is layered. At the top sits the
[**metadata model**](metadata-model.md) — connections, dataflows, and
transforms defined as JSON, YAML, or Excel. The
[**DataCoolieDriver**](orchestration.md) reads that metadata and coordinates
execution. It delegates data processing to an [**engine**](engines.md) (Polars
or Spark), which uses a [**platform**](platforms.md) (local, AWS, Fabric,
Databricks) for file I/O and secrets. Between read and write, a
[**transformer pipeline**](transformers-and-pipeline.md) applies schema hints,
deduplication, computed columns, and filters. The write step uses a
[**load strategy**](load-strategies.md) — append, merge, or SCD2. Finally,
[**watermarks**](watermarks.md) track progress so the next run picks up only
new data.

Reading in the order below gives you the full picture, but you can jump to any
page independently.

This is the best starting section if you want to understand the workflow,
terminology, and operating model without running code.

Start with [Architecture](architecture.md) if you're new to the framework.
Otherwise jump to the concept you need:

- [Architecture](architecture.md) — component diagram, dependency directions, runtime flow.
- [Engines](engines.md) — `BaseEngine[DF]`, the `fmt` parameter contract, format dispatch.
- [Platforms](platforms.md) — file I/O and secret-provider responsibilities, per-cloud specifics.
- [Metadata model](metadata-model.md) — connections, dataflows, transforms.
- [Metadata providers](metadata-providers.md) — file vs database vs API, picking the right one.
- [Sources & destinations](sources-and-destinations.md) — plugin registries, format → reader mapping.
- [Transformers & pipeline](transformers-and-pipeline.md) — ordering slots, tracking labels.
- [Load strategies](load-strategies.md) — append / overwrite / merge / SCD2.
- [Watermarks](watermarks.md) — raw-JSON contract, `__datetime__` sentinel.
- [Orchestration](orchestration.md) — driver, job distributor, parallel executor, retry handler.
- [Logging](logging.md) — ETL logger vs system logger, `LogPurpose`, partitioning.
- [Secrets](secrets.md) — provider vs resolver, `secrets_ref` schema.

## Related sections

- Need task recipes instead of explanations? → [How-to guides](../how-to/index.md)
- Need exact field names and API signatures? → [Reference](../reference/index.md)
- Want to add a new engine, source, or transformer? → [Extending](../extending/index.md)
- Looking for production guidance? → [Operations](../operations/index.md)
