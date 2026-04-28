# Concepts

Explanation-tier documentation. Each page answers **"why does DataCoolie work
this way?"** rather than "how do I do X?". For task recipes see
[How-to](../how-to/index.md); for precise field-level contracts see
[Reference](../reference/index.md).

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
