---
title: Architecture — DataCoolie Concepts
description: Understand the DataCoolie architecture, plugin roles, control flow, and how engines, platforms, metadata, logs, and watermarks interact during a run.
video:
  name: "DataCoolie Architecture Explained: Metadata-Driven ETL Framework"
  description: "A short visual overview of DataCoolie's metadata, driver, engine, platform, source, transformer, destination, watermark and logger boundaries."
  thumbnail_url: https://i.ytimg.com/vi/x_z8J92NM4w/hqdefault.jpg
  upload_date: "2026-06-24T02:28:19-07:00"
  duration: PT3M36S
  embed_url: https://www.youtube-nocookie.com/embed/x_z8J92NM4w
  content_url: https://www.youtube.com/watch?v=x_z8J92NM4w
---

# Architecture

**TL;DR** Think of DataCoolie as a **conductor** (the `DataCoolieDriver`) and an
**orchestra of swappable musicians** (plugins). The conductor reads sheet music
(metadata), tells each musician when to play, and writes the recording
(watermarks + logs). Each extension targets an abstract base class. Built-ins
are registered lazily by the runtime bootstrap when a factory or registry is
first requested, while installed extensions are discovered through Python
**entry points**.

<div class="dc-video-embed">
  <iframe
    src="https://www.youtube-nocookie.com/embed/x_z8J92NM4w"
    title="DataCoolie Architecture Explained: Metadata-Driven ETL Framework"
    loading="lazy"
    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
    referrerpolicy="strict-origin-when-cross-origin"
    allowfullscreen>
  </iframe>
</div>

[Watch the architecture overview on YouTube](https://www.youtube.com/watch?v=x_z8J92NM4w)

## The mental model

![DataCoolie architecture overview](../../images/architecture/datacoolie-architecture.png)

*DataCoolie separates pipeline intent, execution, state, and observability into
explicit boundaries. The Mermaid diagram below provides the same model in a
text-friendly form.*

```mermaid
flowchart LR
    META[("📋 Metadata<br/>JSON · DB · API")] -->|dataflows| DRV
    WM[("🔖 Watermarks")] <-->|last-run state| DRV
    DRV(["🎼 DataCoolieDriver<br/><i>orchestrator</i>"])
    DRV -->|reads| SRC["📥 Source"]
    SRC -->|DataFrame| TP["⚙️ Transformer pipeline"]
    TP -->|DataFrame| DST["📤 Destination"]
    DST -->|rows written| DRV
    ENG[["🧠 Engine<br/>Spark · Polars"]] -.runs on.-> PLT[["☁️ Platform<br/>local · fabric · aws · databricks"]]
    SRC -. uses .-> ENG
    TP  -. uses .-> ENG
    DST -. uses .-> ENG
    DRV --> LOG["📜 Loggers<br/>Execution · System"]

    linkStyle 6,7,8,9 stroke-dasharray: 6 4, stroke-width: 2.25px, stroke-linecap: round;
```

- **Solid arrows** = data flow.
- **Dashed arrows** = "uses / delegates to".
- The **engine** is the only box that knows what a DataFrame *is*; the
  **platform** is the only box that knows what a *filesystem* is.

## The eight roles

| # | Role | Base class | What it decides | Example plugins |
|---|------|------------|-----------------|-----------------|
| 1 | **Metadata provider** | `BaseMetadataProvider` | *Where do dataflow definitions live?* | `file`, `database`, `api` |
| 2 | **Watermark manager** | `BaseWatermarkManager` | *How do we remember where we left off?* | `WatermarkManager` (wraps any metadata provider) |
| 3 | **Engine** | `BaseEngine[DF]` | *What computes the DataFrame?* | `spark`, `polars` |
| 4 | **Platform** | `BasePlatform` | *Where do files, tables, and secrets live?* | `local`, `fabric`, `aws`, `databricks` |
| 5 | **Source reader** | `BaseSourceReader` | *How do we load this format into a DataFrame?* | `delta`, `iceberg`, `csv`, `sql`, `api`, … |
| 6 | **Transformer** | `BaseTransformer` | *How do we shape the DataFrame before writing?* | `column_value_transformer`, `schema_converter`, `deduplicator`, `column_projector`, … |
| 7 | **Destination writer** | `BaseDestinationWriter` | *How do we persist the DataFrame?* | `delta`, `iceberg`, `parquet`, … |
| 8 | **Secret provider** | `BaseSecretProvider` | *Where do connection secrets come from?* | platform-native providers via `local`, `fabric`, `aws`, `databricks` |

Secret resolvers (`BaseSecretResolver`) are companion syntax adapters around
that provider layer, not a ninth execution role. See [Secrets](secrets.md) and
[ADR-0002](../../project/decisions/0002-secret-provider-resolver-split.md).

## Who depends on whom

At the extension boundary, third-party plugins depend on *abstract bases only*
and do not import sibling implementations. Built-in driver wiring may infer
concrete components when callers leave them unset, including `FileProvider`,
`WatermarkManager`, and engine/source/destination backend adapters. That
convenience belongs below the plugin boundary.

```mermaid
flowchart TB
    DRV["DataCoolieDriver"] --> BMP["BaseMetadataProvider"]
    DRV --> BWM["BaseWatermarkManager"]
    DRV --> BENG["BaseEngine[DF]"]
    DRV --> BSR["BaseSourceReader"]
    DRV --> BTR["BaseTransformer"]
    DRV --> BDW["BaseDestinationWriter"]
    DRV --> BSP["BaseSecretProvider"]
    DRV --> LOG["ExecutionLogger / SystemLogger"]
    BENG --> BPLT["BasePlatform"]
    BPLT -. also is .-> BSP
    BSR -. typed by .-> BENG
    BTR -. typed by .-> BENG
    BDW -. typed by .-> BENG
    BWM -->|raw JSON text| BMP

  linkStyle 9,10,11,12 stroke-dasharray: 6 4, stroke-width: 2.25px, stroke-linecap: round;
```

Key invariants:

- **Driver ↔ extension contracts** — third-party wiring uses the base classes;
  built-in defaults may select concrete providers and adapters as described
  above.
- **Engine owns platform control-plane access** — `engine.platform.list_files(...)`
  is the framework path for metadata, log, watermark, and other control-plane
  filesystem operations. Business-data reads and writes use native engine
  connectors/adapters; platform credentials and paths do not replace them.
- **Secret provider is abstract too** — `DataCoolieDriver` accepts an explicit
  `secret_provider`; otherwise it falls back to `engine.platform` because
  `BasePlatform` subclasses `BaseSecretProvider`.
- **Sources, transformers, destinations are generic over the engine DataFrame
  type**, which lets a type checker catch mismatches in correctly typed plugin
  and application code.
- **Watermark manager wraps the metadata provider** — provider returns raw JSON
  text, manager parses `Dict[str, Any]`. See
  [ADR-0004](../../project/decisions/0004-raw-json-watermark-contract.md).

## Runtime flow (one dataflow)

```mermaid
sequenceDiagram
    autonumber
    participant Drv as DataCoolieDriver
    participant MP as MetadataProvider
    participant WM as WatermarkManager
    participant SR as SourceReader
    participant TP as TransformerPipeline
    participant DW as DestinationWriter
    participant EL as ExecutionLogger

    Drv->>MP: get_dataflows(stage)
    MP-->>Drv: List[DataFlow]
    Note over Drv: Resolve secrets → distribute → run in parallel

    loop per dataflow
      Drv->>WM: get_watermark(dataflow_id)
      WM-->>Drv: {last_value: "2026-04-19T…"}
      Drv->>SR: read(source, watermark, operator=">")
      Note over SR: watermark filter → source.filter_expression
      SR-->>Drv: DataFrame (native)
      Drv->>TP: transform(df, dataflow)
      Note over TP: value rules → schema → hashes → dedup → computed columns → row filter → SCD2 → system → partitions → masking → projection → sanitize
      TP-->>Drv: DataFrame (reshaped)
      Drv->>DW: write(df, dataflow)
      DW-->>Drv: DestinationRuntimeInfo
      Drv->>WM: save_watermark(dataflow_id, new_watermark)
      Drv->>EL: log dataflow entry
    end
    Drv->>EL: log job summary
```

Inside the driver, three helpers split the work:

- **`JobDistributor`** — given `(job_num, job_index)`, keeps only the slice of
  dataflows this worker owns. Lets you shard a run across N pods.
- **`ParallelExecutor`** — runs the selected slice through one coordinator pool. `max_workers`
  is the global dataflow concurrency cap for that invocation; grouped scheduling does not create
  nested pools. See [Orchestration](orchestration.md).
- **`RetryHandler`** — wraps each dataflow with configurable retries/backoff.

## Why `BaseEngine[DF]` is generic

`BaseEngine` is parameterised by `DF`, the *native* DataFrame type:

| Engine | `DF` binds to | Why it matters |
|---|---|---|
| `SparkEngine` | `pyspark.sql.DataFrame` | `mypy --strict` sees Spark-only methods (`.withColumn`, …) |
| `PolarsEngine` | `polars.LazyFrame` | `mypy --strict` sees Polars-only methods (`.with_columns`, …) |

Sources, destinations, and transformers carry the same `DF` parameter. This
improves static checking for plugin implementations; the non-generic driver
still relies on registry wiring and runtime contracts, so it is not a universal
compile-time guarantee for arbitrary dynamically loaded combinations.

The `fmt=` parameter on engine methods (`read_table(fmt="delta")`,
`merge_to_table(..., fmt="iceberg")`, `table_exists_by_name(*, fmt="delta")`)
unifies Delta Lake and Apache Iceberg at the engine level. See
[Engines](engines.md) and [ADR-0001](../../project/decisions/0001-engine-fmt-parameter.md).

## Plugin boundary: how swap-ability actually works

The runtime exposes six plugin registries — engine, platform, source,
destination, transformer, and resolver — through the lazy package surface in
[`datacoolie/__init__.py`](https://github.com/datacoolie/datacoolie/blob/main/src/datacoolie/__init__.py).
The registries are constructed and built-ins are registered lazily by
`_bootstrap` when a factory or registry is first requested:

```python
engine_registry:      PluginRegistry[BaseEngine]      = PluginRegistry("datacoolie.engines", BaseEngine)
platform_registry:    PluginRegistry[BasePlatform]    = PluginRegistry("datacoolie.platforms", BasePlatform)
source_registry:      PluginRegistry[BaseSourceReader]      = PluginRegistry("datacoolie.sources", BaseSourceReader)
destination_registry: PluginRegistry[BaseDestinationWriter] = PluginRegistry("datacoolie.destinations", BaseDestinationWriter)
transformer_registry: PluginRegistry[BaseTransformer] = PluginRegistry("datacoolie.transformers", BaseTransformer)
resolver_registry:    PluginRegistry[BaseSecretResolver]    = PluginRegistry("datacoolie.resolvers", BaseSecretResolver)
```

Secret providers are typically supplied by platforms, so there is no separate
provider registry. Metadata providers and watermark managers are constructor-
injected rather than registry types. Resolver plugins handle prefixed
`secrets_ref` sources; the provider role is satisfied by the active platform
unless you inject a different `BaseSecretProvider` into the driver.

`PluginRegistry` also performs lazy entry-point discovery: the first
`.get(...)`, `.list_plugins()`, or `.is_available(...)` call scans the matching
installed entry-point group (`datacoolie.engines`, …). A third-party package
can ship a plugin by declaring:

```toml
[project.entry-points."datacoolie.engines"]
duckdb = "my_pkg.duckdb_engine:DuckDbEngine"
```

…with no import of `datacoolie` at install time. See
[Plugin entry points](../plugin-entry-points.md) for the full
generated table.

## Driver execution modes

The driver supports three execution modes through separate entry points:

| Method | Purpose | Watermark behaviour |
|--------|---------|---------------------|
| `run()` / `run_dataflow()` | Normal incremental ETL | Reads `>` last saved, saves new max after write |
| `run_replay(dataflows, replay)` | Bounded historical re-processing in chunks | Operator `>=` (inclusive); saves per-chunk only when `save_watermark=True` |
| `run_maintenance(connection)` | `OPTIMIZE` / `VACUUM` for lakehouse tables | N/A — no data movement |

Normal ETL loaded from metadata uses `JobDistributor` for active/stage
selection and job sharding, then `ParallelExecutor` and `RetryHandler` for
execution. `run_replay` uses a flat `ParallelExecutor` over the dataflows it is
given and does not apply sharding itself; callers should pass the result of
`load_dataflows(...)` when they need selection and sharding. Maintenance loaded
from metadata is deduplicated and distributed, while an explicitly supplied
`dataflows` list is deduplicated but not re-sharded. Replay adds sequential
chunk iteration *within* each dataflow.

See [Orchestration](orchestration.md) for details on each mode.
