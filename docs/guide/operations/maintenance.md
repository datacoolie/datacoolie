---
title: Maintenance, Vacuum, and Optimize — DataCoolie User Guide
description: Run DataCoolie optimize and vacuum maintenance safely for Delta Lake and Apache Iceberg destinations, with retention and execution guidance.
---

# Maintenance (vacuum / optimize)

Maintain existing Delta or Iceberg destinations after inspecting the targets
and the engine capabilities below. Compaction reduces small files; cleanup
removes eligible historical files or snapshots. Choose a retention period
that covers running readers, recovery and any required time travel.

## Invoke

```python
result = driver.run_maintenance(
    connection=["bronze", "silver"],  # optional – filter by connection name
    do_compact=True,                  # run OPTIMIZE (default: True)
    do_cleanup=True,                  # run VACUUM  (default: True)
)
```

## Deduplication

When multiple dataflows write to the **same physical destination** (fan-in),
DataCoolie deduplicates before dispatching maintenance. Only the winning
dataflow emits a maintenance log row; covered dataflows are implicitly
covered.

An inactive dataflow or one referencing an inactive source or destination
connection is skipped with a reason. Inactive candidates cannot displace an
active dataflow targeting the same physical destination. Each blocked
candidate is recorded as skipped; runnable duplicates retain the normal
single-winner behavior.

Deduplication applies within one `run_maintenance()` invocation, before shard
distribution. It is not a lock across Driver sessions or external jobs.
Serialize overlapping maintenance invocations for the same physical target;
different connection names pointing to the same location do not make those
jobs independent. An invalid target or unqualified named backend raises a
configuration error before maintenance dispatch.

Named equality is currently qualified for local PyIceberg 0.12 SQLite file
catalogs and loaded Spark 3.5 catalogs: explicit `spark_catalog.schema.table`
with an in-memory metastore and `spark.sql.caseSensitive=false`, or Iceberg
SparkCatalog/HadoopCatalog with a local warehouse and default file I/O.
Initialize those catalogs through normal native use before maintenance; target
resolution never loads them. Relative/current-catalog Spark names, SQLite memory
catalogs and other profiles remain unqualified. The bounded REST profile below
adds explicit deployment and table-incarnation qualification. Ordinary writes
may use a valid handle without this maintenance qualification.

## Retention

`DataCoolieRunConfig.retention_hours` controls `VACUUM` retention (default:
`DEFAULT_RETENTION_HOURS` = 168 hours / 7 days). Pass it when constructing
the driver:

```python
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.orchestration.driver import DataCoolieDriver

with DataCoolieDriver(
    engine=engine,
    platform=platform,
    metadata_provider=metadata,
    config=DataCoolieRunConfig(retention_hours=168),
) as driver:
    result = driver.run_maintenance(connection="silver")
```

The retention value controls eligibility; engine and catalog capabilities
determine which cleanup steps actually run. Polars Delta calls delta-rs vacuum
with `enforce_retention_duration=False`, so the framework does not enforce a
seven-day minimum. Spark Delta uses the runtime's retention safety checks and
configuration. Keep the 168-hour example until the data owner has chosen a
shorter policy with the required recovery and reader guarantees.

## Engine and format capabilities

| Engine / destination | Compaction | Cleanup and limits |
|---|---|---|
| Polars / Delta path | delta-rs `optimize.compact()` | Vacuum by retention hours; native minimum-retention enforcement is disabled |
| Spark / Delta path or catalog table | Delta `OPTIMIZE` | Delta `VACUUM`; runtime retention checks apply |
| Polars / named Iceberg table | Unsupported requests fail | Snapshot expiration only; orphan removal fails. Expiration updates metadata and does not claim file reclamation |
| Spark / named Iceberg (qualified REST) | Native data-file, position-delete and manifest actions on the prepared table | Native snapshot expiration and orphan removal; requires the verified JVM helper and pinned runtime |
| Spark / named Iceberg (qualified local) | Existing catalog procedures | Existing snapshot-expiration and orphan-removal procedures |

Polars uses paths for Delta and a configured catalog for named Iceberg tables.
Plain file formats such as CSV and Parquet are excluded from metadata-driven
maintenance selection. The built-in maintenance writers call the engine's
default compaction and cleanup actions; inspect the selected engine and catalog
before requesting them.
A supported action with no eligible work succeeds with its observed zero effect.
Unsupported REST requests fail before any selected action runs. Check the final attempt's
`destination.operation_details`, then inspect table
history, snapshots or files for the intended effect.

## REST Iceberg maintenance

REST maintenance requires an operator-attested `RestMaintenanceDeployment`
passed as `maintenance_deployment` to the engine. Supply the physical backend
instance identity, verified route aliases, warehouse, native continuity-sentinel
identifier and UUID, and the executing catalog label. Route strings and catalog
labels alone cannot establish backend equality. The sentinel must already exist
and belong to that warehouse. Authenticated, tenant-routed, prefix-mismatched
and arbitrary REST deployments remain outside this qualified profile.

Polars requires PyIceberg 0.12.0. Spark requires Spark 3.5.9 with the exact
Iceberg Spark 3.5/Scala 2.12 runtime 1.10.1 JAR and an explicitly built helper.
Build the helper before starting Spark with
`python -m datacoolie.engines._spark.iceberg.native_build --output-dir <directory> --runtime-jar <pinned-jar> --spark-jars-dir <spark-jars>`.
Add the resulting `guard.jar` to the session's JARs and pass its `manifest.json`
as `maintenance_guard_manifest` to `SparkEngine`. Runtime verification rejects
stale sources, mismatched artifacts and duplicate classes; it does not download,
compile or modify a borrowed session.
Configure `spark.sql.catalog.<catalog-label>.cache-enabled=false` before creating
the session so the qualified REST catalog exposes the bound native table.

Driver preparation carries catalog and namespace separately from the hydrated
destination. Foreign catalogs are rejected before lookup; namespace components
remain intact even when one equals the catalog label. Direct engine preparation
accepts a two-part `namespace.table` string. For catalog-qualified or deeper
names, pass `MaintenanceTableAddress(catalog, namespace_tuple, table)` as
`table_address`; ambiguous strings with three or more parts are rejected.
Preparation captures the table UUID, and native refresh/commit boundaries reject
a replacement incarnation. Serialize external DDL and overlapping maintenance;
orphan discovery/deletion is not atomic with concurrent external DDL.

Request supported-only Polars expiration with:

```python
result = driver.run_maintenance(
    connection="silver",
    do_compact=False,
    cleanup_options={"expire_snapshots": True, "remove_orphan_files": False},
)
```

`compact_options` accepts `rewrite_data_files`, `rewrite_position_delete_files`
and `rewrite_manifests`; `cleanup_options` accepts the two keys above. Values
must be booleans. `None`, `{}`, and omitted known keys preserve default `True`
within enabled groups. Nonempty options for a disabled group, unknown keys and
nonboolean values fail validation. Setting every action in every enabled group
to `False` succeeds without native effects, with skipped action-group detail.
Missing targets remain skipped. Tagged/branched snapshots and the current
snapshot are protected by native expiration semantics.

Preparation happens once per unique routed candidate within each public call;
aliases deduplicate by deployment scope and UUID before metadata sharding.
`load_maintenance_dataflows()` releases its bindings and returns declarative
metadata. A subsequent run prepares again. Retries retain the execution snapshot
and borrowed binding; the Driver does not close borrowed native sessions.

## How deduplication works

When you call `run_maintenance()`, the driver:

1. Loads all dataflows from metadata (optionally filtered by connection).
2. Separates inactive candidates, then deduplicates runnable dataflows by
   **physical destination** using the executing engine's selected address and
   qualified backend equality. Polars Delta uses its path even with catalog
   metadata; supported Spark catalog targets use their names. Catalog case and
   prefix aliases are collapsed only where native equivalence is qualified.
3. Distributes the deduplicated list via `JobDistributor`.
4. Dispatches `OPTIMIZE` and/or `VACUUM` in parallel (bounded by `max_workers`).

Only the winning runnable dataflow per destination produces a maintenance log row;
inactive candidates produce skipped rows.
This prevents concurrent `OPTIMIZE` calls from racing on the same table — a
common source of commit conflicts in Delta Lake.

## Load maintenance dataflows directly

For advanced control, load and inspect the deduplicated list before running:

```python
flows = driver.load_maintenance_dataflows(connection="bronze", active_only=True)
print(f"Maintenance candidates: {len(flows)}")
result = driver.run_maintenance(dataflows=flows)
```

Review the resolved targets before dispatch. Metadata-driven selection is
restricted to lakehouse destinations; a caller supplying `dataflows` directly
owns that selection. Dry-run validates preparation and records validation
results without executing backend compaction or cleanup.

## Local runner

Download the canonical local Polars maintenance runner
([source](../../examples/source/runners/local/maintenance.py.md) ·
[raw](../../examples/files/runners/local/maintenance.py)) and adapt it to your
project. After reviewing an existing Delta/Iceberg target in your metadata,
run from the project directory:

```powershell
python maintenance.py `
    --metadata-path ./metadata `
    --watermark-base-path ./.runtime/watermarks `
    --log-base-path ./.runtime/logs `
    --connection silver `
    --retention-hours 168 `
    --confirm-maintenance
# Omit compact or cleanup steps individually:
#   --no-compact   skip OPTIMIZE
#   --no-cleanup   skip VACUUM
```

`--metadata-path` accepts a metadata document or directory. The watermark and
log roots are required runner inputs; the watermark root provides FileProvider
state configuration and maintenance does not advance source watermarks.
Use `--working-directory` when relative connection paths belong to another
directory. `--confirm-maintenance` is required by this runner before mutation;
the Python Driver API leaves authorization to the caller. Enabling neither
compaction nor cleanup is rejected by the runner. Its nonzero exit code reports
failed dataflows. This reference runner configures local Delta; REST Iceberg needs
the engine setup described above and an explicit supported action selection.
For Spark hosts, use the [runner examples](../../examples/runners.md).

## Maintenance outcomes and diagnostics

A returned maintenance result must report `succeeded`, `skipped`, or `failed`.
Its status and message survive unavailable diagnostic metrics or timing. Failed
work remains retryable and honors `stop_on_error`; successful work is not
repeated for a capture failure, and skipped work does not retry. Unknown metrics
remain nullable. A typed result with a nonterminal status is a contract error.

## When to schedule maintenance

| Scenario | Recommended interval |
|----------|---------------------|
| High-throughput append (many small files) | Every 1–4 hours |
| Standard daily loads | Once per day (after the load completes) |
| Low-frequency batch | Weekly |

`OPTIMIZE` compacts small files into larger ones for better read performance.
For Delta, `VACUUM` removes eligible unreferenced files. Iceberg cleanup uses
snapshot expiration and, where supported, orphan-file removal; backend behavior
and supported action details must be checked separately.

!!! warning "Do not set retention below the longest running query"
    If a query is reading old files and VACUUM removes them, the query fails.
    The default is 168 hours (7 days). Choose a longer period when required by
    your readers, time-travel or recovery policy.

## Related

- [Concepts · Orchestration · Maintenance path](../../reference/concepts/orchestration.md#maintenance-path)
