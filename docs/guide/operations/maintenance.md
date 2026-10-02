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
jobs independent. A target whose identity cannot be resolved is omitted with
a warning, so reconcile the selected destinations with the intended inventory.

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
| Polars / named Iceberg table | Logs a warning and skips compaction | PyIceberg snapshot expiration when supported; expiration errors are logged as warnings. Orphan-file removal logs a warning and is skipped |
| Spark / named Iceberg table | Catalog procedures for data files, position deletes and manifests | Catalog procedures for snapshot expiration and orphan-file removal; requires an Iceberg runtime/catalog supporting those procedures |

Polars uses paths for Delta and a configured catalog for named Iceberg tables.
Plain file formats such as CSV and Parquet are excluded from metadata-driven
maintenance selection. The built-in maintenance writers call the engine's
default compaction and cleanup actions; inspect the selected engine and catalog
before requesting them.
A successful aggregate does not establish that every requested backend action
ran. Check warning logs and `destination_operation_details`, then inspect table
history, snapshots or files for the intended effect.

## How deduplication works

When you call `run_maintenance()`, the driver:

1. Loads all dataflows from metadata (optionally filtered by connection).
2. Separates inactive candidates, then deduplicates runnable dataflows by
   **physical destination** — flows that share the same catalog-qualified
   table or storage path are collapsed into one.
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
failed dataflows; warnings about unsupported backend actions still need review.
For Spark hosts, use the [runner examples](../../examples/runners.md).

## When to schedule maintenance

| Scenario | Recommended interval |
|----------|---------------------|
| High-throughput append (many small files) | Every 1–4 hours |
| Standard daily loads | Once per day (after the load completes) |
| Low-frequency batch | Weekly |

`OPTIMIZE` compacts small files into larger ones for better read performance.
For Delta, `VACUUM` removes eligible unreferenced files. Iceberg cleanup uses
snapshot expiration and, where supported, orphan-file removal; backend behavior
and capability warnings must be checked separately.

!!! warning "Do not set retention below the longest running query"
    If a query is reading old files and VACUUM removes them, the query fails.
    The default is 168 hours (7 days). Choose a longer period when required by
    your readers, time-travel or recovery policy.

## Related

- [Concepts · Orchestration · Maintenance path](../../reference/concepts/orchestration.md#maintenance-path)
