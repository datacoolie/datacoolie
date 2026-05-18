---
title: Maintenance, Vacuum, and Optimize — DataCoolie How-to
description: Run DataCoolie maintenance operations such as optimize and vacuum for Delta and Iceberg destinations.
---

# Maintenance (vacuum / optimize)

**Prerequisites** · Existing Delta or Iceberg destinations.
**End state** · Periodic `OPTIMIZE` / `VACUUM` runs safely in parallel without racing fan-in topologies.

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

This prevents concurrent `OPTIMIZE` calls from racing on the same table — a
common source of commit failures in Delta.

## Retention

`DataCoolieRunConfig.retention_hours` controls `VACUUM` retention (default:
`DEFAULT_RETENTION_HOURS` = 168 hours / 7 days). Pass it when constructing
the driver:

```python
from datacoolie import DataCoolieDriver, DataCoolieRunConfig

driver = DataCoolieDriver(config=DataCoolieRunConfig(retention_hours=72))
```

## How deduplication works

When you call `run_maintenance()`, the driver:

1. Loads all dataflows from metadata (optionally filtered by connection).
2. Deduplicates by **physical destination** — dataflows that share the same
   catalog-qualified table or storage path are collapsed into one.
3. Distributes the deduplicated list via `JobDistributor`.
4. Dispatches `OPTIMIZE` and/or `VACUUM` in parallel (bounded by `max_workers`).

Only the winning dataflow per destination produces a maintenance log row.
This prevents concurrent `OPTIMIZE` calls from racing on the same table — a
common source of commit conflicts in Delta Lake.

## Load maintenance dataflows directly

For advanced control, load and inspect the deduplicated list before running:

```python
flows = driver.load_maintenance_dataflows(connection="bronze", active_only=True)
print(f"Maintenance targets: {len(flows)} unique destinations")
result = driver.run_maintenance(connection="bronze")
```

## CLI

```powershell
python usecase-sim/runner/maintenance.py `
    --connection local_bronze `
    --retention-hours 72
# Omit compact or cleanup steps individually:
#   --no-compact   skip OPTIMIZE
#   --no-cleanup   skip VACUUM
```

## When to schedule maintenance

| Scenario | Recommended interval |
|----------|---------------------|
| High-throughput append (many small files) | Every 1–4 hours |
| Standard daily loads | Once per day (after the load completes) |
| Low-frequency batch | Weekly |

`OPTIMIZE` compacts small files into larger ones for better read performance.
`VACUUM` removes files that are no longer referenced by the Delta/Iceberg log
after the retention period expires.

!!! warning "Do not set retention below the longest running query"
    If a query is reading old files and VACUUM removes them, the query fails.
    The default 168 hours (7 days) is safe for most workloads.

## Related

- [Concepts · Orchestration · Maintenance path](../concepts/orchestration.md#maintenance-path)
