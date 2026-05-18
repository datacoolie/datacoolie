---
title: Run a Stage — DataCoolie How-to
description: Run one DataCoolie stage or a filtered set of dataflows with the main runner scripts and execution flags.
---

# Run a stage

**Prerequisites** · Metadata loaded via any provider · engine attached to a platform.  
**End state** · One or more stages executed with parallel dataflows and a returned `ExecutionResult`.

## Single stage

```python
with DataCoolieDriver(engine=engine, platform=platform, metadata_provider=metadata) as driver:
    result = driver.run(stage="bronze2silver")

assert result.failed == 0
```

## Multiple stages in order

```python
result = driver.run(stage=["ingest2bronze", "bronze2silver", "silver2gold"])
```

Passing a list or comma-separated string filters metadata to those stage names.
Execution still follows each dataflow's `group_number` and `execution_order`,
not the order of stage names in the list.

If you need strict stage-by-stage progression, call `run()` separately:

```python
with DataCoolieDriver(engine=engine, platform=platform, metadata_provider=metadata) as driver:
    for stage_name in ["ingest2bronze", "bronze2silver", "silver2gold"]:
        result = driver.run(stage=stage_name)
        assert result.failed == 0
```

## Column name mode

By default, all output column names are lowercased. Pass `column_name_mode` to
change the behaviour:

```python
result = driver.run(stage="bronze2silver", column_name_mode="snake")
```

| Mode | Behaviour |
|------|-----------|
| `"lower"` (default) | Lowercase without inserting underscores |
| `"snake"` | Convert to `snake_case` (inserts underscores at case boundaries) |

## Pre-loading and filtering dataflows

```python
# Skip anything is_active = False
loaded = driver.load_dataflows(stage="bronze2silver", active_only=True)
result = driver.run(dataflows=loaded)
```

`driver.run(stage=...)` is shorthand for loading matching metadata first, then
executing those dataflows. Pass `dataflows=...` when you want to inspect or
filter the loaded list before execution.

## Dry run

```python
driver = DataCoolieDriver(
    engine=engine,
    platform=platform,
    metadata_provider=metadata,
    config=DataCoolieRunConfig(dry_run=True),
)
with driver:
    result = driver.run(stage="bronze2silver")
```

Nothing is read or written. Logs show what *would* have happened.

## Sharded across workers

```python
cfg = DataCoolieRunConfig(job_num=4, job_index=2)  # worker 2 of 4
```

Use this for horizontal scaling across cluster tasks. See [Orchestration](../concepts/orchestration.md).

## `DataCoolieRunConfig` reference

| Field | Default | Purpose |
|-------|---------|---------|
| `job_id` | auto-generated UUID | Unique identifier for this run |
| `job_num` | `1` | Total number of parallel workers |
| `job_index` | `0` | This worker's index (0-based, must be < `job_num`) |
| `max_workers` | `8` | Thread pool size for concurrent dataflow execution |
| `stop_on_error` | `False` | Halt remaining dataflows on first failure |
| `retry_count` | `0` | Number of retry attempts per failed dataflow |
| `retry_delay` | `5.0` | Seconds between retry attempts |
| `dry_run` | `False` | Plan without reading or writing |
| `retention_hours` | `168` | VACUUM retention for maintenance (7 days) |
| `allowed_function_prefixes` | `[]` | Restrict which Python modules can be imported by function sources |

## `ExecutionResult` fields

| Field | Meaning |
|-------|---------|
| `total` | Dataflows submitted for execution |
| `succeeded` | Completed with `status == "succeeded"` |
| `failed` | Raised an exception (after all retries exhausted) |
| `skipped` | Not executed (dry-run, or short-circuited by `stop_on_error`) |

## Other execution modes

| Method | Use case |
|--------|----------|
| `driver.run_replay(dataflows, replay)` | Re-process a bounded historical range in chunks |
| `driver.run_maintenance(connection=...)` | OPTIMIZE / VACUUM for lakehouse tables |

See [Replay & backfill](replay-and-backfill.md) and
[Maintenance](maintenance-vacuum-optimize.md).
