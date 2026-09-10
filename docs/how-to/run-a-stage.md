---
title: Run a Stage — DataCoolie How-to
description: Run a DataCoolie pipeline stage or filtered dataflows with the main runner scripts, execution flags, metadata providers, and failure handling.
---

# Run a stage

**Prerequisites** · Metadata loaded via any provider · engine attached to a platform.  
**End state** · One or more stages executed with parallel dataflows and a returned `ExecutionResult`.

## Single stage

Stage names are defined by your project. The names below are examples; replace them with the
values in your metadata and order invocations using your actual dependencies.

```python
with DataCoolieDriver(engine=engine, platform=platform, metadata_provider=metadata) as driver:
    result = driver.run(stage="bronze2silver")

assert result.failed == 0
```

## Multiple-stage selection

```python
result = driver.run(stage=["ingest2bronze", "bronze2silver", "silver2gold"])
```

Passing a list or comma-separated string filters metadata to those stage names.
Execution still follows each dataflow's `group_number` and `execution_order`,
not the order of stage names in the list.

Prefer separate calls for dependent stages so failures and quality checks are isolated:

```python
with DataCoolieDriver(engine=engine, platform=platform, metadata_provider=metadata) as driver:
    for stage_name in ["ingest2bronze", "bronze2silver", "silver2gold"]:
        result = driver.run(stage=stage_name)
        if result.has_failures:
            raise RuntimeError(f"Stage failed: {stage_name}")
        # Check required freshness, completeness, and quality evidence here.
```

When stages run across several jobs, the external orchestrator must wait for **all** upstream
shards and their required checks before starting the next stage. Each shard progressing on its
own is insufficient for cross-shard dependencies. See [Orchestration](../concepts/orchestration.md).

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

Nothing is read or written. Metadata is loaded and filtered, and logs show the
selected targets. Backend connectivity and transform expressions are not
validated. `result.total` reports how many targets were selected; the current
dry-run path leaves every status counter, including `pending`, at zero.

## Sharded across workers

```python
cfg = DataCoolieRunConfig(job_num=4, job_index=2)  # worker 2 of 4
```

Use this for horizontal scaling across cluster tasks. See [Orchestration](../concepts/orchestration.md).

## `DataCoolieRunConfig` reference

| Field | Default | Purpose |
|-------|---------|---------|
| `job_id` | auto-generated UUID | Unique identifier for this run |
| `job_num` | `1` | Total job shards; the external orchestrator launches them |
| `job_index` | `0` | This invocation's shard (0-based, must be < `job_num`) |
| `max_workers` | `8` | Per-pool concurrency; nested group pools can exceed this total |
| `stop_on_error` | `False` | Stop later buckets of a failing normal-ETL group; not global fail-fast |
| `retry_count` | `0` | Number of retry attempts per failed dataflow |
| `retry_delay` | `5.0` | Base retry delay; doubles per retry, capped at 60 seconds |
| `dry_run` | `False` | Plan without reading or writing |
| `retention_hours` | `168` | VACUUM retention for maintenance (7 days) |
| `allowed_function_prefixes` | `[]` | Restrict which Python modules can be imported by function sources |

## `ExecutionResult` fields

| Field | Meaning |
|-------|---------|
| `total` | Dataflows submitted for execution |
| `succeeded` | Completed with `status == "succeeded"` |
| `failed` | Raised an exception (after all retries exhausted) |
| `skipped` | Explicit skipped status, for example no eligible source rows |
| `running` | Exposed field; current executor does not count live in-flight work |
| `pending` | Not in collected terminal counters; after early stop, does not prove nothing ran |

## Other execution modes

| Method | Use case |
|--------|----------|
| `driver.run_replay(dataflows, replay)` | Re-process a bounded historical range in chunks |
| `driver.run_maintenance(connection=...)` | OPTIMIZE / VACUUM for lakehouse tables |

See [Replay & backfill](replay-and-backfill.md) and
[Maintenance](maintenance-vacuum-optimize.md).
