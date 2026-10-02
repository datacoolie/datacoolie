---
title: Run a Stage — DataCoolie User Guide
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
    expected_ids = {"orders_bronze2silver"}  # your required metadata IDs
    selected = driver.load_dataflows(stage="bronze2silver", active_only=True)
    if {flow.dataflow_id for flow in selected} != expected_ids:
        raise RuntimeError("Stage selection does not match the required dataflows")
    result = driver.run(dataflows=selected)
    if result.total != len(selected) or result.failed or result.pending:
        raise RuntimeError("Stage did not complete all selected dataflows")
```

Set `expected_ids` to your project's required active flows before execution.
`failed == 0` alone also holds for an empty selection. Review skipped reasons
and validate required output freshness/completeness before releasing a dependent
stage; an empty incremental read may be valid while an inactive required flow
or an unexpected empty bootstrap needs attention.

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
        if result.total == 0 or result.has_failures or result.pending:
            raise RuntimeError(f"Stage failed: {stage_name}")
        # Reconcile required IDs and review allowed skips and output quality.
```

When stages run across several jobs, the external orchestrator must wait for **all** upstream
shards and their required checks before starting the next stage. Each shard progressing on its
own is insufficient for cross-shard dependencies. See [Orchestration · Job distribution](../../reference/concepts/orchestration.md#job-distribution).

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
# Omit inactive dataflows from normal metadata selection
loaded = driver.load_dataflows(stage="bronze2silver", active_only=True)
result = driver.run(dataflows=loaded)
```

`driver.run(stage=...)` is shorthand for loading matching metadata first, then
executing those dataflows. Pass `dataflows=...` when you want to inspect or
filter the loaded list before execution.

Every selected or directly supplied dataflow is also checked when execution
begins. If its own flag or either connection's `is_active` flag is false, the
result is `SKIPPED` with a reason and no source/destination I/O. Filtered-out
metadata contributes no execution record. See [Dataflow activation](../metadata/dataflows.md#activation-and-selection).

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

No business data, secrets, readers, writers, transforms, or watermarks are
accessed. SQL file references, selected roots, and replay window structure are
validated; valid targets are returned as `skipped` and invalid preparation is
returned as `failed`. `result.total` reports selected targets and the status
counters describe those validation results. Inactive targets skip before SQL
file and replay validation.

## Sharded across workers

```python
cfg = DataCoolieRunConfig(job_num=4, job_index=2)  # worker 2 of 4
```

Use this for horizontal scaling across cluster tasks. See [Orchestration · Job distribution](../../reference/concepts/orchestration.md#job-distribution).

The Driver assigns selected work to this shard; it does not launch other
workers. A shard may legitimately return `total == 0` when it has no assigned
work. Validate the intended selection before distribution, then reconcile
completion across all shards in the external orchestrator. The unsharded
example's nonempty requirement is a project policy, not a blanket rule for
every shard.

## `DataCoolieRunConfig` reference

| Field | Default | Purpose |
|-------|---------|---------|
| `job_id` | auto-generated UUID | Unique identifier for this run |
| `job_num` | `1` | Total job shards; the external orchestrator launches them |
| `job_index` | `0` | This invocation's shard (0-based, must be < `job_num`) |
| `max_workers` | `8` | Global dataflow concurrency cap for one Driver operation |
| `stop_on_error` | `False` | Stop admitting new dataflows after the first terminal failure; admitted work drains |
| `retry_count` | `0` | Number of retry attempts per failed dataflow |
| `retry_delay` | `5.0` | Base retry delay; doubles per retry, capped at 60 seconds |
| `dry_run` | `False` | Plan without reading or writing |
| `retention_hours` | `168` | VACUUM retention for maintenance (7 days) |
| `allowed_function_prefixes` | `[]` | Restrict which Python modules can be imported by function sources |
| `run_attributes` | `None` | Caller-owned JSON object for external run correlation; persisted in JobRuntime only |

## `ExecutionResult` fields

| Field | Meaning |
|-------|---------|
| `total` | Dataflows submitted for execution |
| `succeeded` | Completed with `status == "succeeded"` |
| `failed` | Terminally failed dataflow, including an exception after retries |
| `skipped` | Explicit skipped status, for example no eligible source rows |
| `running` | `0` when the executor returns; admitted work is drained |
| `pending` | Work never admitted, including dataflows withheld after `stop_on_error` |

## Other execution modes

| Method | Use case |
|--------|----------|
| `driver.run_replay(dataflows, replay)` | Re-process a bounded historical range in chunks |
| `driver.run_maintenance(connection=...)` | OPTIMIZE / VACUUM for lakehouse tables |

See [Replay & backfill](replay-and-backfill.md) and
[Maintenance](maintenance.md).
