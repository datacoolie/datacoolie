---
title: Run a Stage — DataCoolie How-to
description: Run one DataCoolie stage or a filtered set of dataflows with the main runner scripts and execution flags.
---

# Run a stage

**Prerequisites** · Metadata loaded via any provider · engine attached to a platform.
**End state** · One or more stages executed with parallel dataflows and a returned `ExecutionResult`.

## Single stage

```python
with DataCoolieDriver(engine=engine, metadata_provider=metadata) as driver:
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
with DataCoolieDriver(engine=engine, metadata_provider=metadata) as driver:
    for stage_name in ["ingest2bronze", "bronze2silver", "silver2gold"]:
        result = driver.run(stage=stage_name)
        assert result.failed == 0
```

## Filtering by active flag

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
    metadata_provider=metadata,
    config=DataCoolieRunConfig(dry_run=True),
)
result = driver.run(stage="bronze2silver")
```

Nothing is read or written. Logs show what *would* have happened.

## Sharded across workers

```python
cfg = DataCoolieRunConfig(job_num=4, job_index=2)  # worker 2 of 4
```

Use this for horizontal scaling across cluster tasks. See [Orchestration](../concepts/orchestration.md).
