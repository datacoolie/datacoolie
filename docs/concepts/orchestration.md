# Orchestration

**TL;DR** `DataCoolieDriver` is a thin coordinator. Heavy lifting is split
across `JobDistributor` (multi-job sharding), `ParallelExecutor`
(thread-level), and `RetryHandler` (per-dataflow retry with backoff).

## Driver

```python
with DataCoolieDriver(
    engine=engine,
    platform=platform,                # or attached via engine
    metadata_provider=metadata,
    watermark_manager=None,            # auto-created from metadata
    config=DataCoolieRunConfig(job_num=4, job_index=0, max_workers=4),
    secret_provider=None,              # defaults to platform
    base_log_path="logs/",             # auto-creates ETLLogger + SystemLogger
) as driver:
    result = driver.run(stage=["bronze2silver"])
```

Key behaviours:

- **Constructor injection** for every dependency — no global state.
- **Auto-creates `WatermarkManager`** when a metadata provider is supplied and
  no manager is passed.
- **Auto-creates loggers** under `base_log_path/{system,etl}_logs` when you
  don't bring your own.
- **Resource cleanup** via context manager — loggers flush, connections close.
- **Platform-type guard** — refuses to run if `platform` and `engine.platform`
  are different concrete types.

## Job distribution

`JobDistributor` is a hash-based sharder for horizontally scaling a single
metadata set across multiple worker processes or cluster tasks:

```python
config = DataCoolieRunConfig(job_num=4, job_index=2)
# → this worker picks up dataflows where hash(dataflow.id) % 4 == 2
```

Use this when running four Spark jobs in parallel to split 1000 dataflows
evenly. Each dataflow runs exactly once across the fleet.

## Parallel execution

`ParallelExecutor` uses a `ThreadPoolExecutor` (not processes — Spark and
Polars both release the GIL during I/O and compute). `ExecutionResult` fields:

- `total` — dataflows submitted
- `succeeded` — finished with `status == "succeeded"`
- `failed` — raised
- `skipped` — not executed (e.g. after `stop_on_error=True` short-circuit)


## Retry

`RetryHandler` wraps each dataflow with:

- `retry_count` attempts
- `retry_delay` seconds (fixed delay between attempts)

If all attempts fail the error is recorded and the executor moves on (or
stops, per `stop_on_error`).

Retry is **dataflow-scoped**, not pipeline-scoped. A transient network blip
reads the source again on retry — including re-applying the current watermark,
which means retries are *idempotent* for incremental loads.

## Maintenance path

`driver.run_maintenance(connection=..., do_compact=True, do_cleanup=True)` is a parallel
variant for `OPTIMIZE` / `VACUUM`. It:

1. Loads the same dataflow metadata.
2. Deduplicates by destination so fan-in topologies don't race on the same
   table.
3. Dispatches to `BaseDestinationWriter.run_maintenance`.

See [How-to · Maintenance](../how-to/maintenance-vacuum-optimize.md).

## Dry run

`DataCoolieRunConfig(dry_run=True)` logs what *would* happen without reads,
writes, or watermark updates. Useful for validating new metadata before the
first real run.

## Related

- [`reference/api/orchestration`](../reference/api/orchestration.md)
- [Concepts · Logging](logging.md)
