---
title: Orchestration Model — DataCoolie Concepts
description: Understand how DataCoolieDriver, JobDistributor, ParallelExecutor, and RetryHandler coordinate multi-job execution, concurrency, and retries.
---

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

- **Constructor injection** for runtime dependencies. Plugin registries are
  process-level registries populated when `datacoolie` is imported.
- **Auto-creates `WatermarkManager`** when a metadata provider is supplied and
  no manager is passed.
- **Auto-creates loggers** under `base_log_path/{system,etl}_logs` when you
  don't bring your own.
- **Resource cleanup** via context manager — loggers flush, connections close.
- **Platform-type guard** — refuses to run if `platform` and `engine.platform`
  are different concrete types.

When `run()` receives an explicit `dataflows` list, it executes that list as
given: it does not reload metadata or apply stage, active, or job-shard
selection. `run_replay()` likewise executes the supplied list with a flat
executor. Load and filter through `load_dataflows(...)` first when those
selection rules are required. `run_maintenance(dataflows=...)` deduplicates the
supplied list but does not apply job sharding; the metadata/connection path
does.

## Job distribution

`JobDistributor` is a deterministic sharder for horizontally scaling a single
metadata set across multiple worker processes or cluster tasks:

```python
config = DataCoolieRunConfig(job_num=4, job_index=2)
# group_number set: group_number % 4 == 2
# group_number absent: MD5(dataflow_id) % 4 == 2
```

For one job, omit both parameters: the defaults are `job_num=1`, `job_index=0`.
For scale-out, the external orchestrator launches every index `0..N-1` with the same N,
metadata snapshot, environment, and stage selection. DataCoolie filters each job's work; it does
not launch the other jobs or wait for them. Assignment is deterministic, not random or balanced
by duration. Each selected flow belongs to one shard, but duplicate launches/retries can repeat
execution; sharding does not guarantee exactly-once writes. Changing N can move assignments.

## Parallel execution

`ParallelExecutor` uses a `ThreadPoolExecutor` (not processes — Spark and
Polars both release the GIL during I/O and compute). `ExecutionResult` fields:

- `total` — dataflows submitted
- `succeeded` — finished with `status == "succeeded"`
- `failed` — raised
- `skipped` — explicitly returned a skipped status (for example, no eligible source rows)
- `running` — exposed field; the current executor does not track live in-flight work
- `pending` — not represented in the collected terminal counters, including after early stop;
  this is not proof that no work ran

Different `group_number` buckets are dispatched concurrently. Within one
group, lower `execution_order` buckets complete first; dataflows with the same
`execution_order` run in parallel.

Omit group/order for independent flows. `group_number=None` keeps flows independent even when
`execution_order` is set: sorting submissions does not enforce completion order. In a non-null
group (including group `0`), missing order is treated as `0`. For A/B then C, give all three the
same group, A/B order 10, and C order 20. Different groups have no ordering guarantee, even on
the same job. A whole group is assigned to one job, so one large group limits scale-out.

`max_workers` bounds each pool. Normal ETL uses an outer pool and an inner pool for each parallel
order bucket; two groups with two tied flows each can run four flows with `max_workers=2`.
It is not a global dataflow or engine-thread cap.

Stage lists and comma strings select a union of flows; they do not order stages or load missing
prerequisites. Prefer [separate stage runs](../how-to/run-a-stage.md). With multiple jobs, wait for
all upstream shards and required quality checks before starting downstream shards. Combined
stages need their dependent flows in shared groups with increasing orders. A join across groups
requires regrouping the dependency set or an external barrier.


## Retry

`RetryHandler` wraps each dataflow with:

- `retry_count` retries after the initial attempt
- `retry_delay` as the base delay; delay doubles on each retry and is capped at
  60 seconds

If all attempts fail the error is recorded. With default `stop_on_error=False`, later buckets
can still execute. In normal ETL, `stop_on_error=True` prevents later buckets in the failing
numbered group, but other groups and independent flows continue. Already-running peers can
finish; a skipped producer does not block later buckets. Check execution results and required
quality evidence before advancing stages. Flat replay/maintenance execution currently continues
other dataflows after a returned failed status, even when `stop_on_error=True`.

Retry is **dataflow-scoped**. Retrying reads the source again using the watermark. Idempotency
still depends on the load strategy and keys, particularly if output succeeded before watermark
persistence failed.

## Maintenance path

`driver.run_maintenance(connection=..., do_compact=True, do_cleanup=True)` is a parallel
variant for `OPTIMIZE` / `VACUUM`. It:

1. Loads the dataflow metadata when no explicit `dataflows` list is supplied.
2. Deduplicates by destination so fan-in topologies don't race on the same
   table.
3. Distributes metadata-loaded targets through `JobDistributor`, then dispatches
   the selected targets to `BaseDestinationWriter.run_maintenance`.

See [How-to · Maintenance](../how-to/maintenance-vacuum-optimize.md).

## Dry run

`DataCoolieRunConfig(dry_run=True)` loads and filters metadata, then logs the
selected targets without reads, writes, or watermark updates. It does not
validate backend connectivity or execute transforms. Returned targets remain
unclassified: `ExecutionResult.total` is set, while all status counters
(`succeeded`, `failed`, `skipped`, `running`, `pending`) remain zero.

## Replay / backfill

`driver.run_replay(dataflows, replay: ReplayConfig)` re-processes a bounded
historical range in sequential, calendar-aligned chunks without disturbing the
production watermark.

Each dataflow is processed concurrently (bounded by `max_workers`); chunks
within a single dataflow always run sequentially. Group/order does not sequence replay dataflows.
Use `load_dataflows(stage=...)` first to apply active filtering and job assignment to the supplied
list, and replay dependent stages separately. A `ReplayConfig` specifies:

- `start` / `end` — inclusive/exclusive bounds (timestamps, dates, or integers).
- `chunk_interval` — chunking unit such as `{"months": 1}` or `{"days": 7}`.
- `save_watermark` — when `True`, enables crash-resume by saving the chunk
  upper bound as the watermark after each successful chunk.
- `chunk_column` — overrides the auto-resolved column (defaults to
  `watermark_columns[0]`).

```python
from datacoolie.core.models import ReplayConfig

replay = ReplayConfig(
    start="2025-01-01",
    end="2025-04-01",
    chunk_interval={"months": 1},
)
result = driver.run_replay(dataflows=dataflows, replay=replay)
```

See [How-to · Replay & backfill](../how-to/replay-and-backfill.md).

## Related

- [`reference/api/orchestration`](../reference/api/orchestration.md)
- [Concepts · Logging](logging.md)
