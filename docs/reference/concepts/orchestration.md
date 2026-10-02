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
    metadata_base_path=None,           # or an auto-created FileProvider root
    watermark_manager=None,            # auto-created from metadata
    config=DataCoolieRunConfig(job_num=4, job_index=0, max_workers=4),
    secret_provider=None,              # defaults to platform
    log_base_path="logs/",              # auto-creates ExecutionLogger + SystemLogger
) as driver:
    result = driver.run(stage=["bronze2silver"])
```

Key behaviours:

- **Constructor injection** for runtime dependencies. Plugin registries are
  process-level registries populated when `datacoolie` is imported.
- **Auto-creates `WatermarkManager`** when the resolved metadata provider is
  present and no manager is passed.
- **File-provider inference** — when no provider is injected, supplying
  `metadata_base_path` creates a `FileProvider` for that directory; supplying
  only `artifact_base_path` creates one for `<artifact_base_path>/metadata`.
  Supplying neither keeps provider-less explicit-dataflow execution available.
- **Explicit path conflicts fail fast** — an injected provider must accept the
  typed metadata context; a `FileProvider` with `config_path` cannot also
  receive `metadata_base_path`, and a different established metadata root is
  rejected. `create_driver()` and direct `DataCoolieDriver()` construction use
  the same rules.
- **Auto-creates loggers** under `log_base_path/{system_logs,execution_logs}` when you
  don't bring your own.
- **Resource cleanup** via context manager — loggers flush, connections close.
- **Platform identity guard** — accepts the same `platform` instance supplied
  to both Driver and engine, but rejects distinct instances even when their
  concrete types match.
- **Provider startup boundary** — providers are assembled without metadata
  reads; Driver validates injected logger instances, binds provider-owned
  artifact/state paths, activates the inert SystemLogger/ExecutionLogger
  session, and then calls each provider `initialize()`. A successfully
  constructed Driver therefore has a validated full metadata scope,
  including inactive entries. Provider records during initialization are
  captured; `session.ready` is emitted only after initialization succeeds.
- **Single-operation lifecycle** — public load/run entrypoints admit one
  operation at a time, reject use after `close()`, and reject `close()` while
  work is active. The admission state is released on both success and failure.
- **Failure-safe lifecycle** — `KeyboardInterrupt`/`SystemExit` at a Driver
  boundary marks the session `failed`, preserves the exception for the caller,
  and still attempts accepted logger/provider cleanup. Lifecycle diagnostics are
  best effort; ETL and maintenance completion observations are persisted before
  their optional completion hooks run.
- **Terminal teardown boundary** — Driver-owned provider cleanup runs before the
  final JobRuntime status is committed. ExecutionLogger closes before
  SystemLogger, and every accepted component gets a close attempt. A primary
  business exception always wins over cleanup/finalization failures; with no
  primary exception, the first provider/contract interruption is raised after
  cleanup. Ordinary log-storage failures remain best effort.
- **Error ownership** — ExecutionLogger's JobRuntime summary lists failed
  dataflow identities (`name [id]`, or the available ID) separated by `; `.
  Full phase/error details stay on dataflow runtime rows and system tracebacks;
  Driver adds only independent session, scheduler, startup, and teardown
  failures, avoiding a second copy of an already observed dataflow error.

When `run()` receives an explicit `dataflows` list, it executes that list as
given: it does not reload metadata or apply stage or job-shard
selection. Execution still skips an inactive dataflow or one referencing an
inactive source or destination connection. `run_replay()` likewise accepts the supplied list with a flat
executor. Load and filter through `load_dataflows(...)` first when those
selection rules are required. `run_maintenance(dataflows=...)` deduplicates the
eligible supplied list but does not apply job sharding; the metadata/connection
path does. Blocked maintenance candidates are recorded as skipped and cannot
displace an active representative of the same physical destination.

## Job distribution

`JobDistributor` is a deterministic sharder for horizontally scaling a single
metadata set across multiple worker processes or cluster tasks:

```python
config = DataCoolieRunConfig(job_num=4, job_index=2)
# group_number set: group_number % 4 == 2
# group_number absent: int(MD5(dataflow_id), 16) % 4 == 2
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
- `failed` — terminally failed dataflows, including process exceptions converted into a failed runtime
- `skipped` — explicitly returned a skipped status (for example, no eligible source rows)
- `running` — always `0` when the executor returns; admitted work is drained
- `pending` — never admitted, including work withheld after `stop_on_error`

Different `group_number` buckets are dispatched concurrently. Within one
group, lower `execution_order` buckets complete first; dataflows with the same
`execution_order` run in parallel.

Omit group/order for independent flows. `group_number=None` keeps flows independent even when
`execution_order` is set: sorting submissions does not enforce completion order. In a non-null
group (including group `0`), missing order is treated as `0`. For A/B then C, give all three the
same group, A/B order 10, and C order 20. Different groups have no ordering guarantee, even on
the same job. A whole group is assigned to one job, so one large group limits scale-out.

`max_workers` is the global dataflow concurrency cap for one executor invocation, including
independent flows, groups and tied order buckets. It does not cap engine-internal threads or
other Driver instances. The scheduler admits only ready work, so grouped execution does not
create nested pools.

Stage lists and comma strings select a union of flows; they do not order stages or load missing
prerequisites. Prefer [separate stage runs](../../guide/operations/run-stage.md). With multiple jobs, wait for
all upstream shards and required quality checks before starting downstream shards. Combined
stages need their dependent flows in shared groups with increasing orders. A join across groups
requires regrouping the dependency set or an external barrier.


## Retry

`RetryHandler` wraps each dataflow with:

- `retry_count` retries after the initial attempt
- `retry_delay` as the base delay; delay doubles on each retry and is capped at
  60 seconds

If all attempts fail the error is recorded. With default `stop_on_error=False`, later buckets
can still execute. With `stop_on_error=True`, the scheduler stops admitting new dataflows after
the first terminal failure, regardless of whether the process returned a failed runtime or raised.
Already-admitted dataflows finish and are counted; withheld work remains `pending`. A replay
dataflow remains one scheduler item, while its own chunks stop at the first failed chunk.
Check execution results and required quality evidence before advancing stages.

Retry is **dataflow-scoped**. Retrying reads the source again using the watermark. Idempotency
still depends on the load strategy and keys, particularly if output succeeded before watermark
persistence failed.

SQL-file reads and connection-secret hydration happen once during preparation
before this retry boundary. A preparation failure is terminal for that
dataflow and is logged without an executed source action.

## Maintenance path

`driver.run_maintenance(connection=..., do_compact=True, do_cleanup=True)` is a parallel
variant for `OPTIMIZE` / `VACUUM`. It:

1. Loads the dataflow metadata when no explicit `dataflows` list is supplied.
2. Deduplicates by destination so fan-in topologies don't race on the same
   table.
3. Distributes metadata-loaded targets through `JobDistributor`, then dispatches
   the selected targets to `BaseDestinationWriter.run_maintenance`.

See [User guide · Maintenance](../../guide/operations/maintenance.md).

## Dry run

`DataCoolieRunConfig(dry_run=True)` loads and filters metadata, validates SQL
file references and replay ranges, then records each target as `skipped` (or
`failed` when preparation validation fails). It does not resolve secrets,
construct readers/writers, execute transforms, touch business data, or read or
write watermarks.

## Query files and preparation

`Source.query` remains one string. Inline SQL is passed through unchanged;
relative values ending in `.sql` (for example `orders/incremental.sql` or
`sql/orders/incremental.sql`) are read during preparation. The optional
`artifact:/sql/orders.sql` form explicitly selects the artifact root and can
refer to filenames containing spaces or extensions other than `.sql`.

`sql_base_path` accepts one root or a sequence of roots. With one root, a
root-relative reference such as `orders/incremental.sql` is accepted; the
qualified form using the root folder name is accepted too. With multiple
roots, the first path segment must exactly match one root's final folder name
(`sql1/orders.sql` selects a root ending in `sql1`). When an environment
artifact is supplied without explicit SQL roots, the complete declared path
is joined directly below the artifact root (`sql/orders.sql` means
`<artifact>/sql/orders.sql`). Runtime never reads `manifest.json` or infers an
SQL folder convention.

The runtime has optional `artifact_base_path`, `metadata_base_path`,
`state_base_path`, `sql_base_path`, and `log_base_path` values. Provider
configuration owns its declared `sql_base_path`; Driver startup context offers
its value as a session fallback. `metadata_base_path` is consumed by
`FileProvider`; it is not duplicated in `DataCoolieRunConfig`. When both SQL
root values are explicit, equal normalized roots are accepted and different
roots fail before provider initialization. Relative SQL uses provider roots
when present, then Driver roots, then the artifact-only fallback;
`artifact:/...` explicitly selects the artifact root. Preparation deep-copies
the declarative metadata, resolves the file and connection secrets once, and
supplies fresh copies to retries. Execution logs retain the original
`source.query`; the runtime `source_action["query"]` records the exact SQL
submitted by the reader.

Preparation lives under `datacoolie.orchestration.preparation`: `dataflow.py`
owns the execution-copy boundary and `query.py` owns string classification and
scoped file reads. There is no separate query-file compatibility layer; callers
should use the preparation package when they need to classify or resolve a
query reference.

## Replay / backfill

`driver.run_replay(dataflows, replay: ReplayConfig)` re-processes a bounded
historical range in sequential, calendar-aligned chunks. By default,
`save_watermark=False` leaves production state unchanged. With
`save_watermark=True`, successful chunks save source-derived candidates through
the reader's merge policy; saved state never skips a later replay invocation.

Each dataflow is processed concurrently (bounded by `max_workers`); chunks
within a single dataflow always run sequentially. Group/order does not sequence replay dataflows.
Use `load_dataflows(stage=...)` first to apply active filtering and job assignment to the supplied
list, and replay dependent stages separately. A `ReplayConfig` specifies:

Replay prepares each dataflow once before chunk iteration. Each chunk receives a
deep copy of that prepared execution baseline, and each retry receives a fresh
attempt copy; query files and secrets are not re-resolved for every chunk or
retry, and a mutated attempt copy is never reused.

- `start` / `end` — inclusive/exclusive bounds (timestamps, dates, or integers).
- `chunk_interval` — chunking unit such as `{"months": 1}` or `{"days": 7}`.
- `save_watermark` — when `True`, saves the reader-produced watermark after
  each successful chunk. It does not create a replay checkpoint or skip the
  requested range on a later invocation.
- `chunk_column` — overrides the auto-resolved column (defaults to
  `watermark_columns[0]`). Database, lakehouse, file, and function readers
  may support an independent bounded-read column; API readers require a
  matching `range_param_mapping` field with lower and upper bindings. The API
  selection field may be outside `source.watermark_columns`; selection and
  persisted watermark state are separate contracts.

```python
from datacoolie.core.models.run_config import ReplayConfig

replay = ReplayConfig(
    start="2025-01-01",
    end="2025-04-01",
    chunk_interval={"months": 1},
)
result = driver.run_replay(dataflows=dataflows, replay=replay)
```

See [User guide · Replay & backfill](../../guide/operations/replay-and-backfill.md).

## Related

- [`reference/api/orchestration`](../api/orchestration.md)
- [Concepts · Logging](logging.md)
