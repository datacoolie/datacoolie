---
title: Replay & Backfill — DataCoolie User Guide
description: Re-process a bounded historical range in sequential chunks using DataCoolieDriver.run_replay() and ReplayConfig.
---

# Replay & backfill

**Prerequisites** · Familiarity with [watermark replay behaviour](../../reference/concepts/watermarks.md#replay-watermark-behaviour) and a running `DataCoolieDriver`.  
**End state** · A completed historical replay of a bounded range, with an explicit choice about whether the source watermark is updated.

`driver.run_replay()` re-processes a bounded window of historical data in
sequential chunks. By default it leaves the production watermark untouched;
`save_watermark=True` persists the reader-produced candidate after each
successful destination write. The candidate can represent an observed maximum
or a source-confirmed request end. Every invocation still re-runs the
requested range; a saved watermark is not a replay checkpoint. Use it to:

- **Backfill** a new destination table from historical source data.
- **Repair** a corrupt range after a source system issue.
- **Re-run** a date range after a schema or logic change.

The [ReplayConfig reference](../../reference/runtime-configuration.md#replay-configuration)
lists the fields and defaults. If a runner injects its own watermark manager,
use the [watermark API](../../reference/api/watermark.md) for provider-backed
read/write contracts and the [Driver API](../../reference/api/orchestration.md)
for `run_replay` and constructor arguments.

---

## Quick start

```python
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.core.models.run_config import ReplayConfig

replay = ReplayConfig(
    start="2025-01-01",          # inclusive lower bound
    end="2025-04-01",            # exclusive upper bound
    chunk_interval={"months": 1},
)

with DataCoolieDriver(engine=engine, platform=platform, metadata_provider=metadata) as driver:
    dataflows = driver.load_dataflows(stage="bronze2silver")
    result = driver.run_replay(dataflows=dataflows, replay=replay)

print(f"Dataflows: succeeded={result.succeeded}, failed={result.failed}")
```

Replay applies the same [activation check](../metadata/dataflows.md#activation-and-selection)
as a normal run, including when dataflows are supplied directly. An inactive
dataflow or source/destination connection produces one skipped outer result;
no chunks or watermark operations start for it.

This replays January, February, and March 2025 as three independent chunks:
`[Jan 1, Feb 1)`, `[Feb 1, Mar 1)`, `[Mar 1, Apr 1)`.

---

## `ReplayConfig` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start` | `str`, `date`, `datetime`, or `int` | — | **Inclusive** lower bound of the replay range |
| `end` | `str`, `date`, `datetime`, or `int` | — | **Exclusive** upper bound of the replay range |
| `chunk_interval` | `Dict[str, int]` or `None` | `None` | Chunking interval (see below). `None` = single-shot replay |
| `save_watermark` | `bool` | `False` | When `True`, saves the reader-produced watermark candidate after each successful chunk. It does not skip chunks on a later invocation |
| `chunk_column` | `str` or `None` | `None` | Column used to select chunks. It may be independent of `source.watermark_columns` when the source reader supports bounded reads |

### Range convention: `[start, end)`

The range is **left-closed, right-open**:

- `start` is **included** in the first chunk.
- `end` is **excluded** (the first instant NOT replayed).

This aligns with Python `range()`, Spark partitioning, and ISO 8601 interval conventions.

---

## `chunk_interval` keys

| Key | Alignment | Example |
|-----|-----------|---------|
| `years` | Calendar years | `{"years": 1}` |
| `months` | Calendar months | `{"months": 1}` or `{"months": 3}` |
| `weeks` | ISO weeks (Mon–Sun) | `{"weeks": 1}` |
| `days` | Calendar days | `{"days": 1}` |
| `hours` | Hours | `{"hours": 6}` |
| `minutes` | Minutes | `{"minutes": 30}` |
| `step` | Integer step | `{"step": 10000}` |

Use `step` when `watermark_columns` holds a row-number or integer sequence id
instead of a timestamp.

```python
# Integer watermark — replay rows 0 to 1,000,000 in batches of 100k
ReplayConfig(
    start=0,
    end=1_000_000,
    chunk_interval={"step": 100_000},
)
```

`chunk_interval=None` is also valid for a finite numeric range: it performs one
bounded read using the exact numeric `[start, end)` values. `step` creates
sequential integer chunks. Calendar keys (`years`, `months`, `weeks`, `days`,
`hours`, and `minutes`) create temporal chunks and require a date/datetime
compatible source column; they are not interchangeable with integer stepping.

!!! note "Single-shot replay"
    Set `chunk_interval=None` (the default) to replay the entire range as one
    operation. Useful for small ranges or when chunking is not needed.

---

## Chunk column resolution

DataCoolie auto-resolves the chunk column from `dataflow.source.watermark_columns[0]`.

If your dataflow has multiple watermark columns and the first is not the
column you want to chunk on, set `chunk_column` explicitly. Database,
lakehouse, file, and function readers can use a source-supported bounded-read
column that is not persisted as a watermark. API readers can do the same when
the selected field has a canonical `range_param_mapping` entry with both a
lower and an upper binding. The API mapping is the endpoint contract for the
selected field; it does not add that field to `source.watermark_columns`.

For example, an API can select a historical `created_at` range while keeping
`updated_at` as the only persisted watermark:

```json
{
  "watermark_columns": ["updated_at"],
  "configure": {
    "range_param_mapping": {
      "created_at": {
        "lower": {"name": "created_from", "operator": ">="},
        "upper": {"name": "created_to", "operator": "<"},
        "format": "iso",
        "response_column": "created_at",
        "watermark_value": "observed_max"
      }
    }
  }
}
```

`save_watermark=True` still persists only the source watermark columns. The API
response must include an `updated_at` observation for that key to advance; a
slice that returns only `created_at` rows has no `updated_at` value to save. A
saved `updated_at` maximum from a `created_at` slice is an observed value, not
proof that every `updated_at` value in the replay interval was covered.

```python
ReplayConfig(
    start="2025-01-01",
    end="2025-04-01",
    chunk_interval={"months": 1},
    chunk_column="event_date",   # override; uses this column instead of watermark_columns[0]
)
```

---

## Persisting the reader watermark candidate (`save_watermark=True`)

By default (`save_watermark=False`), the production watermark is never touched
during replay.  This is safe for backfill into a new table.

When `save_watermark=True`, DataCoolie saves the reader's candidate after each
successful destination write. For an observed-max binding, the candidate is the
maximum non-null source value; for a request-end binding, it is the exclusive
end that the source contract confirms. The requested chunk upper bound is never
substituted for a missing observation. An empty read or an all-null candidate
does not save state, while a legitimate zero remains a valid observation. The
driver merges that candidate with stored state using the reader's
source-qualified ordering semantics before it saves. On a later invocation the
same `[start, end)` range is read again:

```python
ReplayConfig(
    start="2025-01-01",
    end="2025-07-01",
    chunk_interval={"months": 1},
    save_watermark=True,   # persist source observations per successful chunk
)
```

!!! warning "Coordinate with normal incremental runs"
    When `save_watermark=True`, a successful chunk may update production state.
    Source-qualified ordered keys never move backward: a historical candidate
    below a higher saved value leaves that value unchanged. Coordinate replay
    with normal incremental runs that share the same dataflow state.

    Saving the maximum of a partially loaded history does not prove that every
    earlier record has been loaded. Complete the intended historical coverage
    before relying on that state for normal incremental selection.

!!! note "No chunk checkpoint"
    Chunks run sequentially and later chunks stop after the first failure.
    A restart intentionally runs every requested chunk again. Use an idempotent
    destination strategy such as `merge_upsert` when repeated replay writes
    must reconcile business rows.

!!! warning "Destination write and state save are separate"
    A successful destination write happens before the watermark save. A process
    termination or storage error in that gap can leave committed output with
    the old watermark. Rerun the complete requested range; use a keyed or
    otherwise idempotent destination strategy when duplicate delivery would be
    harmful. A later replay never treats the saved watermark as a completed
    chunk marker.

---

## Combining with `source.filter_expression`

If your source dataflow already has a `source.filter_expression`, the source
reader keeps it as a separate post-read or pushdown filter. A replay range is
owned by the source reader and is applied independently; your original filter
is preserved:

```json
"source": {
  "connection_name":   "orders_db",
  "table":             "orders",
  "watermark_columns": ["updated_at"],
  "filter_expression": "status = 'active'"
}
```

For a database source, the effective predicate for each chunk is:
```sql
(updated_at >= '<lower>' AND updated_at < '<upper>')
  AND (status = 'active')
```

The range is always `[start, end)`. A date-folder file partition may be used
to prune folders, but its folder key is internal and cannot be a
`chunk_column`; file modification time remains the selector for files.

## CLI boundary values

The downloadable [`replay_recovery.py`](../../examples/files/operations/replay_recovery.py)
wrapper accepts `--start` and `--end` as text. A string made only of an optional
sign and decimal digits becomes an integer; every other value remains a string
for the source's date/datetime parser. The wrapper does not parse arbitrary
floating-point text or attach a timezone to a date string. `--chunk-interval-json`
must decode to an object. Saving state is an explicit opt-in: pass both
`--save-watermark` and `--confirm-save-watermark`; omitting them uses
`save_watermark=False`.

---

## Execution model

- **Dataflows are processed concurrently** (bounded by `max_workers`).
- **Chunks within a single dataflow run sequentially** — a failed chunk stops
  further chunks for that dataflow but does not affect other dataflows.
- With `stop_on_error=True`, no new replay dataflow is admitted after a terminal
  failure; already-admitted dataflows finish and withheld dataflows remain pending.
- Each chunk records a separate `DataFlowRuntimeInfo` in the execution log with
  `operation_type = "replay"`.
- Rows written by a chunk receive that chunk's `__dataflow_run_id`; they do not
  receive the outer replay aggregate ID.
- Each chunk produces a separate execution log row.
- Chunk observers use the chunk completion hook; the dataflow completion hook
  receives one aggregate result. Observer errors are logged and do not change
  pipeline status.
- `ExecutionResult.total` / `succeeded` / `failed` count the outer
  **dataflows**, because `_process_replay` aggregates its chunks into one
  `DataFlowRuntimeInfo` before returning to `ParallelExecutor`.

---

## Full example: one-year backfill

```python
from datetime import date
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.core.models.run_config import ReplayConfig

replay = ReplayConfig(
    start=date(2024, 1, 1),
    end=date(2025, 1, 1),     # full year 2024
    chunk_interval={"months": 1},
    save_watermark=False,      # backfill: leave production watermark untouched
)

with DataCoolieDriver(
    engine=engine,
    platform=platform,
    metadata_provider=metadata,
    log_base_path="logs/",
) as driver:
    dataflows = driver.load_dataflows(stage="bronze2silver")
    result = driver.run_replay(dataflows=dataflows, replay=replay)

print(f"Total dataflows: {result.total}")
print(f"Succeeded:    {result.succeeded}")
print(f"Failed:       {result.failed}")
```

---

## Related

- [Concepts · Watermarks · Replay watermark behaviour](../../reference/concepts/watermarks.md#replay-watermark-behaviour)
- [Concepts · Orchestration · Replay / backfill](../../reference/concepts/orchestration.md#replay-backfill)
- [User guide · Source patterns](../metadata/source-patterns.md)
