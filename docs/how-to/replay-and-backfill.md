---
title: Replay & Backfill — DataCoolie How-to
description: Re-process a bounded historical time range in sequential calendar-aligned chunks using DataCoolieDriver.run_replay() and ReplayConfig.
---

# Replay & backfill

**Prerequisites** · Familiarity with [watermarks](../concepts/watermarks.md) and a running `DataCoolieDriver`.  
**End state** · A completed historical replay of a bounded date range, with optional resume support.

`driver.run_replay()` re-processes a bounded window of historical data in
sequential chunks without disturbing the production watermark.  Use it to:

- **Backfill** a new destination table from historical source data.
- **Repair** a corrupt range after a source system issue.
- **Re-run** a date range after a schema or logic change.

---

## Quick start

```python
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.core.models import ReplayConfig

replay = ReplayConfig(
    start="2025-01-01",          # inclusive lower bound
    end="2025-04-01",            # exclusive upper bound
    chunk_interval={"months": 1},
)

with DataCoolieDriver(engine=engine, platform=platform, metadata_provider=metadata) as driver:
    dataflows = driver.load_dataflows(stage="bronze2silver")
    result = driver.run_replay(dataflows=dataflows, replay=replay)

print(f"Chunks: succeeded={result.succeeded}, failed={result.failed}")
```

This replays January, February, and March 2025 as three independent chunks:
`[Jan 1, Feb 1)`, `[Feb 1, Mar 1)`, `[Mar 1, Apr 1)`.

---

## `ReplayConfig` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start` | `str`, `date`, `datetime`, or `int` | — | **Inclusive** lower bound of the replay range |
| `end` | `str`, `date`, `datetime`, or `int` | — | **Exclusive** upper bound of the replay range |
| `chunk_interval` | `Dict[str, int]` or `None` | `None` | Chunking interval (see below). `None` = single-shot replay |
| `save_watermark` | `bool` | `False` | When `True`, saves watermark after each chunk (enables crash-resume) |
| `chunk_column` | `str` or `None` | `None` | Override the auto-resolved chunk column |

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

!!! note "Single-shot replay"
    Set `chunk_interval=None` (the default) to replay the entire range as one
    operation. Useful for small ranges or when chunking is not needed.

---

## Chunk column resolution

DataCoolie auto-resolves the chunk column from `dataflow.source.watermark_columns[0]`.

If your dataflow has multiple watermark columns and the first is not the
temporal column you want to chunk on, set `chunk_column` explicitly:

```python
ReplayConfig(
    start="2025-01-01",
    end="2025-04-01",
    chunk_interval={"months": 1},
    chunk_column="event_date",   # override; uses this column instead of watermark_columns[0]
)
```

---

## Resume after failure (`save_watermark=True`)

By default (`save_watermark=False`), the production watermark is never touched
during replay.  This is safe for backfill into a new table.

When `save_watermark=True`, DataCoolie saves the chunk upper bound as the
watermark after each successful chunk.  On re-run, completed chunks are skipped
automatically:

```python
ReplayConfig(
    start="2025-01-01",
    end="2025-07-01",
    chunk_interval={"months": 1},
    save_watermark=True,   # crash-resume mode: saves watermark per chunk
)
```

!!! warning "Only use `save_watermark=True` for init / migration scenarios"
    When `save_watermark=True`, the production watermark advances with each
    chunk.  Do not use this if you have an active incremental pipeline running
    in parallel — the watermark change will skip data in the next normal run.

---

## Combining with `source.filter_expression`

If your source dataflow already has a `source.filter_expression`, DataCoolie
**appends** the chunk upper-bound condition automatically.  Your original
filter is preserved:

```json
"source": {
  "connection_name":   "orders_db",
  "table":             "orders",
  "watermark_columns": ["updated_at"],
  "filter_expression": "status = 'active'"
}
```

During replay, the effective WHERE clause for each chunk becomes:
```sql
updated_at >= '<lower>'
  AND (status = 'active') AND (updated_at < '<upper>')
```

---

## Execution model

- **Dataflows are processed concurrently** (bounded by `max_workers`).
- **Chunks within a single dataflow run sequentially** — a failed chunk stops
  further chunks for that dataflow but does not affect other dataflows.
- Each chunk records a separate `DataFlowRuntimeInfo` in the ETL log with
  `operation_type = "replay"`.
- `ExecutionResult.succeeded` / `failed` counts **chunks**, not dataflows.

---

## Full example: quarterly backfill

```python
from datetime import date
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.core.models import ReplayConfig

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
    base_log_path="logs/",
) as driver:
    dataflows = driver.load_dataflows(stage="bronze2silver")
    result = driver.run_replay(dataflows=dataflows, replay=replay)

print(f"Total chunks: {result.total}")
print(f"Succeeded:    {result.succeeded}")
print(f"Failed:       {result.failed}")
```

---

## Related

- [Concepts · Watermarks](../concepts/watermarks.md)
- [Concepts · Orchestration](../concepts/orchestration.md)
- [How-to · Source patterns](metadata-guide/source-patterns.md)
