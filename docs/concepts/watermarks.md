---
title: Watermarks — DataCoolie Concepts
description: Learn how DataCoolie stores, parses, and updates raw JSON watermarks for incremental loads across metadata providers.
---

# Watermarks

**TL;DR** A watermark is a JSON object stored by the metadata provider and
parsed by `WatermarkManager`. The `__datetime__` sentinel lets datetime values
round-trip through JSON losslessly.

## Contract

- `BaseMetadataProvider.get_watermark(dataflow_id: str) -> Optional[str]` —
  returns raw JSON text (or `None`).
- `BaseMetadataProvider.update_watermark(dataflow_id, watermark_value, *, job_id, dataflow_run_id)` —
  persists raw serialised watermark text.
- `WatermarkManager.get_watermark(dataflow_id: str) -> Dict[str, Any] | None` —
  deserialises via `WatermarkSerializer`.
- `WatermarkManager.save_watermark(dataflow_id, watermark: Dict[str, Any], *, job_id=None, dataflow_run_id=None)` —
  serialises and delegates persistence to the metadata provider.

Providers never touch datetimes. See
[ADR-0004](../adr/0004-raw-json-watermark-contract.md) for why.

## Serialisation format

```json
{"updated_at": {"__datetime__": "2026-04-03T09:15:00+00:00"}}
```

Sentinels handled by `WatermarkSerializer`:

- `__datetime__` → `datetime.fromisoformat(value)`
- `__date__` → `date.fromisoformat(value)`
- `__time__` → `time.fromisoformat(value)`

Everything else is plain JSON (ints, floats, strings, nested dicts, lists).

## Read-side push-down

Source readers apply the watermark **during** the read when possible:

- **Parquet / Delta / Iceberg** — predicate pushdown on the watermark column.
- **Database (JDBC / connectorx)** — appended `WHERE watermark_col > ...`.
- **API** — injected into request params/body via `source.configure`
  keys such as `watermark_param_mapping`, `watermark_to_param`,
  `watermark_param_location`, and `watermark_param_format`.
- **CSV / JSON / Excel** — full scan, then filter in the engine (fallback).

### Watermark operator

By default the comparison is `>` (strict greater-than), meaning "only rows
newer than the last saved value".  During **replay**, the driver passes
`watermark_operator=">="` (inclusive) so that the lower-bound chunk boundary is
included in the read.  Source readers forward this operator to the engine's
`apply_watermark_filter` method.

| Mode | Operator | Semantics |
|------|----------|-----------|
| Normal ETL | `>` | Exclusive — skip the exact last-saved row |
| Replay chunk | `>=` | Inclusive — include the chunk start value |

## Backward look-back

`Connection.date_backward` reads backward offsets from the `configure` dict:

```yaml
configure:
  backward_days: 7
  # or nested:
  backward:
    days: 7
    months: 1
    closing_day: 25
```

Useful when upstream systems occasionally correct historical rows and you want
to replay a window rather than just "> last watermark".

## Write-side update

After a write succeeds, the source reader's computed watermark is persisted by
the driver through `WatermarkManager.save_watermark(...)`. Readers compute the
next watermark from the configured `source.watermark_columns` during the read
flow, and the driver saves that value only after the destination write
succeeds. On failure nothing is written — the next run retries from the same
watermark.

### Replay watermark behaviour

During `run_replay()`, watermark persistence is controlled by
`ReplayConfig.save_watermark`:

| `save_watermark` | Behaviour |
|------------------|-----------|
| `False` (default) | Production watermark is **never touched** — safe for backfill into new tables |
| `True` | Chunk upper bound is saved after each successful chunk — enables crash-resume |

When `save_watermark=True`, already-completed chunks are skipped on re-run by
comparing the stored watermark against chunk boundaries.

### `watermark_window` (range-based delete)

After reading, the driver calls `DataFlow.apply_watermark_window(source_runtime)`
to compute a `{column: (lower, upper)}` mapping from the source's effective
watermark bounds.  This window is stored on `dataflow.watermark_window` and
used by `MergeOverwriteStrategy` when `replace_by_watermark` is enabled to
delete all target rows in the range before re-inserting fresh data.

The window is only computed when:

- `destination.replace_by_watermark` is `True`
- Both lower and upper bounds are available (i.e. `source_runtime.watermark_before` and `source_runtime.watermark_after` are populated)

See [Destination · replace_by_watermark](../how-to/metadata-guide/destination-and-load-patterns.md#replace_by_watermark-range-based-delete).

See [How-to · Replay & backfill](../how-to/replay-and-backfill.md).

## Empty watermark semantics

`is_watermark_empty(wm)` returns `True` when:

- `wm is None`
- `wm == {}`
- every value in `wm` is `None`

Empty watermark means "read everything". The first run of a new dataflow
always has an empty watermark.

## Related

- [Concepts · Metadata providers](metadata-providers.md)
- [`reference/api/watermark`](../reference/api/watermark.md)
