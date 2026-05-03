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
