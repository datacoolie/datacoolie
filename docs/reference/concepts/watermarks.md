---
title: Watermarks — DataCoolie Concepts
description: Learn how DataCoolie stores, parses, and updates raw JSON watermarks for incremental loads across metadata providers.
---

# Watermarks

**TL;DR** A watermark is a JSON object stored by the metadata provider and
parsed by `WatermarkManager`. Tagged scalar values round-trip through JSON
without changing their Python type before the next read.

## Contract

- `BaseMetadataProvider.get_watermark(dataflow_id: str) -> Optional[str]` —
  returns raw JSON text (or `None`).
- `BaseMetadataProvider.update_watermark(dataflow_id, watermark_value, *, job_id, dataflow_run_id)` —
  persists raw serialised watermark text.
- Backend failures are errors, not empty-watermark signals. Providers may
  return `None` only for an explicit empty value or a confirmed missing
  resource; callers receive `WatermarkError` for transport, authorization,
  server, malformed-response, or storage failures.
- `WatermarkManager.get_watermark(dataflow_id: str) -> Dict[str, Any] | None` —
  deserialises via `WatermarkSerializer`.
- `WatermarkManager.save_watermark(dataflow_id, watermark: Dict[str, Any], *, job_id=None, dataflow_run_id=None)` —
  serialises and delegates persistence to the metadata provider.

Providers never touch datetimes. See
[ADR-0004](../../project/decisions/0004-raw-json-watermark-contract.md) for why.

## Storage ownership and path binding

`WatermarkManager` only serializes values and calls provider `get`/`save`
operations. Storage layout belongs to the provider. `FileProvider` accepts an
optional `watermark_base_path`; when it is omitted, the Driver may bind
`state_base_path/watermarks`, or (if state is absent) the parent of the
effective `log_base_path` plus `watermarks`. A provider with no resolved root
remains usable for metadata-only reads, but watermark operations fail instead
of silently using the metadata directory. Database and API providers retain
their own storage configuration.

## Serialisation format

```json
{"updated_at": {"__datetime__": "2026-04-03T09:15:00+00:00"}}
```

Sentinels handled by `WatermarkSerializer`:

- `__datetime__` → `datetime.fromisoformat(value)`
- `__date__` → `date.fromisoformat(value)`
- `__time__` → `time.fromisoformat(value)`
- `__decimal__` → finite `Decimal(value)` (never converted through `float`)
- `__binary__` → base64-decoded `bytes`

Everything else is plain JSON (ints, floats, strings, nested dicts, lists).
An untagged temporal-looking string such as `"09:15:00"` remains a Python
`str`; only an explicit tag such as `{"__time__": "09:15:00"}` restores a
`datetime.time`. Existing untagged string and integer checkpoints remain
readable. Invalid or ambiguous tagged values fail as corrupted checkpoint data. Decimal values
must be finite. Binary values are reversible at the checkpoint layer, but their
ordering and SQL comparison semantics remain disabled until a concrete backend
and driver are qualified; a SQL reader fails before issuing a query for a
binary watermark rather than comparing a textual encoding.

Temporal values remain typed through Polars and Spark engine filters. A string
watermark is converted only when the destination column is a native temporal
column (or the column itself is string); the serializer does not stringify a
typed date or datetime globally.

## Read-side filtering

Source readers apply the watermark **during** the read when possible:

- **Parquet / Delta / Iceberg** — read through the engine, then apply the
  engine's DataFrame watermark filter.
- **Database (JDBC / connectorx)** — appended `WHERE watermark_col > ...`.
- **API** — canonical `source.configure.range_param_mapping` injects active
  lower/upper bounds into query parameters or top-level JSON body fields.
  `watermark_to_param_timezone` and the `watermark_range_*` controls remain
  available for timezone selection and canonical incremental splitting.
- **CSV / JSON / Excel** — full scan, then filter in the engine (fallback).
  Schema hints are applied by the transform layer, not by the reader; when a
  weakly typed source is used for a watermark, configure the source's native
  inference/read options (for example CSV `inferSchema`) or provide values
  that already have a comparable source type. A transform-only hint cannot
  repair a reader-side watermark comparison after the read.

### Watermark operator

For ordinary observed-max state the comparison is `>` (strict
greater-than), meaning "only rows newer than the last saved value". A source
whose persisted state represents an exclusive covered request end resolves
omitted continuation to `>=`. Replay carries its own exact `[start, end)` range
through `SourceReadRange` and does not infer the range from persisted state.

| Mode | Operator | Semantics |
|------|----------|-----------|
| Normal ETL, observed max | `>` | Exclusive — skip the exact last-saved row |
| Normal ETL, saved request end | `>=` | Inclusive — resume at the first uncovered value |
| Replay chunk | `>=` / `<` | Exact left-closed, right-open source range |

### API range state and manual cutover

For canonical API bindings, `watermark_value: "observed_max"` means that the
saved value is the greatest response value actually observed. A lower-only
historical range must keep that meaning. `watermark_value: "request_end"` means
that the saved value is an exclusive covered request end; use it only after the
endpoint's complete-pagination guarantee and checkpoint meaning are verified.
The next lower operator is `>` for an observed maximum and `>=` for a covered
request end.

An inclusive endpoint operator (`<=`) is paired with response-side residual
filtering for the exact exclusive request end; do not subtract an epsilon. Do
not reset, coerce, or infer saved state from the shape of a scalar. If precision
or completeness is uncertain, pause continuation and use a controlled,
operator-approved replay for a verified interval. Rolling back a binary or
metadata file does not roll back destination rows or saved state; reconcile or
replay from operator-held snapshots.

## Backward look-back

`Source.date_backward` is the effective look-back configuration. Backward
settings in `source.configure` override the referenced connection's fallback in
`connection.configure`; when the source has no backward setting, the connection
value is used:

```yaml
source:
  configure:
    backward_days: 7
    # or nested:
    # backward:
    #   days: 7
    #   months: 1
    #   closing_day: 25
connection:
  configure:
    backward:
      days: 7
```

Useful when upstream systems occasionally correct historical rows and you want
to replay a window rather than just "> last watermark".

### Runtime selection and watermark evidence

The reader's `SourceReadResult.selection` is the source of control for one
read. It records the effective mode (`full`, `incremental`, or `range`), lower
and upper values, and the operators. `SourceReadResult.candidate` is the typed
source observation produced by that read. `SourceRuntimeInfo` carries timing,
status, message, nullable row-count, source-action, and selection diagnostics;
it does not replace the typed result or control the replacement window. Range
snapshots are not per-page API request traces, and API-generated split
boundaries are not inferred into them.

The canonical attempt watermark record is `WatermarkRuntimeInfo`. It keeps
provider load state (`load_status` and `loaded_watermark`), the merge base
(`merge_base`), the typed source candidate (`candidate`), the prepared
payload (`watermark_to_save`), and the provider acknowledgement
(`save_status` and `saved_watermark`) separate. `load_status` distinguishes
`loaded` from `reused` (and can report `load_error`); save status is one of
`not_requested`, `not_attempted`, `no_candidate`, `saved`, or `save_error`.
Only `saved` includes `saved_watermark`, copied from the exact merged payload
after the provider save returns successfully. `save_error` is ambiguous: the
provider may have committed the value before raising. Existing source
watermark fields retain their observation meanings and are not rewritten as
save receipts.

## Write-side update

When watermark persistence is enabled, before a destination write the pipeline
validates the exact watermark payload that could be persisted. This catches
unsupported or non-finite values before a destructive write. A non-empty reader
result supplies a candidate; the driver asks that reader to merge it with
stored state, validates the merged value, writes the destination, and only
then calls `WatermarkManager.save_watermark(...)`. The manager is the storage
and serialization boundary; source readers own the merge semantics. The
driver invokes `source.merge_watermark(existing, result.candidate)`, where
the candidate parameter is `WatermarkCandidate | None`. A source result with
`frame is None` or `rows_read == 0` skips transform, write, and save, including
for a bounded replay; its candidate remains attempt evidence and its save
status stays `not_attempted`. An empty or all-null candidate is otherwise not a
save request; a real zero remains valid. If the destination write fails, state
is unchanged. If the later state save fails or the process stops after the
write, the destination may already contain the committed rows while the old
watermark remains. Rerun the complete requested range and use an idempotent
destination strategy when that gap could deliver duplicates.

File sources with `connection.configure.date_folder_partitions` are a special
source-owned discovery case: they persist the internal folder frontier even
when `source.watermark_columns` is empty, and load it on the next ordinary run
to prune older folders. Folder boundary discovery remains inclusive; use an
authored row watermark or `__file_modification_time` when exact row/file
selection is required.

### Replay watermark behaviour

During `run_replay()`, watermark persistence is controlled by
`ReplayConfig.save_watermark`:

| `save_watermark` | Behaviour |
|------------------|-----------|
| `False` (default) | Production watermark is **never touched** — safe for backfill into new tables |
| `True` | Reader-produced candidate (observed maximum or source-confirmed request end) is merged and saved after each successful, non-empty chunk |

`save_watermark=True` requires a configured `watermark_manager`; replay fails
during preparation before a reader or destination is created when persistence
is requested without one. The default `False` mode does not require a
watermark store.

Replay never uses the stored watermark as replay progress. A later invocation
re-runs every requested chunk, even when `save_watermark=True`. Ordered
watermark values merge monotonically per key; opaque cursor values replace
their own key, while keys absent from a candidate remain stored.

### `watermark_window` (range-based replacement)

After reading, the execution pipeline creates an immutable, attempt-local
window from `SourceReadResult.selection` and
`SourceReadResult.candidate`. Explicit replay bounds therefore remain
the requested `[start, end)` operators, while an ordinary incremental run uses
the reader's effective lower selection and source candidate. Mutable runtime
fields do not control this window. The window is passed directly to the
destination strategy and engine; it is not stored on authored `DataFlow`,
`RunConfig`, logger context, or the watermark provider. Each retry receives a
fresh result and window.

`MergeOverwriteStrategy` calls the engine-owned `replace_window` operation.
The engine validates the final transformed columns and owns the format-native
predicate plus delete/append ordering. The portable fallback is delete then
append and is not atomic.

The window is only computed when:

- `destination.replace_by_watermark` is `True`
- Both bounds for the selected contract are available. Ordinary runs use the
  result's lower selection and source candidate; replay uses its explicit
  source range. The lower selection includes any configured source look-back.
- The upper operator follows the state meaning: an observed row maximum is an
  inclusive replacement boundary, while an exclusive request-end observation
  remains exclusive. A date-folder discovery key is internal and does not
  authorize a row replacement window.
- Built-in rename/sanitization must map each active replacement column to the
  final destination schema. Dropped, ambiguous, or unknown mappings fail
  before a delete; ordinary incremental loads may still drop source watermark
  columns.
- A source result with no rows is skipped before replacement, even when the
  replay range is bounded, so it cannot infer a delete scope. If a non-empty
  source result becomes empty during transformation, no post-transform count
  changes the result; the writer receives the empty frame and a replacement
  writer may delete the selected scope. It never creates an absent destination
  solely from an empty transformed batch.

See [Replace a complete watermark window](../../guide/metadata/watermark-window-replacement.md).

See [User guide · Replay & backfill](../../guide/operations/replay-and-backfill.md).

## Empty watermark semantics

`is_watermark_empty(wm)` returns `True` when:

- `wm is None`
- `wm == {}`
- every value in `wm` is `None`

Empty watermark means "read everything". The first run of a new dataflow
always has an empty watermark.

## Related

- [Concepts · Metadata providers](metadata-providers.md)
- [`reference/api/watermark`](../api/watermark.md)
