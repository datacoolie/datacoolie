---
title: Sources and Destinations — DataCoolie Concepts
description: Understand how DataCoolie sources read into engine dataframes and destinations write or maintain tables across files, databases, Delta, and Iceberg.
---

# Sources & destinations

**TL;DR** Sources and destinations are plugins keyed by **format name**. A
`FileReader` serves `parquet`, `csv`, `json`, `jsonl`, `avro`, `excel` — the
plugin registry maps a format string to the reader/writer class at runtime.

## Registry mapping

From `pyproject.toml` (see [Plugin entry points](../reference/plugin-entry-points.md)
for the generated table):

| Format | Source | Destination |
|---|---|---|
| `delta` | `DeltaReader` | `DeltaWriter` |
| `iceberg` | `IcebergReader` | `IcebergWriter` |
| `parquet` / `csv` / `json` / `jsonl` / `avro` / `excel` | `FileReader` | `FileWriter`* |
| `sql` | `DatabaseReader` | — |
| `api` | `APIReader` | — |
| `function` | `PythonFunctionReader` | — |

\* Excel is **read-only**; `FileWriter` handles the writable formats.

## Source contract

`BaseSourceReader[DF]` declares:

- `__init__(engine)` — the reader is constructed with an engine
- `read(source, watermark, *, watermark_operator=">") -> DF` — public entry point (Template Method)
- Subclasses implement `_read_internal(source, watermark)` and optionally `_read_data(source, configure)`
- Must honour `source.read_options` and `source.connection.read_options`

The `watermark_operator` parameter controls the comparison used for the
incremental filter (`">"` for normal ETL, `">=" ` for replay's inclusive
lower bound).

The source reader is also responsible for **watermark push-down** — applying
the watermark predicate *during* the read when the backend supports it (e.g.
parquet predicate pushdown, JDBC WHERE clause). If pushdown is not possible the
reader returns the full DataFrame and the framework filters afterwards.

### `filter_expression` (post-read filter)

After watermark filtering, every reader calls `_apply_filter_expression(df,
source)`.  When `source.filter_expression` is non-empty, the engine evaluates
it as a SQL WHERE clause against the in-memory DataFrame:

```
watermark filter  →  source.filter_expression  →  count / new watermark
```

This lets you exclude rows at read time using raw source columns before the
transformer pipeline runs.  For database readers the expression is pushed into
the generated WHERE clause alongside the watermark condition.

### Secret resolution

Before the reader is created, the driver calls `_resolve_connection_secrets()`
which processes `connection.secrets_ref` and replaces placeholder values in
`configure` with actual credentials from the active secret provider.  Readers
never handle secret fetching directly.

## Destination contract

`BaseDestinationWriter[DF]` declares:

- `__init__(engine)`
- `write(df, dataflow) -> DestinationRuntimeInfo` — public entry point (Template Method)
- Subclasses implement `_write_internal(df, dataflow) -> None`
- `run_maintenance(dataflow, *, do_compact=True, do_cleanup=True, retention_hours=None) -> DestinationRuntimeInfo`

`write` dispatches on `load_type`:

- `append` → `engine.write_to_*(mode="append")`
- `overwrite` / `full_load` → `engine.write_to_*(mode="overwrite")`
- `merge_upsert` → `engine.merge_to_*`
- `merge_overwrite` → `engine.merge_overwrite_to_*`
- `scd2` → `engine.scd2_*`

See [Load strategies](load-strategies.md) for semantics.

## Non-obvious behaviours

- **`_attach_schema_hints` uses the source connection/table**, not the
  destination. The goal is to cast the incoming DataFrame *into* the declared
  column types before writing.
- **Column-case mode** defaults to `ColumnCaseMode.LOWER` on the driver — the
  `ColumnNameSanitizer` downcases and strips at write time.
- **Decimal precision / scale** are honoured when writing to Delta; some Polars
  readers upcast to `Float64` by default — `SchemaHint` with
  `data_type="decimal"` fixes that.
- **Two filter points exist** — `source.filter_expression` (read-time, raw
  columns) and `transform.filter_expression` (order 35, after ColumnAdder
  creates computed columns). See
  [Transformers & pipeline](transformers-and-pipeline.md) for the pipeline
  ordering.

## Related

- [Transformers & pipeline](transformers-and-pipeline.md)
- [Load strategies](load-strategies.md)
- [Writing a source](../extending/writing-a-source.md)
- [Writing a destination](../extending/writing-a-destination.md)
