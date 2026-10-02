---
title: Sources and Destinations — DataCoolie Concepts
description: Understand how DataCoolie sources read into engine dataframes and destinations write or maintain tables across files, databases, Delta, and Iceberg.
---

# Sources & destinations

**TL;DR** Sources and destinations are plugins keyed by **format name**. A
`FileReader` serves `parquet`, `csv`, `json`, `jsonl`, `avro`, `excel` — the
plugin registry maps a format string to the reader/writer class at runtime.

Install the capability profile for the format and engine you use: compose
`polars-delta` or `polars-iceberg` for Polars lakehouse work, and
`spark-delta` for a local or CI Spark + Delta runtime. Native Fabric,
Databricks, and AWS Glue runtimes provide their Spark/Delta libraries; add only
the external platform or source profile needed by that job.

## Registry mapping

From `pyproject.toml` (see [Plugin entry points](../plugin-entry-points.md)
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
- `read(source, watermark_start=None, *, watermark_start_operator=None, watermark_end=None, watermark_end_operator=None, read_range=None, preserve_empty=False) -> DF | None` — public entry point (Template Method)
- Subclasses implement `_read_internal(source, watermark_start, *, watermark_end=None)` and optionally `_read_data(source, configure)`

Read options follow the reader's format. File, lakehouse, and database readers
consume `source.read_options`, which merges `source.connection.read_options`
with `source.configure["read_options"]` (source values override connection
defaults). `APIReader` reads endpoint, pagination, mapping, header, and related
settings directly from `source.configure`; it does not automatically forward a
generic `read_options` map. `PythonFunctionReader` resolves
`source.python_function` and passes the `Source` model plus bounds/range to the
user function; function-specific options remain in `source.configure`.

`watermark_start` is the lower bound (previously named `watermark`).
`watermark_start_operator` controls the comparison (`">"` for normal ETL,
`">="` for replay's inclusive lower bound).  `watermark_end` provides an
optional upper ceiling for replay chunks; `watermark_end_operator` defaults
to `"<"` (exclusive).

`read_range` is the source-owned `SourceReadRange` contract. It is mutually
exclusive with explicit watermark bounds and is accepted only by a reader that
opts in through `_supports_read_range()`. The reader must enforce the exact
range on its returned frame before it counts rows or produces a watermark
candidate; returning `True` without that filter is not bounded-read support.
`preserve_empty=True` is reserved for a confirmed bounded replacement where a
typed zero-row frame represents a real window.

When watermark persistence is enabled after a successful read, the driver
obtains the reader's candidate and calls its
`merge_watermark(existing, candidate)` boundary before writing. It saves the
merged value only after the destination write succeeds. Empty or all-null
candidates do not advance state, and a source-qualified ordered candidate must
not move an existing value backward. See [Writing a source](../../extensions/writing-a-source.md)
for the custom-reader hooks.

The source reader is responsible for watermark filtering. Database readers
push a `WHERE` clause into SQL and API readers can map bounds into request
parameters. File, Delta, Iceberg, and function readers apply the active
engine's DataFrame filter after reading (file readers can additionally prune
date folders or files by modification time).

### `filter_expression` (post-read filter)

All built-in readers combine or apply `source.filter_expression` after the
logical watermark condition. Database readers push the expression into their
generated SQL `WHERE` clause; file, Delta, Iceberg, API, and function readers
apply it through the engine after reading. For in-process readers, the engine
evaluates it as a SQL predicate against the returned DataFrame:

```
watermark filter  →  source.filter_expression  →  count / new watermark
```

This excludes rows before the transformer pipeline runs. Reference columns
available in the reader result: for database `source.query`, the reader wraps
the SQL as a derived table, so the predicate uses its returned columns or
aliases. Database readers combine it with the watermark condition in the
generated `WHERE` clause.

### Secret resolution

During execution preparation, the driver processes `connection.secrets_ref`
and replaces placeholder values in `configure` with actual credentials from
the active secret provider. Readers never handle secret fetching directly.

## Destination contract

`BaseDestinationWriter[DF]` declares:

- `__init__(engine)`
- `write(df, dataflow, *, watermark_window: Optional[WindowSpec] = None) -> DestinationRuntimeInfo` — public entry point (Template Method)
- Subclasses implement `_write_internal(df, dataflow, *, watermark_window: Optional[WindowSpec] = None) -> None`
- `run_maintenance(dataflow, *, do_compact=True, do_cleanup=True, retention_hours=None) -> DestinationRuntimeInfo`

`watermark_window` is keyword-only and attempt-local. When a valid bounded
replacement window is present, the writer passes the immutable `WindowSpec`
through to the selected load strategy; ordinary writes leave it unset.

`write` dispatches on `load_type`:

- `append` → `engine.write_to_*(mode="append")`
- `overwrite` / `full_load` → `engine.write_to_*(mode="overwrite")`
- `merge_upsert` → `engine.merge_to_*`
- `merge_overwrite` → `engine.replace_window(..., options=destination.write_options)` when `replace_by_watermark` has a usable window; otherwise `engine.merge_overwrite(..., options=destination.merge_options, write_options=destination.write_options)`
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
- **Two filter points exist** — `source.filter_expression` (read-time, reader
  output columns) and `transform.filter_expression` (order 35, after ColumnAdder
  creates computed columns). See
  [Transformers & pipeline](transformers-and-pipeline.md) for the pipeline
  ordering.

## Related

- [Transformers & pipeline](transformers-and-pipeline.md)
- [Load strategies](load-strategies.md)
- [Writing a source](../../extensions/writing-a-source.md)
- [Writing a destination](../../extensions/writing-a-destination.md)
