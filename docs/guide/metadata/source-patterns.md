---
title: Source configuration — DataCoolie User Guide
description: Configure every DataCoolie source type and its incremental behavior — file, Delta, Iceberg, SQL table/query, REST API, and Python function.
---

# Source

**Prerequisites** · A reusable [connection](connections.md), an inline
connection, and a dataflow envelope from [Dataflows](dataflows.md).  
**End state** · A working `connections` entry and `source` block for any DataCoolie source type.

A source connection describes **where DataCoolie reads data from**. Two things
work together: the [Connection](../../reference/metadata-schema.md#connection)
(shared, reusable endpoint definition) and the [Source](../../reference/metadata-schema.md#source)
block inside each dataflow (table/query/function selector and
incremental options).

[`connections[].configure`](../../reference/metadata-schema.md#connectionsconfigure)
carries reusable settings; [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure) carries
source-specific settings. Precedence depends on the field: `read_options` merge
key by key (source wins on matching keys), while a source look-back replaces
the entire connection look-back. API settings have their own placement and
precedence rules; there is no universal `configure` merge. See
[connection defaults](connections.md#put-reusable-defaults-on-the-connection)
and [look-back overrides](source-incremental.md#override-the-connection-for-one-source).

For the read lifecycle, bounded ranges and reader-specific option routing,
see [Sources & destinations](../../reference/concepts/sources-and-destinations.md#source-contract).

## Source at a glance

This is a valid query-source fragment. Here, `table` is a short, human-readable
alias for the query result, not the table DataCoolie reads. A table source uses
`table` without `query`; a function source uses `python_function` and may also
set `table`. The framework calls the function named by `python_function`, while
the function may use any fields on the `Source` it receives, including `table`.

```json
"source": {
  "connection_name":    "postgres_src",
  "table":              "active_orders",
  "query":              "SELECT updated_at, status, order_id FROM sales.orders",
  "watermark_columns":  ["updated_at"],
  "filter_expression":  "status = 'active'"
}
```

| Field | When you use it |
|-------|------------------|
| `connection_name` or `connection` | Select a named reusable connection with `connection_name` (or a string `connection` reference), or supply an inline `connection` object; use only one of the two fields |
| `schema_name` | Table mode: folder / schema namespace. Query mode: optional metadata namespace, such as for shared schema-hint matching; not needed merely because SQL names a schema |
| `table` | File folder or lakehouse/database table; optional logical alias alongside `query`, or metadata that a Python function can use |
| `query` | Inline SQL or a relative/artifact `.sql` file; takes precedence over `table` when reading |
| `python_function` | Function-source mode instead of a physical table |
| `watermark_columns` | Incremental loading |
| `filter_expression` | Read-stage SQL predicate evaluated against reader-result columns before transforms; query mode uses returned columns/aliases |
| `configure` | Source-specific settings such as `read_options` or API endpoint/params; precedence varies by key |

In practice, choose the read mode first:

- `table` for direct file, lakehouse, or database table reads.
- `query` for database or lakehouse SQL reads when one executable SQL
  statement expresses the source, including joins, filters, and CTEs.
- `python_function` when reading data needs more complex Python logic or a
  source case the built-in readers do not cover.

These are choices about how data is read, not strict complexity levels. A CTE
still belongs in `query` if the result can be produced by one SQL statement;
use `python_function` when SQL alone is not the right read boundary.

For a query source, optional `table` is a readable logical alias; the SQL
controls the read. For a Python function source, `table` is also optional, but
it may be a real input to the user function because the function receives the
whole `Source` object. The same applies to other Source fields: configure them
according to that function's contract. The framework still selects the
function by `source.python_function`.

An uncommon but supported case is attaching top-level `schema_hints` to a
query or function source. The provider matches those hints using the source
connection and `source.table`, plus `source.schema_name` when supplied. Only
use a matching group if its hints describe the resulting columns. Usually SQL
or custom function code already handles types; a few casts can be defined in
`transform.schema_hints`. See [Datatypes and schema hints](data-types.md).

`watermark_columns` enables **incremental loading**. The saved watermark and filter
depend on the reader: SQL may push bounds into the query, while files and
lakehouse sources can filter in the engine. API request push-down is optional;
without it, the API reader can still filter fetched records locally. For a
predicate that uses only source-result columns, prefer `source.filter_expression`
when an earlier read-stage filter is useful. Pushdown is reader-specific and
not guaranteed, and DataCoolie does not automatically move a transform filter
to the source. See
[Incremental windows and look-back](source-incremental.md#incremental-windows-and-look-back) for
bounds and look-back.

## Decide source type

| Source family | Connection shape | Typical source fields |
|---------------|------------------|------------------------|
| File | `connection_type: file`, file format | `schema_name`, `table`, `watermark_columns`, `configure.read_options` |
| Lakehouse | `connection_type: lakehouse`, `delta` or `iceberg` | `table` or `query`, optional `schema_name`, `watermark_columns` |
| Database | `connection_type: database`, `format: sql` | `table` or `query`, `schema_name`, `watermark_columns` |
| REST API | `connection_type: api`, `format: api` | `configure.endpoint`, `configure.params/body`, pagination, watermark push-down; `source.table` is optional |
| Python function | `connection_type: function`, `format: function` | `python_function`, optional `table` and other Source inputs, `watermark_columns`, custom `source.configure` |

---

## File source (CSV, Parquet, JSON, JSONL, Avro, Excel)

[File source (CSV, Parquet, JSON, JSONL, Avro, Excel)](source-files.md#file-source-csv-parquet-json-jsonl-avro-excel).

<span id="per-dataflow-file-overrides"></span>
[Per-dataflow file overrides](source-files.md#per-dataflow-file-overrides).

<span id="file-source-incremental-cases"></span>
[File-source incremental cases](source-files.md#file-source-incremental-cases).

## Lakehouse source (Delta or Iceberg)

[Lakehouse source (Delta or Iceberg)](source-lakehouse.md#lakehouse-source-delta-or-iceberg).

<span id="lakehouse-sql-query"></span>
[Lakehouse SQL query](source-lakehouse.md#lakehouse-sql-query).

## Database source

[Database source](source-database.md#database-source).

<span id="database-authentication-types"></span>
[Database authentication types](source-database.md#database-authentication-types).

<span id="database-transport-options"></span>
[Database transport options](source-database.md#database-transport-options).

<span id="polars-database-result-typing"></span>
[Polars database result typing](source-database.md#polars-database-result-typing).

<span id="inline-sql-or-sql-file-query-sources"></span>
[Inline SQL or SQL-file query sources](source-database.md#inline-sql-or-sql-file-query-sources).

<span id="read-a-sql-file"></span>
[Read a SQL file](source-database.md#read-a-sql-file).

<span id="database-source-sql-via-sqlalchemy"></span>

## REST API source

[REST API source](source-api.md#rest-api-source).

## API source configuration

[API source configuration](source-api.md#api-source-configuration).

<span id="match-the-request-and-response-shape"></span>
[Match the request and response shape](source-api.md#match-the-request-and-response-shape).

<span id="choose-the-pagination-contract"></span>
[Choose the pagination contract](source-api.md#choose-the-pagination-contract).

<span id="one-response-no-pagination"></span>
[One response (no pagination)](source-api.md#one-response-no-pagination).

<span id="offset-pagination-with-provider-specific-parameter-names"></span>
[Offset pagination with provider-specific parameter names](source-api.md#offset-pagination-with-provider-specific-parameter-names).

<span id="cursor-pagination"></span>
[Cursor pagination](source-api.md#cursor-pagination).

<span id="next-link-pagination"></span>
[Next-link pagination](source-api.md#next-link-pagination).

<span id="push-down-and-split-watermark-ranges"></span>
[Push down and split watermark ranges](source-api.md#push-down-and-split-watermark-ranges).

<span id="avoid-invalid-combinations"></span>
[Avoid invalid combinations](source-api.md#avoid-invalid-combinations).

## Python function source

[Python function source](source-functions.md#python-function-source).

## Incremental reads with `watermark_columns`

[Incremental reads with `watermark_columns`](source-incremental.md#incremental-reads-with-watermark_columns).

## Incremental windows and look-back

[Incremental windows and look-back](source-incremental.md#incremental-windows-and-look-back).

<span id="choose-a-fixed-look-back"></span>
[Choose a fixed look-back](source-incremental.md#choose-a-fixed-look-back).

<span id="override-the-connection-for-one-source"></span>
[Override the connection for one source](source-incremental.md#override-the-connection-for-one-source).

<span id="use-a-closing-day-strategy-for-monthly-corrections"></span>
[Use a closing-day strategy for monthly corrections](source-incremental.md#use-a-closing-day-strategy-for-monthly-corrections).

<span id="combine-look-back-with-watermark-window-replacement"></span>
[Combine look-back with watermark-window replacement](source-incremental.md#combine-look-back-with-watermark-window-replacement).

<span id="relate-look-back-to-api-ranges-and-replay"></span>
[Relate look-back to API ranges and replay](source-incremental.md#relate-look-back-to-api-ranges-and-replay).

<span id="validate-an-incremental-configuration"></span>
[Validate an incremental configuration](source-incremental.md#validate-an-incremental-configuration).

## Filter rows at read time (`source.filter_expression`)

[Filter rows at read time (`source.filter_expression`)](source-filtering.md#filter-rows-at-read-time-sourcefilter_expression).

<span id="database-sources"></span>
[Database sources](source-filtering.md#database-sources).

<span id="file-delta-iceberg-api-and-function-sources"></span>
[File, Delta, Iceberg, API, and function sources](source-filtering.md#file-delta-iceberg-api-and-function-sources).

<span id="when-to-use-sourcefilter_expression-vs-transformfilter_expression"></span>
[When to use `source.filter_expression` vs `transform.filter_expression`](source-filtering.md#when-to-use-sourcefilter_expression-vs-transformfilter_expression).

## Common mistakes

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `ValidationError: format not allowed` | `connection_type` and `format` don't match | Use `lakehouse` + `delta`, not `file` + `delta` |
| Path not found | `base_path / schema_name / table` resolves wrong | Check `base_path` exists; `schema_name` is optional |
| `APIReader requires 'base_url'` | API connection used `url` instead of `base_url` | Put the root URL in `connection.configure.base_url` |
| `PythonFunctionReader requires source.python_function` | Function path was put on the connection or omitted | Put a dotted path like `mypkg.sources.load_orders` on `source.python_function` |
| No incremental filtering | `watermark_columns` missing from `source` block | Add `"watermark_columns": [...]` and verify the selected reader's watermark behavior |
| API fetches all pages despite incremental filtering | Watermark columns are set, but request push-down mapping is absent | Configure API watermark mapping when the provider supports it; local filtering alone does not reduce fetch volume |
| Credentials exposed in logs | URL/token/password is hardcoded | Move the field to `configure`, store the secret name there, and resolve with `secrets_ref` |
| API pages repeat or skip records | Pagination/range boundary does not match the provider contract | Use the exact pagination paths/parameter names and review [API source configuration](source-api.md#api-source-configuration) |

---

## Next

→ [Transform patterns](transform-patterns.md) · [Destination & load patterns](destination-and-load-patterns.md) · [Datatypes and schema hints](data-types.md)
