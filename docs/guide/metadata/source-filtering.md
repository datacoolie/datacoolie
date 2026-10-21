---
title: Source row filtering | DataCoolie
description: Configure source row filtering with metadata, examples and backend-specific guidance.
---

# Source row filtering

[All source recipes](source-patterns.md).

## Filter rows at read time (`source.filter_expression`)

`filter_expression` is a SQL predicate combined with or applied **after** the
watermark condition by the source reader, before transforms. It references
columns in the reader's result. For a database `source.query`, use columns or
aliases returned by that query, not columns hidden inside it.

```json
"source": {
  "connection_name":   "postgres_src",
  "schema_name":       "public",
  "table":             "orders",
  "watermark_columns": ["updated_at"],
  "filter_expression": "status = 'active' AND region = 'US'"
}
```

This is particularly useful when the source table holds multiple logical
datasets and you only want one segment, or when you want to exclude known
bad data before it enters the pipeline at all.

### Database sources

For SQL table/query sources, `filter_expression` is combined with the generated
`WHERE` clause alongside the watermark condition. In query mode, the reader
wraps your SQL as a derived table and filters its output columns, so the query
must be valid when nested and expose the watermark and predicate columns:

```sql
-- generated SQL (conceptual)
SELECT * FROM orders
WHERE updated_at > '2024-01-01'
  AND status = 'active'
  AND region = 'US'
```

### File, Delta, Iceberg, API, and function sources

For file, lakehouse, API, and Python function readers, the predicate is applied
by the engine to the returned DataFrame after the read. For API reads this
happens after all pages/ranges have been fetched and before the transform
pipeline starts; it does not reduce HTTP volume.

### When to use `source.filter_expression` vs `transform.filter_expression`

| | `source.filter_expression` | `transform.filter_expression` |
|---|---|---|
| **Stage** | Read time (earliest possible) | Transformer order 35 (after ColumnAdder) |
| **Available columns** | Reader output columns (including query aliases and available watermark columns), but not columns added by transforms | Reader output + columns from `additional_columns` |
| **Best for** | Excluding rows that should never enter the pipeline | Filtering on computed/derived columns |

For an incremental source, the pipeline selects the reader's new watermark
before transform filters run and saves it after a successful write. Rows removed
only by `transform.filter_expression` do not automatically return if you widen
that filter later. Plan a bounded [replay](../operations/replay-and-backfill.md)
when previously filtered history must be restored.
