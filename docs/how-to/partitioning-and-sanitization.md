---
title: Partitioning and Column Sanitization — DataCoolie How-to
description: Configure DataCoolie partition columns and column-name sanitization rules for reliable file and table outputs.
---

# Partitioning & column sanitization

**Prerequisites** · Destination is a lakehouse or file format that supports partitioning.
**End state** · Partition columns derived via SQL expressions; column names normalised for all downstream tools.

## Declarative partition columns

```json
"destination": {
  "partition_columns": [
    {"column": "order_date", "expression": "date(updated_at)"},
    {"column": "region"}
  ]
}
```

Two shapes:

- `{"column": "x"}` — use the existing column `x` as a partition column.
- `{"column": "x", "expression": "..."}` — derive `x` from a SQL expression,
  then partition on it.

The `PartitionHandler` transformer (order 80) runs *after* `SystemColumnAdder`
(order 70), so you can partition on framework audit columns like
`__updated_at`:

```json
{"column": "etl_date", "expression": "CAST(__updated_at AS DATE)"}
```

## SQL expression portability

Expressions go through `engine.add_column(df, name, expression)` which uses:

- Spark: `F.expr(expression)` — full Spark SQL surface.
- Polars: `pl.sql_expr(expression)` — **subset** of ANSI SQL.

### Polars limitations to know

| Spark works | Polars equivalent |
|---|---|
| `current_timestamp()` | `"CAST('2026-01-01' AS TIMESTAMP)"` — use a literal or the `SystemColumnAdder`'s `__updated_at`. |
| `NOW()` | Same as above. |
| `year(col)` | `EXTRACT(YEAR FROM col)` |
| `date_format(col, "yyyy-MM-dd")` | `CAST(col AS DATE)` |

Pick expressions that work on both engines if you plan to swap.

## Column name sanitization

Runs last (order 90) via `ColumnNameSanitizer`. Actions:

- Lowercase by default (`ColumnCaseMode.LOWER`).
- Replace spaces, dots, dashes with underscores.
- Strip leading digits or invalid characters.

Override by subclassing `DataCoolieDriver` and setting
`self._column_name_mode`.

## Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| `pl.sql_expr` error `"unknown function current_timestamp"` | Polars engine | Use `__updated_at` from `SystemColumnAdder`. |
| Partition column not present at write | Expression references a source-only column dropped by `ColumnAdder` | Move expression into `additional_columns` instead. |
| "yyyy-MM-dd" format error | Java-style format | Remove format string; Polars auto-detects ISO dates via `CAST(... AS DATE)`. |
