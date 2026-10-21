---
title: Computed columns and row filters | DataCoolie
description: Configure computed columns and row filters with metadata, examples and backend-specific guidance.
---

# Computed columns and row filters

[All transform recipes](transform-patterns.md).

## ColumnAdder

**When to use:** you need a new column whose value is calculated from existing
columns (derived date parts, string concatenation, status labels, etc.).

Each `transform.additional_columns` item follows the [Additional column](../../reference/metadata-schema.md#additional-column)
shape: it defines a `column`
and its `expression`.

```json
"transform": {
  "additional_columns": [
    { "column": "order_year",   "expression": "EXTRACT(YEAR FROM order_date)" },
    { "column": "order_month",  "expression": "EXTRACT(MONTH FROM order_date)" },
    { "column": "full_name",    "expression": "first_name || ' ' || last_name" },
    { "column": "is_large",     "expression": "CASE WHEN amount > 1000 THEN true ELSE false END" }
  ]
}
```

Expressions are **SQL** evaluated against the DataFrame after schema casting.
Use standard SQL scalar functions — the Polars and Spark engines both support
`EXTRACT`, `CASE WHEN`, string functions, and arithmetic.

The class also removes stale framework system columns from the incoming data,
even when `additional_columns` is empty. `SystemColumnAdder` adds the current
execution's audit columns later.

### Constants, replacing a column, and dependent expressions

Items run in declaration order. You can add a constant, replace an existing
business column, then use that result in the next expression:

```json
"transform": {
  "additional_columns": [
    {"column": "source_system", "expression": "'ERP'"},
    {"column": "amount", "expression": "amount * 100"},
    {"column": "is_large", "expression": "CASE WHEN amount > 100000 THEN true ELSE false END"}
  ]
}
```

The last expression sees the updated `amount`. SQL string constants need SQL
quotes inside the JSON string. Each expression must be non-empty. Use scalar
SQL expressions supported by the selected engine; arbitrary queries, joins,
and Python function calls belong in the source query/function workflow.

!!! warning "Polars SQL limitations"
  Polars does not support `current_timestamp()` or `NOW()`.
    Use `EXTRACT(YEAR FROM col)` instead of `year(col)`.
    Use `CAST(col AS DATE)` instead of `date(col)`.

!!! warning "Do not reference system columns here"
  `additional_columns` runs at transformer order 30. System columns are only
  added later at order 70, so expressions here cannot rely on `__created_at`,
  `__updated_at`, `__updated_by`, or `__dataflow_run_id`. Let the framework add
  those columns for you and use them after the transform stage, not inside it.

---

## RowFilter

Configure `transform.filter_expression`
when you need to drop rows before writing to the destination.
The [Transform reference](../../reference/metadata-schema.md#transform)
defines this field.
The predicate runs at order **35**, _after_ `ColumnAdder` (30), so it can
reference columns you created in `additional_columns`.

```json
"transform": {
  "filter_expression": "status = 'active' AND amount > 0"
}
```

`filter_expression` is a **SQL predicate** — everything you would normally
place after `WHERE`. Both Polars and Spark engines evaluate it against the
DataFrame.

### Reference a computed column

Because `RowFilter` runs _after_ `ColumnAdder`, you can filter on columns
added by `additional_columns`:

```json
"transform": {
  "additional_columns": [
    { "column": "order_year", "expression": "EXTRACT(YEAR FROM order_date)" }
  ],
  "filter_expression": "order_year >= 2023"
}
```

### Combine multiple conditions

```json
"transform": {
  "filter_expression": "region = 'US' AND status NOT IN ('cancelled', 'draft') AND amount > 0"
}
```

!!! note "vs `source.filter_expression`"
    There are two distinct filter hooks:

    | Field | Stage | Scope |
    |-------|-------|-------|
    | `source.filter_expression` | Read time (combined with or after the logical watermark condition) | Reader output columns, including aliases returned by `source.query`; not columns added by transforms |
    | `transform.filter_expression` | Order 35 (post-ColumnAdder) | Source columns + computed columns |

    Prefer `source.filter_expression` for predicates that use only reader-output
    columns when an earlier read-stage filter is useful. The selected reader may
    push that predicate to its backend or evaluate it after reading; pushdown is
    not guaranteed, and DataCoolie does not automatically relocate a transform
    filter. Use `transform.filter_expression` when you need to filter on a
    column added by `additional_columns` or otherwise need the transform stage.

### Null conditions and skipping the filter

```json
"transform": {"filter_expression": "email IS NOT NULL AND amount >= 0"}
```

Only rows for which the predicate is true survive. Use `IS NULL` / `IS NOT NULL`
for null checks; comparisons such as `amount >= 0` do not retain null amounts.
Omit the field or use `null` to leave the row set unchanged:

```json
"transform": {"filter_expression": null}
```

Filtering runs before the new SCD2 and system columns are added, so its
expression cannot depend on those columns.
