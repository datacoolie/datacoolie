---
title: System columns and partition layout | DataCoolie
description: Configure system columns and partition layout with metadata, examples and backend-specific guidance.
---

# System columns and partition layout

[All transform recipes](transform-patterns.md).

## SCD2ColumnAdder

**When to use:** the destination keeps Type 2 history. Set `load_type` to
`scd2` in [Destination](../../reference/metadata-schema.md#destination), and set
`scd2_effective_column` in
[`destination.configure`](../../reference/metadata-schema.md#dataflowsdestinationconfigure):

```json
"destination": {
  "connection_name": "gold",
  "table": "customer",
  "load_type": "scd2",
  "merge_keys": ["customer_id"],
  "configure": {"scd2_effective_column": "updated_at"}
}
```

At order 60, this class adds `__valid_from` from the effective column,
`__valid_to` as null, and `__is_current` as true for the destination writer.
The effective column must be available at this stage. No `transform` setting
is needed. See [SCD2 configuration](destination-and-load-patterns.md#scd2-slowly-changing-dimension-type-2)
and [incremental SCD2](merge-and-scd2.md) for the history-writing workflow.

For an incoming row with `updated_at = 2026-09-25 11:10:00`, this stage produces
`__valid_from = 2026-09-25 11:10:00`, `__valid_to = null`, and
`__is_current = true`. Closing existing history is the destination writer's
responsibility. Other load types skip this transformer. Keep
`scd2_effective_column` configured for SCD2; without it this stage adds no SCD2
columns.

---

## SystemColumnAdder

This class automatically adds audit columns at order 70 on every
driver-managed dataflow. There is no metadata field to enable it or define
these columns:

| Column | Content |
|--------|---------|
| `__created_at` | Framework timestamp when the row was first written |
| `__updated_at` | Framework timestamp of the current write |
| `__updated_by` | Configured audit author |
| `__dataflow_run_id` | ID of the ETL execution or replay chunk that produced the current row/version |

You do not configure these. If a merge destination already has `__created_at`
from a previous run, the engine preserves its original value on the matched row
and sets `__updated_at` to the current run timestamp.

Retries reuse one `__dataflow_run_id`; replay chunks have their own chunk IDs.
For SCD2, closing an old version preserves its original run ID while the new
version receives the current ID. System columns cannot be referenced by
`additional_columns` (order 30), but can be used in partition expressions
(order 80).

For example, a driver-managed input containing only `order_id` leaves this
stage with `order_id`, `__created_at`, `__updated_at`, `__updated_by`, and
`__dataflow_run_id`. The timestamps, author, and run ID come from the framework
execution; no JSON entry is needed in `additional_columns`. Direct standalone
use of this class only adds `__dataflow_run_id` when its caller supplies an ID.

---

## PartitionHandler

**When to use:** the destination needs partition columns, optionally derived
from expressions. Put `partition_columns` in the
[Destination](../../reference/metadata-schema.md#destination) block. Each
[Partition column](../../reference/metadata-schema.md#partition-column) item
names a `column` and may supply an `expression`:

```json
"destination": {
  "partition_columns": [
    {"column": "order_date", "expression": "CAST(updated_at AS DATE)"},
    {"column": "region"}
  ]
}
```

Without an expression, the column must already exist. Expressions run at order
80, so they can use computed and system columns from earlier stages. The
destination writer then uses these columns to partition its output. See
[Destination partitioning](destination-and-load-patterns.md#partition_columns-partition-the-output-table)
for expression portability and further examples.

### Existing, derived, and system-column partitions

The example above combines a derived date with an existing `region` column,
which gives a two-level partition layout. An expression can also replace an
existing column with its derived value. To partition by the current ingestion
date, reference the system timestamp added at order 70:

```json
"destination": {
  "partition_columns": [
    {"column": "etl_date", "expression": "CAST(__updated_at AS DATE)"}
  ]
}
```

An empty or omitted `partition_columns` list skips this class. Destination
storage options, such as date-folder layout or writer-specific partition
behavior, are configured separately; see [Destination and load patterns](destination-and-load-patterns.md#partition_columns-partition-the-output-table).

---

## ColumnNameSanitizer

After all configured transforms run, `ColumnNameSanitizer` applies the
driver's `column_name_mode`: `lower` by default, or `snake` when requested.
This is a runtime option, so there is no field for it in metadata:

```python
# Choose one mode for a run.
result = driver.run(stage="bronze2silver", column_name_mode="lower")  # default
```

Or use word-boundary conversion:

```python
result = driver.run(stage="bronze2silver", column_name_mode="snake")
```

| Input name | `lower` | `snake` |
|---|---|---|
| `CustomerID` | `customerid` | `customer_id` |
| `HTTPStatus` | `httpstatus` | `http_status` |
| `order-date` | `order_date` | `order_date` |
| `123code` | `_123code` | `_123code` |
| `__created_at` | unchanged | unchanged |

Both modes clean special characters. Names starting with `_` are preserved;
if cleanup would produce an empty name, the original name is retained. A name
collision such as `order-date` and `order date` fails instead of creating two
`order_date` columns. Resolve it earlier with `rename_columns` or `drop_columns`.
This class also moves known system/file-info columns to the end. There is no
metadata switch to disable it; already-clean names remain unchanged.

Plan for normalized destination columns when the source uses mixed case,
quoted identifiers, or API keys like `CustomerID`. The normalized output name
must still match every downstream partition and merge field.
