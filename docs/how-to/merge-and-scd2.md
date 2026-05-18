---
title: Merge and SCD2 Loads — DataCoolie How-to
description: Configure DataCoolie merge keys, upserts, and slowly changing dimension type 2 behavior for incremental table loads.
---

# Merge & SCD2

**Prerequisites** · Destination is a lakehouse (Delta or Iceberg). Keys exist on both source and destination.
**End state** · Incremental merges working correctly; SCD Type 2 history tracked.

## `merge_upsert` — standard upsert

```json
{
  "destination": {
    "connection": "bronze",
    "schema_name": "sales",
    "table": "customers",
    "load_type": "merge_upsert",
    "merge_keys": ["customer_id"]
  }
}
```

Matched rows are **updated**, unmatched rows are **inserted**. Nothing is
deleted. Ideal for CDC-style incremental loads where the source delivers only
changed rows.

## `merge_overwrite` — rolling window

```json
{
  "destination": {
    "load_type": "merge_overwrite",
    "merge_keys": ["event_date", "device_id"]
  }
}
```

For every `(event_date, device_id)` in the source, destination rows with the
same keys are **deleted** and the source rows are inserted. Use when the
source always holds the *full current state* for its window.

## `scd2` — Type 2 with history

```json
{
  "destination": {
    "load_type": "scd2",
    "merge_keys": ["customer_id"],
    "configure": {
      "scd2_effective_column": "updated_at"
    }
  }
}
```

`scd2_effective_column` names a source column whose value is taken as the row's
business validity start. The `SCD2ColumnAdder` transformer (order 60) adds
three framework columns before write:

- `__valid_from` — copied from `scd2_effective_column`
- `__valid_to` — NULL on new rows
- `__is_current` — `true` on new rows

On write the engine's `scd2_*` method runs a two-step MERGE:

1. **Close step** — for every source row whose `merge_keys` match a target row
   where `__is_current = true` *and* the source `__valid_from` is later than
   the target `__valid_from` (late-arrival guard), set
   `__valid_to = source.__valid_from` and `__is_current = false`.
2. **Append step** — insert all source rows as new versions.

There is **no stored hash column**. Change detection is driven by the
effective-date column you nominate — if the source delivers a row with a
newer `updated_at` than the current version, a new version is created. If it
delivers a row with an equal or older `updated_at`, the late-arrival guard
skips the close step.

### Reading "current state only"

```sql
SELECT * FROM customers_scd2 WHERE __is_current = true
```

### Reading "point-in-time"

```sql
SELECT * FROM customers_scd2
WHERE __valid_from <= CAST('2026-01-01' AS TIMESTAMP)
  AND COALESCE(__valid_to, CAST('9999-12-31' AS TIMESTAMP)) > CAST('2026-01-01' AS TIMESTAMP)
```

---

## Deduplication before merge

All merge strategies benefit from deduplication in the `transform` block.
When CDC feeds or replayed data contain duplicate keys, deduplication keeps only
the latest row per key *before* the merge executes — preventing redundant
writes and version inflation in SCD2:

```json
"transform": {
  "deduplicate_columns": ["customer_id"],
  "latest_data_columns": ["updated_at"]
}
```

If `deduplicate_columns` is not set, DataCoolie falls back to
`destination.merge_keys`. If `latest_data_columns` is not set, it falls back
to `source.watermark_columns`.

---

## Pre-merge filtering

Use `transform.filter_expression` to discard rows that should never reach the
destination — for example, soft-deleted or test rows:

```json
"transform": {
  "filter_expression": "is_deleted = false AND environment != 'test'"
}
```

This runs at transformer order 35, after computed columns are available but
before SCD2 columns are added.

For conditions on raw source columns that you want filtered as early as
possible, use `source.filter_expression` instead:

```json
"source": {
  "connection_name": "cdc_source",
  "table": "orders_changes",
  "watermark_columns": ["updated_at"],
  "filter_expression": "region = 'US'"
}
```

---

## Which strategy to pick

See the decision tree in [Concepts · Load strategies](../concepts/load-strategies.md).

---

## Common merge issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Duplicate versions in SCD2 | Same key arrives multiple times per run | Add `deduplicate_columns` in transform |
| Merge fails: "table does not exist" | First run on an empty destination | First run auto-creates the table with an initial overwrite |
| SCD2 `__valid_to` never set | `scd2_effective_column` is always the same value | Ensure the effective column changes between versions |
| Merge overwrites too many rows | `merge_keys` too broad (e.g. only date column) | Include all natural key columns in `merge_keys` |
| `merge_upsert` inserts duplicates | Source has duplicate rows for the same key | Add deduplication or verify `merge_keys` covers the full unique key |
