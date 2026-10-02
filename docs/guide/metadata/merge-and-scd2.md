---
title: SCD2 with incremental inputs — DataCoolie User Guide
description: Coordinate incremental source selection, batch deduplication, effective time and partition identity for SCD2.
---

# SCD2 with incremental inputs

Choose [Destination and load patterns](destination-and-load-patterns.md#scd2-slowly-changing-dimension-type-2)
for the `scd2` configuration and first-load behavior. This example combines
incremental selection, within-batch deduplication, and history writing. The
[Source](../../reference/metadata-schema.md#source),
[Transform](../../reference/metadata-schema.md#transform), and
[Destination](../../reference/metadata-schema.md#destination) sections give
the exact field shapes.

## Accept strictly newer versions

Assume an upstream query returns only rows that represent a *new version*
of each customer. For each key, every accepted `updated_at` must be strictly
newer than the `__valid_from` of the current stored version. This is a
data contract to enforce upstream; `scd2` metadata does not enforce it.
This dataflow fragment shows the interacting fields:

```json
{
  "name": "customers_history",
  "source": {
    "connection_name": "customers_db", "table": "customer_changes",
    "watermark_columns": ["updated_at"]
  },
  "transform": {
    "deduplicate_columns": ["customer_id"],
    "latest_data_columns": ["updated_at"]
  },
  "destination": {
    "connection_name": "customers_delta", "table": "customers_scd2",
    "load_type": "scd2", "merge_keys": ["customer_id"],
    "configure": {"scd2_effective_column": "updated_at"}
  }
}
```

When a batch contains two versions of one key, the deduplicator keeps the
latest `updated_at` in that batch. For a strictly newer row, the writer closes
the matched current version and appends the new current row with its validity
start. If a batch has ties, add a deterministic source ordering or tie-breaker
as explained in [Deduplicator](transform-patterns.md#deduplicator).

## Equal and older events

The close step acts only when the incoming effective time is **strictly
newer** than the current target time. The append step still inserts every
incoming row. An equal or older event can therefore add another current row;
deduplication within this run cannot compare it with history already stored.
Do not present a metadata flag as automatic retroactive restatement. Exclude
such events in an upstream query/function or handle historical repair as a
separately designed process. For source choices see
[Source patterns](source-patterns.md#inline-sql-or-sql-file-query-sources).

If the destination uses `partition_columns`, DataCoolie extends the effective
merge keys with those partition columns. A changed partition value may leave
the old current row unmatched, even when `customer_id` is unchanged. Keep
partition identity stable for this pattern or design a separate relocation
process. Review [partitioning](destination-and-load-patterns.md#partition_columns-partition-the-output-table)
before adding those fields.

## Related load choices

For key-based [`merge_upsert`](destination-and-load-patterns.md#merge_upsert-upsert-by-key-scd1-cdc)
and [`merge_overwrite`](destination-and-load-patterns.md#merge_overwrite-rolling-overwrite-by-key),
use their Configure sections. A complete-window replacement for corrections
and deletions has its own [worked case](watermark-window-replacement.md).
Replay execution and historical repair procedures are in
[Operations](../operations/replay-and-backfill.md).

## Replace a watermark window

This older section link now routes to
[Replace a complete watermark window](watermark-window-replacement.md).

## `scd2` — Type 2 with history

See [SCD2 configuration](destination-and-load-patterns.md#scd2-slowly-changing-dimension-type-2)
and the [strictly newer version](#accept-strictly-newer-versions) case above.
