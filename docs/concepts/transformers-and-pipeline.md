---
title: Transformers and Pipeline — DataCoolie Concepts
description: Understand transformer ordering, built-in transformers, and how the DataCoolie pipeline mutates dataframes before writing to destinations.
---

# Transformers & pipeline

**TL;DR** `TransformerPipeline` runs a fixed ordered list of transformers
between read and write. Each transformer has an `order` integer (lower first);
DataCoolie's seven built-ins claim the slots **10 / 20 / 30 / 60 / 70 / 80 / 90**,
leaving **40–50** free for user plugins.

## The seven built-ins (order → responsibility)

| Order | Name | Responsibility |
|---|---|---|
| **10** | `SchemaConverter` | Cast columns to `schema_hints` types first — everything downstream sees the target schema. |
| **20** | `Deduplicator` | Drop duplicates by `transform.deduplicate_columns` (partition keys) and `dataflow.order_columns` (latest-row selector, from `transform.latest_data_columns` or `source.watermark_columns`) before any compute work is wasted on them. |
| **30** | `ColumnAdder` | User-configured calculated columns from `transform.additional_columns`. |
| **60** | `SCD2ColumnAdder` | For `load_type="scd2"` only: copy `scd2_effective_column` into `__valid_from`, seed `__valid_to = NULL`, `__is_current = true`. No-op otherwise. |
| **70** | `SystemColumnAdder` | Framework audit columns: `__created_at`, `__updated_at`, `__updated_by`. |
| **80** | `PartitionHandler` | Derive partition values from SQL expressions. |
| **90** | `ColumnNameSanitizer` | Last — sanitise casing and special chars for the destination engine. |

The default pipeline assembled by `DataCoolieDriver`:

```python
DEFAULT_TRANSFORMERS = [
    "schema_converter",       # 10
    "deduplicator",           # 20
    "column_adder",           # 30
    "scd2_column_adder",      # 60
    "system_column_adder",    # 70
    "partition_handler",      # 80
    "column_name_sanitizer",  # 90
]
```

(List order is informational; `TransformerPipeline` sorts by the transformer's
`order` attribute, so `ColumnAdder` (30) still runs before `SCD2ColumnAdder`
(60).)

## Why slots jump from 30 to 60?

Slots **40–50 are reserved for your plugins**. A common third-party addition
is a PII masker at 40 or a validator at 50. See
[ADR-0003](../adr/0003-transformer-ordering-slots.md).

## Tracking applied transformers

`BaseTransformer` exposes:

- `_mark_applied()` — record `ClassName` in `transformers_applied`
- `_mark_applied("detail")` — record `ClassName(detail)`
- `_mark_skipped()` — record nothing (no-op transformer)
- no call → default: record `ClassName`

The tracking label ends up in `TransformRuntimeInfo.transformers_applied` and
is surfaced by `ETLLogger` as a column on every `dataflow_entry`.

## Failure semantics

Transformers that raise `TransformError` are **not wrapped** — the driver sees
the original exception. Any other exception is wrapped by the pipeline into a
`TransformError` with `details={"applied": [...so_far]}` so the log records
which transformers ran before the failure.

## Related

- [Concepts · Metadata model · Transform](metadata-model.md)
- [How-to · Merge & SCD2](../how-to/merge-and-scd2.md)
- [Writing a transformer](../extending/writing-a-transformer.md)
- [`reference/api/transformers`](../reference/api/transformers.md)
