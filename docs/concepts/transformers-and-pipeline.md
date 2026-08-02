---
title: Transformers and Pipeline — DataCoolie Concepts
description: Understand transformer ordering, built-in transformers, and how the DataCoolie pipeline mutates dataframes before writing to destinations.
---

# Transformers & pipeline

**TL;DR** `TransformerPipeline` runs a fixed ordered list of transformers
between read and write. Each transformer has an `order` integer (lower first);
DataCoolie's twelve built-ins claim the slots **5 / 10 / 18 / 20 / 30 / 35 / 60 / 70 / 80 / 84 / 85 / 90**,
leaving **40–50** free for user plugins.

## The twelve built-ins (order → responsibility)

| Order | Name | Responsibility |
|---|---|---|
| **5** | `ColumnValueTransformer` | Apply typed value rules in stable `(order, declaration)` order before schema casting. |
| **10** | `SchemaConverter` | Cast columns to `schema_hints` types after value normalization — downstream typed transforms see the target schema. |
| **18** | `HashColumnAdder` | Add stable SHA-256 business hashes from typed canonical payloads. |
| **20** | `Deduplicator` | Drop duplicates by `transform.deduplicate_columns` (partition keys) and `dataflow.order_columns` (latest-row selector, from `transform.latest_data_columns` or `source.watermark_columns`) before any compute work is wasted on them. |
| **30** | `ColumnAdder` | User-configured calculated columns from `transform.additional_columns`. |
| **35** | `RowFilter` | Discard rows by `transform.filter_expression` after computed columns exist but before SCD2 logic. |
| **60** | `SCD2ColumnAdder` | For `load_type="scd2"` only: copy `scd2_effective_column` into `__valid_from`, seed `__valid_to = NULL`, `__is_current = true`. No-op otherwise. |
| **70** | `SystemColumnAdder` | Framework audit columns: `__created_at`, `__updated_at`, `__updated_by`. |
| **80** | `PartitionHandler` | Derive partition values from SQL expressions. |
| **84** | `DataMasker` | Mask structured scalar PII after business transforms and before projection. |
| **85** | `ColumnProjector` | Resolve select/drop against source names, then apply atomic renames. |
| **90** | `ColumnNameSanitizer` | Last — sanitise casing and special chars for the destination engine. |

The default pipeline assembled by `DataCoolieDriver`:

```python
DEFAULT_TRANSFORMERS = [
    "column_value_transformer",  # 5
    "schema_converter",       # 10
    "hash_column_adder",      # 18
    "deduplicator",           # 20
    "column_adder",           # 30
    "row_filter",             # 35 — post-column_adder, pre-scd2
    "scd2_column_adder",      # 60
    "system_column_adder",    # 70
    "partition_handler",      # 80
    "data_masker",            # 84
    "column_projector",       # 85
    "column_name_sanitizer",  # 90
]
```

(List order is informational; `TransformerPipeline` sorts by the transformer's
`order` attribute, so `ColumnAdder` (30) still runs before `RowFilter` (35)
and `SCD2ColumnAdder` (60).)

## Why slots jump from 35 to 60?

Slots **40–50 are reserved for your plugins**. A common third-party addition
is a validator at 50. See
[ADR-0003](../adr/0003-transformer-ordering-slots.md).

## Tracking applied transformers

`BaseTransformer` exposes:

- `_mark_applied()` — record `ClassName` in `transformers_applied`
- `_mark_applied("detail")` — record `ClassName(detail)`
- `_mark_skipped()` — record nothing (no-op transformer)
- no call → default: record `ClassName`

The tracking label ends up in `TransformRuntimeInfo.transformers_applied` and
is surfaced by `ETLLogger` as a column on every `dataflow_run_log` row.
When `missing_column_policy="ignore"`, typed value and masking transformers
count only rules that reference at least one existing column. If every rule is
skipped, the transformer is not added to `transformers_applied`. Schema hints
that do not match runtime columns produce one summary warning and do not mark
`SchemaConverter` applied. Projection and final sanitization are also recorded
only when they add a real rename/select/drop/reorder operation.

Value and masking operations are compiled to native expressions. One rule that
targets multiple columns resolves schema once and adds one Spark `withColumns`
or Polars `with_columns` projection. Rules still run sequentially so metadata
order and multiple rules on the same column remain observable. No Python UDF,
Spark action, or Polars data collection is used by these transformers.

String normalization follows a portable contract: `trim` removes ASCII U+0020
only, `regex_replace` accepts DataCoolie's conservative cross-engine regex v1,
and replacement text is literal. Typed fill/redact values are validated before
the native plan is changed. Partial masking never passes a short non-empty PII
value through unchanged.

## Failure semantics

Transformers that raise `TransformError` are **not wrapped** — the driver sees
the original exception. Any other exception is wrapped by the pipeline into a
`TransformError` with `details={"applied": [...so_far]}` so the log records
which transformers ran before the failure. Both branches populate
`TransformRuntimeInfo.error_message`.

## Related

- [Concepts · Metadata model · Transform](metadata-model.md)
- [How-to · Merge & SCD2](../how-to/merge-and-scd2.md)
- [Writing a transformer](../extending/writing-a-transformer.md)
- [`reference/api/transformers`](../reference/api/transformers.md)
