---
title: Transform Patterns — DataCoolie User Guide
description: Configure casts, hashes, deduplication, computed columns, masking, projection, renaming, partitions, and SCD2 in DataCoolie metadata.
---

# Transform patterns

**Prerequisites** · A configured source from [Source patterns](source-patterns.md).
Choose the destination load strategy before finalizing transforms when the case
uses merge keys, SCD2 columns, partitions, or watermark replacement.  
**End state** · A correct `transform` block in each dataflow that needs data shaping.

The recipe links below follow execution order. Each topic explains the
metadata and gives copyable examples. For source-aware datatype rules, use [Datatypes and schema hints](data-types.md).

The [Transform](../../reference/metadata-schema.md#transform) block is **optional**.
Most business transformations are configured there. SCD2 and partition columns
use `destination` metadata; system columns are automatic, and column-name
sanitization uses a Driver run option. The built-in pipeline runs between read
and write.

See the canonical [built-in order and responsibilities](../../reference/concepts/transformers-and-pipeline.md#the-twelve-built-ins-order-responsibility).

Every selected built-in must be registered and load successfully, including
`DataMasker` when masking configuration is absent. A missing or failed default
raises before destination mutation or checkpoint save; unused external plugins
do not invalidate the pipeline. Transformers retain their normal no-op behavior
when their configuration is absent.

!!! info "System columns are always added"
    `__created_at`, `__updated_at`, `__updated_by`, and
    `__dataflow_run_id` are added to every driver-managed dataflow output.
    You do not configure them — just expect them in the destination table.

## ColumnValueTransformer

[ColumnValueTransformer](transform-values.md#columnvaluetransformer).

<span id="trim-remove-surrounding-spaces"></span>
[`trim`: remove surrounding spaces](transform-values.md#trim-remove-surrounding-spaces).

<span id="case-lowercase-or-uppercase"></span>
[`case`: lowercase or uppercase](transform-values.md#case-lowercase-or-uppercase).

<span id="regex_replace-replace-every-matching-substring"></span>
[`regex_replace`: replace every matching substring](transform-values.md#regex_replace-replace-every-matching-substring).

<span id="empty_to_null-convert-empty-strings-to-null"></span>
[`empty_to_null`: convert empty strings to null](transform-values.md#empty_to_null-convert-empty-strings-to-null).

<span id="fill_null-supply-a-typed-default"></span>
[`fill_null`: supply a typed default](transform-values.md#fill_null-supply-a-typed-default).

<span id="map-translate-string-codes"></span>
[`map`: translate string codes](transform-values.md#map-translate-string-codes).

<span id="combine-rules-in-a-deliberate-order"></span>
[Combine rules in a deliberate order](transform-values.md#combine-rules-in-a-deliberate-order).

## SchemaConverter

[SchemaConverter](transform-schema.md#schemaconverter).

<span id="cast-selected-columns-in-one-dataflow"></span>
[Cast selected columns in one dataflow](transform-schema.md#cast-selected-columns-in-one-dataflow).

<span id="reuse-global-hints-or-override-them-locally"></span>
[Reuse global hints or override them locally](transform-schema.md#reuse-global-hints-or-override-them-locally).

<span id="decimal-precision-and-formatted-dates"></span>
[Decimal precision and formatted dates](transform-schema.md#decimal-precision-and-formatted-dates).

<span id="interpret-hints-from-a-source-datatype-system"></span>
[Interpret hints from a source datatype system](transform-schema.md#interpret-hints-from-a-source-datatype-system).

<span id="disable-one-hint-or-all-hint-based-casts"></span>
[Disable one hint or all hint-based casts](transform-schema.md#disable-one-hint-or-all-hint-based-casts).

<span id="convert-timezone-free-timestamps"></span>
[Convert timezone-free timestamps](transform-schema.md#convert-timezone-free-timestamps).

## HashColumnAdder

[HashColumnAdder](transform-keys.md#hashcolumnadder).

<span id="business-key-hashes-sha-256-or-xxhash64"></span>
[Business-key hashes: SHA-256 or XXHash64](transform-keys.md#business-key-hashes-sha-256-or-xxhash64).

<span id="choose-the-hash-use-case-deliberately"></span>
[Choose the hash use case deliberately](transform-keys.md#choose-the-hash-use-case-deliberately).

<span id="row-hash-hashdiff-for-change-detection"></span>
[Row hash / hashdiff for change detection](transform-keys.md#row-hash-hashdiff-for-change-detection).

<span id="use-a-generated-hash-for-deduplication-and-merging"></span>
[Use a generated hash for deduplication and merging](transform-keys.md#use-a-generated-hash-for-deduplication-and-merging).

<span id="input-types-serialization-and-migration"></span>
[Input types, serialization, and migration](transform-keys.md#input-types-serialization-and-migration).

## Deduplicator

[Deduplicator](transform-keys.md#deduplicator).

<span id="composite-keys-and-deterministic-tie-breaking"></span>
[Composite keys and deterministic tie-breaking](transform-keys.md#composite-keys-and-deterministic-tie-breaking).

<span id="fallback-behavior-you-should-know"></span>
[Fallback behavior you should know](transform-keys.md#fallback-behavior-you-should-know).

<span id="keep-ties-with-rank-instead-of-row-number"></span>
[Keep ties with rank instead of row-number](transform-keys.md#keep-ties-with-rank-instead-of-row-number).

## ColumnAdder

[ColumnAdder](transform-expressions.md#columnadder).

<span id="constants-replacing-a-column-and-dependent-expressions"></span>
[Constants, replacing a column, and dependent expressions](transform-expressions.md#constants-replacing-a-column-and-dependent-expressions).

## RowFilter

[RowFilter](transform-expressions.md#rowfilter).

<span id="reference-a-computed-column"></span>
[Reference a computed column](transform-expressions.md#reference-a-computed-column).

<span id="combine-multiple-conditions"></span>
[Combine multiple conditions](transform-expressions.md#combine-multiple-conditions).

<span id="null-conditions-and-skipping-the-filter"></span>
[Null conditions and skipping the filter](transform-expressions.md#null-conditions-and-skipping-the-filter).

## SCD2ColumnAdder

[SCD2ColumnAdder](transform-layout.md#scd2columnadder).

## SystemColumnAdder

[SystemColumnAdder](transform-layout.md#systemcolumnadder).

## PartitionHandler

[PartitionHandler](transform-layout.md#partitionhandler).

<span id="existing-derived-and-system-column-partitions"></span>
[Existing, derived, and system-column partitions](transform-layout.md#existing-derived-and-system-column-partitions).

## DataMasker

[DataMasker](transform-masking.md#datamasker).

<span id="redact-replace-non-null-values-with-a-constant"></span>
[`redact`: replace non-null values with a constant](transform-masking.md#redact-replace-non-null-values-with-a-constant).

<span id="nullify-remove-the-value"></span>
[`nullify`: remove the value](transform-masking.md#nullify-remove-the-value).

<span id="partial-keep-a-prefix-or-suffix"></span>
[`partial`: keep a prefix or suffix](transform-masking.md#partial-keep-a-prefix-or-suffix).

<span id="numeric_bucket-reduce-numeric-precision"></span>
[`numeric_bucket`: reduce numeric precision](transform-masking.md#numeric_bucket-reduce-numeric-precision).

<span id="date_truncate-reduce-date-or-timestamp-precision"></span>
[`date_truncate`: reduce date or timestamp precision](transform-masking.md#date_truncate-reduce-date-or-timestamp-precision).

<span id="protected-columns-and-missing-inputs"></span>
[Protected columns and missing inputs](transform-masking.md#protected-columns-and-missing-inputs).

## ColumnProjector

[ColumnProjector](transform-projection.md#columnprojector).

<span id="keep-and-reorder-business-columns"></span>
[Keep and reorder business columns](transform-projection.md#keep-and-reorder-business-columns).

<span id="drop-unwanted-columns"></span>
[Drop unwanted columns](transform-projection.md#drop-unwanted-columns).

<span id="rename-business-columns"></span>
[Rename business columns](transform-projection.md#rename-business-columns).

<span id="combine-selectionremoval-with-renaming"></span>
[Combine selection/removal with renaming](transform-projection.md#combine-selectionremoval-with-renaming).

## ColumnNameSanitizer

[ColumnNameSanitizer](transform-layout.md#columnnamesanitizer).

## Transform `configure` flags

The reference collects these flags under [`dataflows[].transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure).

These four settings in `transform.configure` change built-in behavior:

| Key | Default | Effect |
|-----|---------|--------|
| `convert_timestamp_ntz` | `false` | Converts `timestamp_ntz` columns to `timestamp` after schema hints when `timestamp_timezone` is provided |
| `timestamp_timezone` | unset | Timezone used for an explicit NTZ-to-instant conversion |
| `deduplicate_by_rank` | `false` | Uses rank-based deduplication instead of row-number semantics |
| `missing_column_policy` | `error` | Controls absent columns in typed value/hash/masking rules and projection; schema hints warn and skip, while dedup remains strict |

The class sections above show timestamp and rank cases. Missing-column policy
applies across several classes:

### Missing-column policy

Use `error` (the default) when every configured input must exist:

```json
"transform": {
  "value_rules": [{"operation": "trim", "columns": ["name", "legacy_name"]}],
  "configure": {"missing_column_policy": "error"}
}
```

If `legacy_name` is absent, this fails. For an optional legacy field, use:

```json
"transform": {
  "value_rules": [{"operation": "trim", "columns": ["name", "legacy_name"]}],
  "configure": {"missing_column_policy": "ignore"}
}
```

This still trims `name` and skips the absent `legacy_name`.

| Class | What `ignore` does |
|---|---|
| `ColumnValueTransformer`, `DataMasker` | Apply a rule to its existing target columns |
| `HashColumnAdder` | Skip the whole hash definition if any input is missing; never hash a partial key |
| `ColumnProjector` | Skip absent select/drop/rename source references |
| `SchemaConverter` | Independent policy: missing hinted columns warn and skip |
| `Deduplicator` | Independent policy: missing effective key/order columns fail |
| `ColumnAdder`, `RowFilter`, `PartitionHandler` | This flag does not make SQL expressions tolerate missing inputs |

`ignore` does not bypass datatype validation, protected-column checks, invalid
expressions, or name collisions. Keep `error` when skipping a mask would allow
sensitive data through. With no configured operation, each optional class
skips its work; framework system-column handling and sanitization still run.

---

## Full example: multi-pattern `transform` block

This example normalizes an email before masking it, casts the order ID before
hashing it, then renames the masked email before writing. It assumes the source
includes `order_id`, `email`, `amount`, `order_date`, and `updated_at`.

```json
{
  "name":  "orders_to_silver",
  "stage": "bronze2silver",
  "source": {
    "connection_name":   "bronze",
    "schema_name":       "sales",
    "table":             "orders",
    "watermark_columns": ["updated_at"]
  },
  "destination": {
    "connection_name":   "silver",
    "schema_name":       "sales",
    "table":             "orders",
    "load_type":         "merge_upsert",
    "merge_keys":        ["order_id"],
    "partition_columns": [
      { "column": "order_date" }
    ]
  },
  "transform": {
    "value_rules": [
      { "operation": "trim", "columns": ["email"] }
    ],
    "schema_hints": [
      { "column_name": "order_id",   "data_type": "long" },
      { "column_name": "amount",     "data_type": "decimal", "precision": 18, "scale": 2 },
      { "column_name": "order_date", "data_type": "date" },
      { "column_name": "updated_at", "data_type": "timestamp" }
    ],
    "hash_columns": [
      { "target_column": "order_hash", "columns": ["order_id"], "algorithm": "sha256" }
    ],
    "deduplicate_columns": ["order_id"],
    "latest_data_columns": ["updated_at"],
    "additional_columns": [
      { "column": "order_year", "expression": "EXTRACT(YEAR FROM order_date)" }
    ],
    "masking_rules": [
      { "method": "partial", "columns": ["email"], "keep_start": 1, "keep_end": 3 }
    ],
    "rename_columns": {"email": "masked_email"},
    "configure": {
      "convert_timestamp_ntz": false
    }
  }
}
```

---

## Common mistakes

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Duplicate rows after merge | No dedup key or no ordering columns | Add `deduplicate_columns` and `latest_data_columns`, or let ordering fall back to `source.watermark_columns` |
| Wrong types in destination table | No `schema_hints`, or `use_schema_hint` is disabled on the source connection | Add hints and confirm `source.connection.use_schema_hint` is `true` |
| `year()` function fails on Polars | Polars SQL doesn't support `year()` | Use `EXTRACT(YEAR FROM col)` |
| `date()` function fails on Polars | Polars SQL doesn't support `date(col)` | Use `CAST(col AS DATE)` |
| Partition column missing | `expression` references a column that doesn't exist yet | Cast or add the column via `schema_hints` or `additional_columns` first |
| Hash input column missing | It is only created by `additional_columns`, which runs after hashing | Hash existing source columns or calculate the value in a later/custom step |
| Masked field still has its original name | Masking changes values, not names | Use `rename_columns` after `masking_rules` |
| `__updated_at` not found in `additional_columns` | System columns are added later in the pipeline | Do not reference system columns in `additional_columns` |
| Destination columns are unexpectedly lowercase | `ColumnNameSanitizer` runs at the end and defaults to `lower` | Expect lowercase output or run with `column_name_mode="snake"` |

---

## Next

→ [Destination & load patterns](destination-and-load-patterns.md) · [Validation checklist](validation-checklist.md)
