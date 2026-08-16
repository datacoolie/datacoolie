---
title: Transform Patterns — DataCoolie Metadata How-to
description: Configure casts, hashes, deduplication, computed columns, masking, projection, renaming, partitions, and SCD2 in DataCoolie metadata.
---

# Transform patterns

**Prerequisites** · Completed [Destination & load patterns](destination-and-load-patterns.md).  
**End state** · A correct `transform` block in each dataflow that needs data shaping.

The `transform` block is **optional**. When present it controls DataCoolie's
built-in transformer pipeline, which runs between read and write in this order:

| Order | Transformer | Triggered by |
|-------|-------------|--------------|
| 5 | `ColumnValueTransformer` | `transform.value_rules` |
| 10 | `SchemaConverter` | `transform.schema_hints` |
| 18 | `HashColumnAdder` | `transform.hash_columns` |
| 20 | `Deduplicator` | `transform.deduplicate_columns` |
| 30 | `ColumnAdder` | `transform.additional_columns` |
| 35 | `RowFilter` | `transform.filter_expression` |
| 60 | `SCD2ColumnAdder` | `destination.load_type = "scd2"` |
| 70 | `SystemColumnAdder` | Always (adds audit columns and driver-managed `__dataflow_run_id`) |
| 80 | `PartitionHandler` | `destination.partition_columns` |
| 84 | `DataMasker` | `transform.masking_rules` |
| 85 | `ColumnProjector` | `select_columns`, `drop_columns`, or `rename_columns` |
| 90 | `ColumnNameSanitizer` | Always (`lower` by default; configurable as `snake`) |

!!! info "System columns are always added"
    `__created_at`, `__updated_at`, `__updated_by`, and
    `__dataflow_run_id` are added to every driver-managed dataflow output.
    You do not configure them — just expect them in the destination table.

## Transform at a glance

```json
"transform": {
  "schema_hints": [
    { "column_name": "order_id", "data_type": "long" }
  ],
  "deduplicate_columns": ["order_id"],
  "latest_data_columns": ["updated_at"],
  "additional_columns": [
    { "column": "order_year", "expression": "EXTRACT(YEAR FROM order_date)" }
  ],
  "select_columns": ["order_id", "email", "order_year"],
  "rename_columns": {"email": "contact_email"},
  "value_rules": [
    {"operation": "trim", "columns": ["email"], "order": 10},
    {"operation": "case", "columns": ["email"], "mode": "lower", "order": 20}
  ],
  "masking_rules": [
    {"method": "partial", "columns": ["email"], "keep_start": 1, "keep_end": 3}
  ],
  "hash_columns": [
    {"target_column": "order_hash", "columns": ["order_id"], "algorithm": "sha256"}
  ],
  "configure": {
    "convert_timestamp_ntz": true,
    "deduplicate_by_rank": false
  }
}
```

| Field | Purpose |
|-------|---------|
| `schema_hints` | Cast weakly typed columns into the types you want |
| `deduplicate_columns` | Define the grouping key for deduplication |
| `latest_data_columns` | Define the ordering columns for deduplication |
| `additional_columns` | Add SQL-derived columns |
| `filter_expression` | Discard rows by a SQL WHERE-style predicate after computed columns are available |
| `value_rules` | Normalize source values with native engine expressions before schema casting |
| `masking_rules` | Irreversibly mask structured scalar columns late in the pipeline |
| `hash_columns` | Add stable SHA-256 business hashes from explicitly ordered source columns |
| `select_columns` / `drop_columns` | Keep or remove business columns; the two fields are mutually exclusive |
| `rename_columns` | Atomically rename columns with an old-name to new-name object |
| `configure` | Control transformer behavior such as `convert_timestamp_ntz` and `deduplicate_by_rank` |

`partition_columns` stays on the **destination**, not in `transform`.

### Normalize, mask, and project columns

```json
"transform": {
  "value_rules": [
    { "operation": "trim", "columns": ["email"], "order": 10 },
    { "operation": "case", "columns": ["email"], "mode": "lower", "order": 20 },
    { "operation": "map", "columns": ["status"], "mapping": {"A": "active", "I": "inactive"} }
  ],
  "masking_rules": [
    { "method": "partial", "columns": ["phone"], "keep_end": 4, "mask_char": "*" },
    { "method": "numeric_bucket", "columns": ["age"], "bucket_size": 10 },
    { "method": "date_truncate", "columns": ["birth_date"], "unit": "year" }
  ],
  "drop_columns": ["raw_payload"],
  "rename_columns": {"phone": "masked_phone"},
  "configure": {"missing_column_policy": "error"}
}
```

`hash_columns` is separate from masking and never becomes a merge or dedup key
implicitly:

```json
"hash_columns": [
  {
    "target_column": "customer_hash",
    "columns": ["country_code", "customer_id"],
    "algorithm": "sha256"
  }
]
```

Hash inputs currently support string, integer, boolean, and date columns. The
declared column order is significant. DataCoolie builds a shared canonical
payload with type tags, null markers, and UTF-8 byte lengths, so Spark and
Polars produce identical lowercase SHA-256 output and distinguish null from an
empty string. This immutable format is named `dc_hash_v1`; changing the format
requires a new serialization name and target column. Polars loads `polars-hash`
only when this feature runs; install it
with `pip install 'datacoolie[polars-hash]'`.

Plain SHA-256 is suitable for deterministic business identifiers, but not for
protecting low-entropy PII such as phone numbers or national identifiers. Use
the explicit masking methods above; keyed pseudonymization remains outside the
current contract.

Supported value operations are `trim`, `case`, `regex_replace`,
`empty_to_null`, `fill_null`, and `map`. Rules default to order `100`; ties
retain metadata declaration order. String operations require string columns.
`trim` removes ASCII U+0020 spaces only; tabs, newlines, and non-breaking
spaces remain.

`regex_replace` uses DataCoolie portable regex v1 rather than the complete Java
or Rust dialect. Use literals, explicit character classes/ranges, `.`, anchors,
grouping, alternation, and ordinary quantifiers. Lookaround, backreferences,
named groups, inline flags, `\d`/`\w`/`\s`/`\b`, possessive quantifiers, and
quantified nested groups are rejected while loading metadata. Patterns are
limited to 4,096 characters. Replacement text is always literal, so `$` and
backslash are emitted as written rather than expanding capture groups.

`fill_null` and masking `redact` literals are validated against the actual
column type before a native expression is added. Invalid values fail the same
way on Spark and Polars and do not depend on Spark ANSI settings.

Supported masking methods are `redact`, `nullify`, `partial`,
`numeric_bucket`, and `date_truncate`. Partial masking collapses the hidden
middle segment to one `mask_char`, while retaining the configured prefix and
suffix. Empty strings remain empty; a non-empty value whose length is less than
or equal to `keep_start + keep_end` becomes exactly one `mask_char` instead of
passing through raw. Masking merge keys, partition columns, and
framework-reserved columns is rejected. This is column-level PII masking, not
dataset anonymization.

Projection uses pre-rename names. It preserves framework trailing columns and
rejects removal or renaming of merge and partition keys. Missing configured
columns in typed rules and projection fail by default; set
`missing_column_policy` to `ignore` to skip them. Missing schema-hint columns
are skipped and reported together in one warning because catalog hints may be
a superset of the runtime schema. Deduplication remains strict: once
configured, a missing partition or order column always fails.
Keep the default `error` policy for PII-sensitive pipelines so schema drift
cannot silently bypass a configured masking rule.

---

## Pattern 1 — Cast column types (`schema_hints`)

**When to use:** your source data has weak types (CSV strings, JSON mixed types)
and you need specific types in the destination.

Add `schema_hints` inside `transform`:

```json
"transform": {
  "schema_hints": [
    { "column_name": "order_id",    "data_type": "int" },
    { "column_name": "customer_id", "data_type": "int" },
    { "column_name": "amount",      "data_type": "decimal", "precision": 18, "scale": 2 },
    { "column_name": "order_date",  "data_type": "date" },
    { "column_name": "created_at",  "data_type": "timestamp" },
    { "column_name": "is_active",   "data_type": "boolean" }
  ]
}
```

### Supported `data_type` values

| `data_type` | Notes |
|-------------|-------|
| `int` / `integer` | 32-bit integer |
| `long` / `bigint` | 64-bit integer |
| `float` | 32-bit float |
| `double` | 64-bit float |
| `decimal` | Requires `precision` and `scale` |
| `string` / `str` | UTF-8 string |
| `boolean` / `bool` | True/False |
| `date` | Calendar date (no time) |
| `timestamp` | Date + time with microsecond precision |
| `binary` | Raw bytes |

### `decimal` example

```json
{ "column_name": "price", "data_type": "decimal", "precision": 18, "scale": 4 }
```

`precision` = total significant digits; `scale` = digits after the decimal point.

### Less-common schema-hint fields

The schema-hint model supports more than just `column_name` and `data_type`:

| Field | Use it when |
|-------|-------------|
| `format` | The engine needs a format hint while casting or parsing |
| `default_value` | You want a documented default for custom logic or downstream tooling |
| `ordinal_position` | You want to preserve external column-order metadata |
| `is_active` | You want to keep a hint row in metadata but disable it temporarily |

!!! note "How schema hints are applied"
  - Matching is case-insensitive
  - Missing columns are skipped, not fatal
  - Hints only apply when `source.connection.use_schema_hint` is truthy
  - `timestamp_ntz` conversion happens after hint-based casting when enabled

!!! tip "Only list the columns you want to cast"
    You do not need a hint for every column. Columns not listed keep their
    inferred type from the source reader.

---

## Pattern 2 — Deduplicate rows

**When to use:** your source can deliver duplicate rows for the same key (common
with CDC feeds, API pagination overlaps, or file re-deliveries).

```json
"transform": {
  "deduplicate_columns": ["order_id"],
  "latest_data_columns": ["updated_at"]
}
```

| Field | Meaning |
|-------|---------|
| `deduplicate_columns` | The column(s) that define a unique record — usually your natural key |
| `latest_data_columns` | Which column to use to pick the "winner" when duplicates exist — usually a timestamp |

`Deduplicator` groups by `deduplicate_columns`, orders by `latest_data_columns`
descending, and keeps the first row per group.

### Fallback behavior you should know

If you omit some dedup fields, DataCoolie still has a few convenience fallbacks:

| Missing input | Fallback |
|---------------|----------|
| `deduplicate_columns` | Falls back to destination `merge_keys` |
| `latest_data_columns` | Falls back to `source.watermark_columns` |
| Both missing | Deduplication becomes a no-op |

**Full example with `merge_upsert`:**

```json
{
  "name":  "orders_cdc_to_bronze",
  "stage": "ingest",
  "source": {
    "connection_name":   "cdc_source",
    "table":             "orders_changes",
    "watermark_columns": ["updated_at"]
  },
  "destination": {
    "connection_name": "bronze",
    "schema_name":     "sales",
    "table":           "orders",
    "load_type":       "merge_upsert",
    "merge_keys":      ["order_id"]
  },
  "transform": {
    "deduplicate_columns": ["order_id"],
    "latest_data_columns": ["updated_at"],
    "schema_hints": [
      { "column_name": "order_id",   "data_type": "long" },
      { "column_name": "updated_at", "data_type": "timestamp" }
    ]
  }
}
```

!!! note "Relationship to `merge_keys`"
    `deduplicate_columns` is usually the same value as `merge_keys` but it
    lives in `transform`, not `destination`. They serve different pipeline
    stages: deduplication happens **before** the merge.

### Keep ties with rank instead of row-number

Normally DataCoolie keeps a single winner per key. If you want rank-style
deduplication instead, enable it in `transform.configure`:

```json
"transform": {
  "deduplicate_columns": ["order_id"],
  "latest_data_columns": ["updated_at"],
  "configure": {
    "deduplicate_by_rank": true
  }
}
```

`merge_overwrite` also uses rank-based dedup automatically when merge keys are
available and explicit `deduplicate_columns` are not set.

---

## Pattern 3 — Add computed columns

**When to use:** you need a new column whose value is calculated from existing
columns (derived date parts, string concatenation, status labels, etc.).

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

## Pattern 4 — Filter rows (`transform.filter_expression`)

**When to use:** you need to drop rows before writing to the destination.
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
    | `source.filter_expression` | Read time (before watermark) | Raw source columns only |
    | `transform.filter_expression` | Order 35 (post-ColumnAdder) | Source columns + computed columns |

    Use `source.filter_expression` when you want the filter pushed as close to
    the source as possible. Use `transform.filter_expression` when you need
    to filter on a column that is added by `additional_columns`.

---

## Pattern 5 — Partition the output

**When to use:** your destination table will be large and you want query
engines to skip irrelevant data via partition pruning.

Partition columns go on the **`destination`** block, not inside `transform`:

```json
"destination": {
  "connection_name":   "silver",
  "schema_name":       "sales",
  "table":             "orders",
  "load_type":         "overwrite",
  "partition_columns": [
    { "column": "order_date", "expression": "CAST(created_at AS DATE)" }
  ]
}
```

If the partition column already exists in the DataFrame (no derivation needed),
omit `expression`:

```json
"partition_columns": [
  { "column": "region" }
]
```

Multi-level partitioning:

```json
"partition_columns": [
  { "column": "order_year",  "expression": "EXTRACT(YEAR FROM order_date)" },
  { "column": "order_month", "expression": "EXTRACT(MONTH FROM order_date)" }
]
```

Partition expressions run late in the pipeline, after schema hints,
deduplication, computed columns, SCD2 columns, and system columns. That means
they can rely on columns created earlier in the pipeline.

---

## Pattern 6 — SCD2 audit columns

When `load_type` is `scd2`, the `SCD2ColumnAdder` transformer automatically
adds three columns. You do **not** configure them in `transform` — just set the
effective column in the destination `configure`:

```json
"destination": {
  "connection_name": "gold",
  "schema_name":     "dims",
  "table":           "customer",
  "load_type":       "scd2",
  "merge_keys":      ["customer_id"],
  "configure":       { "scd2_effective_column": "updated_at" }
}
```

Columns added automatically:

| Column | Type | Meaning |
|--------|------|---------|
| `__valid_from` | timestamp | Copied from `scd2_effective_column` |
| `__valid_to` | timestamp (nullable) | `NULL` = still current |
| `__is_current` | boolean | `true` for the active version |

---

## Pattern 7 — System columns (always present)

`SystemColumnAdder` runs on **every** dataflow, regardless of configuration.
Driver-managed outputs receive these four columns:

| Column | Content |
|--------|---------|
| `__created_at` | Framework timestamp when the row was first written |
| `__updated_at` | Framework timestamp of the current write |
| `__updated_by` | Configured audit author |
| `__dataflow_run_id` | ID of the ETL execution or replay chunk that produced the current row/version |

You do not configure these. If a merge destination already has `__created_at`
from a previous run, the engine preserves its original value on the matched row
and sets `__updated_at` to the current run timestamp.

Retries reuse one `__dataflow_run_id`. Replay chunks use their own chunk run
IDs rather than the outer replay aggregate ID. For SCD2, closing an old version
does not replace its original run ID; the new version receives the current ID.

Remember the ordering: these columns are always present in the written output,
but they are **not** available to `additional_columns` because they are added
later in the pipeline.

---

## Transform `configure` flags

Three flags in `transform.configure` change how built-in transformers behave:

| Key | Default | Effect |
|-----|---------|--------|
| `convert_timestamp_ntz` | `true` | Converts `timestamp_ntz` columns to `timestamp` after schema hints |
| `deduplicate_by_rank` | `false` | Uses rank-based deduplication instead of row-number semantics |
| `missing_column_policy` | `error` | Controls absent columns in typed value/hash/masking rules and projection; schema hints warn and skip, while dedup remains strict |

Example:

```json
"transform": {
  "schema_hints": [
    { "column_name": "created_at", "data_type": "timestamp" }
  ],
  "configure": {
    "convert_timestamp_ntz": false,
    "deduplicate_by_rank": true
  }
}
```

---

## Final step: column-name sanitization

After all configured transforms run, `ColumnNameSanitizer` applies the
driver's `column_name_mode`: `lower` by default, or `snake` when requested.
Plan for normalised destination columns when the source uses mixed case,
quoted identifiers, or API keys like `CustomerID`.

---

## Full example: multi-pattern `transform` block

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
      { "column": "order_date", "expression": "CAST(created_at AS DATE)" }
    ]
  },
  "transform": {
    "deduplicate_columns": ["order_id"],
    "latest_data_columns": ["updated_at"],
    "additional_columns": [
      { "column": "order_year",  "expression": "EXTRACT(YEAR FROM order_date)" }
    ],
    "schema_hints": [
      { "column_name": "order_id",   "data_type": "long" },
      { "column_name": "amount",     "data_type": "decimal", "precision": 18, "scale": 2 },
      { "column_name": "order_date", "data_type": "date" },
      { "column_name": "updated_at", "data_type": "timestamp" }
    ],
    "configure": {
      "convert_timestamp_ntz": true
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
| `__updated_at` not found in `additional_columns` | System columns are added later in the pipeline | Do not reference system columns in `additional_columns` |
| Destination columns are unexpectedly lowercase | `ColumnNameSanitizer` runs at the end and defaults to `lower` | Expect lowercase output or run with `column_name_mode="snake"` |

---

## Next

→ [Validation checklist](validation-checklist.md)
