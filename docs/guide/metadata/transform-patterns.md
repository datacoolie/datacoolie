---
title: Transform Patterns — DataCoolie User Guide
description: Configure casts, hashes, deduplication, computed columns, masking, projection, renaming, partitions, and SCD2 in DataCoolie metadata.
---

# Transform patterns

**Prerequisites** · A configured source from [Source patterns](source-patterns.md).
Choose the destination load strategy before finalizing transforms when the case
uses merge keys, SCD2 columns, partitions, or watermark replacement.  
**End state** · A correct `transform` block in each dataflow that needs data shaping.

Each section below follows one transformer class: what it does, which metadata
configures it, and an example. The table links to those sections in execution
order. For source-aware datatype rules, use [Datatypes and schema hints](data-types.md).

The [Transform](../../reference/metadata-schema.md#transform) block is **optional**.
Most business transformations are configured there. SCD2 and partition columns
use `destination` metadata; system columns are automatic, and column-name
sanitization uses a Driver run option. The built-in pipeline runs between read
and write in this order:

| Order | Transformer class | Metadata / configuration |
|-------|-------------------|--------------|
| 5 | [`ColumnValueTransformer`](#columnvaluetransformer) | `transform.value_rules` |
| 10 | [`SchemaConverter`](#schemaconverter) | `transform.schema_hints`; timestamp conversion in `transform.configure` |
| 18 | [`HashColumnAdder`](#hashcolumnadder) | `transform.hash_columns` |
| 20 | [`Deduplicator`](#deduplicator) | `transform.deduplicate_columns`, `latest_data_columns`; merge/watermark fallbacks |
| 30 | [`ColumnAdder`](#columnadder) | `transform.additional_columns`; automatic stale-system-column cleanup |
| 35 | [`RowFilter`](#rowfilter) | `transform.filter_expression` |
| 60 | [`SCD2ColumnAdder`](#scd2columnadder) | `destination.load_type = "scd2"` and `destination.configure.scd2_effective_column` |
| 70 | [`SystemColumnAdder`](#systemcolumnadder) | Automatic |
| 80 | [`PartitionHandler`](#partitionhandler) | `destination.partition_columns` |
| 84 | [`DataMasker`](#datamasker) | `transform.masking_rules` |
| 85 | [`ColumnProjector`](#columnprojector) | `transform.select_columns`, `drop_columns`, `rename_columns` |
| 90 | [`ColumnNameSanitizer`](#columnnamesanitizer) | Automatic; Driver `column_name_mode` |

!!! info "System columns are always added"
    `__created_at`, `__updated_at`, `__updated_by`, and
    `__dataflow_run_id` are added to every driver-managed dataflow output.
    You do not configure them — just expect them in the destination table.

## ColumnValueTransformer

**When to use:** incoming strings, nulls, or category codes need cleanup before
datatype conversion. Configure `transform.value_rules`. Each
[Value rule](../../reference/metadata-schema.md#value-rule)
specifies an `operation`, target `columns`, and any operation-specific options.
The rule's own `order` controls ordering **within** `value_rules`; the whole
value-rule transformer still runs at pipeline order 5.

The six operations below are individual items in `transform.value_rules`.
All except `fill_null` require string columns. Existing nulls stay null unless
you explicitly use `fill_null`.

### `trim`: remove surrounding spaces

```json
{"operation": "trim", "columns": ["email", "customer_name"]}
```

`"  Alice  "` becomes `"Alice"`; `"   "` becomes `""`. Only ASCII U+0020
spaces are removed. Tabs, newlines, and non-breaking spaces remain. One rule
can target several columns.

### `case`: lowercase or uppercase

Use `lower` for normalized emails or `upper` for codes:

```json
"transform": {
  "value_rules": [
    {"operation": "case", "columns": ["email"], "mode": "lower"},
    {"operation": "case", "columns": ["country_code"], "mode": "upper"}
  ]
}
```

`"Alice@Example.COM"` becomes `"alice@example.com"`; `"vn"` becomes `"VN"`.
These are the two supported modes; case conversion does not trim spaces.

### `regex_replace`: replace every matching substring

Remove formatting from a phone number:

```json
{"operation": "regex_replace", "columns": ["phone"], "pattern": "[^0-9]", "replacement": ""}
```

`"(+84) 123-456"` becomes `"84123456"`. Set a non-empty `replacement` to
replace matches with text; omitting it uses the empty string.

The pattern uses DataCoolie portable regex v1 rather than the complete Java
or Rust dialect. Use literals, explicit character classes/ranges, `.`, anchors,
grouping, alternation, and ordinary quantifiers. Lookaround, backreferences,
named groups, inline flags, `\d`/`\w`/`\s`/`\b`, possessive quantifiers, and
quantified nested groups are rejected while loading metadata. Patterns are
limited to 4,096 characters. Replacement text is always literal, so `$` and
backslash are emitted as written rather than expanding capture groups.

### `empty_to_null`: convert empty strings to null

```json
{"operation": "empty_to_null", "columns": ["middle_name", "country_code"]}
```

Only `""` becomes null. `"   "` remains unchanged unless an earlier `trim`
rule first removes its spaces. Non-empty values remain unchanged.

### `fill_null`: supply a typed default

```json
"transform": {
  "value_rules": [
    {"operation": "fill_null", "columns": ["country_code"], "value": "UNKNOWN"},
    {"operation": "fill_null", "columns": ["quantity"], "value": 0},
    {"operation": "fill_null", "columns": ["is_active"], "value": false}
  ]
}
```

Only nulls change. The example assumes `country_code` is already a string,
`quantity` an integer, and `is_active` a boolean. `value` must match the
**current** column type; schema hints run later and cannot make an incompatible
literal valid at this stage.

| Current column type | Example JSON `value` | Requirement |
|---|---|---|
| String | `"UNKNOWN"` | Use a JSON string, including `""` if intentional |
| Integer | `0` | Integer within the target range; `"0"` and booleans are rejected |
| Float | `0.5` | Finite JSON number |
| Decimal | `"0.00"` | Integer or decimal string that fits the column's precision/scale |
| Boolean | `false` | JSON boolean, not `"false"` or `0` |
| Date | `"2026-01-01"` | ISO date |
| Timezone-free timestamp | `"2026-01-01T00:00:00"` | ISO datetime without an offset |
| Timezone-aware timestamp | `"2026-01-01T00:00:00+00:00"` | ISO datetime with an offset |

Binary, nested, and untyped-null columns are not supported literal targets.
Invalid literals fail before the native expression is applied, independently
of Spark ANSI settings. Empty strings and NaN are not nulls for this operation.

### `map`: translate string codes

```json
{"operation": "map", "columns": ["status"], "mapping": {"A": "active", "I": "inactive"}, "on_unmapped": "keep"}
```

`"A"` becomes `"active"`; an unknown `"X"` stays `"X"` with `keep`, the
default. Keys and values must be strings, and matching is exact: `"a"` does
not match `"A"`. Use an earlier `case` rule if needed.

Use `null` when unknown codes should become null:

```json
{
  "operation": "map",
  "columns": ["status"],
  "mapping": {"A": "active", "I": "inactive"},
  "on_unmapped": "null"
}
```

Only `keep` and `null` are supported. This setting controls unmapped values,
not absent columns.

### Combine rules in a deliberate order

The following turns spaces-only input into `"UNKNOWN"`:

```json
"transform": {
  "value_rules": [
    {"operation": "trim", "columns": ["country_code"], "order": 10},
    {"operation": "empty_to_null", "columns": ["country_code"], "order": 20},
    {"operation": "fill_null", "columns": ["country_code"], "value": "UNKNOWN", "order": 30}
  ],
  "configure": {"missing_column_policy": "error"}
}
```

Rules default to order `100`; ties retain declaration order. An empty or omitted
`value_rules` list does nothing. Missing configured columns follow
[`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure);
see [Missing-column policy](#missing-column-policy) for the `error`/`ignore` cases.

---

## SchemaConverter

**When to use:** your source data has weak types (CSV strings, JSON mixed types)
and you need specific types in the destination.

### Cast selected columns in one dataflow

Add `schema_hints` inside `transform`. The [Schema hint](../../reference/metadata-schema.md#schema-hint)
entry defines each item:

```json
"transform": {
  "schema_hints": [
    { "column_name": "order_id",    "data_type": "int" },
    { "column_name": "customer_id", "data_type": "int" },
    { "column_name": "amount",      "data_type": "decimal(18,2)" },
    { "column_name": "order_date",  "data_type": "date" },
    { "column_name": "created_at",  "data_type": "timestamp" },
    { "column_name": "is_active",   "data_type": "boolean" }
  ]
}
```

For source-specific type names, decimals, timezone conversion, and the choice
between global and dataflow hints, use [Datatypes and schema hints](data-types.md).
Reusable root-level `schema_hints` groups follow the
[Shared schema hint](../../reference/metadata-schema.md#shared-schema-hint)
contract and are attached to matching dataflows by the metadata provider.
Less-common hint fields such as `format`, `default_value`, `ordinal_position`,
and `is_active` are listed in the [Schema hint reference](../../reference/metadata-schema.md#schema-hint).

!!! note "How schema hints are applied"
  - Matching is case-insensitive
  - Missing columns are skipped, not fatal
  - `use_schema_hint` in the source [connection configure](../../reference/metadata-schema.md#connectionsconfigure) must be enabled for hint-based casts
  - `timestamp_ntz` conversion happens after hint-based casting when enabled with `timestamp_timezone`; see [Timestamp semantics](data-types.md#timestamp-semantics)

!!! tip "Only list the columns you want to cast"
    You do not need a hint for every column. Columns not listed keep their
    inferred type from the source reader.

### Reuse global hints or override them locally

At the metadata document root, a shared group can describe the source table:

```json
"schema_hints": [
  {
    "connection_name": "erp_postgres",
    "schema_name": "sales",
    "table_name": "orders",
    "hints": [
      {"column_name": "order_id", "data_type": "bigint"},
      {"column_name": "amount", "data_type": "numeric(18,2)"}
    ]
  }
]
```

The provider attaches this group to a source using that connection, schema,
and table. A non-empty `transform.schema_hints` list replaces the whole shared
group for that dataflow. To keep some shared casts, repeat them in the local
list. See [Global hints for a source table](data-types.md#global-hints-for-a-source-table)
for matching rules, including query/function sources.

### Decimal precision and formatted dates

```json
"transform": {
  "schema_hints": [
    {"column_name": "amount", "data_type": "decimal", "precision": 18, "scale": 2},
    {"column_name": "order_date", "data_type": "date", "format": "dd/MM/yyyy"},
    {"column_name": "created_at", "data_type": "timestamp_ntz", "format": "dd/MM/yyyy HH:mm:ss"}
  ]
}
```

`decimal` with `precision: 18, scale: 2` is equivalent to `decimal(18,2)`.
If both forms supply parameters, they must agree. The format examples parse
`"25/09/2026"` and `"25/09/2026 11:10:00"`. Use formats supported by the
selected engine; these common Spark-style patterns are translated by Polars.
Arbitrary vendor format strings are not a portable contract.

### Interpret hints from a source datatype system

When a weak source reuses database type declarations, set the source
[connection configure](../../reference/metadata-schema.md#connectionsconfigure):

```json
"configure": {"schema_hint_type_system": "postgresql"}
```

For example, a PostgreSQL `int8` hint then means a signed 64-bit integer.
Database connections normally supply their dialect through `database_type`.
See [Which type system is read?](data-types.md#which-type-system-is-read)
for all supported systems and precedence.

### Disable one hint or all hint-based casts

Keep a hint in metadata while temporarily leaving its column unchanged:

```json
"transform": {
  "schema_hints": [
    {"column_name": "amount", "data_type": "decimal(18,2)", "is_active": false}
  ]
}
```

To disable both global and inline casts for a source connection, set its
`configure` as follows:

```json
"configure": {"use_schema_hint": false}
```

Missing hinted columns are warned about and skipped; duplicate hint names are
rejected case-insensitively. `default_value` does not fill missing/null data in
the built-in converter; use `ColumnValueTransformer.fill_null` for that.
`ordinal_position` orders shared hint records during provider resolution; it
does not reorder output columns. Use `ColumnProjector.select_columns` to do so.

### Convert timezone-free timestamps

To interpret NTZ values as instants, set `convert_timestamp_ntz` and
`timestamp_timezone` in [`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure):

```json
"transform": {
  "configure": {
    "convert_timestamp_ntz": true,
    "timestamp_timezone": "Asia/Ho_Chi_Minh"
  }
}
```

This conversion runs after hint-based casts and can also apply to NTZ columns
already present in the source schema. The timezone identifies the source
wall-clock time; see [Timestamp semantics](data-types.md#timestamp-semantics)
for supported timezone forms and a conversion example.

To retain timezone-free values, leave the default or set:

```json
"transform": {"configure": {"convert_timestamp_ntz": false}}
```

Hint-based casting and NTZ conversion are separate switches: disabling
`use_schema_hint` does not disable an explicitly enabled NTZ conversion.
Columns already carrying timezone-aware timestamps retain their instant.

---

## HashColumnAdder

**When to use:** you need a stable row or business-key identifier from existing
columns. Hashing runs after schema conversion but before deduplication and
computed columns. Its inputs can use normalized, cast source columns, but not
columns created later by `additional_columns`.

Each `transform.hash_columns` item follows the [Hash column](../../reference/metadata-schema.md#hash-column)
shape: declare a `target_column`, ordered input `columns`, and an `algorithm`.
Hashing does not infer inputs from `deduplicate_columns` or `merge_keys`.
Targets must be new columns. Missing inputs follow `missing_column_policy` in
[`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure).

### Business-key hashes: SHA-256 or XXHash64

```json
"transform": {
  "hash_columns": [
    {
      "target_column": "customer_hash",
      "columns": ["country_code", "customer_id"],
      "algorithm": "sha256"
    },
    {
      "target_column": "customer_key",
      "columns": ["country_code", "customer_id"],
      "algorithm": "xxhash64"
    }
  ]
}
```

### Choose the hash use case deliberately

| Use case | Input columns | Choice and caution |
|---|---|---|
| Compact surrogate-key-style value | Complete natural/business key | `xxhash64` is compact, but can collide; use an identity/mapping table for authoritative uniqueness. |
| Cross-system identifier | Explicitly ordered stable identifier columns | Prefer `sha256` when collision resistance matters. Keep column order and types consistent across producers. |
| Row hash / hashdiff | Non-key attributes whose changes matter | Prefer `sha256`; omit volatile audit or ingestion timestamps unless they define a change. |
| Low-entropy PII protection | Do not use an unkeyed hash | Plain SHA-256 is vulnerable to dictionary attack; use masking or an approved keyed pseudonymization service. |
| File/payload integrity | Not a row-hash use case | Use a digest of object bytes, not `hash_columns`. |

For a surrogate-key-style hash, explicitly repeat the intended business key:

```json
{
  "destination": {
    "load_type": "merge_upsert",
    "merge_keys": ["country_code", "customer_id"]
  },
  "transform": {
    "deduplicate_columns": ["country_code", "customer_id"],
    "hash_columns": [{
      "target_column": "customer_sk",
      "columns": ["country_code", "customer_id"],
      "algorithm": "xxhash64"
    }]
  }
}
```

This repetition prevents a later deduplication or merge change from silently
changing persisted hash values. A generated hash target can itself be used as
a merge key, but never as its own hash input. If deduplication columns and merge
keys differ, choose the hash input deliberately.

### Row hash / hashdiff for change detection

Hash the attributes whose changes matter separately from the identity:

```json
"transform": {
  "hash_columns": [
    {"target_column": "customer_key", "columns": ["country_code", "customer_id"], "algorithm": "xxhash64"},
    {"target_column": "customer_hashdiff", "columns": ["customer_name", "is_active", "signup_date"], "algorithm": "sha256"}
  ]
}
```

Here the hashdiff inputs are string, boolean, and Date columns. The generated
value does not automatically enable CDC or change the destination merge/SCD2
strategy; configure its use separately. Multiple definitions run in declaration
order, and all target names must be distinct and non-reserved.

### Use a generated hash for deduplication and merging

Hashing runs before deduplication, so the new column can be the key:

```json
{
  "destination": {"load_type": "merge_upsert", "merge_keys": ["customer_hash"]},
  "transform": {
    "hash_columns": [{"target_column": "customer_hash", "columns": ["country_code", "customer_id"]}],
    "deduplicate_columns": ["customer_hash"],
    "latest_data_columns": ["updated_at"]
  }
}
```

Omitting `algorithm` selects `sha256`. Keep this generated key in any later
projection and do not mask or rename it while it is a merge key.

### Input types, serialization, and migration

Hash inputs currently support string, integer, boolean, and date columns. Input
order matters. The canonical payload uses type tags, null markers, and UTF-8
byte lengths, so Spark and Polars distinguish null from an empty string and
produce identical output. `sha256` returns a lowercase 64-character String.
`xxhash64` uses fixed seed `42` and returns a signed BIGINT; negative values are
normal. Only `dc_hash_v1` serialization is currently supported; seed and salt
are not configurable metadata fields. Null input is encoded, so the hash
itself is still a value; null and empty string produce different payloads.
Polars loads `polars-hash` only when this
feature runs; install `datacoolie[polars-hash]` if needed.

XXHash64 is not collision-free. Do not apply `abs()` or discard its sign bit,
which reduces the key space. Add a collision quality check when using it as a
key. Changing an existing target from SHA-256 to XXHash64 also changes its type
from String to BIGINT; use a new target or plan a destination schema migration.
Plain SHA-256 is not protection for low-entropy PII.
Decimal, float, timestamp, binary, and nested inputs are not supported directly.
If those values must participate, define their supported representation
deliberately in the source query/function or a suitable earlier schema cast.
`additional_columns` runs too late to prepare a hash input. An empty or omitted
`hash_columns` list adds nothing.

---

## Deduplicator

**When to use:** your source can deliver duplicate rows for the same key (common
with CDC feeds, API pagination overlaps, or file re-deliveries).

Set `deduplicate_columns` and `latest_data_columns` in the
[Transform](../../reference/metadata-schema.md#transform) block:

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

### Composite keys and deterministic tie-breaking

```json
"transform": {
  "deduplicate_columns": ["country_code", "customer_id"],
  "latest_data_columns": ["updated_at", "event_sequence"]
}
```

The ordering is descending across the tuple: newest `updated_at`, then highest
`event_sequence`. Both ordering columns must already exist. If all ordering
values tie, the default single winner is not a deterministic business choice;
add a tie-breaker or explicitly keep ties with rank.

### Fallback behavior you should know

If you omit some dedup fields, DataCoolie still has a few convenience fallbacks:

| Missing input | Fallback |
|---------------|----------|
| `deduplicate_columns` | Falls back to destination `merge_keys` |
| `latest_data_columns` | Falls back to `source.watermark_columns` |
| Either effective column list is empty after fallbacks | Deduplication becomes a no-op |

The fallback fields are described under [Destination](../../reference/metadata-schema.md#destination)
for merge keys and [Source](../../reference/metadata-schema.md#source) for watermarks.

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
deduplication instead, enable `deduplicate_by_rank` in
[`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure):

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

For example, this fragment takes its grouping and ordering from destination
and source metadata and keeps tied latest rows:

```json
{
  "source": {"watermark_columns": ["updated_at"]},
  "destination": {"load_type": "merge_overwrite", "merge_keys": ["order_id"]},
  "transform": {}
}
```

For a key with ordering values `10, 10, 9`, rank keeps both rows at `10`;
row-number keeps one. To use row-number for this `merge_overwrite` case, supply
explicit `deduplicate_columns` and leave `deduplicate_by_rank` false. Setting
`deduplicate_by_rank: true` explicitly keeps ties for other load types too.
With multiple ordering columns, ties are compared across the full tuple.

An empty `transform` does not disable deduplication when both fallback lists
exist. If either effective list is empty it skips; if a configured column is
absent, it fails even when `missing_column_policy` is `ignore`.

---

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

    Use `source.filter_expression` when you want the filter pushed as close to
    the source as possible. Use `transform.filter_expression` when you need
    to filter on a column that is added by `additional_columns`.

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

---

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

## DataMasker

**When to use:** structured scalar values need irreversible masking before
they are written. Configure `transform.masking_rules`; each
[Masking rule](../../reference/metadata-schema.md#masking-rule) specifies a
`method`, target `columns`, and method-specific options. This class runs at
order 84, after SCD2, system, and partition columns are available, and before
projection and renaming.

The five methods below are items in `transform.masking_rules`. A column may
appear in only one masking rule. Use separate columns for the alternatives
below, or choose one method for a given column.

### `redact`: replace non-null values with a constant

```json
"transform": {
  "masking_rules": [
    {"method": "redact", "columns": ["email"], "value": "[REDACTED]"},
    {"method": "redact", "columns": ["salary"], "value": "0.00"}
  ]
}
```

Here `email` is a string and `salary` is a decimal column. All non-null values,
including empty strings, become the constant; existing nulls stay null.
The literal must match the column's type at this stage. The same
[typed-literal rules](#fill_null-supply-a-typed-default) used by `fill_null`
apply, but masking sees the types **after** schema conversion.

### `nullify`: remove the value

```json
{"method": "nullify", "columns": ["national_id", "private_notes"]}
```

Every value becomes null, retaining the column and its datatype. Use
`ColumnProjector.drop_columns` if the column itself should disappear.

### `partial`: keep a prefix or suffix

```json
"transform": {
  "masking_rules": [
    {"method": "partial", "columns": ["phone"], "keep_end": 4},
    {"method": "partial", "columns": ["account_code"], "keep_start": 2, "keep_end": 2, "mask_char": "#"}
  ]
}
```

`"12345678"` becomes `"*5678"`; `"AB123456YZ"` becomes `"AB#YZ"`.
The hidden segment becomes exactly one mask character, not a character per
hidden position. Both keep counts default to zero and must be non-negative;
`mask_char` defaults to `"*"` and must be one character. Set both counts to
zero to mask a non-empty string completely.

Only string columns are supported. Null and empty string stay unchanged.
A non-empty value no longer than `keep_start + keep_end` becomes exactly one
mask character, preventing short values from passing through unchanged.

### `numeric_bucket`: reduce numeric precision

```json
{"method": "numeric_bucket", "columns": ["age"], "bucket_size": 10}
```

The result is `floor(value / bucket_size) * bucket_size`, cast back to the
original numeric type: `37` becomes `30`, and `-3` becomes `-10`. The bucket
size must be positive. Use a size appropriate to the column's datatype;
null stays null, and strings must be converted to numeric before this stage.

### `date_truncate`: reduce date or timestamp precision

```json
"transform": {
  "masking_rules": [
    {"method": "date_truncate", "columns": ["birth_date"], "unit": "year"},
    {"method": "date_truncate", "columns": ["invoice_date"], "unit": "month"},
    {"method": "date_truncate", "columns": ["event_time"], "unit": "day"},
    {"method": "date_truncate", "columns": ["received_at"], "unit": "hour"}
  ]
}
```

| Unit | Example input | Result |
|---|---|---|
| `year` | Date `2026-09-25` | Date `2026-01-01` |
| `month` | Date `2026-09-25` | Date `2026-09-01` |
| `day` | Timestamp `2026-09-25 11:37:42` | Timestamp `2026-09-25 00:00:00` |
| `hour` | Timestamp `2026-09-25 11:37:42` | Timestamp `2026-09-25 11:00:00` |

These are the supported units. The datatype is preserved; `hour` requires a
timestamp, and `day` on an existing Date leaves its date unchanged. String dates
need schema conversion first. Null stays null.

### Protected columns and missing inputs

Masking merge keys, partition columns, and framework-reserved columns is
rejected. This is column-level PII masking, not dataset anonymization. Keep the
default `missing_column_policy = "error"` in
[`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure)
for PII-sensitive pipelines so schema drift cannot silently bypass a masking
rule. An unkeyed `hash_columns` value is
not a substitute for masking low-entropy PII.
An empty or omitted `masking_rules` list performs no masking.

---

## ColumnProjector

**When to use:** the destination needs only part of the shaped data or different
business-column names. The projector runs at order 85, **after masking**.
Configure `select_columns`, `drop_columns`, and `rename_columns` in
[Transform](../../reference/metadata-schema.md#transform).
`select_columns` and `drop_columns` are mutually exclusive. Selection/removal
uses pre-rename names; `rename_columns` then renames atomically.

### Keep and reorder business columns

List the business columns in the desired output order:

```json
"transform": {
  "select_columns": ["customer_id", "country_code", "phone"]
}
```

Include every merge and partition key. Framework trailing columns are retained
automatically and placed after the selected business columns.

### Drop unwanted columns

```json
"transform": {"drop_columns": ["raw_payload", "debug_notes"]}
```

All other columns remain. Do not configure `select_columns` and `drop_columns`
together.

### Rename business columns

```json
"transform": {"rename_columns": {"phone": "contact_phone", "name": "customer_name"}}
```

Rename targets must be distinct and must not overwrite other existing columns.
Chains (`a` to `b`, then `b` to `c`), swaps, and no-op renames are rejected.
Final names still pass through `ColumnNameSanitizer` afterwards.

### Combine selection/removal with renaming

You may combine either `select_columns` or `drop_columns` with renaming in the
same block. Resolve the list using names **before** renaming. If `DataMasker`
already masked `phone`, you can make that explicit in the output name:

```json
"transform": {
  "drop_columns": ["raw_payload"],
  "rename_columns": {"phone": "masked_phone"}
}
```

Projection preserves framework trailing columns and rejects removal or
renaming of merge, partition, and framework-reserved columns. Missing
configured columns fail by default; set
[`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure)
`missing_column_policy` to `ignore` only when skipping missing inputs is safe.
Omitting all three projection fields leaves the business columns unchanged.

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

---

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
