---
title: Hashes and deduplication | DataCoolie
description: Configure hashes and deduplication with metadata, examples and backend-specific guidance.
---

# Hashes and deduplication

[All transform recipes](transform-patterns.md).

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
