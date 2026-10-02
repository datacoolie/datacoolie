---
title: Stable keys and protected output — DataCoolie User Guide
description: Combine DataCoolie value normalization, stable hashes, deduplication, masking and projection in execution order.
---

# Stable keys and protected output

Use this case when a source may repeat an entity and a destination needs a
stable generated key while sensitive fields are masked. The individual
features and every transformer class are explained in
[Transform patterns](transform-patterns.md); their exact metadata shapes are
under [Transform](../../reference/metadata-schema.md#transform),
[Hash column](../../reference/metadata-schema.md#hash-column), and
[Masking rule](../../reference/metadata-schema.md#masking-rule).

## Coordinate the stages

Assume the source returns `country_code`, `customer_id`, `updated_at`,
`email`, `name`, and `raw_payload`. The example is a dataflow fragment; source
and destination connection definitions live elsewhere in the document.

```json
{
  "source": {
    "connection_name": "customers_source", "table": "customers",
    "watermark_columns": ["updated_at"]
  },
  "transform": {
    "value_rules": [
      {"operation": "trim", "columns": ["country_code", "customer_id"]}
    ],
    "hash_columns": [
      {"target_column": "customer_key", "columns": ["country_code", "customer_id"], "algorithm": "sha256"}
    ],
    "deduplicate_columns": ["customer_key"],
    "latest_data_columns": ["updated_at"],
    "masking_rules": [
      {"method": "redact", "columns": ["email"], "value": "[REDACTED]"}
    ],
    "drop_columns": ["raw_payload"],
    "configure": {"missing_column_policy": "error"}
  },
  "destination": {
    "connection_name": "customers_delta", "table": "customers",
    "load_type": "merge_upsert", "merge_keys": ["customer_key"]
  }
}
```

For two rows with the same trimmed country and customer ID, the hash is the
same. Deduplication selects the newest `updated_at` in the incoming batch.
The chosen row's `email` is redacted; `raw_payload` is dropped. The generated
`customer_key` survives projection and identifies the destination merge.
Normalizing **after** hashing would produce a different key for values that
only differ by whitespace. A column made by `additional_columns` is also too
late to become a hash input; prepare such a value in the source query or
Python function when needed.

The transformer sequence is value normalization, schema conversion, hash,
deduplication, computed columns, later masking and projection, then final
sanitization. Partition columns are generated before masking and projection.
Do not mask, drop or rename merge and partition columns. The runtime rejects
protected-column changes. If a destination adds `partition_columns`, preserve
each one in the output and review the effect on matching identity in
[Destination and load patterns](destination-and-load-patterns.md#partition_columns-partition-the-output-table).

`missing_column_policy` applies across several transform classes. With
`ignore`, a missing hash input skips the **entire** hash definition, and a
missing mask target may be skipped. Keep `error` where a key or privacy rule
is mandatory. The hash here is an identifier; plain unkeyed SHA-256 is not a
safe anonymization method for low-entropy personal data. Batch deduplication
also does not decide what to do with an older row already stored by another
run; select an appropriate destination strategy and source contract.
