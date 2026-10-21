---
title: Data masking | DataCoolie
description: Configure data masking with metadata, examples and backend-specific guidance.
---

# Data masking

[All transform recipes](transform-patterns.md).

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
[typed-literal rules](transform-values.md#fill_null-supply-a-typed-default) used by `fill_null`
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
