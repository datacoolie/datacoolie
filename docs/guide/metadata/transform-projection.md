---
title: Column selection and renaming | DataCoolie
description: Configure column selection and renaming with metadata, examples and backend-specific guidance.
---

# Column selection and renaming

[All transform recipes](transform-patterns.md).

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
