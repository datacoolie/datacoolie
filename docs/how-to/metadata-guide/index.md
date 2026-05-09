---
title: Metadata Guide for New Users — DataCoolie How-to
description: Metadata authoring guide for new DataCoolie users — connections, sources, destinations, transforms, providers, load types, and validation.
---

# Metadata guide for new users

If you are a new Data Engineer or Data Analyst who just installed DataCoolie
and is not sure how to configure your first pipeline, start here.

DataCoolie is **driven entirely by metadata** — a JSON (or YAML, or Excel)
document that tells the framework what to read, how to transform it, where to
write it, and when to re-run incrementally. You do **not** write Python for
each pipeline; you fill in a structured document.

This guide walks through that document from zero, but it also covers the cases
that usually appear right after the first successful run: incremental loads,
query-based sources, API pagination, function sources, partitioning, merge
strategies, secrets, and metadata-provider differences.

## Sequence: read in this order

!!! tip "New user path"
    Work through the four pages in order on your first day. After that, the
    individual pages work as standalone references.

| Step | Page | You will learn |
|------|------|----------------|
| 1 | [Build your first metadata file](first-metadata-file.md) | Minimum valid document — two connections, one dataflow |
| 2 | [Source patterns](source-patterns.md) | How to configure any source type (file, database, API, Delta, Iceberg, function) |
| 3 | [Destination & load patterns](destination-and-load-patterns.md) | Which `load_type` to pick and what extra fields it needs |
| 4 | [Transform patterns](transform-patterns.md) | Cast types, deduplicate, add computed columns, partition output |
| 5 | [Validation checklist](validation-checklist.md) | Catch mistakes before the first run |

## Coverage map

| Area | Covered in this guide | Key cases |
|------|-----------------------|-----------|
| Metadata shape | Yes | `connections[]`, `dataflows[]`, optional orchestration fields, `configure`, `secrets_ref` |
| Metadata backends | Yes | JSON, YAML, Excel, database provider, API provider |
| Source types | Yes | File, Delta, Iceberg, database table/query, REST API, Python function |
| Destination types | Yes | File outputs, Delta, Iceberg, partitioned writes, lakehouse registration |
| Load strategies | Yes | `append`, `overwrite`, `full_load`, `merge_upsert`, `merge_overwrite`, `scd2` |
| Transform features | Yes | `schema_hints`, deduplication, computed columns, partition expressions, SCD2/system columns |
| Validation & safety | Yes | `dry_run`, smoke tests, secret resolution, common errors |

!!! info "Important edge cases"
  This guide covers the real behavior of the current framework, including:

  - `connection_type` can be derived automatically from `format`
  - Excel is a supported **source** format, not a writable destination
  - flat-file destinations support `append`, `overwrite`, and `full_load`, but not merge or SCD2
  - `connection_type: "streaming"` exists in the model but has no supported formats yet

## Where metadata lives

DataCoolie supports three metadata backends. Choose one:

| Backend | Good for | Operational note | How-to |
|---------|----------|------------------|--------|
| **JSON / YAML / Excel file** | Local dev, small teams, single-machine runs | JSON should stay canonical; YAML/Excel are alternative views or generated siblings | [Configure file metadata](../configure-file-metadata.md) |
| **Relational database** | Shared team configuration, multi-workspace governance | Rows are workspace-scoped via `workspace_id` | [Configure database metadata](../configure-database-metadata.md) |
| **REST API** | Enterprise ops, Git-backed or approval-gated config | Endpoints are workspace-scoped under `/workspaces/{workspace_id}/...` | [Configure API metadata](../configure-api-metadata.md) |

!!! note "Recommendation for beginners"
    Start with a **JSON file**. The file backend requires no database and no
    service — just create a `.json` file and point `FileProvider` at it.
    You can migrate to the database or API backend later without changing a
    single field in your metadata.

## What metadata tells the framework

```
metadata.json
├── connections[]            ← WHERE to read from and write to
│   ├── name / format / configure
│   ├── catalog / database / base_path
│   └── secrets_ref / is_active / workspace_id
└── dataflows[]              ← HOW to move data
  ├── name / stage / description
  ├── group_number / execution_order / processing_mode / is_active
  ├── source               ← which connection + table/query/function to read
  ├── destination          ← which connection + table + load_type to write
  └── transform            ← schema hints, dedup, computed columns (optional)
```

Start with `connections` and `dataflows`. The rest is optional and you can
add it incrementally.

If you are unsure where a field belongs, use this rule:

- `Connection.configure` = reusable endpoint defaults
- `source.configure` / `destination.configure` = per-dataflow overrides
- `transform.configure` = transformer behavior flags

## Quick example (30 seconds)

```json
{
  "connections": [
    {
      "name": "csv_input",
      "connection_type": "file",
      "format": "csv",
      "configure": { "base_path": "data/input" }
    },
    {
      "name": "bronze",
      "format": "delta",
      "configure": { "base_path": "data/output/bronze" }
    }
  ],
  "dataflows": [
    {
      "name": "orders_to_bronze",
      "stage": "ingest",
      "source":      { "connection_name": "csv_input", "table": "orders" },
      "destination": { "connection_name": "bronze", "schema_name": "sales", "table": "orders", "load_type": "append" }
    }
  ]
}
```

This reads `data/input/orders` (a folder of CSV files) and appends to a Delta
table at `data/output/bronze/sales/orders`. In this example DataCoolie derives
`connection_type: "lakehouse"` from `format: "delta"`.

→ **Next**: [Build your first metadata file](first-metadata-file.md)
