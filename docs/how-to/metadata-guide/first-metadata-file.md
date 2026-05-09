---
title: Build Your First Metadata File — DataCoolie How-to
description: Build a minimum-valid DataCoolie metadata JSON file from scratch — connections, source, destination, stage, and load type, field by field.
---

# Build your first metadata file

**Prerequisites** · DataCoolie installed (`pip install datacoolie`).  
**End state** · A valid `metadata.json` you can pass to `FileProvider` and run.

This page uses **JSON** because it is the easiest format to learn and the
recommended canonical source. The same model also works in YAML, Excel,
database-backed metadata, and API-backed metadata. Only the storage backend
changes; the connection, source, destination, and transform fields stay the
same.

## Step 1 — Create the file skeleton

Create a file called `metadata.json` in your project folder. The document
always has two top-level arrays:

```json
{
  "connections": [],
  "dataflows":   []
}
```

`connections` describes **where** data lives.  
`dataflows` describes **how** to move it.

You can stop there for a minimum-valid file. As your project grows, the same
top-level document can also carry optional fields such as `workspace_id`,
`description`, `group_number`, `execution_order`, `processing_mode`,
`is_active`, and per-block `configure` overrides.

## Step 2 — Add a source connection

A connection has four required fields in the beginner case:

```json
{
  "name":            "my_source",
  "connection_type": "file",
  "format":          "csv",
  "configure":       { "base_path": "data/input" }
}
```

| Field | Required | What to put there |
|-------|----------|-------------------|
| `name` | yes | A unique label you pick — used to reference this connection from a dataflow |
| `connection_type` | usually | One of `file`, `lakehouse`, `database`, `api`, `function`. If omitted, DataCoolie derives it from `format` |
| `format` | yes | The data format — must match the connection type (see table below) |
| `configure` | yes | A JSON object of backend-specific settings. `base_path` is the root folder for file connections |
| `workspace_id` | no | Used by database/API metadata providers and in structured logging |
| `catalog` / `database` | no | Use for registered lakehouse tables (Databricks, Fabric, Glue) instead of path-only addressing |
| `secrets_ref` | no | Tells DataCoolie which `configure` fields must be resolved from a secret source |
| `is_active` | no | Set `false` to disable a connection without deleting it |

### Valid connection\_type → format pairs

| `connection_type` | Allowed `format` values |
|-------------------|-------------------------|
| `file` | `csv`, `parquet`, `json`, `jsonl`, `avro`, `excel` |
| `lakehouse` | `delta`, `iceberg` |
| `database` | `sql` |
| `api` | `api` |
| `function` | `function` |

!!! warning "Mismatched type + format"
    DataCoolie validates `connection_type` against `format` at load time.
    For example, `connection_type: "file"` with `format: "delta"` raises an
    error. Use `connection_type: "lakehouse"` for Delta/Iceberg.

!!! info "`connection_type` can be derived"
  This is also valid:

  ```json
  {
    "name": "bronze",
    "format": "delta",
    "configure": { "base_path": "data/output/bronze" }
  }
  ```

  Because `format: "delta"` belongs to the lakehouse family, DataCoolie
  derives `connection_type: "lakehouse"` automatically.

!!! note "Streaming is reserved, not active"
  The model defines `connection_type: "streaming"`, but no formats are wired
  to it yet. Treat DataCoolie metadata today as **batch-first**: `batch`,
  `microbatch`, and `streaming` may appear as `processing_mode` values on a
  dataflow, but the connection-side streaming type is not yet usable.

## Step 3 — Add a destination connection

Add a second connection for where you want to write:

```json
{
  "name":            "bronze",
  "connection_type": "lakehouse",
  "format":          "delta",
  "configure":       { "base_path": "data/output/bronze" }
}
```

Your `connections` array now has two entries:

```json
"connections": [
  {
    "name": "my_source", "connection_type": "file", "format": "csv",
    "configure": { "base_path": "data/input" }
  },
  {
    "name": "bronze", "connection_type": "lakehouse", "format": "delta",
    "configure": { "base_path": "data/output/bronze" }
  }
]
```

## Step 4 — Add a dataflow

A dataflow is one read → transform → write unit. It refers to your connections
by name:

```json
{
  "name":  "orders_to_bronze",
  "stage": "ingest",
  "source":      { "connection_name": "my_source", "schema_name": "sales", "table": "orders" },
  "destination": { "connection_name": "bronze",    "schema_name": "sales", "table": "orders", "load_type": "append" }
}
```

### Key dataflow fields

| Field | Required | What to put there |
|-------|----------|-------------------|
| `name` | yes | Unique label for this dataflow |
| `stage` | yes | Free string — the filter you pass to `driver.run(stage="ingest")`. Group logically related dataflows under the same stage name |
| `source.connection_name` | yes | Must match a `name` in your `connections` array |
| `source.schema_name` | no | Subdirectory or schema; combined with `table` to build the path. Omit if not needed |
| `source.table` | yes | The folder name (file sources) or table name (database/lakehouse) |
| `destination.connection_name` | yes | Must match a `name` in your `connections` array |
| `destination.schema_name` | no | Output subdirectory or schema |
| `destination.table` | yes | Output folder or table name |
| `destination.load_type` | yes | How to write — `append`, `overwrite`, `merge_upsert`, `merge_overwrite`, or `scd2`. See [Destination & load patterns](destination-and-load-patterns.md) |

### Common optional dataflow fields

These are the fields teams typically add after the first successful run:

| Field | What it does |
|-------|---------------|
| `description` | Free-text description of the pipeline's business purpose |
| `workspace_id` | Important for database/API metadata backends and workspace-scoped logging |
| `group_number` | Groups dataflows for orchestration; lower groups run first |
| `execution_order` | Orders dataflows within a group |
| `processing_mode` | `batch` by default; `microbatch` and `streaming` are model values for future/specialized flows |
| `is_active` | Set `false` to keep the metadata but skip execution |
| `configure` | Arbitrary per-dataflow settings for custom readers/writers/extensions |

### Source block: more than `connection_name + table`

The minimum source block uses just a connection name and a table. The full
model supports several additional cases:

| Source field | Use it when |
|--------------|-------------|
| `schema_name` | The source is under a folder / schema namespace |
| `watermark_columns` | You want incremental reads |
| `query` | The source is a SQL query instead of a table |
| `python_function` | The source is a custom Python loader, for example `mypkg.loaders.load_orders` |
| `configure.read_options` | You need to override connection-level read options for only one dataflow |
| `configure.endpoint`, `configure.params`, `configure.pagination_*` | The source is an API and the per-dataflow endpoint differs |

Examples:

Database query source:

```json
"source": {
  "connection_name": "warehouse",
  "query": "SELECT * FROM sales.orders WHERE status = 'OPEN'",
  "watermark_columns": ["updated_at"]
}
```

Python function source:

```json
"source": {
  "connection_name": "python_src",
  "python_function": "mypkg.loaders.load_orders",
  "watermark_columns": ["updated_at"]
}
```

### Destination block: extra fields appear as complexity grows

| Destination field | Use it when |
|-------------------|-------------|
| `merge_keys` | `merge_upsert`, `merge_overwrite`, or `scd2` needs a business key |
| `partition_columns` | You want partitioned writes |
| `configure.write_options` | You need one dataflow to override connection-level write options |
| `configure.scd2_effective_column` | Required for `scd2` |

Example destination for a merge:

```json
"destination": {
  "connection_name": "silver",
  "schema_name": "sales",
  "table": "orders",
  "load_type": "merge_upsert",
  "merge_keys": ["order_id"],
  "configure": {
    "write_options": { "mergeSchema": true }
  }
}
```

### How `base_path + schema_name + table` become a path

For file and lakehouse connections:

```
{base_path}/{schema_name}/{table}

Example:  data/output/bronze / sales / orders
Result:   data/output/bronze/sales/orders
```

If you omit `schema_name`, the path is `{base_path}/{table}`.

For catalog-registered tables, DataCoolie uses a qualified name instead of a
path. Examples:

- Databricks Unity Catalog: `` `catalog`.`database`.`table` `` when you leave
  `schema_name` empty
- Fabric / Glue / other metastore-backed layouts:
  `` `catalog`.`database`.`schema_name`.`table` `` when all levels are used

## Step 5 — Complete file and run

Here is the complete minimal document:

```json
{
  "connections": [
    {
      "name": "my_source",
      "connection_type": "file",
      "format": "csv",
      "configure": { "base_path": "data/input" }
    },
    {
      "name": "bronze",
      "connection_type": "lakehouse",
      "format": "delta",
      "configure": { "base_path": "data/output/bronze" }
    }
  ],
  "dataflows": [
    {
      "name":  "orders_to_bronze",
      "stage": "ingest",
      "source":      { "connection_name": "my_source", "schema_name": "sales", "table": "orders" },
      "destination": { "connection_name": "bronze",    "schema_name": "sales", "table": "orders", "load_type": "append" }
    }
  ]
}
```

Run it with:

```python
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform

platform  = LocalPlatform()
engine    = PolarsEngine(platform=platform)
metadata  = FileProvider(config_path="metadata.json", platform=platform)

from datacoolie.orchestration.driver import DataCoolieDriver

with DataCoolieDriver(engine=engine, metadata_provider=metadata) as driver:
    result = driver.run(stage="ingest")

print(result)
# ExecutionResult(total=1, succeeded=1, failed=0, skipped=0)
```

## Adding more dataflows

You can have as many dataflows as you need. Add them to the `dataflows` array.
They can share connections and run in the same stage or different stages:

```json
"dataflows": [
  { "name": "orders_to_bronze",   "stage": "ingest", ... },
  { "name": "customers_to_bronze","stage": "ingest", ... },
  { "name": "orders_to_silver",   "stage": "transform", ... }
]
```

Run one stage at a time:

```python
driver.run(stage="ingest")
driver.run(stage="transform")
```

Or run multiple at once:

```python
driver.run(stage=["ingest", "transform"])
```

If stage order matters, use `group_number` and `execution_order` explicitly
instead of relying on array position alone.

```json
"dataflows": [
  {
    "name": "orders_to_bronze",
    "stage": "daily",
    "group_number": 1,
    "execution_order": 10,
    "source": { "connection_name": "raw", "table": "orders" },
    "destination": { "connection_name": "bronze", "table": "orders", "load_type": "append" }
  },
  {
    "name": "orders_to_silver",
    "stage": "daily",
    "group_number": 2,
    "execution_order": 10,
    "source": { "connection_name": "bronze", "table": "orders" },
    "destination": { "connection_name": "silver", "table": "orders", "load_type": "merge_upsert", "merge_keys": ["order_id"] }
  }
]
```

## Same metadata model in other backends

Once the JSON version works, the same logical metadata can live in other
providers:

| Backend | What changes | What stays the same |
|---------|--------------|---------------------|
| YAML file | Syntax only | Same fields and nesting |
| Excel file | Nested objects become JSON cells or flattened columns | Same connection/dataflow semantics |
| Database metadata | Rows are stored in SQL tables and filtered by `workspace_id` | Same model fields |
| API metadata | Objects are served over `/workspaces/{workspace_id}/...` endpoints | Same model fields |

The practical rule: learn the JSON structure first, then move it to the
provider your team needs.

## What to read next

| Goal | Next page |
|------|-----------|
| Configure a database or API source | [Source patterns](source-patterns.md) |
| Use merge or SCD2 instead of append | [Destination & load patterns](destination-and-load-patterns.md) |
| Cast types, deduplicate, or add columns | [Transform patterns](transform-patterns.md) |
| Check your file is correct before running | [Validation checklist](validation-checklist.md) |
