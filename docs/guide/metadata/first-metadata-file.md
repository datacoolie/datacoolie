---
title: Build Your First Metadata File — DataCoolie User Guide
description: Build a minimum-valid DataCoolie metadata JSON file from scratch — connections, source, destination, stage, and load type, field by field.
---

# Build your first metadata file

**Prerequisites** · DataCoolie installed (`pip install datacoolie`).  
**End state** · A valid `metadata.json` you can pass to `FileProvider` and run.

This page uses **JSON** because it is the easiest format to learn and the
recommended canonical source. The same model also works in YAML, Excel,
database-backed metadata, and API-backed metadata. Only the storage backend
changes; the connection, source, destination, and transform fields stay the
same. Excel uses a row-oriented subset of the model; its required columns and
source selectors are listed below.

## Step 1 — Create the file skeleton

Create a file called `metadata.json` in your project folder. This walkthrough
uses two top-level arrays:

```json
{
  "connections": [],
  "dataflows":   []
}
```

`connections` describes **where** data lives.  
`dataflows` describes **how** to move it.

This is the skeleton; fill in its connections and dataflows below before
running it. Later, connections can add fields such as `workspace_id` and
`is_active`; dataflows can add `description`, `group_number`,
`execution_order`, `processing_mode`, and their own `configure`.
The [Metadata document reference](../../reference/metadata-schema.md#metadata-document)
shows the exact root fields.

## Step 2 — Add a source connection

For this beginner file-source example, specify these four fields:

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
| `name` | yes | A nonblank label you pick — keep it unique when dataflows reference the connection by name |
| `connection_type` | usually | One of `file`, `lakehouse`, `database`, `api`, `function`. If omitted, DataCoolie derives it from `format` |
| `format` | yes | The data format — must match the connection type (see table below) |
| `configure` | yes | A JSON object of backend-specific settings. `base_path` is the root folder for file connections |
| `workspace_id` | no | Used by database/API metadata providers and in structured logging |
| `catalog` / `database` | no | Use for registered lakehouse tables (Databricks, Fabric, Glue) instead of path-only addressing |
| `secrets_ref` | no | Tells DataCoolie which `configure` fields must be resolved from a secret source |
| `is_active` | no | Set `false` to prevent dataflows using this connection as source or destination from running; the metadata record remains available |

Need to check a field's allowed values or type? The shared contract is under
[Connection](../../reference/metadata-schema.md#connection); backend-specific
settings are grouped under [Connection settings by endpoint type](../../reference/metadata-schema.md#connection-settings-by-endpoint-type).
[Connections](connections.md) explains other endpoint families and secrets.

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

A [dataflow](../../reference/metadata-schema.md#dataflow) is one read → transform → write unit. It refers to your connections
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
| `name` | recommended | Unique label for name-based authoring. Omit it only when you supply a usable explicit `dataflow_id`; name references still require an unambiguous name |
| `dataflow_id` | conditional | Stable explicit identity for ID-based integrations. If both ID and name are omitted or blank, validation fails |
| `stage` | no | Free string — the filter you pass to `driver.run(stage="ingest")`. Group logically related dataflows under the same stage name |
| `source.connection_name` | conditional | Must match a `name` in your `connections` array when using a named connection; an inline `source.connection` is another option |
| `source.schema_name` | no | Subdirectory or schema; combined with `table` to build the path. Omit if not needed |
| `source.table` | conditional | The folder name (file sources) or table name (database/lakehouse). With `query`, an optional logical alias; with `python_function`, an optional input the custom function may use. |
| `destination.connection_name` | conditional | Must match a `name` in your `connections` array when using a named connection; an inline `destination.connection` is another option |
| `destination.schema_name` | no | Output subdirectory or schema |
| `destination.table` | yes | Output folder or table name |
| `destination.load_type` | no | How to write; defaults to `append`. Other options include `overwrite`, `full_load`, `merge_upsert`, `merge_overwrite`, and `scd2`. See [Destination & load patterns](destination-and-load-patterns.md) |

### Common optional dataflow fields

These are the fields teams typically add after the first successful run:

| Field | What it does |
|-------|---------------|
| `description` | Free-text description of the pipeline's business purpose |
| `workspace_id` | Important for database/API metadata backends and workspace-scoped logging |
| `group_number` | Co-locates dataflows on one job; different groups run independently |
| `execution_order` | Orders buckets in a non-null group; equal orders may run in parallel |
| `processing_mode` | `batch` by default; the model accepts `microbatch` and `streaming`, but the built-in driver currently runs the normal ETL path as batch |
| `is_active` | Set `false` to keep the metadata but skip execution |
| `configure` | Arbitrary per-dataflow settings for custom readers/writers/extensions |

### Source block: more than `connection_name + table`

The minimum [Source](../../reference/metadata-schema.md#source) block uses just a connection name and a table. The full
model supports several additional cases:

| Source field | Use it when |
|--------------|-------------|
| `schema_name` | The source is under a folder / schema namespace |
| `watermark_columns` | You want incremental reads |
| `query` | Run SQL rather than read `source.table` directly; `table` may still be a logical label |
| `python_function` | The source is a custom Python loader, for example `mypkg.loaders.load_orders`; the function receives the full `Source` object and may use `table` and other fields |
| `configure.read_options` | You need to override connection-level read options for only one dataflow |
| `configure.endpoint`, `configure.params`, `configure.pagination_*` (`pagination_type`) | The source is an API and the per-dataflow endpoint differs |

Source selector cases:

Database query source (metadata fragment):

```json
"source": {
  "connection_name": "warehouse",
  "query": "SELECT * FROM sales.orders WHERE status = 'OPEN'",
  "watermark_columns": ["updated_at"]
}
```

The same `source.query` field can point to a SQL file. The Driver resolves a
relative `.sql` path during preparation; it does not mean that the database
reader receives the path as SQL text:

```json
"source": {
  "connection_name": "warehouse",
  "query": "sql/orders/incremental.sql",
  "watermark_columns": ["updated_at"]
}
```

Use `artifact:/sql/orders.sql` when the artifact root must be selected
explicitly. See [Source patterns](source-patterns.md#read-a-sql-file) for
single-root, multiple-root and artifact path rules.

Python function source:

```json
"source": {
  "connection_name": "python_src",
  "table": "loaded_orders",
  "python_function": "mypkg.loaders.load_orders",
  "watermark_columns": ["updated_at"]
}
```

### Destination block: extra fields appear as complexity grows

The [Destination](../../reference/metadata-schema.md#destination) block adds fields for merges,
partitioning, and write options:

| Destination field | Use it when |
|-------------------|-------------|
| `merge_keys` | `merge_upsert`, `scd2`, or key-based `merge_overwrite` needs a business key; a usable watermark replacement window can remove this requirement |
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
path forms:

- Databricks Unity Catalog: `` `catalog`.`database`.`table` `` when you leave
  `schema_name` empty
- Fabric / Glue / other metastore-backed layouts:
  `` `catalog`.`database`.`schema_name`.`table` `` when all levels are used

## Step 5 — Complete and validate the file

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

Before the first run, follow the [Validation checklist](validation-checklist.md).
Pay particular attention to connection names, source selector choice, SQL-file
roots, secrets, destination support, and conditional load fields. The checklist
is a reusable gate; return to it after changing a source, transform, or load
strategy.

## Step 6 — Run after validation

After validation passes, run it with:

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
result = driver.run(stage="ingest")
if result.has_failures:
    raise RuntimeError("Ingest failed; do not start transform")
# Check required freshness/quality evidence before progressing.
result = driver.run(stage="transform")
```

Or run multiple at once:

```python
driver.run(stage=["ingest", "transform"])
```

Prefer separate stage calls for operational control. A combined selection needs explicit
ordering for dependent flows: the same non-null `group_number` and increasing `execution_order`.
Use `stop_on_error=True` to prevent later buckets in that group after a failure. Stage list
position and different group numbers provide no dependency barrier. Independent flows can omit
both fields. For example, run the following combined selection with `stage="daily"`:

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
    "group_number": 1,
    "execution_order": 20,
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
| Excel file | Nested objects become JSON cells or flattened columns | Supported rows require connection `name` and `connection_type`; a source row also needs a connection plus `source_table`, `source_query`, or `source_python_function` |
| Database metadata | Rows are stored in SQL tables and filtered by `workspace_id` | Same model fields |
| API metadata | Objects are served over `/workspaces/{workspace_id}/...` endpoints | Same model fields |

The practical rule: learn the JSON structure first, then move it to the
provider your team needs.

Excel is not a byte-for-byte representation of JSON/YAML. Connection type is
required in the workbook even when JSON/YAML can derive it from `format`, and
an endpoint-only API source is not a complete Excel source row because the row
parser requires one of the source selector columns. Use JSON or YAML when the
configuration depends on inline nested connection objects or a representation
that the workbook columns do not expose.

## What to read next

| Goal | Next page |
|------|-----------|
| Understand reusable endpoints | [Connections](connections.md) |
| Understand the dataflow envelope | [Dataflows](dataflows.md) |
| Configure a database or API source | [Source patterns](source-patterns.md) |
| Use merge or SCD2 instead of append | [Destination & load patterns](destination-and-load-patterns.md) |
| Cast types, deduplicate, or add columns | [Transform patterns](transform-patterns.md) |
| Configure API auth, pagination, or incremental windows | [Connections](connections.md#api-authentication), [API source configuration](source-patterns.md#api-source-configuration), and [Incremental windows](source-patterns.md#incremental-windows-and-look-back) |
| Check your file is correct before running | [Validation checklist](validation-checklist.md) |
