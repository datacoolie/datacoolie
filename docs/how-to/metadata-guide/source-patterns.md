---
title: Source Patterns — DataCoolie Metadata How-to
description: Configure every DataCoolie source type — file, Delta, Iceberg, SQL table/query, REST API, and Python function — with accurate field-by-field examples.
---

# Source patterns

**Prerequisites** · Completed [Build your first metadata file](first-metadata-file.md).  
**End state** · A working `connections` entry and `source` block for any DataCoolie source type.

A source connection describes **where DataCoolie reads data from**. Two things
work together: the `Connection` (shared, reusable endpoint definition) and the
`Source` block inside each dataflow (table/query/function selector and
incremental options).

`Connection.configure` carries reusable defaults. `source.configure` carries
per-dataflow overrides. For read behavior, DataCoolie merges them with
**source overrides winning over connection defaults**.

## Source at a glance

```json
"source": {
  "connection_name":    "my_conn",                    
  "schema_name":        "sales",                     
  "table":              "orders",                    
  "query":              "SELECT * FROM sales.orders",
  "python_function":    "mypkg.loaders.load_orders", 
  "watermark_columns":  ["updated_at"],
  "filter_expression":  "status = 'active'",
  "configure": {
    "read_options": { "separator": ";" },
    "endpoint": "/orders"
  }
}
```

| Field | When you use it |
|-------|------------------|
| `connection_name` | Always — must match a connection name |
| `schema_name` | The source lives under a folder / schema namespace |
| `table` | File folder, lakehouse table, or database table |
| `query` | Database query mode instead of table mode |
| `python_function` | Function-source mode instead of a physical table |
| `watermark_columns` | Incremental loading |
| `filter_expression` | SQL predicate applied at read time on raw source columns, after the watermark filter |
| `configure` | Per-dataflow overrides such as `read_options`, API endpoint, API params |

In practice, you normally use **one selector style per source**:

- `table` for file/lakehouse/database table reads
- `query` for database query reads
- `python_function` for function reads

`watermark_columns` enables **incremental loading** — DataCoolie remembers the
max value from the previous run and only reads new rows on the next run.

## Decide source type

| Source family | Connection shape | Typical source fields |
|---------------|------------------|------------------------|
| File | `connection_type: file`, file format | `schema_name`, `table`, `watermark_columns`, `configure.read_options` |
| Lakehouse | `connection_type: lakehouse`, `delta` or `iceberg` | `schema_name`, `table`, `watermark_columns` |
| Database | `connection_type: database`, `format: sql` | `table` or `query`, `schema_name`, `watermark_columns` |
| REST API | `connection_type: api`, `format: api` | `configure.endpoint`, `configure.params/body`, pagination, watermark push-down |
| Python function | `connection_type: function`, `format: function` | `python_function`, `watermark_columns`, custom `source.configure` |

---

## File source (CSV, Parquet, JSON, JSONL, Avro, Excel)

Use when your data is in flat files on local disk, cloud object storage (S3,
ADLS, GCS), or a Fabric/Databricks lakehouse path.

```json
{
  "name":            "raw_files",
  "connection_type": "file",
  "format":          "csv",
  "configure": {
    "base_path":             "data/input",
    "read_options":          { "separator": ";" },
    "use_hive_partitioning": true,
    "date_folder_partitions": "{year}/{month}/{day}"
  }
}
```

| Option | Required | Notes |
|--------|----------|-------|
| `base_path` | yes | Root folder. For S3: `s3://my-bucket/raw`. For ADLS: `abfss://container@account.dfs.core.windows.net/raw` |
| `read_options` | no | Reader defaults for this connection. `source.configure.read_options` can override them per dataflow |
| `use_hive_partitioning` | no | Enables partition-folder discovery like `country=VN/year=2026/` |
| `date_folder_partitions` | no | Date-folder pattern such as `{year}/{month}/{day}` for folder pruning |
| `backward_days` / `backward` | no | Re-read a historical window behind the last watermark |
| `use_schema_hint` | no | Defaults to `true`; disable to ignore `schema_hints` for this connection |
| `format` | yes | One of `csv`, `parquet`, `json`, `jsonl`, `avro`, `excel` |

**Dataflow source block:**

```json
"source": { "connection_name": "raw_files", "schema_name": "sales", "table": "orders" }
```

Reads the folder at `data/input/sales/orders`.

### Per-dataflow file overrides

If one dataset needs special reader settings, override the connection-level
defaults in `source.configure`:

```json
"source": {
  "connection_name": "raw_files",
  "schema_name": "sales",
  "table": "orders",
  "configure": {
    "read_options": {
      "separator": "|",
      "encoding": "utf8-lossy"
    }
  }
}
```

### File-source incremental cases

File readers support more than simple row-level watermarks:

| Pattern | How it works |
|---------|--------------|
| Row-column watermark | CSV/JSON/Excel are fully scanned, then filtered in the engine |
| File modification watermark | Use `__file_modification_time` in `watermark_columns` |
| Date-folder pruning | `date_folder_partitions` lets the reader prune old folders before reading |
| Historical replay window | `backward_days` / `backward` re-opens a look-back window |

File reads also inject file lineage columns such as `__file_name`,
`__file_path`, and `__file_modification_time`.

!!! note "Internal folder watermark"
    When you use `date_folder_partitions`, DataCoolie stores the folder-level
    watermark internally as `__date_folder_partition__`. You normally do not
    need to author that field yourself; the reader maintains it.

!!! note "Excel is read-only"
    Excel (`format: "excel"`) can only be a **source**. You cannot write Excel
    as a destination.

---

## Lakehouse source (Delta or Iceberg)

Use when reading from a Delta table on a lakehouse (Databricks, Fabric, local
Delta folder, or S3 Delta Lake), or an Iceberg table registered in a catalog.

```json
{
  "name":            "bronze_lake",
  "connection_type": "lakehouse",
  "format":          "delta",
  "configure": {
    "base_path": "data/output/bronze"
  }
}
```

**Dataflow source block:**

```json
"source": {
  "connection_name":   "bronze_lake",
  "schema_name":       "sales",
  "table":             "orders",
  "watermark_columns": ["updated_at"]
}
```

For Databricks Unity Catalog or Fabric Lakehouse, set `catalog` and `database`
instead of `base_path`:

```json
{
  "name":            "unity_bronze",
  "connection_type": "lakehouse",
  "format":          "delta",
  "catalog":         "my_catalog",
  "database":        "bronze",
  "configure":       {}
}
```

Table is then referenced by name: `schema_name` maps to the schema, `table` to
the table. Leave `schema_name` empty when the Unity Catalog layout only uses
three parts (`catalog.database.table`).

Same shape for Iceberg, just change `format`:

```json
{
  "name":            "iceberg_lake",
  "connection_type": "lakehouse",
  "format":          "iceberg",
  "catalog":         "glue_catalog",
  "database":        "raw",
  "configure":       {}
}
```

| Addressing mode | What to configure |
|-----------------|-------------------|
| Path-based local/cloud table | `configure.base_path` + `schema_name` + `table` |
| Registered metastore table | `catalog`, `database`, optional `schema_name`, `table` |

Delta and Iceberg both support predicate push-down for watermarks when the
engine supports it.

---

## Database source (SQL via SQLAlchemy)

Use when reading from PostgreSQL, MySQL, MSSQL, Oracle, SQLite, or any
SQLAlchemy-supported database.

```json
{
  "name":            "postgres_src",
  "connection_type": "database",
  "format":          "sql",
  "configure": {
    "database_type": "postgresql",
    "host":          "warehouse.internal",
    "port":          5432,
    "database":      "analytics",
    "username":      "DC_DB_USER",
    "password":      "DC_DB_PASSWORD"
  },
  "secrets_ref": {
    "env": ["username", "password"]
  }
}
```

!!! warning "Never hardcode passwords"
    Use `secrets_ref` instead of putting credentials in `configure`. See
    [Concepts · Secrets](../../concepts/secrets.md) and the credential section
    below.

You can connect with either of these shapes:

| Pattern | When to use |
|---------|-------------|
| `configure.url` | You already have one connection string |
| `database_type` + `host` + `port` + `database` (+ credentials) | You want DataCoolie to assemble database options more explicitly |

**Resolving a full URL from environment variables:**

```json
{
  "name":            "postgres_src",
  "connection_type": "database",
  "format":          "sql",
  "configure": {
    "url": "DC_POSTGRES_URL"
  },
  "secrets_ref": {
    "env": ["url"]
  }
}
```

Set `DC_POSTGRES_URL=postgresql+psycopg2://realuser:realpass@host:5432/mydb`
in your environment. DataCoolie replaces `configure.url` at runtime.

**Dataflow source block:**

```json
"source": {
  "connection_name":   "postgres_src",
  "schema_name":       "public",
  "table":             "orders",
  "watermark_columns": ["updated_at"]
}
```

`schema_name` maps to the SQL schema, `table` to the SQL table name.

### Query mode

When the source is a SQL query, use `source.query` instead of `source.table`:

```json
"source": {
  "connection_name":   "postgres_src",
  "query":             "SELECT * FROM sales.orders WHERE status = 'OPEN'",
  "watermark_columns": ["updated_at"]
}
```

If `watermark_columns` are set, DataCoolie wraps the query as a subquery and
applies the watermark filter outside it.

---

## REST API source

Use when reading from a paginated REST API.

```json
{
  "name":            "orders_api",
  "connection_type": "api",
  "format":          "api",
  "configure": {
    "base_url":        "https://api.example.com/v1",
    "auth_type":       "bearer",
    "auth_token":      "DC_ORDERS_API_TOKEN",
    "timeout":         30,
    "default_headers": { "Accept": "application/json" }
  },
  "secrets_ref": {
    "env": ["auth_token"]
  }
}
```

Put request shape and pagination on the **source**, not on the connection:

```json
"source": {
  "connection_name":   "orders_api",
  "table":             "orders",
  "watermark_columns": ["updated_at"],
  "configure": {
    "endpoint":        "/orders",
    "method":          "GET",
    "params":          { "status": "open" },
    "pagination_type": "offset",
    "page_size":       200,
    "total_path":      "meta.total",
    "data_path":       "data.items"
  }
}
```

| Connection-level key | Purpose |
|----------------------|---------|
| `base_url` | Required root URL |
| `auth_type` | `bearer`, `basic`, `api_key`, `oauth2_client_credentials`, `aws_sigv4` |
| `auth_token` / `username` / `password` / `api_key_*` | Auth credentials |
| `token_url`, `client_id`, `client_secret` | OAuth2 client-credentials flow |
| `default_headers`, `timeout` | Shared request defaults |

| Source-level key | Purpose |
|------------------|---------|
| `endpoint` | Path appended to `base_url` |
| `method`, `params`, `body` | Request shape |
| `pagination_type` | `offset`, `cursor`, or `next_link` |
| `page_size`, `max_pages`, `data_path` | Response traversal and size |
| `total_path`, `offset_max_workers` | Parallel offset pagination |
| `rate_limit_delay`, `max_retries` | Backoff behavior |

### Watermark push-down for APIs

Instead of filtering only after the response arrives, DataCoolie can inject the
last saved watermark into request params or body:

```json
"source": {
  "connection_name":   "orders_api",
  "watermark_columns": ["updated_at"],
  "configure": {
    "endpoint":                 "/orders",
    "watermark_param_mapping":  { "updated_at": "updated_since" },
    "watermark_to_param":       "updated_before",
    "watermark_param_location": "params",
    "watermark_param_format":   "iso"
  }
}
```

### Range-split API fetch

For large historical windows, the API reader can split a time range into
parallel calls:

```json
"source": {
  "connection_name":   "orders_api",
  "watermark_columns": ["updated_at"],
  "configure": {
    "endpoint":                       "/orders",
    "watermark_to_param":            "updated_before",
    "watermark_range_interval_unit": "day",
    "watermark_range_interval_amount": 1,
    "watermark_range_start":         "2026-01-01T00:00:00Z",
    "watermark_range_max_workers":   4
  }
}
```

!!! note "`table` is optional for APIs"
    The API reader uses `connection.configure.base_url` plus
    `source.configure.endpoint`. `source.table` is best treated as a logical
    label for your metadata or logging, not as the actual HTTP path.

---

## Python function source

Use when your data comes from a custom Python function (streaming generator,
SDK call, in-memory dataset, etc.).

```json
{
  "name":            "custom_src",
  "connection_type": "function",
  "format":          "function",
  "configure":       {}
}
```

The actual function path goes on `source.python_function`:

**Dataflow source block:**

```json
"source": {
  "connection_name":   "custom_src",
  "python_function":   "mypkg.sources.load_orders",
  "watermark_columns": ["updated_at"],
  "configure": {
    "api_base": "https://partner.example.com"
  }
}
```

`load_orders` must accept `(engine, source, watermark)` and return a DataFrame
compatible with the active engine. Read custom arguments from `source.configure`
inside your function.

If you want to restrict which Python functions may be imported from metadata,
pass `allowed_function_prefixes` in `DataCoolieRunConfig`.

---

## Incremental reads with `watermark_columns`

For any source type, add `watermark_columns` to the `source` block to enable
incremental loading:

```json
"source": {
  "connection_name":   "postgres_src",
  "schema_name":       "public",
  "table":             "orders",
  "watermark_columns": ["updated_at"]
}
```

On the first run DataCoolie reads everything. After a successful run it saves
the max `updated_at` value. On subsequent runs it adds `WHERE updated_at > <saved value>`.

Behavior differs by source family:

| Source family | Watermark behavior |
|---------------|--------------------|
| Parquet / Delta / Iceberg | Predicate push-down during read |
| Database | `WHERE` clause pushed to SQL |
| API | Injected into request params/body when configured |
| CSV / JSON / Excel | Read then filter in the engine |
| Python function | Watermark is passed into the function first, then the framework filters again after the function returns |

See [Concepts · Watermarks](../../concepts/watermarks.md) for the storage
format and provider interaction.

---

## Filter rows at read time (`source.filter_expression`)

`filter_expression` is a SQL predicate applied **after** the watermark filter,
inside every source reader. It lets you pre-filter rows based on raw source
columns before the transformer pipeline runs.

```json
"source": {
  "connection_name":   "postgres_src",
  "schema_name":       "public",
  "table":             "orders",
  "watermark_columns": ["updated_at"],
  "filter_expression": "status = 'active' AND region = 'US'"
}
```

This is particularly useful when the source table holds multiple logical
datasets and you only want one segment, or when you want to exclude known
bad data before it enters the pipeline at all.

### Database sources

For SQL table/query sources, `filter_expression` is appended to the generated
`WHERE` clause alongside the watermark condition:

```sql
-- generated SQL (conceptual)
SELECT * FROM orders
WHERE updated_at > '2024-01-01'
  AND status = 'active'
  AND region = 'US'
```

### File, Delta, Iceberg, and function sources

For in-process readers (file, lakehouse, Python function), the predicate is
applied by the engine as an in-memory filter after the read.

### When to use `source.filter_expression` vs `transform.filter_expression`

| | `source.filter_expression` | `transform.filter_expression` |
|---|---|---|
| **Stage** | Read time (earliest possible) | Transformer order 35 (after ColumnAdder) |
| **Available columns** | Raw source columns and watermark columns | Source columns + columns from `additional_columns` |
| **Best for** | Excluding rows that should never enter the pipeline | Filtering on computed/derived columns |

---

## Common mistakes

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `ValidationError: format not allowed` | `connection_type` and `format` don't match | Use `lakehouse` + `delta`, not `file` + `delta` |
| Path not found | `base_path / schema_name / table` resolves wrong | Check `base_path` exists; `schema_name` is optional |
| `APIReader requires 'base_url'` | API connection used `url` instead of `base_url` | Put the root URL in `connection.configure.base_url` |
| `PythonFunctionReader requires source.python_function` | Function path was put on the connection or omitted | Put a dotted path like `mypkg.sources.load_orders` on `source.python_function` |
| Full load every run even with watermarks | `watermark_columns` missing from `source` block, or push-down config missing for APIs | Add `"watermark_columns": [...]` and API watermark mapping when needed |
| Credentials exposed in logs | URL/token/password is hardcoded | Move the field to `configure`, store the secret name there, and resolve with `secrets_ref` |

---

## Next

→ [Destination & load patterns](destination-and-load-patterns.md)
