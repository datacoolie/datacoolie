---
title: Source configuration — DataCoolie User Guide
description: Configure every DataCoolie source type and its incremental behavior — file, Delta, Iceberg, SQL table/query, REST API, and Python function.
---

# Source

**Prerequisites** · A reusable [connection](connections.md), an inline
connection, and a dataflow envelope from [Dataflows](dataflows.md).  
**End state** · A working `connections` entry and `source` block for any DataCoolie source type.

A source connection describes **where DataCoolie reads data from**. Two things
work together: the [Connection](../../reference/metadata-schema.md#connection)
(shared, reusable endpoint definition) and the [Source](../../reference/metadata-schema.md#source)
block inside each dataflow (table/query/function selector and
incremental options).

[`connections[].configure`](../../reference/metadata-schema.md#connectionsconfigure)
carries reusable settings; [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure) carries
source-specific settings. Precedence depends on the field: `read_options` merge
key by key (source wins on matching keys), while a source look-back replaces
the entire connection look-back. API settings have their own placement and
precedence rules; there is no universal `configure` merge. See
[connection defaults](connections.md#put-reusable-defaults-on-the-connection)
and [look-back overrides](#override-the-connection-for-one-source).

For the read lifecycle, bounded ranges and reader-specific option routing,
see [Sources & destinations](../../reference/concepts/sources-and-destinations.md#source-contract).

## Source at a glance

This is a valid query-source fragment. Here, `table` is a short, human-readable
alias for the query result, not the table DataCoolie reads. A table source uses
`table` without `query`; a function source uses `python_function` and may also
set `table`. The framework calls the function named by `python_function`, while
the function may use any fields on the `Source` it receives, including `table`.

```json
"source": {
  "connection_name":    "postgres_src",
  "table":              "active_orders",
  "query":              "SELECT updated_at, status, order_id FROM sales.orders",
  "watermark_columns":  ["updated_at"],
  "filter_expression":  "status = 'active'"
}
```

| Field | When you use it |
|-------|------------------|
| `connection_name` or `connection` | Select a named reusable connection with `connection_name` (or a string `connection` reference), or supply an inline `connection` object; use only one of the two fields |
| `schema_name` | Table mode: folder / schema namespace. Query mode: optional metadata namespace, such as for shared schema-hint matching; not needed merely because SQL names a schema |
| `table` | File folder or lakehouse/database table; optional logical alias alongside `query`, or metadata that a Python function can use |
| `query` | Inline SQL or a relative/artifact `.sql` file; takes precedence over `table` when reading |
| `python_function` | Function-source mode instead of a physical table |
| `watermark_columns` | Incremental loading |
| `filter_expression` | SQL predicate evaluated against the reader result before transforms; query mode uses returned columns/aliases |
| `configure` | Source-specific settings such as `read_options` or API endpoint/params; precedence varies by key |

In practice, choose the read mode first:

- `table` for direct file, lakehouse, or database table reads.
- `query` for database or lakehouse SQL reads when one executable SQL
  statement expresses the source, including joins, filters, and CTEs.
- `python_function` when reading data needs more complex Python logic or a
  source case the built-in readers do not cover.

These are choices about how data is read, not strict complexity levels. A CTE
still belongs in `query` if the result can be produced by one SQL statement;
use `python_function` when SQL alone is not the right read boundary.

For a query source, optional `table` is a readable logical alias; the SQL
controls the read. For a Python function source, `table` is also optional, but
it may be a real input to the user function because the function receives the
whole `Source` object. The same applies to other Source fields: configure them
according to that function's contract. The framework still selects the
function by `source.python_function`.

An uncommon but supported case is attaching top-level `schema_hints` to a
query or function source. The provider matches those hints using the source
connection and `source.table`, plus `source.schema_name` when supplied. Only
use a matching group if its hints describe the resulting columns. Usually SQL
or custom function code already handles types; a few casts can be defined in
`transform.schema_hints`. See [Datatypes and schema hints](data-types.md).

`watermark_columns` enables **incremental loading**. The saved watermark and filter
depend on the reader: SQL may push bounds into the query, while files and
lakehouse sources can filter in the engine. API request push-down is optional;
without it, the API reader can still filter fetched records locally. See
[Incremental windows and look-back](#incremental-windows-and-look-back) for
bounds and look-back.

## Decide source type

| Source family | Connection shape | Typical source fields |
|---------------|------------------|------------------------|
| File | `connection_type: file`, file format | `schema_name`, `table`, `watermark_columns`, `configure.read_options` |
| Lakehouse | `connection_type: lakehouse`, `delta` or `iceberg` | `table` or `query`, optional `schema_name`, `watermark_columns` |
| Database | `connection_type: database`, `format: sql` | `table` or `query`, `schema_name`, `watermark_columns` |
| REST API | `connection_type: api`, `format: api` | `configure.endpoint`, `configure.params/body`, pagination, watermark push-down; `source.table` is optional |
| Python function | `connection_type: function`, `format: function` | `python_function`, optional `table` and other Source inputs, `watermark_columns`, custom `source.configure` |

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
    "read_options":          { "separator": ";" }
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
"source": {
  "connection_name": "raw_files",
  "schema_name": "sales",
  "table": "orders",
  "watermark_columns": ["__file_modification_time"]
}
```

Reads the folder at `data/input/sales/orders`. For incremental ingestion of
new or modified files, `__file_modification_time` is a common watermark: the
reader lists files and selects those newer than the saved modification time.
It is not an automatic default; omit `watermark_columns` for a full read, or
use a reliable row column such as `updated_at` when the requirement is to
filter records rather than files. A modified file is read again as a whole,
so the destination load strategy must handle reprocessed rows appropriately.
The normal lower bound is strict (`>`): a file arriving later with the same or
an older modification time can be missed. If this is possible, use a deliberate
[look-back window](#incremental-windows-and-look-back) and an idempotent
destination load.

When a source does not use `__file_modification_time` as a watermark, a file
whose platform listing has no modification time is still read; its
`__file_modification_time` lineage value is null. When the field is configured
as a watermark, every discovered file must provide a modification time. A
missing value fails the source before file reading or watermark advancement,
including on the first run. Fix the platform metadata or source listing and
retry from the unchanged watermark.

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
| Row-column watermark | Use a trustworthy column in the file data; the reader filters rows after reading |
| File modification watermark | Use `__file_modification_time` in `watermark_columns` to select new/modified files before reading; requires file listing with modification times |
| Date-folder pruning | `date_folder_partitions` lets the reader prune old folders before reading |
| Historical replay window | `backward_days` / `backward` re-opens a look-back window |
| Independent bounded read | Replay may use `chunk_column="__file_modification_time"` even when mtime is not a persisted watermark; the reader still selects files by actual mtime |

For a partitioned dataset under folders such as
`data/input/sales/orders/year=2026/month=09/day=23/`, configure both options
on its connection:

```json
{
  "name": "partitioned_files",
  "connection_type": "file",
  "format": "parquet",
  "configure": {
    "base_path": "data/input",
    "use_hive_partitioning": true,
    "date_folder_partitions": "year={year}/month={month}/day={day}"
  }
}
```

Then use a source with `connection_name: "partitioned_files"`,
`schema_name: "sales"`, and `table: "orders"`. If you also use
`__file_modification_time`, folder pruning happens first: a file modified in
an older, pruned date folder will not be discovered unless the folder window
is reopened.

File reads also inject file lineage columns such as `__file_name`,
`__file_path`, and `__file_modification_time`.

!!! note "Internal folder watermark"
    When you use `date_folder_partitions`, DataCoolie stores the folder-level
    watermark internally as `__date_folder_partition__`. You normally do not
    need to author that field yourself; the reader maintains it. It is used
    only for conservative folder discovery. Folder boundaries are inclusive;
    the file's modification time decides whether the file is read. The
    internal folder key cannot be a replay `chunk_column`. The reader also
    reloads this internal frontier on later ordinary runs when the source has
    no authored `watermark_columns`, so folders older than the saved frontier
    are not scanned again.

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

Choose path or catalog addressing based on the table format, engine, and
governance rules—not simply on whether the platform is Databricks or Fabric.
For a named Delta table such as a Databricks Unity Catalog managed table,
configure its catalog/database scope instead of a storage `base_path`:

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

For Iceberg, prefer catalog addressing for an end-to-end read/write pipeline:

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
| Delta by path | `configure.base_path`, `table`, optional `schema_name`; a good default for local/cloud Delta and Fabric when direct path access is appropriate |
| Delta by registered name | `catalog`, `database`, `table`, optional `schema_name`; use for governed named tables, including Unity Catalog managed tables |
| Iceberg by catalog | `catalog`, `database`, `table`, optional `schema_name`; preferred for portable reads and writes |

Fabric Delta can use either a table name or a OneLake path when the runtime
and access policy permit it. Databricks Unity Catalog managed tables must be
accessed by name; external tables have different path-access rules. With
OneLake security enabled on a Fabric table, direct path access can be blocked
for non-privileged users, so use the named table. See the platform guidance for
[Unity Catalog paths](https://docs.databricks.com/aws/en/volumes/paths) and
[Fabric OneLake security](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-onelake-security).

Engine support also matters: Polars Delta uses paths, not named Delta tables.
For Iceberg, use catalog addressing for a full read/write pipeline: Polars
can read an Iceberg path at the engine level but requires a catalog table name
for writes. Path-read fallbacks should not be treated as a portable Iceberg
write contract.

Delta and Iceberg readers apply watermark bounds through the engine's
DataFrame filter after the table read.

### Lakehouse SQL query

Delta and Iceberg sources can also use `source.query` when one SQL statement
expresses the read, including a `WITH` CTE. For example:

```json
"source": {
  "connection_name": "unity_bronze",
  "table": "recent_orders",
  "query": "WITH recent AS (SELECT * FROM my_catalog.bronze.orders WHERE status = 'OPEN') SELECT * FROM recent"
}
```

Here `table` is an optional readable alias; the SQL determines which relations
are read. Lakehouse readers execute the query through the selected engine and
apply watermark bounds to the resulting DataFrame. SQL-file references use
the same [query-file resolution rules](#read-a-sql-file) described below.
With Polars, the SQL relations must be registered in the engine before the
query runs (for example, Delta or Iceberg table discovery in the runner);
metadata alone does not register them. Install the SQL resolver dependency and
follow [Qualified SQL relations in Polars](../../reference/concepts/engines.md#qualified-sql-relations-in-polars).

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
    "env:": ["username", "password"]
  }
}
```

!!! warning "Never hardcode passwords"
    Use `secrets_ref` instead of putting credentials in `configure`. See
    [Concepts · Secrets · `secrets_ref` schema](../../reference/concepts/secrets.md#secrets_ref-schema) and the credential section
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
    "env:": ["url"]
  }
}
```

Set `DC_POSTGRES_URL=postgresql+psycopg2://realuser:realpass@host:5432/mydb`
in your environment. DataCoolie replaces `configure.url` at runtime.

### Database authentication types

By default, database connections use **username/password** auth. The optional
`auth_type`
field enables alternative authentication methods:

| `auth_type` | Required fields | Use case |
|-------------|-----------------|----------|
| `password` (default) | `username`, `password`, unless the connection URL supplies credentials | All databases — standard SQL auth |
| `service_principal` | `username` (= client ID), `password` (= client secret), `tenant_id` | Azure SQL, Fabric SQL via Azure AD/Entra |
| `managed_identity` | none (or `username` = client ID for user-assigned MI) | Azure-hosted runtimes (AKS, App Service, Fabric) |
| `access_token` | `token` (+ optional `username` for non-MSSQL) | Pre-fetched token from any provider (Azure, AWS IAM, GCP) |

**Service principal example (Azure SQL):**

```json
{
  "name":            "azure_sql_spn",
  "connection_type": "database",
  "format":          "sql",
  "configure": {
    "database_type": "mssql",
    "auth_type":     "service_principal",
    "host":          "myserver.database.windows.net",
    "port":          1433,
    "database":      "mydb",
    "username":      "AZURE_CLIENT_ID",
    "password":      "AZURE_CLIENT_SECRET",
    "tenant_id":     "AZURE_TENANT_ID"
  },
  "secrets_ref": { "env:": ["username", "password", "tenant_id"] }
}
```

**Managed identity example (zero-credential, Fabric):**

```json
{
  "name":            "fabric_sql_mi",
  "connection_type": "database",
  "format":          "sql",
  "configure": {
    "database_type": "mssql",
    "auth_type":     "managed_identity",
    "host":          "xyz.datawarehouse.fabric.microsoft.com",
    "port":          1433,
    "database":      "mydb"
  }
}
```

**Pre-fetched access token example (AWS RDS IAM):**

```json
{
  "name":            "rds_postgres_iam",
  "connection_type": "database",
  "format":          "sql",
  "configure": {
    "database_type": "postgresql",
    "auth_type":     "access_token",
    "host":          "mydb.xxx.us-east-1.rds.amazonaws.com",
    "port":          5432,
    "database":      "analytics",
    "username":      "iam_db_user",
    "token":         "RDS_IAM_TOKEN"
  },
  "secrets_ref": { "env:": ["token"] }
}
```

!!! info "Fabric SQL endpoint"
    Fabric SQL endpoints (`*.datawarehouse.fabric.microsoft.com`) only accept
    Entra ID auth. DataCoolie rejects `auth_type: "password"` for these hosts
    at validation time.

!!! info "Engine notes"
    **Spark (JDBC):** SPN/MI auth uses native MSSQL JDBC driver properties —
    no extra Python packages needed.
    **Polars:** Non-password MSSQL auth routes through `pyodbc` + ODBC Driver
    18 instead of connectorx. Other databases use token-as-password via
    connectorx.

### Database transport options

The database reader passes unhandled connection configuration keys through to
the selected database transport. This is useful for provider-specific options
such as MSSQL TLS settings:

```json
{
  "configure": {
    "database_type": "mssql",
    "url": "MSSQL_DATABASE_URL",
    "encrypt": "yes",
    "trustServerCertificate": "false"
  },
  "secrets_ref": {"env:": ["url"]}
}
```

Pass-through behavior depends on the selected engine, driver, and installed
database capability. Use the exact spelling expected by that transport and do
not assume a SQLAlchemy/ODBC/JDBC option works on every engine. The generated
[Connection reference](../../reference/metadata-schema.md#connection) lists
DataCoolie-defined keys; provider-specific keys remain an open configure map.

### Polars database result typing

The Polars reader preserves the result values from the selected source driver
before `schema_hints` are applied. Install
`datacoolie[source-db-native-polars]` for the default precision-preserving
MySQL and MSSQL paths, and `datacoolie[source-db-oracle-polars]` for Oracle.
These paths keep unsigned integers, `DECIMAL`/`NUMBER`, and temporal values in
their native Python representation so a source-aware hint can make the
intentional engine-owned cast.

`configure.database_read_engine` is an explicit transport override:

```json
{
  "database_type": "mysql",
  "url": "DC_MYSQL_URL",
  "database_read_engine": "native"
}
```

Resolve `DC_MYSQL_URL` through `secrets_ref` on the surrounding connection,
as in the [database URL example](#database-source-sql-via-sqlalchemy); do not
put credentials in the metadata file.

`"native"` is the default for MySQL, MSSQL and Oracle. Set
`"connectorx"` only when that transport is required and its result typing is
acceptable; ConnectorX may project unsigned or high-precision numeric values
to floating point, and a later cast cannot restore precision that was already
lost. PostgreSQL and SQLite continue to use their existing URI readers unless
an explicit source-specific reader is selected.

The native MySQL/MSSQL readers apply only the connection coordinates and the
documented read-size options. Unsupported URI query parameters (for example
driver-specific TLS flags) fail explicitly instead of being ignored; select
ConnectorX or a dedicated ODBC configuration when those transport options are
required.

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

### Inline SQL or SQL-file query sources

When the source is a SQL query, put the executable SQL in `source.query`.
Optionally add `source.table` as a concise logical label:

```json
"source": {
  "connection_name":   "postgres_src",
  "table":             "open_orders",
  "query":             "SELECT * FROM sales.orders WHERE status = 'OPEN'",
  "watermark_columns": ["updated_at"]
}
```

For database sources, if `watermark_columns` are set, DataCoolie wraps the
query as a subquery and applies the watermark filter outside it. Lakehouse
readers filter the resulting DataFrame instead.

The same `query` field accepts a conservative file shorthand during Driver
preparation. A single relative token ending in `.sql` is read below the
provider's `sql_base_path` when the metadata provider declares SQL roots, or
below the Driver's SQL roots when it supplies the session fallback. Otherwise
the complete relative path is read directly below `artifact_base_path`; no
`sql/` folder is assumed:

#### Read a SQL file

```json
"source": {
  "connection_name": "postgres_src",
  "query": "sql/orders/incremental.sql"
}
```

`orders/incremental.sql` is also valid and is not prefixed with `sql/`.
Use `artifact:/sql/orders.sql` when the artifact root must be selected
explicitly or the filename does not match the shorthand. File resolution is a
Driver preparation step; direct reader calls still require executable SQL. The
path is metadata, while the resolved SQL text is the value passed to the
database reader. The provider keeps the path configuration but does not need
the Driver platform to store it.

For multiple SQL roots, pass a sequence of roots and qualify the reference by
the final folder name of the selected root (for example, `sql1/orders.sql` or
`queries/customers.sql`). Prefixes must be unique; a single configured root
also accepts the root-relative form without its folder prefix.

The query file still belongs in `source.query`; an optional `source.table`
is a logical label (and possible shared-hint lookup key), not the SQL-file
selector. It does not affect which SQL file is resolved or executed. Validate
the resolved path with the same artifact and SQL-root arguments used by the
runner; a path that works only from the repository checkout is not a portable
metadata contract.

---

## REST API source

Use when reading JSON records from a REST API, with or without pagination.
For request-shape fields, see [Connection settings by endpoint type](../../reference/metadata-schema.md#connection-settings-by-endpoint-type)
and [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure)
in the Metadata reference.

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
    "env:": ["auth_token"]
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
    "max_pages":       1000,
    "total_path":      "meta.total",
    "data_path":       "data.items"
  }
}
```

This example expects a response shaped like
`{"meta":{"total":2},"data":{"items":[{"id":1,"updated_at":"2026-01-01T00:00:00Z"},{"id":2,"updated_at":"2026-01-02T00:00:00Z"}]}}`.
Check `data_path` against a real response: a missing path produces zero records,
not a path-validation error. Omit `pagination_type` and `total_path` for a
single-response endpoint. `watermark_columns` alone filters rows **after**
fetching; it does not make the API return only changed records. Use
[watermark push-down](#push-down-and-split-watermark-ranges) when the endpoint accepts
incremental parameters. The API source also applies `source.filter_expression`
locally to the returned DataFrame before transforms; it does not add the
predicate to request parameters or body.

If `pagination_type` is present, it must be `offset`, `cursor`, or `next_link`.
An omitted or null value means one response. A typo or unsupported value fails
before credentials are resolved or an API request is sent, so an incremental
run cannot advance its watermark without a known completion contract.

With `total_path`, offset pages are fetched concurrently (four workers by
default), and `rate_limit_delay` is **not** applied. `max_pages` is a safety
budget: if the response still requires another page at that limit, the read
fails instead of returning a partial result. Match `page_size`, `max_pages`, and
`offset_max_workers` to the provider's contract and rate limit. For sequential
offset requests, omit `total_path`; then `rate_limit_delay` applies between
pages. See [pagination contracts](#choose-the-pagination-contract).

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
| `rate_limit_delay`, `max_retries` | Delay between sequential pages; retry HTTP 429 responses |

`source.filter_expression` is deliberately absent from the request table. Put
server-side filters in the endpoint-specific `params` or `body` when the API
supports them, and use the source predicate for a framework-side safety filter
over the returned columns.

The table above is the basic API shape. Use
[Connections](connections.md#api-authentication) for authentication and the
[API source configuration](#api-source-configuration) section below for all
request, pagination, timezone, and split-range variants. For a combined example see
[Incremental API with pagination](api-advanced.md).

For API incremental loading, choose between local filtering, request
watermark push-down, and split ranges. The complete field combinations,
timezone handling, and inclusive-boundary rules are documented in
[API source configuration](#api-source-configuration), especially
[Push down and split watermark ranges](#push-down-and-split-watermark-ranges).

## API source configuration

This section owns the request, response, pagination, and API watermark fields
that belong to `source.configure`. [Connections](connections.md#api-authentication)
owns the base URL, authentication, and secrets. The exact source field types
and allowed values are in the [Source reference](../../reference/metadata-schema.md#source)
and [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure).

An authenticated source can use any supported pagination and watermark mode.
The examples below are source fragments, not complete metadata documents.

| Decision | Choose | Where to configure |
|---|---|---|
| Response | Root JSON list, nested list, or one nested object | `source.configure.data_path` when not a root list |
| Pagination | One response, record offset, cursor, or next-link | `source.configure.pagination_type` and its matching path/parameter keys |
| Incremental read | Fetch then filter, API push-down, or bounded split ranges | `source.watermark_columns` plus the applicable `source.configure` keys |

### Match the request and response shape

`source.configure.method` is an HTTP method string (`GET` by default), not a
DataCoolie enum. `params` becomes URL query parameters and `body` becomes a
JSON request body. For example, a POST endpoint that returns one JSON object
under `result` can use:

```json
{
  "source": {
    "connection_name": "orders_api_oauth",
    "table": "order_summary",
    "configure": {
      "endpoint": "/reports/orders",
      "method": "POST",
      "params": {"region": "west"},
      "body": {"status": "open"},
      "data_path": "result"
    }
  }
}
```

The response `{"result":{"count":12}}` becomes one record. With no
`data_path`, the response must be a root-level JSON list. A wrong path yields
zero records. These keys belong to the source because different endpoints on
one connection may use different request and response shapes.

### Choose the pagination contract

Pagination settings belong to `source.configure` because different endpoints
on the same API may return different response shapes. For every paginated
mode, choose `max_pages` large enough for the intended result. Reaching the cap
while another page is required raises a source error instead of returning
partial data. Use a provider-defined stable sort or snapshot when available,
especially for concurrent offset requests against changing data.

#### One response (no pagination)

Omit `pagination_type` for an endpoint that returns all records in one
response. For a root-level list, omit `data_path` too; for a nested list, set
`data_path` to its dot-separated path. The reader makes one request and does
not follow a cursor, link, or offset unless a pagination mode is configured.

#### Offset pagination with provider-specific parameter names

```json
{
  "source": {
    "connection_name": "orders_api_key",
    "table": "orders",
    "configure": {
      "endpoint": "/orders",
      "pagination_type": "offset",
      "page_size": 200,
      "offset_param": "skip",
      "limit_param": "per_page",
      "total_path": "meta.total",
      "data_path": "data.items",
      "offset_max_workers": 4
    }
  }
}
```

With `total_path`, the first response provides the total and remaining pages
can be fetched concurrently. Without it, offset pages are fetched
sequentially until a short page or empty response is returned. The reader
sends **record offsets** (`0`, `200`, `400`, ... in this example), not page
numbers. A provider whose `page` parameter expects `1`, `2`, `3`, ... does not
fit this built-in offset mode; do not merely rename `offset_param` to `page`.

The first response above must contain a numeric `meta.total` and an array at
`data.items`. A wrong `data_path` is treated as an empty result, so test it
against a real response. If `total_path` is missing or non-numeric, the
concurrent path fails. Setting `total_path` also enables up to four concurrent
offset workers by default; `rate_limit_delay` only delays **sequential** page
requests. `max_pages` defaults to 1,000 and is a safety budget: the reader
fails when the declared total needs more pages than that budget or when the
aggregate fetched count does not equal the declared total. `max_retries`
retries HTTP 429 responses, not arbitrary HTTP failures.

#### Cursor pagination

```json
{
  "source": {
    "connection_name": "orders_api_oauth",
    "table": "orders",
    "configure": {
      "endpoint": "/orders",
      "pagination_type": "cursor",
      "page_size": 100,
      "cursor_path": "paging.next_cursor",
      "cursor_param": "after",
      "data_path": "results"
    }
  }
}
```

The reader sends the returned cursor under `cursor_param`. The default response
path is `next_cursor` and the default request parameter is `cursor`. The
authored request options are grouped under
[`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure).

#### Next-link pagination

```json
{
  "source": {
    "connection_name": "orders_api_basic",
    "table": "orders",
    "configure": {
      "endpoint": "/orders",
      "pagination_type": "next_link",
      "next_link_path": "paging.next",
      "data_path": "data"
    }
  }
}
```

When the response contains an absolute next-link URL, the reader follows it
verbatim and uses the link's parameters. The continuation token is opaque: the
reader does not decode it or append the original query bounds by default. Set
`next_link_bound_mode: "repeat_query_bounds"` only for an endpoint whose
contract explicitly requires the original range parameters on every
continuation; matching values are checked and conflicting/duplicate values
fail before the request. It stops when the configured path is empty.
Relative links are resolved against the current page URL. A continuation must
stay on the configured HTTP(S) origin (scheme, host, and effective port); a
foreign host or port, scheme downgrade, userinfo, or non-HTTP(S) link fails
before the next request. Connection credentials therefore remain scoped to the
configured origin. Redirect following is disabled for the API client, so a
redirect cannot bypass the continuation check. Providers that require
cross-origin continuation need an explicit credential-scoping policy outside
this built-in mode.

### Push down and split watermark ranges

For a bounded replay or another source-owned range, use the explicit
`range_param_mapping` contract. Each field declares its lower and upper API
parameter, the wire format, the response field used for residual filtering,
and how a successful read advances state:

```json
{
  "source": {
    "connection_name": "orders_api",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "endpoint": "/orders",
      "range_param_mapping": {
        "updated_at": {
          "lower": {"name": "created_from", "operator": ">="},
          "upper": {"name": "created_before", "operator": "<"},
          "format": "iso",
          "response_column": "updated_at",
          "watermark_value": "observed_max"
        }
      },
      "pagination_type": "next_link",
      "next_link_path": "paging.next"
    }
  }
}
```

`watermark_value: "observed_max"` saves the maximum response value after a
successful read. `watermark_value: "request_end"` saves the exclusive upper
bound that the endpoint confirmed. Omitted incremental lower operators resolve
to `>` for `observed_max` and `>=` for `request_end`; active fields with mixed
semantics are rejected before HTTP. A bounded replay uses `[start, end)` and
requires endpoint operators and a `response_column` that can enforce any
boundary the endpoint cannot enforce itself. Legacy `watermark_param_mapping`
does not provide this independent bounded-range contract.

A tracked `observed_max` binding must have a non-null `response_column`; the
reader rejects a missing column mapping before HTTP, even when watermark saving
is disabled. A `request_end` binding can omit it when the endpoint enforces the
requested operators exactly and guarantees complete interval pagination.

New bindings encode explicit bounds without rounding. `iso` retains the
datetime offset and microseconds; nonzero ISO fractional digits beyond six
cannot be represented and are rejected. ISO timezone offsets that Python
cannot parse without precision loss are also rejected. `date` requires midnight, `datetime`
requires whole seconds, and `datetime_ms` / `timestamp_ms` require millisecond
alignment. `timestamp` preserves fractional Unix seconds using exact decimal
encoding. For example, `12:30:00.123456` cannot use `datetime_ms`; use `iso` or
supply a bound that is already aligned. Validation happens before HTTP for
both incremental and bounded reads. Legacy `watermark_param_mapping` retains
its existing formatting behavior.

When the reader generates an incremental upper bound from the current time,
it chooses a ceiling compatible with the binding's precision and logical
date/datetime type before creating requests. Split requests and `request_end`
state use that same covered ceiling. Explicit bounds and stored lower values
are never rounded to make them fit.

The mapped field does not have to be one of `source.watermark_columns` when it
is used only for bounded selection. For example, set `chunk_column="created_at"`
and define `range_param_mapping.created_at` while keeping
`watermark_columns: ["updated_at"]`. Replay sends the `created_at` bounds and
the API reader continues to calculate/persist state only for the configured
watermark columns. A maximum observed from that slice must not be treated as
proof that the complete `updated_at` history was read.

Set a binding's `location` to `params` for query parameters or `body` for a
top-level JSON body field. Nested body paths are outside this generic mapping
contract and require a source-specific adapter.

Use `watermark_param_mapping` to map stored watermark columns to API parameter
names. Add `watermark_to_param` when the API accepts an upper bound:

```json
{
  "source": {
    "connection_name": "orders_api_oauth",
    "watermark_columns": ["updated_at"],
    "configure": {
      "endpoint": "/orders",
      "watermark_param_mapping": {"updated_at": "updated_since"},
      "watermark_to_param": "updated_before",
      "watermark_param_location": "params",
      "watermark_param_format": "iso",
      "watermark_to_param_timezone": "Asia/Ho_Chi_Minh",
      "watermark_range_interval_unit": "day",
      "watermark_range_interval_amount": 1,
      "watermark_range_start": "2026-01-01T00:00:00Z",
      "watermark_range_max_workers": 4,
      "watermark_range_to_exclusive_offset": "1ms"
    }
  }
}
```

The **legacy incremental split** requires
`watermark_range_interval_unit` and `watermark_to_param`. On the first run it
also requires `watermark_range_start`; later runs can start from the saved
watermark when `watermark_param_mapping` is present. The API must accept both
mapped lower and configured upper bounds; omitting the mapping causes every
range request to lack a lower bound. Supported interval units are `hour`,
`day`, `month`, and `year`.

Canonical bounded reads and replay use `range_param_mapping` instead. Each
selected field declares both endpoint bindings and their operators, so a
canonical request can enforce the exact `[start, end)` interval without
`watermark_to_param`, `watermark_range_start`, or legacy interval fields. A
canonical numeric field is read as one finite range; use replay's integer
`chunk_interval: {"step": ...}` when several numeric chunks are needed.

The legacy upper-bound behavior is unchanged. Set
`watermark_range_to_exclusive_offset` to `1ms`, `1s`, or `1day` only when that
API treats its upper bound as inclusive, for example a `BETWEEN from AND to`
query. The offset changes the value sent to the API; adjacent internal range
boundaries remain contiguous. This adjustment does not replace the explicit
operators on a canonical `range_param_mapping` binding.

`watermark_to_param_timezone` accepts an IANA name such as
`Asia/Ho_Chi_Minh` or a UTC offset such as `+07:00`. A source-level value wins
over the same connection-level default. The range endpoint and the stored
watermark must use a precision and timezone that the API understands.

`watermark_param_format` controls how a stored date/datetime becomes the upper
bound request parameter:

| Format | Example output | Behavior |
|---|---|---|
| `iso` | `2026-01-02T03:04:05+00:00` | ISO-8601 output; preserves the datetime's timezone state |
| `date` | `2026-01-02` | Calendar date only |
| `timestamp` | `1767323045.0` | Unix seconds; naive datetimes are treated as UTC |
| `timestamp_ms` | `1767323045000` | Unix milliseconds; naive datetimes are treated as UTC |
| `datetime` | `2026-01-02T03:04:05` | Naive ISO datetime truncated to whole seconds |
| `datetime_ms` | `2026-01-02T03:04:05.123` | Naive ISO datetime retaining millisecond precision |

Unparseable string watermarks pass through unchanged; typed date/datetime values
use the conversion rules above. Choose a format and timezone accepted by the
API rather than relying on a provider-specific default.

### Avoid invalid combinations

- Do not put pagination keys on the connection; they belong to the source.
- Do not enable the **legacy incremental split** without `watermark_to_param`
  and a first-run `watermark_range_start`. Canonical bounded reads and replay
  use `range_param_mapping` instead.
- Do not use `cursor_param` or `next_link_path` with the wrong
  `pagination_type`.
- Do not use a custom or misspelled `pagination_type`; only `offset`, `cursor`,
  and `next_link` are supported, and an omitted value means one response.
- Put auth settings and secrets on the [connection](connections.md#api-authentication).
- Do not let the API response omit a pushed-down watermark column without
  understanding the reader's `now` advancement behavior.

!!! note "`table` is optional for APIs"
    The API reader uses `connection.configure.base_url` plus
    `source.configure.endpoint`. `source.table` is best treated as a logical
    label for your metadata or logging, not as the actual HTTP path. Keep a
    stable `table` when [shared schema hints](data-types.md#global-hints-for-a-source-table)
    must match this source by connection/table identity.

---

## Python function source

Use when reading data needs custom Python logic—for example, combining several
steps or using an SDK—or when the built-in table/query readers do not cover the
source. If one database or lakehouse SQL statement (including CTEs) is enough,
prefer `source.query`.

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
  "table":             "partner_orders",
  "python_function":   "mypkg.sources.load_orders",
  "watermark_columns": ["updated_at"],
  "configure": {
    "api_base": "https://partner.example.com"
  }
}
```

Define `load_orders(engine, source, watermark_start, watermark_end)` for
ordinary incremental reads (the framework passes all four as keyword
arguments). A bounded replay passes a separate `read_range` keyword and sets
`watermark_start=None` and `watermark_end=None`; a bounded function must accept
that keyword explicitly or through `**kwargs`. `read_range` carries the exact
source-owned column, start/end values, and comparison operators. Use it for
push-down when possible, and return rows that honor the range; the built-in
function reader also applies the exact residual range filter before it observes
the candidate watermark. It rejects a legacy function that cannot accept the
keyword. Return a DataFrame compatible with the active engine, or `None` when
there is no data. The function receives the full `Source` object, so it can use
`source.table`, `source.schema_name`, `source.configure`, and other fields as
meaningful inputs. In this example `table` may name the dataset that
`load_orders` reads; it is not necessarily just a display alias.
The framework uses `source.python_function` to choose the function.

To narrow which dotted function paths may be imported from metadata, pass
`allowed_function_prefixes` in `DataCoolieRunConfig`. This is a string-prefix
check, not a sandbox; only run trusted metadata and choose prefixes carefully.

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

The first run has no stored lower bound, unless a configured initial range or
replay window supplies one. Later reads use the stored watermark and any
configured look-back. A database reader adds a SQL watermark condition; other
readers use different mechanisms. Checkpoint advancement also differs: most
readers use the maximum observed watermark, while canonical API bindings use
their configured `watermark_value` (`observed_max` or `request_end`) and the
legacy split path advances from its covered request boundary.

Behavior differs by source family:

| Source family | Watermark behavior |
|---------------|--------------------|
| Parquet / Delta / Iceberg | Engine DataFrame filter after read |
| Database | `WHERE` clause pushed to SQL; query mode filters its returned columns |
| API | Mapped watermark fields are pushed into request params/body; unmapped fields are filtered in the engine after fetching |
| File formats | `__file_modification_time` can select files during listing; row-column watermarks filter in the engine after read |
| Python function | Incremental watermarks are passed into the function first; bounded `read_range` is a separate source-owned contract, then the framework filters again after the function returns |

API request mapping can reduce remote fetches; without it, local filtering
does not prevent a full API fetch. See [API watermark push-down](#push-down-and-split-watermark-ranges)
and [Concepts · Watermarks · Storage ownership and path binding](../../reference/concepts/watermarks.md#storage-ownership-and-path-binding)
for watermark storage and provider interaction.

## Incremental windows and look-back

An incremental source normally starts at the saved watermark. A look-back
reopens part of that range so late-arriving or corrected records can be read
again. The runtime-computed property is called `date_backward`; do not author
`date_backward` in metadata. Author one of the supported `backward_*` fields or
the nested `backward` object in `source.configure`.

### Choose a fixed look-back

Put a reusable default in [`connections[].configure`](../../reference/metadata-schema.md#connectionsconfigure):

```json
{
  "connections": [
    {
      "name": "orders_source",
      "connection_type": "database",
      "format": "sql",
      "configure": {
        "database_type": "postgresql",
        "url": "ORDERS_DATABASE_URL",
        "backward_days": 3
      },
      "secrets_ref": {"env:": ["url"]}
    }
  ]
}
```

The supported top-level shorthand fields are:

| Authored field | Effective meaning |
|---|---|
| `backward_hours` | Subtract hours from the saved datetime watermark |
| `backward_days` | Subtract days from the saved watermark |
| `backward_months` | Subtract calendar months, with calendar-safe clamping |
| `backward_years` | Subtract calendar years, with calendar-safe clamping |
| `backward_closing_day` | Use the closing-day strategy with that day of the month |

You can express the same values in one nested object:

```json
{
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "configure": {
      "backward": {
        "days": 3,
        "hours": 6
      }
    }
  }
}
```

The nested keys are `years`, `months`, `days`, `hours`, and `closing_day`.
Use one clear strategy per connection unless you intentionally need combined
fixed offsets such as three days plus six hours. If the nested object repeats
a unit also supplied by a shorthand field, the nested value wins during
parsing.

On the first run, there is no saved watermark to adjust, so the look-back has
no effect until a saved watermark exists. Non-datetime watermark values pass through
unchanged. When a file reader has a date-folder watermark, the offset applies
**only** to that folder key; `__file_modification_time` remains anchored to its
saved value. Other readers adjust datetime values in the stored watermark. See
[Late-arriving and updated files](late-arriving-files.md) for the two-stage
folder and mtime selection.

### Override the connection for one source

A look-back in [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure)
overrides the entire connection-level look-back when the source has a
non-empty backward configuration:

```json
{
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "backward_hours": 12
    }
  }
}
```

This source uses twelve hours, not “the connection's three days plus twelve
hours”. If a source should inherit the connection default, omit its backward
fields. If it should have a combined offset, author the complete combination
at the source level:

```json
{
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "backward": {"days": 2, "hours": 6}
    }
  }
}
```

### Use a closing-day strategy for monthly corrections

`closing_day` computes an absolute start boundary from the current date rather
than subtracting a fixed number of days. It is useful when upstream closes a
period on a known day of each month:

```json
{
  "source": {
    "connection_name": "orders_source",
    "table": "orders",
    "watermark_columns": ["updated_at"],
    "configure": {
      "backward": {"closing_day": 10}
    }
  }
}
```

When `closing_day` is present, it takes priority over fixed offset keys. The
optional `months` and `years` values can move the closing-day boundary farther
back. Use a day valid for the upstream business calendar and test a boundary
around month/year changes before production use.

### Combine look-back with watermark-window replacement

Window replacement combines a source watermark and authored look-back with
the [Destination](../../reference/metadata-schema.md#destination) setting
[`destination.configure.replace_by_watermark`](../../reference/metadata-schema.md#dataflowsdestinationconfigure)
under `load_type: merge_overwrite`. The effective source window must cover the
destination delete scope. The source must return every row to retain in that
scope and preserve or deterministically map the watermark output column. With
a usable replacement window, `merge_keys` are not required; the key-based
fallback needs them.

The [Cross-boundary combinations](../../reference/metadata-schema.md#cross-boundary-combinations)
section collects these source and destination requirements.

An ordinary empty incremental read does not replace a window. An explicitly
bounded empty replay can delete its window. Follow the [complete window
recipe](watermark-window-replacement.md) for its assumptions, first run,
output mapping, and delete/append failure boundary.

### Relate look-back to API ranges and replay

Look-back changes the lower bound derived from the saved watermark. API range
splitting separately divides a `[from, to)` interval into requests; configure
that on the API source with `watermark_to_param` and a range interval. See
[Push down and split watermark ranges](#push-down-and-split-watermark-ranges)
for timezone and inclusive-upper-bound handling.

Replay supplies an explicit bounded range and can cap an API range's upper
bound. It is an operational choice, not a replacement for ordinary look-back.
Keep replay configuration in the operations guide and use this section only
to understand how the source's effective lower bound is formed.

### Validate an incremental configuration

- Set `watermark_columns` and confirm every selected column is returned by the
  table/query/API response.
- Choose either connection inheritance or a complete source override.
- Do not author computed `date_backward`.
- For `closing_day`, test month-end, leap-year, and upstream period-boundary
  behavior.
- For window replacement, pair `merge_overwrite` with
  `replace_by_watermark: true` and a usable look-back.
- Confirm the source returns complete window coverage and preserves watermark
  columns.
- Test first run, normal empty increment, and explicit replay separately.

---

## Filter rows at read time (`source.filter_expression`)

`filter_expression` is a SQL predicate combined with or applied **after** the
watermark condition by the source reader, before transforms. It references
columns in the reader's result. For a database `source.query`, use columns or
aliases returned by that query, not columns hidden inside it.

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

For SQL table/query sources, `filter_expression` is combined with the generated
`WHERE` clause alongside the watermark condition. In query mode, the reader
wraps your SQL as a derived table and filters its output columns, so the query
must be valid when nested and expose the watermark and predicate columns:

```sql
-- generated SQL (conceptual)
SELECT * FROM orders
WHERE updated_at > '2024-01-01'
  AND status = 'active'
  AND region = 'US'
```

### File, Delta, Iceberg, API, and function sources

For file, lakehouse, API, and Python function readers, the predicate is applied
by the engine to the returned DataFrame after the read. For API reads this
happens after all pages/ranges have been fetched and before the transform
pipeline starts; it does not reduce HTTP volume.

### When to use `source.filter_expression` vs `transform.filter_expression`

| | `source.filter_expression` | `transform.filter_expression` |
|---|---|---|
| **Stage** | Read time (earliest possible) | Transformer order 35 (after ColumnAdder) |
| **Available columns** | Reader output columns (including query aliases and available watermark columns), but not columns added by transforms | Reader output + columns from `additional_columns` |
| **Best for** | Excluding rows that should never enter the pipeline | Filtering on computed/derived columns |

For an incremental source, the pipeline selects the reader's new watermark
before transform filters run and saves it after a successful write. Rows removed
only by `transform.filter_expression` do not automatically return if you widen
that filter later. Plan a bounded [replay](../operations/replay-and-backfill.md)
when previously filtered history must be restored.

---

## Common mistakes

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `ValidationError: format not allowed` | `connection_type` and `format` don't match | Use `lakehouse` + `delta`, not `file` + `delta` |
| Path not found | `base_path / schema_name / table` resolves wrong | Check `base_path` exists; `schema_name` is optional |
| `APIReader requires 'base_url'` | API connection used `url` instead of `base_url` | Put the root URL in `connection.configure.base_url` |
| `PythonFunctionReader requires source.python_function` | Function path was put on the connection or omitted | Put a dotted path like `mypkg.sources.load_orders` on `source.python_function` |
| No incremental filtering | `watermark_columns` missing from `source` block | Add `"watermark_columns": [...]` and verify the selected reader's watermark behavior |
| API fetches all pages despite incremental filtering | Watermark columns are set, but request push-down mapping is absent | Configure API watermark mapping when the provider supports it; local filtering alone does not reduce fetch volume |
| Credentials exposed in logs | URL/token/password is hardcoded | Move the field to `configure`, store the secret name there, and resolve with `secrets_ref` |
| API pages repeat or skip records | Pagination/range boundary does not match the provider contract | Use the exact pagination paths/parameter names and review [API source configuration](#api-source-configuration) |

---

## Next

→ [Transform patterns](transform-patterns.md) · [Destination & load patterns](destination-and-load-patterns.md) · [Datatypes and schema hints](data-types.md)
