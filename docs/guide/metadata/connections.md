---
title: Connections — DataCoolie User Guide
description: Define reusable DataCoolie endpoints for files, lakehouses, databases, APIs, and Python functions before composing dataflows.
---

# Connections

**Prerequisites** · DataCoolie is installed and you know where the source and
destination data will live.  
**End state** · Reusable connection definitions that dataflows can reference by
name.

A [connection](../../reference/metadata-schema.md#connection) describes a reusable endpoint: its family, format, addressing
information, shared read/write defaults, and secret references. A dataflow then
chooses how to use that endpoint as a source or destination.

## Connection at a glance

Define top-level connections before writing dataflows. The array order is not a
runtime dependency; this is an authoring convention that makes references easy
to inspect.

```json
{
  "connections": [
    {
      "name": "orders_input",
      "connection_type": "file",
      "format": "csv",
      "configure": { "base_path": "data/input" }
    },
    {
      "name": "bronze",
      "connection_type": "lakehouse",
      "format": "delta",
      "configure": { "base_path": "data/output/bronze" }
    },
    {
      "name": "warehouse",
      "connection_type": "database",
      "format": "sql",
      "configure": {
        "database_type": "postgresql",
        "host": "warehouse.example",
        "database": "sales"
      }
    },
    {
      "name": "orders_api",
      "connection_type": "api",
      "format": "api",
      "configure": { "base_url": "https://api.example.com/v1" }
    },
    {
      "name": "custom_source",
      "connection_type": "function",
      "format": "function"
    }
  ]
}
```

These are minimal examples for each built-in connection family. The lakehouse
entry uses a path; registered lakehouse names are covered under
[Addressing and workspace scope](#addressing-and-workspace-scope). Database
credentials and API authentication depend on the endpoint and can be added
using the secret references described below. The built-in API and function
connections are source-only: their route or Python callable is selected by
the dataflow source.

The dataflow references these entries with `source.connection_name` and
`destination.connection_name`. Inline `connection` objects remain available
for a one-off endpoint, but reusable named connections are easier to audit and
share across dataflows.

For a one-off source, the inline object replaces `connection_name` and carries
the same connection fields. Keep secrets in `secrets_ref` just as with a named
connection:

```json
{
  "source": {
    "connection": {
      "name": "orders_inline",
      "connection_type": "database",
      "format": "sql",
      "configure": {"url": "ORDERS_DATABASE_URL"},
      "secrets_ref": {"env:": ["url"]}
    },
    "schema_name": "sales",
    "table": "orders"
  }
}
```

Use exactly one of `connection_name` and `connection`. An inline connection
still needs `name` because the model uses it as the endpoint identity; it is
not registered as a reusable top-level connection for other dataflows. Prefer
a named connection when the endpoint is shared or governed centrally.

For normal authoring, keep `name` as the human-readable identity and let the
framework derive the stable ID. If an external provider owns a stable identity,
see [Connection identity](#connection-identity)
before adding `connection_id`.

### Connection identity

Keep `name` unique in the document/provider scope used by name references, and
use `connection_name` in source, destination and shared schema-hint references.
DataCoolie derives `connection_id` from the name if it is omitted. Author an
explicit, stable `connection_id` only when a provider or external system owns
it; do not regenerate it per deployment or reuse it for two connections in the
same shared ID store. Prefer names in normal documents, and keep any
shared-hint `connection_id` consistent with its connection. See the
[Connection](../../reference/metadata-schema.md#connection) contract.

The document loader allows distinct explicit IDs to carry the same display
name, but a name-based reference or shared schema hint rejects an ambiguous
name and asks for `connection_id`. If duplicate display names are intended,
each connection must provide a distinct explicit ID; omitted IDs derive from
the same name and therefore collide. Database and API providers load one
workspace at a time. Supporting duplicate display names across a
mixed-workspace file snapshot still requires an explicit workspace-aware file
scope.

When metadata is loaded for a specific `workspace_id`, name lookups belong to
that workspace. Database and API providers are loaded one workspace at a time;
the file provider currently treats one metadata snapshot as one name scope.
Do not place same-name connections from multiple workspaces in one file
snapshot until a multi-workspace file scope is explicitly configured.

For a provider-managed stable identity, add the ID alongside the readable
name (connection fragment):

```json
{
  "name": "orders_source",
  "connection_id": "conn-orders-prod-v1",
  "connection_type": "database",
  "format": "sql",
  "configure": {"database_type": "postgresql", "url": "ORDERS_DATABASE_URL"},
  "secrets_ref": {"env:": ["url"]}
}
```

### Disable a connection without removing it

Set `is_active: false` on a connection to prevent every dataflow using it as
source or destination from running. The default is `true`. Metadata providers
still retain and can return the connection and its referencing dataflows;
`get_connections(active_only=False)` includes inactive connections. An active
dataflow selected for execution is `SKIPPED` with a reason if either connection
is inactive. A dataflow already in progress is not cancelled by a later edit
to the metadata source. See [Dataflow activation](dataflows.md#activation-and-selection)
and the is_active field.

## Choose the endpoint family

| `connection_type` | Typical `format` | Use it for |
|---|---|---|
| `file` | `csv`, `parquet`, `json`, `jsonl`, `avro`, `excel` | Flat files and file roots; Excel is source-only |
| `lakehouse` | `delta`, `iceberg` | Lakehouse tables and paths |
| `database` | `sql` | Database tables and SQL query sources; built-in SQL is source-only |
| `api` | `api` | REST API sources |
| `function` | `function` | Python function sources |

`connection_type` can be derived from an unambiguous `format`, such as
`delta -> lakehouse`. An explicit type is easier to read when a project has
many endpoint families. The model also names `streaming`, but no built-in
format is currently wired to that connection family.
The [Connection settings by endpoint type](../../reference/metadata-schema.md#connection-settings-by-endpoint-type)
section groups backend-specific settings.

## Put reusable defaults on the connection

Use [`connections[].configure`](../../reference/metadata-schema.md#connectionsconfigure)
for endpoint settings and defaults shared by its
dataflows. Per-dataflow settings go in [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure)
or [`destination.configure`](../../reference/metadata-schema.md#dataflowsdestinationconfigure);
there is no general rule that merges every
`configure` key between these levels.

```json
{
  "name": "warehouse",
  "connection_type": "database",
  "format": "sql",
  "configure": {
    "host": "warehouse.example",
    "database": "sales",
    "database_type": "postgresql",
    "read_options": { "fetchsize": 10000 }
  }
}
```

| Connection setting | Applies to | Per-dataflow behavior |
|---|---|---|
| `read_options` | File, lakehouse, and SQL reads | `source.configure.read_options` is merged one key at a time; source values win for matching keys. |
| `write_options` | File and lakehouse writes | `destination.configure.write_options` is merged one key at a time; destination values win for matching keys. |
| `merge_options` | Lakehouse MERGE, merge-overwrite, and SCD2 operations | `destination.configure.merge_options` is merged one key at a time; destination values win. These options do not flow into the append writer. |
| `backward_hours`, `backward_days`, `backward_months`, `backward_years`, `backward_closing_day`, or `backward` | Source look-back windows | A non-empty source look-back replaces the entire connection look-back, rather than adding to it. |
| `watermark_to_param_timezone` | API watermark requests | `source.configure.watermark_to_param_timezone` wins over the connection default. |
| `base_path` | File and path-based lakehouse endpoints | Read from the connection; `source.schema_name` / `destination.schema_name` and `source.table` / `destination.table` extend the path. For lakehouse paths, see `base_path`. |
| `date_folder_partitions` | File reads and writes | Read from the connection; it is not overridden through source or destination `configure`. |
| `use_hive_partitioning` | File reads | Read from the connection; it is not overridden through `source.configure`. |
| `use_schema_hint`, `schema_hint_type_system` | Source schema hints | Read from the source connection. |

For example, `date_folder_partitions` set to `"{year}/{month}/{day}"` in a file
connection lets the reader prune dated folders and puts flat-file writes under
a folder for the current UTC date. On a flat-file destination,
`destination.partition_columns` takes precedence when both forms of
partitioning are configured. Use a separate or inline connection when one
dataflow needs a different date-folder pattern or base path.

For look-back, `backward_days` set to `7` in `connection.configure` is a shared
default. A source with `"configure": {"backward_hours": 12}` uses twelve
hours, not seven days plus twelve hours. Author the full combination in that
source's `backward` object if both offsets are needed; `date_backward` is a
computed runtime property, not a metadata field. See
[Incremental windows](source-patterns.md#override-the-connection-for-one-source)
for the supported forms, [Source patterns](source-patterns.md) for file reads,
and [Destination & load patterns](destination-and-load-patterns.md#flat-file-outputs-with-date-folders)
for dated file writes. Engine and provider option maps remain open; the
Connection configure reference lists
DataCoolie-defined keys.

## API and function source connections

An API connection needs `connection_type: api`, `format: api`, and
`configure.base_url`. It can also hold shared
`auth_type`, credentials,
`default_headers`, and
`timeout` (30 seconds by default). For an unauthenticated
API, omit `auth_type`. Put the endpoint-specific `endpoint`,
`method`,
`params`,
`body`, response
`data_path`, pagination, and watermark request settings in
`source.configure`. The built-in API connection reads sources; no built-in API
destination writer is registered. See [Source patterns](source-patterns.md#rest-api-source)
for a complete source and [API source configuration](source-patterns.md#api-source-configuration)
for all request, response, pagination and incremental options.

A function connection identifies the `function` source family; set
`source.python_function` to the callable's dotted path. Custom arguments for
that callable belong in `source.configure`. See
[Python function sources](source-patterns.md#python-function-source).

## API authentication

An [API connection](../../reference/metadata-schema.md#connection) sets
`connection_type: api`, `format: api`, `configure.base_url`, and an optional
`configure.auth_type`. For a public endpoint, omit `auth_type`. For authenticated
endpoints, use HTTPS and resolve credential fields through `secrets_ref`; the
strings below are environment-variable names, not credentials. The API reader
also supports bearer authentication with the connection's `auth_token` field.

### HTTP Basic

Use `basic` for an HTTP Basic `Authorization` header:

```json
{
  "name": "orders_api_basic", "connection_type": "api", "format": "api",
  "configure": {
    "base_url": "https://api.example.com/v1", "auth_type": "basic",
    "username": "DC_ORDERS_API_USER", "password": "DC_ORDERS_API_PASSWORD"
  },
  "secrets_ref": {"env:": ["username", "password"]}
}
```

The reader encodes the credentials for the request; Basic auth does not replace
TLS or a secret manager.

### API key

Use `api_key` for a custom header; `X-API-Key` is the default. Set
`api_key_header` when the provider uses a different header:

```json
{
  "name": "orders_api_key", "connection_type": "api", "format": "api",
  "configure": {
    "base_url": "https://api.example.com/v1", "auth_type": "api_key",
    "api_key_header": "X-Client-Key", "api_key_value": "DC_ORDERS_API_KEY"
  },
  "secrets_ref": {"env:": ["api_key_value"]}
}
```

Do not put a key in `source.configure.params` unless the provider explicitly
requires a query parameter and its exposure is acceptable.

### OAuth2 client credentials

Use `oauth2_client_credentials` when the endpoint requires a short-lived token:

```json
{
  "name": "orders_api_oauth", "connection_type": "api", "format": "api",
  "configure": {
    "base_url": "https://api.example.com/v1",
    "auth_type": "oauth2_client_credentials",
    "token_url": "https://identity.example.com/oauth2/token",
    "client_id": "DC_ORDERS_CLIENT_ID",
    "client_secret": "DC_ORDERS_CLIENT_SECRET",
    "scope": "orders.read",
    "token_auth_method": "client_secret_basic",
    "token_request_body_format": "form",
    "token_request_extras": {"audience": "orders-api"}
  },
  "secrets_ref": {"env:": ["client_secret"]}
}
```

`token_url`, `client_id`, and `client_secret` are required. The default token
authentication method is `client_secret_post`; select `client_secret_basic`
when the identity provider requires HTTP Basic. The default token body is
form-encoded; select `json` only when required by the provider. Extras are
forwarded to the token endpoint. Tokens are cached in-process until shortly
before expiry.

### AWS Signature Version 4

Use `aws_sigv4` for API Gateway or another AWS service requiring SigV4:

```json
{
  "name": "orders_api_sigv4", "connection_type": "api", "format": "api",
  "configure": {
    "base_url": "https://abc123.execute-api.us-east-1.amazonaws.com/prod",
    "auth_type": "aws_sigv4", "aws_region": "us-east-1",
    "aws_service": "execute-api"
  }
}
```

The default region is `us-east-1`, and the default service is `execute-api`.
The reader uses the standard AWS credential chain if explicit credentials are
absent. It requires `botocore` and available credentials. If supplying
`aws_access_key_id`, `aws_secret_access_key`, or `aws_session_token` explicitly,
resolve them through a secret source. See the
[API connection settings](../../reference/metadata-schema.md#connection-settings-by-endpoint-type)
for the exact fields.

## Keep credentials out of metadata

Put the *secret name*, not its value, in `configure`. Then map the corresponding
`configure` field to a secret source with
`secrets_ref`.
For example, set the
runner's `WAREHOUSE_USER` and `WAREHOUSE_PASSWORD` environment variables before
running this connection:

```json
{
  "name": "warehouse",
  "connection_type": "database",
  "format": "sql",
  "configure": {
    "host": "warehouse.example",
    "username": "WAREHOUSE_USER",
    "password": "WAREHOUSE_PASSWORD"
  },
  "secrets_ref": {
    "env:": ["username", "password"]
  }
}
```

`env:` selects the built-in environment resolver with no prefix. It reads
`WAREHOUSE_USER` for `configure.username` and `WAREHOUSE_PASSWORD` for
`configure.password`; the names in `secrets_ref` are **configure field names**,
not environment variable names. To use a shared prefix, `"env:APP_"` plus
`"password": "DB_PASSWORD"` looks up `APP_DB_PASSWORD`.

Every listed field must exist in `configure` and may appear under only one
secret source. DataCoolie resolves the references on a runtime copy of the
connection; the authored metadata keeps the secret names. An unprefixed source
instead uses the platform's native provider: for example, a Key Vault URL on
[Fabric](../platforms/fabric.md#5-secrets), a secret scope on
[Databricks](../platforms/databricks.md#6-secrets), or AWS Secrets Manager on
[AWS Glue](../platforms/aws-glue.md#5-secrets). See the
[Secrets reference](../../reference/concepts/secrets.md#secrets_ref-schema) for
the full mapping and provider behavior.

## Addressing and workspace scope

Use `configure.base_path` for path-based endpoints. For lakehouse paths, see its lakehouse definition. For registered lakehouse
tables, DataCoolie builds the qualified name from the non-empty fields in this
order:
`connection.catalog`,
`connection.database`,
`source.schema_name` or
`destination.schema_name`, then
`source.table` or
`destination.table`. Put the
reusable namespace prefix on the connection and the remaining parts on the
dataflow endpoint. `database` is a DataCoolie field name; its meaning can be a
schema or a lakehouse depending on the platform. The lakehouse examples below
set the connection's **top-level** `catalog` and `database`.

| Platform namespace | Connection | Dataflow `source` or `destination` | Resulting table name |
|---|---|---|---|
| Three levels: `catalog.schema.table` (for example, Unity Catalog) | `catalog: main`, `database: sales` | `table: orders`; omit `schema_name` | `main.sales.orders` |
| Four levels: `workspace.lakehouse.schema.table` (schema-enabled Fabric Lakehouse) | `catalog: ops_workspace`, `database: sales_lakehouse` | `schema_name: retail`, `table: orders` | `ops_workspace.sales_lakehouse.retail.orders` |

For the three-level case, a reusable connection can be:

```json
{
  "name": "uc_sales",
  "connection_type": "lakehouse",
  "format": "delta",
  "catalog": "main",
  "database": "sales"
}
```

Use this source inside a dataflow (or the same addressing fields in its
destination):

```json
{
  "connection_name": "uc_sales",
  "table": "orders"
}
```

Do not set `schema_name`: it would introduce a fourth name part.

For the four-level Fabric Lakehouse case, the connection fixes the workspace
and lakehouse shared by its dataflows:

```json
{
  "name": "fabric_sales",
  "connection_type": "lakehouse",
  "format": "delta",
  "catalog": "ops_workspace",
  "database": "sales_lakehouse"
}
```

Use this source inside a dataflow (or the same addressing fields in its
destination):

```json
{
  "connection_name": "fabric_sales",
  "schema_name": "retail",
  "table": "orders"
}
```

Here `catalog` denotes the Fabric workspace name and `database` the lakehouse
name. The four-part name applies to a
[schema-enabled Fabric Lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas);
the namespace required by another Fabric connector may differ.

`workspace_id` is separate: it scopes metadata stored by a database or API
provider; it does not replace `catalog` when naming a Fabric workspace.

### Three-part names when one level is implicit

Some platforms can resolve a namespace level from the connection or runtime
context. In that case, a four-level hierarchy can be written with three name
parts. DataCoolie concatenates the populated fields in order; you can place
those three parts in `catalog.database.table` or
`database.schema_name.table`, depending on how broadly you want to reuse the
connection. Check which abbreviated name the underlying platform resolves;
the field split alone does not select the omitted level.

For example, if Fabric Spark resolves the workspace from its context, both
layouts below name `sales_lakehouse.retail.orders` without recording the
workspace name in metadata:

| Connection scope | Connection fields | Dataflow `source` or `destination` | Resulting table name |
|---|---|---|---|
| One connection per schema | `catalog: sales_lakehouse`, `database: retail` | `table: orders` | `sales_lakehouse.retail.orders` |
| One connection per lakehouse | `database: sales_lakehouse`; omit `catalog` | `schema_name: retail`, `table: orders` | `sales_lakehouse.retail.orders` |

The first layout scopes the connection to a schema, so dataflows supply only
the table. The second scopes it to a lakehouse, so each dataflow selects its
schema. In the first Fabric layout, `catalog` holds the lakehouse rather than
the workspace because the workspace is implicit. For Fabric, use the explicit
four-part name when the runtime cannot resolve the intended workspace from
context, such as when addressing another workspace. Omitting the schema instead
produces `workspace.lakehouse.table`, which resolves to the default `dbo` schema
in a schema-enabled lakehouse.

### SQL database sources

For `connection_type: database` and `format: sql`, usually leave
`catalog` unset. Set
`configure.database`
to the database selected by the connection; top-level
`database` is also accepted as a fallback when `configure.database`
is absent. The SQL reader sends `schema_name.table` when the source has
`schema_name`, or just
`table` when it does not. It does not prepend the
connection's `catalog` or `database` to that SQL table reference.

For example, with `configure.database: warehouse`, a source with
`schema_name`: `sales` and
`table`: `orders` reads `sales.orders` in `warehouse`.
If the SQL connector instead selects a schema directly as its database, use
`configure.database: sales` and a source with only `table`: `orders`; the reader
then requests `orders` in that selected namespace. In this setup the
connection's `database` effectively represents the schema. Use this form only
when the database and driver support selecting the schema that way.

## Connection versus metadata provider

The metadata provider controls where the configuration document is stored and
loaded: JSON/YAML/Excel file, database provider, or API provider. A connection
inside that document controls where pipeline data is read or written. Changing
the metadata provider does not change the `connections`, `source`, or
`destination` model.

If you want a runnable minimal document, start with [Build your first metadata
file](first-metadata-file.md). In the authoring workflow, continue with
[Dataflows](dataflows.md) to compose the read-transform-write unit.

For a prepared per-environment document, use the [environment overlay
workflow](../cli/project.md#environment-overlays) owned by the Project and CLI
guide. An overlay changes the prepared metadata snapshot; it is not a
connection property.
