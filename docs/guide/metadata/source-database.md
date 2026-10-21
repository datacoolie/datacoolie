---
title: Database and SQL sources | DataCoolie
description: Configure database and sql sources with metadata, examples and backend-specific guidance.
---

# Database and SQL sources

[All source recipes](source-patterns.md).

## Database source

<span id="database-source-sql-via-sqlalchemy"></span>

Use the database source for PostgreSQL, MySQL, MSSQL, Oracle or SQLite.
The selected engine determines the transport; a SQLAlchemy dialect alone
does not establish database-source support. Database metadata providers use
SQLAlchemy separately from these data reads.

| Engine / database | Default transport | Python profile / prerequisite | URL shape |
|---|---|---|---|
| Spark | JDBC | `spark` plus the matching Java JDBC driver on the Spark classpath | `jdbc:postgresql://host:5432/db` (PostgreSQL example) |
| Polars / PostgreSQL | ConnectorX | `source-db-polars` | `postgresql://user:password@host:5432/db` |
| Polars / MySQL or password-authenticated MSSQL | Native DB-API (`pymysql` / `pymssql`) | `source-db-native-polars` | `mysql://user:password@host/db` or `mssql://user:password@host/db` |
| Polars / Oracle | `oracledb` | `source-db-oracle-polars` | `oracle://user:password@host:1521/service` |
| Polars / SQLite | Python `sqlite3` | `polars`; no separate database extra | `sqlite:///path/to/db.sqlite` |
| Polars / MSSQL with service principal, managed identity or access token | SQLAlchemy + `pyodbc` | `source-db-mssql-odbc-polars` and system ODBC Driver 18 for SQL Server | Configure the host/database and auth fields below |

For PostgreSQL, `database_read_engine="native"` selects `psycopg2` instead;
install that driver separately. A URL-only Polars connection must also set
`database_type` when relying on database-specific native/default dispatch.
Combine source profiles with the chosen destination profile. See the
[dependency reference](../../reference/dependencies.md) for exact requirements.

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
    "database_type": "postgresql",
    "url": "DC_POSTGRES_URL"
  },
  "secrets_ref": {
    "env:": ["url"]
  }
}
```

For the Polars default, set `DC_POSTGRES_URL` to a PostgreSQL URI using the
`postgresql://` scheme. For Spark, use `jdbc:postgresql://host:5432/mydb` and
resolve `username` and `password` separately. DataCoolie replaces
`configure.url` at runtime; choose the URI syntax for your selected transport.

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
    18 instead of ConnectorX. Other databases use token-as-password via
    the selected native/ConnectorX transport.

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
as in the [database URL example](source-database.md#database-source-sql-via-sqlalchemy); do not
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
