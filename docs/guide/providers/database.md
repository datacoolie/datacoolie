---
title: Configure Database Metadata — DataCoolie User Guide
description: Store DataCoolie metadata in a relational database and configure shared connections, dataflows, schema hints, and watermarks.
---

# Configure database metadata

**Prerequisites** · `pip install "datacoolie[polars,metadata-db]"` for the local
SQLite and Parquet example. For a shared metadata database, provide the
SQLAlchemy URL, its dialect driver, and the appropriate table permissions.
The PostgreSQL URL below needs `psycopg2-binary` separately.

**End state** · `DatabaseProvider` reads the four metadata tables for one
workspace and can persist that dataflow's watermarks.

## Try a local orders dataflow

From a DataCoolie repository checkout, run the maintained
[SQLite example](../../examples/files/configuration/provider_fixtures.py). It
creates two metadata connections and one dataflow in an in-memory database,
then reads and writes three local Parquet rows through a Driver:

| Metadata row | Value in the example |
|---|---|
| Source connection | `file`/`parquet`, local input path |
| Destination connection | `file`/`parquet`, local output path |
| Dataflow | `orders`, stage `bronze2silver`, overwrite load |

```powershell
python docs/examples/files/configuration/provider_fixtures.py --provider sqlite --run-dataflow --work-dir ../.scratch/provider-sqlite-orders
```

Choose a `--work-dir` path that does not exist yet; the example will create it
and keep input, output, and state there. Expect
`provider=sqlite executed=1 connections=2 dataflows=1` and an output file at
`<work-dir>/sqlite/output/orders/orders.parquet`. This demonstrates where
*metadata* lives; the orders themselves still live in local Parquet files.
The [configuration example](../../examples/configuration.md#provider-startup)
shows the provider-to-Driver handoff independently of the fixture.

## Supported dialects

`DatabaseProvider` accepts a SQLAlchemy 2.x Engine or connection URL. Qualify
the chosen dialect and driver against your database schema and workload. The
repository also carries scenario DDL for SQLite, PostgreSQL, MySQL, MSSQL, and
Oracle; see [scenario validation artifacts](#scenario-validation-artifacts).

## Schema

```mermaid
erDiagram
    dc_framework_connections ||--o{ dc_framework_dataflows : "source / destination connection"
    dc_framework_connections ||--o{ dc_framework_schema_hints : "connection_id lookup"
    dc_framework_dataflows o|--o{ dc_framework_schema_hints : "optional dataflow_id lookup"
    dc_framework_dataflows ||--o| dc_framework_watermarks : "unique dataflow_id lookup"
```

The diagram shows logical ownership and lookup paths used by the provider. The
current SQLAlchemy declarations and dialect DDL do not add foreign-key
constraints, so the relationships are not database-enforced. Connections and
dataflows carry `workspace_id` and `deleted_at`; schema hints carry
`connection_id`, optional `dataflow_id`, and `deleted_at`; watermarks are
scoped through their unique `dataflow_id` owner and do not carry workspace or
soft-delete columns. Provider queries still reject records whose owning
connection or dataflow is outside the selected workspace or soft-deleted.

The source predicate column `source_filter_expression` is nullable on the
dataflow table. Fresh DDL includes it; existing installations need the reviewed
additive migration described below.

## Loading

```python
from datacoolie.metadata.database_provider import DatabaseProvider

provider = DatabaseProvider(
    connection_string="postgresql+psycopg2://user:pwd@host:5432/metadata",
    workspace_id="your-workspace-id",
    sql_base_path=["./sql_shared", "./sql_project"],
)
```

The `postgresql+psycopg2` URL requires a PostgreSQL DBAPI package in addition
to SQLAlchemy:

```bash
pip install "datacoolie[metadata-db]" psycopg2-binary
```

Construction only records configuration. Before execution, initialize and
inspect the complete workspace:

```python
provider.initialize()
connections = provider.get_connections(active_only=False)
dataflows = provider.get_dataflows(
    stage="bronze2silver",
    active_only=False,
    attach_schema_hints=False,
)
print(f"connections={len(connections)} dataflows={len(dataflows)}")
```

Then pass the provider to a Driver using the [shared provider handoff](../../examples/configuration.md#provider-startup).
An injected provider and an injected SQLAlchemy engine remain caller-owned;
close the provider after the Driver has finished. A provider-created engine is
disposed during provider cleanup.

The SQL roots belong to `DatabaseProvider` because its metadata may refer to
SQL files, but the provider does not read those files. Driver preparation uses
the execution platform to resolve `source.query`. A Driver-level
`sql_base_path` can supply the session fallback when the provider does not
declare one; conflicting explicit roots fail during startup.

### Schema upgrade for source predicates

Fresh installs include the nullable `source_filter_expression` column on
`dc_framework_dataflows`. Before deploying a runtime/API service that reads an
existing database, compare its schema with the current declaration and review
the matching additive migration sample under
[`usecase-sim/metadata/database/migrations/`](https://github.com/datacoolie/datacoolie/tree/main/usecase-sim/metadata/database/migrations).
Apply the reviewed change through your database migration process. The
repository samples include preflight and postflight queries. `create_tables()`
and the scenario seeder do not alter an existing table, and application startup
does not run migrations.

Existing rows remain `NULL` until an operator performs a reviewed data
correction. Do not reset or reseed a production metadata database to recover a
predicate.

## Populate your metadata database

`DatabaseProvider.create_tables()` creates missing tables for local setup; it
does not insert connections or dataflows. The small SQLite example above shows
the minimum rows and matching `workspace_id` needed for one executable flow.
For a shared database, create and update those rows through your metadata
management process, then initialize the provider for that workspace. Keep
schema changes in a reviewed migration; provider startup does not apply them.

### Scenario validation artifacts

The repository's
[`usecase-sim/scripts/setup_metadata.py`](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/scripts/setup_metadata.py)
fans out a large scenario document to several database dialects and file
formats. Its [dialect DDL](https://github.com/datacoolie/datacoolie/tree/main/usecase-sim/metadata/database)
and [migration samples](https://github.com/datacoolie/datacoolie/tree/main/usecase-sim/metadata/database/migrations)
are useful when validating those scenarios. They are repository testbed assets;
review them for your database before adapting them to an application rollout.

## Concurrency notes

- `DatabaseProvider` opens **one short-lived connection per operation**. It
  does not pin a session across the driver's parallel execution. Safe with
  `max_workers > 1`.
- Watermarks use an update-first upsert: one `UPDATE` rotates
  `previous_value`, a missing row is inserted, and a concurrent insert race is
  retried with `UPDATE`. Transient deadlock/serialization failures are retried.
- With the default cache enabled, call `provider.clear_cache()` after a known
  metadata change; the next read reloads the provider snapshot. Watermark
  writes do not invalidate the metadata cache automatically.

## Common failures

| Symptom | Meaning | Fix |
|---|---|---|
| `NoSuchModuleError` while creating the provider | The SQLAlchemy dialect driver is missing | Install the DBAPI package required by the URL, such as `psycopg2-binary` for `postgresql+psycopg2`. |
| Existing rows do not contain `source_filter_expression` | The database predates the additive column | Run the matching reviewed migration; application startup does not alter an existing table. |
| A connection/dataflow is absent from a workspace read | The row is in another workspace or is soft-deleted | Check `workspace_id`, `deleted_at`, and the provider's configured workspace. |
| Watermark update races or transient serialization errors | Multiple workers updated one dataflow concurrently | Keep the provider's bounded retry behavior and inspect the database transaction logs. |

## Related

- [Metadata guide for new users](../metadata/index.md) — understand the metadata shape (connections, dataflows, sources, destinations) before configuring a backend
- [Concepts · Metadata providers · Database provider](../../reference/concepts/metadata-providers.md#database-provider)
