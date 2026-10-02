---
title: Metadata providers | DataCoolie
description: Choose and configure the file, database, or API provider that stores DataCoolie metadata.
---

# Metadata providers

A metadata provider stores the authored `connections` and `dataflows` that a
runner loads at startup. It is separate from a metadata `connection`, which is
the endpoint a dataflow reads from or writes to.

Choose the provider by how the metadata is governed:

| Provider | Best fit | Guide |
|---|---|---|
| File | Local development, small teams, and version-controlled JSON/YAML/Excel | [File metadata](file.md) |
| Database | Shared configuration, workspace isolation, and database-backed governance | [Database metadata](database.md) |
| API | Service-owned configuration, approvals, or remote workspace access | [API metadata](api.md) |

Start with [File metadata](file.md) unless the project already has a shared
database or API service. All three providers supply the same runtime metadata
models. Their stored representations differ: files contain authored documents,
database tables hold rows, and an API returns JSON envelopes with expanded
connections.

For a first runnable file example, use the
[local CSV-to-Parquet transform recipe](../../examples/dataflows.md#transform-project-recipe).
The SQLite and loopback HTTP examples on the Database and API pages reuse a
small local orders dataflow so the metadata backend is the main difference.

## Common route

1. Choose the backend and install its client extra and any dialect/server
   dependency described on the provider page.
2. Configure the provider, call `initialize()`, and inspect the complete
   workspace scope with `active_only=False` before running a stage.
3. Hand the initialized provider to a Driver using the
   [shared startup example](../../examples/configuration.md#provider-startup).
   Keep ownership clear: an injected provider is closed by the caller after the
   Driver finishes.
4. Use the provider page's cache, watermark, and troubleshooting notes when
   metadata changes or startup fails.

The maintained
[`provider_fixtures.py`](../../examples/files/configuration/provider_fixtures.py)
is a local contract fixture for SQLite and loopback HTTP. It can also execute
a small Parquet dataflow; it is not a general database seeder or a metadata
service template.

## SQL file roots

Every metadata provider accepts an optional `sql_base_path` value. It may be
one root or a list of roots, and belongs to the provider configuration because
the provider supplies the metadata that refers to those SQL files. The Driver
passes its execution platform to preparation, where the SQL file is read; the
provider does not need to perform SQL file I/O or depend on that platform.

```python
from datacoolie.metadata.database_provider import DatabaseProvider

provider = DatabaseProvider(
    connection_string="postgresql+psycopg2://user:pwd@host/metadata",
    workspace_id="your-workspace-id",
    sql_base_path=["./sql_shared", "./sql_project"],
)
# Initialize this provider, then pass it to a Driver together with the
# runner-owned engine and platform; see the shared startup example above.
```

If the provider has no SQL roots, the Driver-level `sql_base_path` is used as
the session fallback. Supplying both values is valid when their normalized
roots are equivalent; different explicit roots fail during startup. See
[runtime path ownership](../operations/runtime-configuration.md#query-preparation)
for root selection, artifact fallback, and multiple-root prefixes.

For the fields inside the document, continue to the [Metadata guide](../metadata/index.md).
For runtime precedence and provider injection, see [Runtime configuration](../operations/runtime-configuration.md).
