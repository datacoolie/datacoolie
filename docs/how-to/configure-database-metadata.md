---
title: Configure Database Metadata — DataCoolie How-to
description: Store DataCoolie metadata in a relational database and configure shared connections, dataflows, schema hints, and watermarks.
---

# Configure database metadata

**Prerequisites** · `pip install "datacoolie[db]"` · an RDBMS the team can share · DDL privileges to create the metadata schema.
**End state** · `DatabaseProvider` reading `dc_framework_connections`, `dc_framework_dataflows`, `dc_framework_watermarks`, and `dc_framework_schema_hints` tables.

## Supported dialects

Any dialect supported by SQLAlchemy 2.x. The usecase-sim testbed ships DDL
for SQLite, PostgreSQL, MySQL, MSSQL, and Oracle — see
[`usecase-sim/metadata/database/`](https://github.com/datacoolie/datacoolie/tree/main/datacoolie/usecase-sim/metadata/database).

## Schema

```mermaid
erDiagram
    dc_framework_connections ||--o{ dc_framework_dataflows : "source / destination connection"
    dc_framework_dataflows ||--o{ dc_framework_schema_hints : "many"
    dc_framework_dataflows ||--o| dc_framework_watermarks : "per dataflow"
```

All tables are workspace-scoped (`workspace_id` column) and honour soft-delete
(`deleted_at IS NULL`).

## Loading

```python
from datacoolie.metadata.database_provider import DatabaseProvider

provider = DatabaseProvider(
    connection_string="postgresql+psycopg2://user:pwd@host:5432/metadata",
    workspace_id="your-workspace-id",
)
```

## Seeding

Use `usecase-sim/scripts/setup_metadata.py --targets db:postgresql` as a
reference implementation. It:

1. Creates the schema via dialect-specific DDL.
2. Parses every canonical JSON metadata file.
3. `INSERT`s connections, dataflows, and children under a single transaction.

## Concurrency notes

- `DatabaseProvider` opens **one short-lived connection per operation**. It
  does not pin a session across the driver's parallel execution. Safe with
  `max_workers > 1`.
- Watermarks are written with `SELECT ... FOR UPDATE` on dialects that support
  it, otherwise an optimistic upsert.

## Related

- [Concepts · Metadata providers](../concepts/metadata-providers.md)
