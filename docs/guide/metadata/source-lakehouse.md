---
title: Lakehouse sources | DataCoolie
description: Configure lakehouse sources with metadata, examples and backend-specific guidance.
---

# Lakehouse sources

[All source recipes](source-patterns.md).

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
the same [query-file resolution rules](source-database.md#read-a-sql-file) described below.
With Polars, the SQL relations must be registered in the engine before the
query runs (for example, Delta or Iceberg table discovery in the runner);
metadata alone does not register them. Install the SQL resolver dependency and
follow [Qualified SQL relations in Polars](../../reference/concepts/engines.md#qualified-sql-relations-in-polars).
