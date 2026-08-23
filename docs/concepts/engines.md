---
title: Engine Abstractions — DataCoolie Concepts
description: Learn the BaseEngine contract, format-aware read and write semantics, and how Polars and Spark share one ETL surface in DataCoolie.
---

# Engines

**TL;DR** `BaseEngine[DF]` is a generic ABC that abstracts read / write /
merge / transform / maintenance across DataFrame libraries. All format-aware
methods take a `fmt=` parameter so Delta and Iceberg share the same surface.

## The `DF` type parameter

```python
from datacoolie.engines.base import BaseEngine

class SparkEngine(BaseEngine[pyspark.sql.DataFrame]): ...
class PolarsEngine(BaseEngine[polars.LazyFrame]):     ...
```

Sources, destinations, and transformers are parameterised by the same `DF`, so
the type system prevents you from passing a Polars DataFrame to a Spark
destination.

## Method sections

`BaseEngine` organises its API into sections that match the abstraction in
`src/datacoolie/engines/base.py`:

| Section | Methods |
|---|---|
| Construction | `__init__`, `platform`, `set_platform` |
| Read | `read_parquet`, `read_delta`, `read_iceberg`, `read_csv`, `read_json`, `read_jsonl`, `read_avro`, `read_excel`, `read_path`, `read_database`, `read_table`, `create_dataframe`, `execute_sql` |
| Write | `write_to_path`, `write_to_table` |
| Merge | `merge_to_path`, `merge_overwrite_to_path`, `scd2_to_path`, `merge_to_table`, `merge_overwrite_to_table`, `scd2_to_table`|
| Transform | `add_column`, `drop_columns`, `select_columns`, `rename_column`, `filter_rows`, `apply_watermark_filter`, `deduplicate`, `deduplicate_by_rank`, `cast_column` |
| System columns | `add_system_columns`, `add_file_info_columns`, `remove_system_columns`, `convert_timestamp_ntz_to_timestamp` |
| Metrics | `count_rows`, `is_empty`, `get_columns`, `get_schema`, `get_max_values`, `get_count_and_max_values` |
| Maintenance | `table_exists_by_path`, `table_exists_by_name`, `get_history_*`, `compact_*`, `cleanup_*` |
| Navigation (concrete dispatch) | `read`, `write`, `merge`, `merge_overwrite`, `scd2`, `exists`, `get_history`, `compact`, `cleanup` |

The **navigation** group (`read`, `write`, ...) is concrete on `BaseEngine` — it
dispatches on connection type and format to the right abstract method. You
rarely override it.

Engine implementations accept an optional execution ID when adding system
columns:

```python
def add_system_columns(
    self,
    df,
    author=None,
    dataflow_run_id=None,
): ...
```

When supplied, it must be added as the string column `__dataflow_run_id`.

## The `fmt` contract

Format-aware methods take a `fmt` string (`"delta"`, `"iceberg"`, `"parquet"`,
`"csv"`, ...). This single parameter unifies lakehouse formats across engines:

```python
engine.read_table("`cat`.`db`.`sales`.`orders`", fmt="iceberg")
engine.merge_to_table(df, table, keys=["id"], fmt="delta", options={"overwriteSchema": "true"})
engine.table_exists_by_name(table, fmt="iceberg")
```

Rules:

- `fmt` defaults to `"delta"` where it would otherwise be required — legacy code
  that predates Iceberg keeps working.
- `merge_to_table` / `read_table` / `table_exists_by_name` all accept `fmt`.
- `table_exists_by_name` uses **keyword-only** `fmt` (`*, fmt="delta"`).
- Engines raise `EngineError` for unsupported `fmt` values rather than silently
  falling back.

See [ADR-0001](../adr/0001-engine-fmt-parameter.md) for history.

## Qualified SQL relations in Polars

`PolarsEngine` can discover Delta tables from a path or Iceberg tables from a
catalog namespace, then expose the same logical SQL naming behavior for both.
Install the SQL resolver separately when you need this feature:

```bash
pip install "datacoolie[polars-sql,polars-delta,polars-iceberg]"
```

Registration is lazy by default. `register_delta_tables` and
`register_iceberg_tables` enumerate and index table descriptors, but do not
create scans or bind frames to `SQLContext`. The first query that references a
table creates its `LazyFrame` and registers one private alias. Later queries on
the same engine reuse that registration; data is still read lazily when the
query result is collected.

```python
engine.register_delta_tables(
    "s3://lake/database_B",
    logical_prefix=("catalog_A", "database_B"),
    recursive=True,
    include="database_B.**.d_*",
    exclude=("**.tmp.**", "**.*_backup"),
)

result = engine.execute_sql("""
    SELECT *
    FROM database_B.sales.d_orders
""")
```

One canonical name contains at most four components. A query may omit only
leading components, so all unique suffixes are valid:

| Indexed name | Valid references when unique |
|---|---|
| `catalog.database.schema.table` | 4, 3, 2, or 1 trailing components |
| `database.schema.table` | 3, 2, or 1 trailing components |
| `schema.table` | 2 or 1 trailing components |
| `table` | 1 component |

If a suffix matches multiple tables, execution raises an ambiguity error and
lists the candidates. Qualify the reference further; the engine never chooses
one table implicitly.

### Delta and Iceberg roots

For Delta, `base_path` is the physical discovery root and each table's relative
folders are appended to `logical_prefix`. Recursive discovery stops at a
directory containing `_delta_log`.

For Iceberg catalog mode, `namespace` narrows catalog enumeration. By default,
the catalog name and root namespace form the logical prefix; supplying
`logical_prefix` replaces that root mapping. Use `logical_prefix=()` when only
the identifier below the selected namespace should appear in the canonical
name.

```python
engine.register_iceberg_tables(
    namespace=("database_B",),
    logical_prefix=("catalog_A", "database_B"),
    recursive=True,
)
```

Choose the narrowest physical root first for performance. Use patterns for
logical selection:

| Intent | Pattern |
|---|---|
| Everything below catalog A / database B | `catalog_A.database_B.**` |
| Database B under any catalog | `database_B.**` |
| `d_` tables below database B, with or without schema levels | `database_B.**.d_*` |
| `d_` tables anywhere | `d_*` |

`*` stays within one name component; `**` crosses zero or more components.
Exclude patterns win over include patterns.

Use `preload=True` only when callers must execute directly through
`engine.sql_context`. Set `on_error="skip"` for observable best-effort
discovery and inspect `engine.last_registration_report`; the default is
fail-fast. Use structured `logical_prefix` for root mapping; there is no flat
prefix or physical-separator configuration.

See [ADR-0005](../adr/0005-polars-qualified-sql-relations.md) for the decision.

## Driver connection keys

`BaseEngine.DRIVER_CONNECTION_KEYS` is a frozenset of JDBC-specific option keys
(`encrypt`, `trustServerCertificate`, `hostNameInCertificate`) that must not
leak into higher-level connection APIs (connectorx, SQLAlchemy). Spark folds
them into the JDBC URL; Polars strips them before handing options to
connectorx. Extend the set when adding new driver-specific keys.

## Platform attachment

An engine is useless without a platform. There are three valid states:

1. Construct engine with platform:
   `SparkEngine(spark_session=spark, platform=p)`.
2. Attach later: `engine.set_platform(p)` before the driver runs.
3. Let the driver attach: pass `platform=` to `DataCoolieDriver(...)`.

The Spark constructor parameter is named `spark_session`; a positional call
also works.

The driver guards against mismatched platform types to prevent silent
"works on my laptop, fails in Fabric" bugs.

## Case-insensitive column resolution

`BaseEngine._resolve_column_name` and `_resolve_column_names` do a
case-insensitive lookup against an actual DataFrame schema. Most transformers
use these helpers so users can write `amount` in metadata even when Spark
inferred `AMOUNT`.

## Related

- [Sources & destinations](sources-and-destinations.md)
- [Writing an engine](../extending/writing-an-engine.md)
- [`reference/api/engines`](../reference/api/engines.md)
- Blog: [Polars vs Spark for ETL](../blog/posts/2026-05-26-polars-vs-spark-for-etl.md)
