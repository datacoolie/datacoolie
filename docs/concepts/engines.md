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
class PolarsEngine(BaseEngine[polars.DataFrame]):      ...
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

## Driver connection keys

`BaseEngine.DRIVER_CONNECTION_KEYS` is a frozenset of JDBC-specific option keys
(`encrypt`, `trustServerCertificate`, `hostNameInCertificate`) that must not
leak into higher-level connection APIs (connectorx, SQLAlchemy). Spark folds
them into the JDBC URL; Polars strips them before handing options to
connectorx. Extend the set when adding new driver-specific keys.

## Platform attachment

An engine is useless without a platform. There are three valid states:

1. Construct engine with platform: `SparkEngine(spark, platform=p)`.
2. Attach later: `engine.set_platform(p)` before the driver runs.
3. Let the driver attach: pass `platform=` to `DataCoolieDriver(...)`.

For Spark, the explicit form `SparkEngine(spark_session=spark, platform=p)` is
the current constructor name; the positional call works too, but the keyword is
clearer in docs.

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
