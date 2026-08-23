---
title: Engines — Python API Reference | DataCoolie
description: Python API reference for the DataCoolie engines package — BaseEngine, PolarsEngine, SparkEngine, and the fmt parameter contract.
---

# Engines

::: datacoolie.engines.base
    options:
      members:
        - BaseEngine
        - DF

::: datacoolie.engines.spark_engine
    options:
      members:
        - SparkEngine

::: datacoolie.engines.polars_engine
    options:
      members:
        - PolarsEngine

### Polars SQL registration contract

`register_delta_tables` and `register_iceberg_tables` return sorted logical
names immediately, but with the default `preload=False` they only index source
descriptors. `execute_sql` resolves and binds referenced tables once per engine
lifetime. Inspect `registered_tables()` for indexed logical names and
`last_registration_report` for indexed, materialized, skipped, and failed
entries.

Qualified/indexed execution requires the `polars-sql` extra. Plain
`register_table` and direct Polars DataFrame/LazyFrame APIs do not.
