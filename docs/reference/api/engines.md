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

## Neutral execution windows

`WindowSpec` is the engine-neutral replacement-window contract. Bounds are
combined with `OR` across columns and with `AND` within a column; the lower and
upper operators are explicit. Engine operations accept this value rather than
an untyped mapping.

::: datacoolie.engines.contracts.windows
    options:
      members:
        - WindowSpec
        - normalize_window

::: datacoolie.engines.spark_engine
    options:
      members:
        - SparkEngine

::: datacoolie.engines.polars_engine
    options:
      members:
        - PolarsEngine

## Datatype interpretation

::: datacoolie.engines.data_types
    options:
      members:
        - TypeSystem
        - LogicalKind
        - TimestampKind
        - ResolvedDataType
        - infer_type_system
        - normalize_type_system
        - resolve_schema_hint

The resolver is dependency-free and only describes source datatype semantics.
Native engines construct their own Spark or Polars datatype objects from the
resolved description; metadata models and readers do not import it.

### Datatype model migration

`ResolvedDataType` now derives signedness from `kind`. Its `unsigned` property
remains available for reads, but `unsigned=` and the unused `length=` constructor
arguments are no longer accepted. Resolve a source declaration with
`resolve_schema_hint(...)` or construct the model with its declared logical
fields instead of maintaining a second signedness flag.

### Polars SQL registration contract

`register_delta_tables` and `register_iceberg_tables` return sorted logical
names immediately, but with the default `preload=False` they only index source
descriptors. `execute_sql` resolves and binds referenced tables once per engine
lifetime. Inspect `registered_tables()` for indexed logical names and
`last_registration_report` for indexed, materialized, skipped, and failed
entries.

Qualified/indexed execution requires the `polars-sql` extra. Plain
`register_table` and direct Polars DataFrame/LazyFrame APIs do not.
