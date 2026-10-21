---
title: Engines — Python API Reference | DataCoolie
description: Python API reference for DataCoolie engines and their format and maintenance contracts.
---

# Engines

::: datacoolie.engines.base
    options:
      members:
        - BaseEngine
        - DF

## Destination addressing hooks

`BaseEngine.preferred_target_addressing(fmt=..., table_name=..., path=...)`
returns `"table"`, `"path"`, or `None` when no handle is usable. Its concrete
default matches table-first dispatch. Polars selects paths for Delta/files and
names for Iceberg. Destination forced-path policy takes precedence.

`target_name_identity(fmt=..., table_name=...)` returns a qualified
`(backend_scope, canonical_native_name)` or `None`. The default is unqualified.
Subclasses need no new abstract method; plugins with different addressing must
override preference, and named maintenance requires a qualified identity hook.
Hooks may inspect attached loaded resources but must not create sessions,
load catalogs, or query storage. Direct engine method signatures stay unchanged.

## Maintenance preparation hooks

`maintenance_preparation()` returns an operation-scoped callable with
`fmt`, `table_name`, `path` and optional `table_address` keyword arguments. The
structured `MaintenanceTableAddress` keeps catalog separate from namespace/name;
REST adapters reject unsupported catalogs before lookup. The default wrapper
passes only the original `fmt`, `table_name` and `path` kwargs to legacy hooks.
It returns a
`PreparedMaintenanceTarget` carrying the selected identity, missing state,
capabilities and optional borrowed native binding. The concrete default delegates
to `prepare_maintenance_target()` and preserves existing local/plugin addressing.
REST adapters qualify deployment routing and load native UUID metadata here;
the destination resolver remains pure.

`execute_maintenance_action(prepared, action=..., retention_hours=168)` executes
an explicitly supported action, and `maintenance_history(prepared, ...)` observes
the same prepared incarnation. `release_maintenance_target(prepared)` is a
concrete no-op for borrowed resources. Plugins need no new abstract method.
The existing default writer and `_maintain_internal` signatures remain valid;
nonempty action maps require the concrete selected-maintenance extension.
Empty mappings and omitted mappings both retain default action intent.

Both built-in engines accept keyword-only `maintenance_deployment` for the
bounded REST profile. Spark additionally accepts `maintenance_guard_manifest`.
See [maintenance setup and supported selections](../../guide/operations/maintenance.md#rest-iceberg-maintenance).

## Column resolution helpers

`BaseEngine.resolve_column_name(actual_columns, column)` and
`resolve_column_names(actual_columns, columns)` are concrete public helpers.
Exact spelling wins, followed by the first `lower()` match in schema order;
missing names raise `EngineError`. The plural helper preserves order and
duplicates and uses the BaseEngine default singular implementation.

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
