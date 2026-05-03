---
title: Write an Engine Plugin — DataCoolie
description: Build a custom DataCoolie engine plugin that implements the BaseEngine contract for a new dataframe backend and format surface.
---

# Write an engine

**Prerequisites** · You have a DataFrame library you want to run DataCoolie pipelines on · you're ready to implement ~40 abstract methods.
**End state** · A new engine that passes the engine conformance tests and can be selected via `create_engine("mylib")`.

!!! warning "Large surface area"
    `BaseEngine` has ~40 abstract methods. Expect a multi-week effort. Start
    by copying `datacoolie.engines.polars_engine.PolarsEngine` as a template —
    it's the smaller of the two built-ins.

## Skeleton

```python
from datacoolie.engines.base import BaseEngine
import mylib


class MyLibEngine(BaseEngine[mylib.DataFrame]):
    def __init__(self, platform=None):
        super().__init__(platform)

    # --- Read ---
    def read_parquet(self, path, options=None): ...
    def read_delta(self, path, options=None):  ...
    def read_iceberg(self, path, options=None): ...
    # ... etc. for csv, json, jsonl, avro, excel

    def read_path(self, path, fmt, options=None):
        # Dispatch on fmt → the right abstract reader
        ...

    def read_database(self, *, table=None, query=None, options=None): ...
    def read_table(self, table_name, fmt="delta", options=None): ...
    def create_dataframe(self, records): ...
    def execute_sql(self, sql, parameters=None): ...

    # --- Write ---
    def write_to_path(self, df, path, mode, fmt, partition_columns=None, options=None): ...
    def write_to_table(self, df, table_name, mode, fmt, partition_columns=None, options=None): ...

    # --- Merge ---
    def merge_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None): ...
    def merge_overwrite_to_path(self, df, path, merge_keys, fmt="delta", partition_columns=None, options=None): ...
    def merge_to_table(self, df, table_name, merge_keys, fmt="delta", options=None): ...
    def merge_overwrite_to_table(self, df, table_name, merge_keys, fmt="delta", options=None): ...

    # --- Transform, system columns, metrics, maintenance, SCD2 ---
    # (see BaseEngine for the full list)
```

## `fmt` parameter contract

Every format-aware method **must** accept a `fmt` string. `read_table`,
`merge_to_table`, and `table_exists_by_name` have contract-specific
signatures:

```python
def read_table(self, table_name: str, fmt: str = "delta", options=None): ...
def merge_to_table(self, df, table_name, merge_keys, fmt: str = "delta", options=None): ...
def table_exists_by_name(self, table_name: str, *, fmt: str = "delta") -> bool: ...
```

`table_exists_by_name` uses **keyword-only** `fmt`.

See [ADR-0001](../adr/0001-engine-fmt-parameter.md).

## Register

```toml
[project.entry-points."datacoolie.engines"]
mylib = "mypkg.engine:MyLibEngine"
```

## Conformance

Run the framework's engine tests against your implementation. At minimum:

- All tests under `tests/unit/engines/` that don't bind to a specific DataFrame class.
- The full `tests/unit/transformers/` suite — transformers exercise many
  engine methods.
- The full `tests/unit/destinations/` suite.

For reference the Polars engine ships 73 dedicated tests; expect similar
coverage for a new engine.
