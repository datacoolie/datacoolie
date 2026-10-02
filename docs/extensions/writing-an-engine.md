---
title: Write an Engine Plugin — DataCoolie
description: Build a custom DataCoolie engine plugin that implements the BaseEngine contract for a new dataframe backend and format surface.
---

# Write an engine

**Prerequisites** · You have a DataFrame library you want to run DataCoolie pipelines on · you're ready to implement the full engine contract.
**End state** · A new engine that implements the `BaseEngine` contract, passes
its plugin-owned native tests and integration checks, and can be selected via
`create_engine("mylib")`.

!!! warning "Large surface area"
    Expect a substantial implementation and conformance effort. Start by
    studying `datacoolie.engines.polars_engine.PolarsEngine` as a behavioral
    template, then use the [BaseEngine API reference](../reference/api/engines.md)
    as the authoritative abstract contract.

## Skeleton

```python
from datacoolie.engines.base import BaseEngine
from datacoolie.engines.contracts.windows import WindowSpec
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
    def merge_overwrite_to_path(
        self, df, path, merge_keys, fmt="delta", partition_columns=None,
        options=None, write_options=None,
    ): ...
    def merge_to_table(self, df, table_name, merge_keys, fmt, partition_columns=None, options=None): ...
    def merge_overwrite_to_table(
        self, df, table_name, merge_keys, fmt="delta", partition_columns=None,
        options=None, write_options=None,
    ): ...

    # --- Transform, system columns, metrics, maintenance, SCD2 ---
    # (see BaseEngine for the full list)
```

The `cast_column` implementation receives the authored source declaration and
its optional dialect context. It must resolve the declaration using the pure
`datacoolie.engines.data_types` contract, then construct the plugin's native
datatype or expression directly:

```python
def cast_column(
    self, df, column_name, target_type, fmt=None, *,
    type_system=None, precision=None, scale=None,
): ...
```

Do not require `SchemaConverter` to pre-serialize a target string, and do not
apply schema hints while reading or calculating watermarks. A plugin may use
different native objects from Spark/Polars, but supported logical ranges,
null/error behavior, and temporal semantics must remain equivalent.

The system-column contract includes the optional driver execution ID:

```python
def add_system_columns(self, df, author=None, dataflow_run_id=None):
    # Add the standard timestamps/author. When dataflow_run_id is provided,
    # add it as the string column __dataflow_run_id.
    ...
```

Do not generate a replacement ID inside the engine: it must remain equal to
the `DataFlowRuntimeInfo.dataflow_run_id` supplied by the driver.

## `fmt` parameter contract

Every format-aware method **must** accept a `fmt` string. `read_table`,
`merge_to_table`, and `table_exists_by_name` have contract-specific
signatures:

```python
def read_table(self, table_name: str, fmt: str = "delta", options=None): ...
def merge_to_table(
    self, df, table_name, merge_keys, fmt: str,
    partition_columns=None, options=None,
): ...
def table_exists_by_name(self, table_name: str, *, fmt: str = "delta") -> bool: ...
```

`table_exists_by_name` uses **keyword-only** `fmt`.

`merge_overwrite_to_path` and `merge_overwrite_to_table` receive
`write_options` separately from `options`; preserve and forward both option
maps to the native writer. Dropping `write_options` changes merge-overwrite
behavior even when the merge keys are correct.

See [ADR-0001](../project/decisions/0001-engine-fmt-parameter.md).

See the [BaseEngine API reference](../reference/api/engines.md) for the full
abstract contract and navigation helpers.

## Register

```toml
[project.entry-points."datacoolie.engines"]
mylib = "mypkg.engine:MyLibEngine"
```

Engine `fmt` support is a backend capability, while an entry point only adds a
runtime registry name. These contracts are separate from authored metadata:
the 0.2.0 schema enumerates built-in connection formats, and omitting
`connection_type` is not a `dc validate` workaround for a custom format. Use a
new entry-point alias; a packaged alias that collides with a built-in name does
not replace the built-in registration.

## Conformance

The framework's generic tests are contract references, not a substitute for a
plugin-owned qualification suite. A new engine should maintain native unit
tests for every abstract method it implements, including format dispatch,
`write_options` forwarding, typed casts, null/error behavior, watermark
filtering, metrics, and system columns. Add integration tests against the
actual dataframe backend that register the engine, read and write at least one
supported path/table format, exercise one supported merge or replacement
operation, and verify the resulting rows and schema. Add explicit negative
tests for unsupported formats and capabilities that prove they fail before
mutating a target. Use the built-in Polars and Spark implementations as
behavioral references and run only the framework tests applicable to the
contract surface your plugin claims.

## `replace_window` — Engine-owned bounded replacement

Engines may override the concrete `replace_window` operation to provide a
native merge/transaction or format-specific materialization. The base
implementation validates the input window, deletes rows in the bounded scope,
and appends the non-empty input. It intentionally does not claim atomicity.

```python
def replace_window(
    self, df, *, table_name=None, path=None, window, fmt="delta",
    partition_columns=None, options=None,
):
    ...
```

The neutral `WindowSpec` carries bound values, lower/upper operators, and OR
composition across columns. Engines render identifiers and literals for their
own backend. Validate the final transformed watermark columns before deleting;
do not resolve schema hints or infer arbitrary custom-transform mappings here.

## `delete_by_window` — Range-based delete primitive

Engines must implement `delete_by_window_path` and `delete_by_window_table`
to support the `replace_by_watermark` destination feature:

```python
@abstractmethod
def delete_by_window_path(
    self,
    path: str,
    window: WindowSpec,
    fmt: str = "delta",
) -> None:
    """Delete rows in a path-based table within the value window."""

@abstractmethod
def delete_by_window_table(
    self,
    table_name: str,
    window: WindowSpec,
    fmt: str = "delta",
) -> None:
    """Delete rows in a named table within the value window."""
```

`WindowSpec.bounds` maps column names to `(lower_bound, upper_bound)` tuples.
Build a predicate like `col > lower AND col <= upper` for each entry and delete
all matching rows.  The lower/upper operators come from the `WindowSpec` and
must be preserved; the default lower bound is **exclusive** to match the
source watermark filter semantics. Predicates are OR-combined across columns
and AND-combined within one column.

The engine must materialize or checkpoint a lazy input before deletion when a
later append would otherwise re-evaluate it against the changed target. Release
only resources created by that operation, and do not hide the original write
error if cleanup also fails.

The base replacement method may use these primitives, and a backend can route
to them internally. New destination strategies should call `replace_window`,
not sequence public delete and append operations themselves.
