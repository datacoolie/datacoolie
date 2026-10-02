---
title: Write a Destination Plugin — DataCoolie
description: Build a custom DataCoolie destination plugin that writes engine dataframes and supports maintenance, metrics, and table formats.
---

# Write a destination

**Prerequisites** · You want to write to a backend not covered by the built-ins · you understand the load-type contract.
**End state** · Destination writer registered under a runtime format name and
exercised through the registry. Metadata consumption also requires a
connection shape and `format` value accepted by the authored metadata schema
and the active runtime. In the 0.2.0 schema, `format` is an enum of built-in
formats; omitting `connection_type` does not make a custom format valid for
`dc validate`.

## Minimal writer

```python
from typing import Any, Dict, List, Optional

from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.constants import LoadType
from datacoolie.engines.contracts.windows import WindowSpec


class MyDestinationWriter(BaseDestinationWriter):
    def _write_internal(
        self,
        df,
        dataflow: DataFlow,
        *,
        watermark_window: Optional[WindowSpec] = None,
    ) -> None:
        # This example does not implement bounded replacement. Reject the
        # window before any backend call or destination mutation.
        if watermark_window is not None:
            raise DestinationError(
                "MyDestinationWriter does not support bounded replacement writes"
            )

        dest = dataflow.destination
        path = dest.path
        if not path:
            raise DestinationError("MyDestinationWriter requires a destination path")

        mode = dataflow.load_type
        if mode in (LoadType.APPEND.value, LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value):
            self._engine.write_to_path(
                df, path,
                mode="overwrite" if mode != LoadType.APPEND.value else "append",
                fmt="myfmt",
                partition_columns=dest.partition_column_names,
                options=dest.write_options or None,
            )
        elif mode == LoadType.MERGE_UPSERT.value:
            self._engine.merge_to_path(df, path, merge_keys=dest.merge_keys, fmt="myfmt")
        else:
            raise NotImplementedError(f"LoadType {mode!r} not supported by MyDestinationWriter")

    def _maintain_internal(
        self,
        dataflow: DataFlow,
        *,
        do_compact: bool,
        do_cleanup: bool,
        retention_hours: int,
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        raise DestinationError("Maintenance is not supported for myfmt")
```

The snippet is an illustrative backend scaffold. Registering a destination
writer does not add `myfmt` to an engine: its `write_to_path(..., fmt="myfmt")`
and `merge_to_path(..., fmt="myfmt")` calls require a selected engine that
implements those format branches. The built-in `PolarsEngine` rejects
`myfmt` as unsupported; do not generalise that result to every engine. Supply
an engine plugin with `myfmt` support, or route the writer to a format the
selected engine already implements, before running the write path.

The framework calls the public
`write(df, dataflow, *, watermark_window=None)` method; subclasses implement
`_write_internal` and `_maintain_internal`. The base class validates the window
and wraps both paths with timing, error handling, and
`DestinationRuntimeInfo` population. This example rejects a non-`None` window
before any destination mutation because it does not implement bounded
replacement. A writer that advertises bounded replacement (`merge_overwrite`
with `replace_by_watermark`) must pass the window to its engine's replacement
operation and test both non-empty and empty input. Key-based `merge_overwrite`
is a separate path. Raise `DestinationError` from `_maintain_internal` when the format
has no maintenance operations.

See the [BaseDestinationWriter API reference](../reference/api/destinations.md)
for the complete public and protected contracts.

## Register

```toml
[project.entry-points."datacoolie.destinations"]
myfmt = "mypkg.writers:MyDestinationWriter"
```

This adds a runtime registry alias; it does not extend the metadata JSON Schema
or prove that the selected engine supports the backend format. In DataCoolie
0.2.0, `dc validate` rejects custom values such as `format: "myfmt"` because
the schema enumerates built-in formats. Omitting `connection_type` is not a
CLI workaround: it only leaves the runtime model's implicit default in direct
programmatic construction. Verify the metadata schema, connection shape, and
backend capability before publishing a new format name. Use a new alias for a
packaged plugin; an entry point that collides with a built-in name does not
replace the built-in registration.

For a runtime-only smoke check, register the class explicitly and construct it
with the engine keyword that the driver passes. This exercises registry
activation without claiming that a `myfmt` metadata document passes authored
schema validation:

```python
from datacoolie import create_destination, create_engine, destination_registry
from mypkg.writers import MyDestinationWriter

engine = create_engine("polars")
destination_registry.register("myfmt", MyDestinationWriter)
writer = create_destination("myfmt", engine=engine)
```

The construction check does not exercise a write; use an engine that actually
supports `myfmt` for that integration path.

## Expectations

- **Idempotent writes** — the framework may retry your `_write_internal()` call.
- **Respect `dest.partition_columns`** when the format supports partitioning.
- **Use `engine.merge_to_path` / `merge_to_table`** for merge strategies rather
  than hand-rolling `DELETE + INSERT`.
- **Don't mutate `df`** — transformers already finalised the DataFrame.
- **Reject unsupported `watermark_window` values before backend mutation** —
  bounded replacement is a separate capability from ordinary append/overwrite.

## Test matrix

At minimum:

- Append + overwrite on an empty target.
- Append + overwrite on a non-empty target.
- Every load type your writer advertises.
- Partitioned and non-partitioned writes.
- Unsupported maintenance fails with `DestinationError` without claiming
  compaction or cleanup support.
- A non-`None` `watermark_window` fails clearly before the backend is called when
  bounded replacement is unsupported; otherwise test the replacement window,
  including an empty input frame.
