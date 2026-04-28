# Write a destination

**Prerequisites** · You want to write to a backend not covered by the built-ins · you understand the load-type contract.
**End state** · Destination writer registered under a new format name, selectable via `format: "myfmt"` on a destination connection.

## Minimal writer

```python
from typing import Optional

from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.core.models import DataFlow, DestinationRuntimeInfo
from datacoolie.core.constants import LoadType


class MyDestinationWriter(BaseDestinationWriter):
    def _write_internal(self, df, dataflow: DataFlow) -> None:
        dest = dataflow.destination
        conn = dataflow.destination.connection
        path = f"{conn.base_path}/{dest.schema_name}/{dest.table}"

        mode = dest.load_type
        if mode in (LoadType.APPEND.value, LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value):
            self._engine.write_to_path(
                df, path,
                mode="overwrite" if mode != LoadType.APPEND.value else "append",
                fmt="myfmt",
                partition_columns=[p.column for p in (dest.partition_columns or [])],
                options=conn.write_options,
            )
        elif mode == LoadType.MERGE_UPSERT.value:
            self._engine.merge_to_path(df, path, merge_keys=dest.merge_keys, fmt="myfmt")
        else:
            raise NotImplementedError(f"LoadType {mode!r} not supported by MyDestinationWriter")

    def run_maintenance(
        self,
        dataflow: DataFlow,
        *,
        do_compact: bool = True,
        do_cleanup: bool = True,
        retention_hours: Optional[int] = None,
    ) -> DestinationRuntimeInfo:
        # Optional — override only if your format supports OPTIMIZE / VACUUM.
        return DestinationRuntimeInfo(status="not_supported")
```

The framework calls the public `write(df, dataflow)` method; subclasses
implement `_write_internal`. The base class wraps it with timing, error
handling, and `DestinationRuntimeInfo` population.

## Register

```toml
[tool.poetry.plugins."datacoolie.destinations"]
myfmt = "mypkg.writers:MyDestinationWriter"
```

## Expectations

- **Idempotent writes** — the framework may retry your `_write_internal()` call.
- **Respect `dest.partition_columns`** when the format supports partitioning.
- **Use `engine.merge_to_path` / `merge_to_table`** for merge strategies rather
  than hand-rolling `DELETE + INSERT`.
- **Don't mutate `df`** — transformers already finalised the DataFrame.

## Test matrix

At minimum:

- Append + overwrite on an empty target.
- Append + overwrite on a non-empty target.
- Every load type your writer advertises.
- Partitioned and non-partitioned writes.
