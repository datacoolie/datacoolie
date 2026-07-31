---
title: Write a Destination Plugin — DataCoolie
description: Build a custom DataCoolie destination plugin that writes engine dataframes and supports maintenance, metrics, and table formats.
---

# Write a destination

**Prerequisites** · You want to write to a backend not covered by the built-ins · you understand the load-type contract.
**End state** · Destination writer registered under a new format name, selectable via `format: "myfmt"` on a destination connection.

## Minimal writer

```python
from typing import Any, Dict, List

from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.core.exceptions import DestinationError
from datacoolie.core.models import DataFlow
from datacoolie.core.constants import LoadType


class MyDestinationWriter(BaseDestinationWriter):
    def _write_internal(self, df, dataflow: DataFlow) -> None:
        dest = dataflow.destination
        conn = dest.connection
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

The framework calls the public `write(df, dataflow)` method; subclasses
implement `_write_internal` and `_maintain_internal`. The base class wraps both
paths with timing, error handling, and `DestinationRuntimeInfo` population.
Raise `DestinationError` from `_maintain_internal` when the format has no
maintenance operations.

## Register

```toml
[project.entry-points."datacoolie.destinations"]
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
