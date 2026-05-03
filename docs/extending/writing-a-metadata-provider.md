---
title: Write a Metadata Provider Plugin — DataCoolie
description: Build a custom metadata provider for DataCoolie to load connections, dataflows, schema hints, and watermarks from your own backend.
---

# Write a metadata provider

**Prerequisites** · You want to store metadata in a backend not covered by file / database / API.
**End state** · A concrete `BaseMetadataProvider` you pass to `DataCoolieDriver(metadata_provider=...)`.

!!! note "No entry-point group"
    Metadata providers are **constructor-injected**, not entry-point plugins.
    The choice of metadata backend is an application-level concern; you wire
    your provider directly into the driver.

## Contract

`BaseMetadataProvider` uses the **Template Method** pattern.  Public `get_*`
methods call protected `_fetch_*` abstract methods, wrapping them with an
optional in-memory cache layer.  Subclasses implement the `_fetch_*` hooks
and the two watermark methods:

```python
from typing import Optional, List
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.core.models import Connection, DataFlow, SchemaHint


class MyProvider(BaseMetadataProvider):
    # --- fetch hooks (I/O layer) -----------------------------------------
    def _fetch_connections(self, *, active_only: bool = True) -> List[Connection]: ...
    def _fetch_connection_by_id(self, connection_id: str) -> Optional[Connection]: ...
    def _fetch_connection_by_name(self, name: str) -> Optional[Connection]: ...

    def _fetch_dataflows(
        self,
        *,
        stages: Optional[List[str]] = None,
        active_only: bool = True,
    ) -> List[DataFlow]: ...
    def _fetch_dataflow_by_id(self, dataflow_id: str) -> Optional[DataFlow]: ...

    def _fetch_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]: ...

    # --- watermark methods (abstract in base) -----------------------------
    def get_watermark(self, dataflow_id: str) -> Optional[str]: ...
    def update_watermark(
        self,
        dataflow_id: str,
        watermark_value: str,
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None: ...
```

The public `get_connection_by_name(name)`, `get_connections()`, and
`get_dataflows(...)` methods are already implemented by the base class on top
of these hooks — do not re-implement them.

## Rules

- **`get_watermark` returns raw JSON text** (or `None`). Never a parsed dict.
  `WatermarkManager` owns deserialisation. See
  [ADR-0004](../adr/0004-raw-json-watermark-contract.md).
- **Construct model objects**, not dicts. The framework relies on validation
  at model construction time.
- **Honour `active_only`** — skip `is_active=False` rows unless the caller
  asks for them.
- **Honour `stage` filtering** — accept single string, comma-separated
  string, or list.
- **Be thread-safe** — `DataCoolieDriver` may call concurrent `get_dataflows`
  from `ParallelExecutor` workers.

## Schema-hint attachment

When `attach_schema_hints=True` the base class calls `_attach_schema_hints`,
which reads schema hints based on the **source** connection + table (not
destination).  `_fetch_schema_hints(connection_id, table_name, schema_name)`
is called with the source connection id and source table.  The schema-converter
transformer then casts the incoming DataFrame *into* this shape.  If your
store has no schema hints, implement `_fetch_schema_hints` to return an empty
list and document that type inference falls back to the source DataFrame.

## Testing

Mirror `tests/unit/metadata/`:

- `get_connections` with zero / one / many connections.
- `get_dataflows` with stage filter combinations.
- Watermark round-trip: write "null", write a real JSON, overwrite.
- Concurrent `get_dataflows` from two threads — no shared mutable state.
