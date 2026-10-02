---
title: Write a Metadata Provider Plugin — DataCoolie
description: Build a custom metadata provider for DataCoolie to load connections, dataflows, schema hints, and watermarks from your own backend.
---

# Write a metadata provider

**Prerequisites** · You want to store metadata in a backend not covered by
file, database, or API providers.
**End state** · A concrete `BaseMetadataProvider` passed to
`DataCoolieDriver(metadata_provider=...)` and qualified against the metadata
contract.

!!! note "Constructor-injected extension"
    Metadata providers are not entry-point plugins. Construct the provider in
    the application and inject it into the Driver; the registry does not
    discover metadata providers by name.

## Contract

`BaseMetadataProvider` is a Template Method boundary. Public `get_*` methods
provide lifecycle, cache, filtering, and schema-hint attachment behavior.
Implement the protected fetch hooks and the two watermark methods below:

```python
from typing import List, Optional

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.transform import SchemaHint
from datacoolie.metadata.base import BaseMetadataProvider


class MyProvider(BaseMetadataProvider):
    # --- required fetch hooks -------------------------------------------
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

    # --- required watermark methods -------------------------------------
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

The `_fetch_dataflows` hook receives `stages`, not `stage`. The public
`get_dataflows(stage=...)` accepts a single name, a comma-separated string, or
a list; the base class normalizes all three to a list (or `None`) before it
calls the hook. Preserve the `active_only` flag in every fetch path.

Return validated `Connection`, `DataFlow`, and `SchemaHint` model objects,
not raw dictionaries. The initialized scope must have unique connection IDs,
valid dataflow identity (a usable `dataflow_id` or `name`), unique dataflow
IDs, and connection references that resolve by ID or name. Schema hints must
refer to a loaded connection. The base class validates these invariants before
publishing a cache snapshot.

The public `get_connections`, `get_connection_by_id`,
`get_connection_by_name`, `get_dataflows`, `get_dataflow_by_id`, and
`get_schema_hints` methods already handle caching and deep-copying. Do not
re-implement them or bypass their lifecycle decorators.

## Required and optional hooks

The eight methods in the skeleton are the required backend contract. The base
class supplies useful defaults for the rest:

| Hook | Default behavior | Override when |
|---|---|---|
| `_bulk_load()` | Calls all connections and dataflows with `active_only=False`, then fetches schema hints per source table. | The backend has a bulk endpoint or query that is cheaper and returns the same complete scope. |
| `_bulk_fetch_schema_hints(...)` | Walks dataflows and calls `_fetch_schema_hints` for each distinct source connection/table. | The backend can load all hints in one request. |
| `_initialize_metadata()` | No-op before the first complete load. | A client, token, or deferred metadata location must be prepared before fetches. |
| `_cleanup_failed_initialization()` | No-op after a failed startup attempt. | A failed `_initialize_metadata` or load needs provider-owned cleanup. |
| `_configure_context(context)` | Rejects `metadata_base_path`; accepts other shared defaults after SQL validation. | Your backend gives a documented meaning to a startup path or platform. |
| `validate_watermark_storage()` | Checks that the provider is open and performs no I/O. | Readiness needs additional local configuration validation. |
| `_close_resources()` | No-op. | The provider owns a client, connection pool, or other resource. |

Keep optional overrides narrow. `_initialize_metadata` and `_bulk_load` run
inside the startup lifecycle; they must use private fetch methods and must not
call public getters or `close()` recursively. If a provider fans out I/O to
worker threads, those workers must not wait on the lifecycle lock held by the
startup caller. Resources injected by the application remain application-owned;
release only resources the provider created.

## Startup, paths, and SQL roots

Construction records configuration only. `initialize()` is the shared startup
boundary: the Driver calls it explicitly, while a standalone caller reaches it
on the first public metadata read. Initialization runs provider preparation,
loads the complete active and inactive scope, validates identities and hints,
and publishes a cache snapshot only after all of those steps succeed. It is
idempotent and retryable after a failed load. `close()` clears the snapshot and
calls `_close_resources()` under the lifecycle lock; later access fails.

The Driver offers a typed `MetadataProviderStartupContext` with:

- `platform`;
- optional `metadata_base_path`, `artifact_base_path`, `state_base_path`, and
  `log_base_path`; and
- `sql_base_path`, as one root or a sequence of roots.

`configure_context(context)` validates SQL roots before invoking your context
hook. The base class treats `metadata_base_path` as a file-provider concern and
rejects it for API or database providers. Override `_configure_context` only
when that path has an explicit meaning in your backend, and validate the full
candidate context before mutating effective state.

Declare SQL roots on the provider with `sql_base_path=...` when the metadata
backend owns them. If the Driver also supplies roots, the normalized root sets
must agree or startup raises a configuration error. When the provider declares
no roots, the Driver context can supply them. `resolve_sql_base_path` only
normalizes and checks this configuration; Driver preparation reads SQL through
the execution platform.

## Watermarks and flow identity

`get_watermark` returns raw serialized JSON text, or `None`; it must not return a
parsed dictionary. `WatermarkManager` owns deserialization and validation. The
optional `job_id` and `dataflow_run_id` arguments let a backend record run
provenance without changing the serialized watermark contract. See
[ADR-0004](../project/decisions/0004-raw-json-watermark-contract.md).

Keep `dataflow_id` stable for the lifetime of a flow. If a file-backed provider
uses human-readable flow components in a path, preserve the backend's
normalization rule: the built-in `FileProvider` joins any present `stage` and
`name` components, in that order, before `dataflow_id`; when neither is
present, the folder is just `dataflow_id`. Do not derive a different watermark
key from the display name in a custom provider unless migration behavior is
explicit.

Watermark reads and writes can run concurrently with parallel dataflows. Protect
provider-owned stores and clients accordingly, and make updates idempotent when
the backend supports retries.

## Schema-hint attachment

When `attach_schema_hints=True` (the default), the base class attaches hints
from the **source** connection and source table, not the destination. It calls
`_fetch_schema_hints(connection_id, table_name, schema_name)` with the source
connection ID. Query-based sources without a table do not receive table hints.
If the backend has no hint store, return an empty list and document that the
source DataFrame's inferred types remain in use. The schema-converter
transformer then casts incoming data into the attached hint shape.

## Testing

Use the focused fixtures under `tests/unit/metadata/` as behavioral references,
then add tests owned by your backend. At minimum cover:

- zero, one, and many connections, including inactive rows;
- no stage filter, one stage, comma-separated stages, and a list of stages;
- connection and dataflow lookup by ID and name, including unknown values;
- duplicate IDs and unresolved references rejected during initialization;
- schema-hint attachment for a source table and no hints for a query source;
- watermark round-trips for `None`, raw `"null"`, real JSON text, and
  overwrite/concurrent access;
- startup failure cleanup, retry, idempotent `initialize()`, and `close()`;
- provider and Driver SQL-root agreement or conflict;
- provider-owned resources released while injected resources remain usable.

Do not call a backend-specific test count a framework conformance result. The
contract is the public base class and its shared validation; backend tests must
also qualify the backend's paging, transactions, retries, and failure modes.

## Troubleshooting

- **The hook gets `stage=` and fails** — implement `_fetch_dataflows(stages=...)`;
  the base class normalizes the public `stage` argument before dispatch.
- **Initialization rejects a record** — construct models before returning and
  check unique connection IDs, dataflow identity/IDs, and references.
- **`metadata_base_path` is rejected** — override `_configure_context` only if
  your backend owns a meaningful path; API/database providers should keep the
  default rejection.
- **SQL roots conflict** — compare normalized provider and Driver roots and
  configure one agreed set.
- **A watermark is unreadable by the runtime** — return serialized text and
  let `WatermarkManager` parse it.
- **A provider closes an application client** — track ownership and release
  only resources constructed by the provider in `_close_resources()`.

## Related

- [Metadata provider concepts](../reference/concepts/metadata-providers.md)
- [Metadata provider API reference](../reference/api/metadata.md)
- [Raw JSON watermark contract](../project/decisions/0004-raw-json-watermark-contract.md)
- [Metadata model](../reference/concepts/metadata-model.md)
- [Project testing strategy](../project/testing.md)
