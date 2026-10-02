---
title: Configure API Metadata — DataCoolie User Guide
description: Connect DataCoolie to an API-backed metadata service for connections, dataflows, schema hints, and watermark reads and writes.
---

# Configure API metadata

**Prerequisites** · `pip install "datacoolie[polars,source-api]"` for the local
loopback and Parquet example. For your own service, use an HTTPS metadata API
that implements the HTTP contract below. The `source-api` extra installs
`httpx` for the DataCoolie client.

**End state** · `APIProvider` reads a complete workspace scope, exposes the
same metadata models as the other providers, and reads/writes watermarks over
HTTP.

## Try a local orders dataflow

From a DataCoolie repository checkout, run the maintained
[loopback example](../../examples/files/configuration/provider_fixtures.py). It
starts a temporary HTTP metadata service on `127.0.0.1`, returns two
connections and one dataflow, and runs the same small local Parquet flow used
by the [Database provider example](database.md#try-a-local-orders-dataflow):

```powershell
python docs/examples/files/configuration/provider_fixtures.py --provider api --run-dataflow --work-dir ../.scratch/provider-api-orders
```

Choose a `--work-dir` path that does not exist yet. Expect
`provider=api executed=1 connections=2 dataflows=1` and an output file at
`<work-dir>/api/output/orders/orders.parquet`. This local service is a contract
fixture: the dataflow reads and writes local files, while `APIProvider` gets
its *metadata* over HTTP. The example does not set up a production service.

## Expected endpoints

All paths are scoped under `/workspaces/{workspace_id}/`.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/workspaces/{workspace_id}/connections` | Paginated connection listing. Supports `name` and `active_only`. |
| `GET` | `/workspaces/{workspace_id}/connections/{connection_id}` | One connection; `404` means it was not found. |
| `GET` | `/workspaces/{workspace_id}/dataflows?stage=X&active_only=true` | Paginated dataflows, optionally filtered by stage and activity. |
| `GET` | `/workspaces/{workspace_id}/dataflows/{dataflow_id}` | One dataflow with expanded source and destination connections. |
| `GET` | `/workspaces/{workspace_id}/schema-hints` | Paginated hints; supports connection/table/schema filters. |
| `GET` | `/workspaces/{workspace_id}/watermarks/{dataflow_id}` | Watermark envelope for one dataflow. |
| `PUT` | `/workspaces/{workspace_id}/watermarks/{dataflow_id}` | Replace the current watermark from a JSON body. |

The client sends the API key in the `X-API-Key` header. Collection endpoints
should return an object with a list under `data` and a pagination object. The
client accepts an omitted `data` as an empty list and an omitted `pagination`
as one page, but a service should send the explicit envelope so other clients
can inspect the response consistently:

```json
{
  "data": [],
  "pagination": {
    "page": 1,
    "page_size": 200,
    "total": 0,
    "total_pages": 1
  }
}
```

Response fields inside `data` follow the metadata models defined in the
focused modules under `datacoolie.core.models` — see
[Reference · Metadata document](../../reference/metadata-schema.md#metadata-document).

### Dataflow response contract

Each dataflow response must include expanded `source.connection` and
`destination.connection` objects. This response describes the same local
orders flow used by the loopback example above:

```json
{
  "dataflow_id": "orders-dataflow",
  "workspace_id": "example-workspace",
  "name": "orders",
  "stage": "bronze2silver",
  "source": {
    "connection": {
      "connection_id": "source-connection",
      "workspace_id": "example-workspace",
      "name": "source",
      "connection_type": "file",
      "format": "parquet",
      "configure": {"base_path": "./input"}
    },
    "table": "orders"
  },
  "destination": {
    "connection": {
      "connection_id": "destination-connection",
      "workspace_id": "example-workspace",
      "name": "destination",
      "connection_type": "file",
      "format": "parquet",
      "configure": {"base_path": "./output"}
    },
    "table": "orders",
    "load_type": "overwrite"
  },
  "transform": {}
}
```

The nested connection objects must include at least `connection_id`, `name`,
`connection_type`, and `format`; the client validates those fields while it
builds the runtime model. A dataflow response must include both expanded
connection objects so startup can validate references without guessing another
API call.

For a source with a row predicate, put `filter_expression` directly in the
`source` object. The API provider preserves it for the source reader; it does
not turn it into HTTP request parameters. Use `source.configure.params` or
`source.configure.body` when a data source requires endpoint push-down.
Send `destination.load_type` explicitly. The API mapper currently falls back to
`overwrite` when the field is missing, while authored metadata models use
`append` as their default. Pinning the value avoids an accidental load strategy
when a service omits a field.

Watermark requests use a separate envelope. A `GET` response for a dataflow
that has no stored watermark is:

```json
{"current_value": null}
```

A `PUT` request sends the serialized JSON watermark string together with the
optional run identifiers:

```json
{
  "current_value": "{\"updated_at\": \"2026-09-28T00:00:00+00:00\"}",
  "job_id": "optional-external-or-framework-job-id",
  "dataflow_run_id": "optional-dataflow-run-id"
}
```

The Python method `get_watermark()` returns the raw serialized value or
`None`; that Python return is not the HTTP response shape. A valid `404` for a
watermark also means that no watermark has been created. Authentication,
transport, server, and malformed-response failures remain errors.

## Loading

```python
import os

from datacoolie.metadata.api_provider import APIProvider

provider = APIProvider(
    base_url=os.environ["DATACOOLIE_METADATA_API_URL"],
    api_key=os.environ["DATACOOLIE_METADATA_API_KEY"],
    workspace_id="your-workspace-id",
    enable_cache=True,
    timeout=10.0,
    sql_base_path="./sql",
)
```

Construction is lazy. Validate the complete workspace before handing the
provider to a Driver:

```python
provider.initialize()
connections = provider.get_connections(active_only=False)
dataflows = provider.get_dataflows(
    stage="bronze2silver",
    active_only=False,
    attach_schema_hints=False,
)
print(f"connections={len(connections)} dataflows={len(dataflows)}")
```

Use [the provider configuration example](../../examples/configuration.md#provider-startup)
for the next step: construct an engine and platform, pass this provider to
`DataCoolieDriver`, inspect the `ExecutionResult`, then close the injected
provider in the caller's `finally` block. Driver closes only providers it
created itself.

`sql_base_path` is configuration carried by the metadata provider, even though
the API provider never reads local SQL files. Driver preparation resolves a
relative `source.query` through its execution platform. If the provider omits
the value, the Driver-level SQL root is the session fallback; two different
explicit values are rejected during startup.

## Caching

Set `enable_cache=True` (the default) to avoid re-fetching connections,
dataflows, and schema hints on every call. Call `provider.clear_cache()` after a
known metadata change to invalidate the snapshot; the next read initializes the
provider again. Watermark reads/writes go directly to the API; a successful
watermark update does not clear the metadata cache. `APIProvider.close()` closes
the HTTP client created by the provider and is safe to call once the Driver has
finished using it.

## Common failures

| Symptom | Meaning | Fix |
|---|---|---|
| `API paginated response must be an object` | A collection endpoint returned a top-level array | Return an object with `data` as a list and optional `pagination` object. |
| `Dataflow response is missing a source or destination connection` | A dataflow omitted an expanded connection | Include both nested connection objects and their identity/type fields. |
| `Invalid watermark response` | The watermark body is not an object with `current_value` | Return `{"current_value": null}` for an uninitialized watermark or a serialized JSON value. |
| HTTP `401`/`403`/transport error | Authentication or service connectivity failed | Check the `X-API-Key`, HTTPS URL, timeout and service logs; do not treat it as an empty metadata scope. |

Never place an API key in committed metadata, examples or logs. Use an
environment variable or the host's secret mechanism and use HTTPS outside a
loopback fixture.

## Larger integration testbed

The repository's
[`pg_api_metadata_server.py`](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/docker/pg_api_metadata_server.py)
is a Flask service backed by the `usecase-sim` PostgreSQL metadata tables. Use
it to validate a larger set of repository scenarios after the local example.
It requires its own Flask, database, schema, and seeded rows; the
`datacoolie[source-api]` client extra does not install or configure that
service. An application service still owns authentication, authorization,
migrations, and deployment.

## Related

- [Metadata guide for new users](../metadata/index.md) — understand the metadata shape (connections, dataflows, sources, destinations) before configuring a backend
- [Concepts · Metadata providers · API provider](../../reference/concepts/metadata-providers.md#api-provider)
- [`reference/api/metadata`](../../reference/api/metadata.md)
