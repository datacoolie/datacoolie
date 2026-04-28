# Configure API metadata

**Prerequisites** · `pip install "datacoolie[api]"` · a REST service matching the expected OpenAPI contract.
**End state** · `APIClient` reading connections, dataflows, schema hints, and watermarks over HTTP.

## Reference implementation

A ready-to-run Flask server lives at
[`usecase-sim/docker/pg_api_metadata_server.py`](https://github.com/datacoolie/datacoolie/blob/main/datacoolie/usecase-sim/docker/pg_api_metadata_server.py).
It reads canonical JSON metadata and serves the endpoints DataCoolie expects.

Use it as an integration target or as a starting point for your own service.

## Expected endpoints

All paths are scoped under `/workspaces/{workspace_id}/`.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/workspaces/{workspace_id}/connections` | All connections (JSON array). |
| `GET` | `/workspaces/{workspace_id}/dataflows?stage=X&active_only=true` | Dataflows, optionally filtered. |
| `GET` | `/workspaces/{workspace_id}/schema-hints` | Schema hints (workspace-level). |
| `GET` | `/workspaces/{workspace_id}/watermarks/{dataflow_id}` | Current watermark (raw JSON string). |
| `PUT` | `/workspaces/{workspace_id}/watermarks/{dataflow_id}` | Upsert watermark (JSON body). |

Response shapes follow the metadata models defined in `datacoolie.core.models` — see
[Reference · Metadata schema](../reference/metadata-schema.md).

## Loading

```python
from datacoolie.metadata.api_client import APIClient

provider = APIClient(
    base_url="https://metadata.internal/api/v1",
    api_key="your-api-key",
    workspace_id="your-workspace-id",
    enable_cache=True,
    timeout=10.0,
)
```

## Caching

Set `enable_cache=True` (the default) to avoid re-fetching `connections` / `dataflows` on every
call. The client invalidates the cache whenever a watermark update succeeds.

## Related

- [Concepts · Metadata providers](../concepts/metadata-providers.md)
- [`reference/api/metadata`](../reference/api/metadata.md)
