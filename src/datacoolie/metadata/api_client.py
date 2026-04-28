"""REST API metadata provider — httpx.

``APIClient`` fetches connections, dataflows, schema hints and watermarks
from a remote metadata API via HTTP.

Authentication is done via an ``X-API-Key`` header.  The client handles
automatic pagination, retry on transient errors (429 / 5xx), and maps
all HTTP failures to ``MetadataError`` or ``WatermarkError``.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Union

_T = TypeVar("_T")
_K = TypeVar("_K")

from datacoolie.core.constants import LoadType, ProcessingMode
from datacoolie.core.exceptions import MetadataError, WatermarkError
from datacoolie.core.models import (
    Connection,
    DataFlow,
    Destination,
    SchemaHint,
    Source,
    Transform,
)
from datacoolie.logging.base import get_logger
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.utils.converters import parse_json
from datacoolie.utils.helpers import ensure_list

logger = get_logger(__name__)


def _import_httpx() -> Any:
    """Lazy-import httpx so it is only required at runtime, not at import."""
    try:
        import httpx

        return httpx
    except ImportError as exc:
        raise MetadataError(
            "The 'httpx' package is required for APIClient. "
            "Install it with: pip install httpx"
        ) from exc


# ============================================================================
# APIClient
# ============================================================================


class APIClient(BaseMetadataProvider):
    """Metadata provider backed by a remote REST API.

    Args:
        base_url: Root URL of the metadata API, including any path prefix
            (e.g. ``https://api.datacoolie.io/api/v1``).
        api_key: Value for the ``X-API-Key`` header.
        workspace_id: Workspace scope for all requests.
        enable_cache: Enable the in-memory metadata cache.
        timeout: Request timeout in seconds (default ``30``).
        max_retries: Number of retries on transient failures (default ``3``).
        retry_backoff: Base backoff in seconds for exponential retry (default ``1.0``).
    """

    # HTTP status codes that trigger a retry
    _RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    # Safety cap on pagination to prevent infinite loops from a
    # malicious or buggy API server that keeps increasing total_pages.
    _MAX_PAGES: int = 1000

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        workspace_id: str,
        enable_cache: bool = True,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
        page_size: int = 200,
        max_workers: int = 8,
        eager_prefetch: bool = False,
    ) -> None:
        super().__init__(enable_cache=enable_cache, eager_prefetch=eager_prefetch)
        httpx = _import_httpx()
        self._base_url = base_url.rstrip("/")
        if not self._base_url.startswith("https"):
            logger.warning(
                "API key is being sent over an insecure connection (%s). "
                "Use HTTPS in production.",
                self._base_url.split("://")[0] if "://" in self._base_url else "unknown",
            )
        self._workspace_id = workspace_id
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._page_size = page_size
        self._max_workers = max(1, max_workers)
        self._client: Any = httpx.Client(
            base_url=self._base_url,
            headers={
                "X-API-Key": api_key,
                "Accept": "application/json",
            },
            timeout=timeout,
        )
        self._maybe_eager_prefetch()

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    @property
    def _ws_prefix(self) -> str:
        """Return ``/workspaces/{workspace_id}``."""
        return f"/workspaces/{self._workspace_id}"

    # ------------------------------------------------------------------
    # Generic request with retry + pagination
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        paginate: bool = False,
    ) -> Any:
        """Execute an HTTP request with retry + optional pagination.

        Returns the parsed JSON body (``dict`` or ``list`` depending on
        the ``paginate`` flag).

        Raises:
            MetadataError: On non-retryable HTTP errors.
        """
        url = f"{self._ws_prefix}{path}"

        # Inject default page_size on paginated GETs so fewer round trips
        # are needed to collect all pages.
        if paginate:
            merged_params: Dict[str, Any] = dict(params or {})
            merged_params.setdefault("page_size", self._page_size)
            params = merged_params

        body = self._execute_with_retry(method, url, params=params, json_body=json_body)
        if paginate:
            return self._collect_pages(method, url, params, body)
        return body

    def _execute_with_retry(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Execute a single (non-paginated) HTTP request with retry.

        Shared by :meth:`_request` and the per-page fetches inside
        :meth:`_collect_pages` so that every HTTP call \u2014 including
        pages 2..N of a paginated listing \u2014 benefits from retry on
        transient failures and uniform ``MetadataError`` wrapping.
        """
        httpx = _import_httpx()
        for attempt in range(self._max_retries + 1):
            try:
                resp = self._client.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                )
                if resp.status_code in self._RETRYABLE_STATUS and attempt < self._max_retries:
                    self._backoff(attempt, resp)
                    continue
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in self._RETRYABLE_STATUS and attempt < self._max_retries:
                    self._backoff(attempt)
                    continue
                raise MetadataError(
                    f"API request failed: {method} {url} \u2192 {exc.response.status_code}",
                    details={"response_text": exc.response.text[:500]},
                ) from exc
            except httpx.HTTPError as exc:
                if attempt < self._max_retries:
                    self._backoff(attempt)
                    continue
                raise MetadataError(
                    f"API request error: {method} {url}",
                ) from exc

        # Unreachable: every loop path either returns or raises, but
        # keep a defensive final raise so the control-flow is explicit.
        raise MetadataError(
            f"API request failed after {self._max_retries + 1} attempts: {method} {url}",
        )

    def _backoff(self, attempt: int, resp: Any = None) -> None:
        """Sleep with exponential back-off."""
        delay = self._retry_backoff * (2 ** attempt)
        # honour Retry-After header when present
        if resp is not None:
            retry_after = getattr(resp, "headers", {}).get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except ValueError:
                    pass
        time.sleep(delay)

    def _collect_pages(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]],
        first_body: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Follow pagination and collect all ``data`` items.

        Pages 2..N are fetched concurrently via a ``ThreadPoolExecutor``
        (``httpx.Client`` is thread-safe) to avoid serial latency on
        long paginated listings.
        """
        first_data: List[Dict[str, Any]] = list(first_body.get("data", []))
        pagination = first_body.get("pagination", {})
        total_pages = int(pagination.get("total_pages", 1))
        current_page = int(pagination.get("page", 1))

        if total_pages <= current_page:
            return first_data

        last_page = min(total_pages, self._MAX_PAGES)
        if total_pages > self._MAX_PAGES:
            logger.warning(
                "Pagination capped at %d pages for %s %s",
                self._MAX_PAGES, method, url,
            )

        page_numbers = list(range(current_page + 1, last_page + 1))

        def _fetch_page(page_num: int) -> List[Dict[str, Any]]:
            page_params = dict(params or {})
            page_params["page"] = page_num
            body = self._execute_with_retry(method, url, params=page_params)
            return list(body.get("data", []))

        results = self._fan_out(page_numbers, _fetch_page)

        all_data: List[Dict[str, Any]] = first_data
        for n in page_numbers:
            all_data.extend(results.get(n, []))
        return all_data

    def _fan_out(
        self,
        keys: List[_K],
        fetch: Callable[[_K], _T],
    ) -> Dict[_K, _T]:
        """Run *fetch(key)* for every key, concurrently when worthwhile.

        Returns ``{key: fetch(key)}``.  Uses a thread pool bounded by
        ``self._max_workers`` when there's more than one key; otherwise
        runs serially to avoid pool overhead.  ``httpx.Client`` is
        thread-safe, so this is the standard pattern used by both
        paginated listings (page numbers) and per-connection prefetch.
        """
        results: Dict[_K, _T] = {}
        if not keys:
            return results
        workers = min(self._max_workers, len(keys))
        if workers <= 1:
            for key in keys:
                results[key] = fetch(key)
            return results
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch, key): key for key in keys}
            for fut in as_completed(futures):
                results[futures[fut]] = fut.result()
        return results

    # ------------------------------------------------------------------
    # Row → Model mappers
    # ------------------------------------------------------------------

    @staticmethod
    def _dict_to_connection(data: Dict[str, Any]) -> Connection:
        """Map an API dict to a ``Connection`` model."""
        return Connection(
            connection_id=data["connection_id"],
            workspace_id=data.get("workspace_id", ""),
            name=data["name"],
            connection_type=data.get("connection_type", ""),
            format=data.get("format", ""),
            catalog=data.get("catalog"),
            database=data.get("database"),
            configure=data.get("configure") or {},
            secrets_ref=data.get("secrets_ref"),
            is_active=data.get("is_active", True),
        )

    @classmethod
    def _dict_to_dataflow(cls, data: Dict[str, Any]) -> DataFlow:
        """Map an API dict to a ``DataFlow`` model."""
        src_data = data.get("source", {})
        dest_data = data.get("destination", {})
        transform_data = data.get("transform", {})

        # Source connection
        src_conn_data = src_data.get("connection", {})
        src_conn = cls._dict_to_connection(src_conn_data) if src_conn_data else Connection(
            connection_id="", workspace_id="", name="", connection_type="", format=""
        )

        # Destination connection
        dest_conn_data = dest_data.get("connection", {})
        dest_conn = cls._dict_to_connection(dest_conn_data) if dest_conn_data else Connection(
            connection_id="", workspace_id="", name="", connection_type="", format=""
        )

        # Source
        source = Source(
            connection=src_conn,
            schema_name=src_data.get("schema_name"),
            table=src_data.get("table"),
            query=src_data.get("query"),
            python_function=src_data.get("python_function"),
            watermark_columns=ensure_list(src_data.get("watermark_columns")),
            configure=src_data.get("configure") or {},
        )

        # Destination
        destination = Destination(
            connection=dest_conn,
            schema_name=dest_data.get("schema_name"),
            table=dest_data.get("table", ""),
            load_type=dest_data.get("load_type", LoadType.OVERWRITE.value),
            merge_keys=ensure_list(dest_data.get("merge_keys")),
            partition_columns=dest_data.get("partition_columns") or [],
            configure=dest_data.get("configure") or {},
        )

        # Transform
        transform = Transform(**transform_data) if transform_data else Transform()

        return DataFlow(
            dataflow_id=data.get("dataflow_id", ""),
            workspace_id=data.get("workspace_id", ""),
            name=data.get("name", ""),
            description=data.get("description"),
            stage=data.get("stage"),
            group_number=data.get("group_number"),
            execution_order=data.get("execution_order"),
            processing_mode=data.get("processing_mode", ProcessingMode.BATCH.value),
            is_active=data.get("is_active", True),
            source=source,
            destination=destination,
            transform=transform,
            configure=data.get("configure") or {},
        )

    # ------------------------------------------------------------------
    # Bulk pre-load — fetch entire workspace metadata concurrently
    # ------------------------------------------------------------------

    def _bulk_load(
        self,
    ) -> Tuple[
        List[Connection],
        List[DataFlow],
        Dict[Tuple[str, Optional[str], str], List[SchemaHint]],
    ]:
        """Fetch connections, dataflows and schema-hints concurrently.

        Each resource type is fetched as a single paginated listing
        (with concurrent page fan-out inside ``_collect_pages``), and
        the three top-level requests are themselves run in parallel.
        """
        def _get_connections() -> List[Dict[str, Any]]:
            return self._request("GET", "/connections", params={}, paginate=True)

        def _get_dataflows() -> List[Dict[str, Any]]:
            return self._request("GET", "/dataflows", params={}, paginate=True)

        def _get_hints() -> List[Dict[str, Any]]:
            return self._request("GET", "/schema-hints", params={}, paginate=True)

        with ThreadPoolExecutor(max_workers=3) as pool:
            f_conn = pool.submit(_get_connections)
            f_df = pool.submit(_get_dataflows)
            f_hints = pool.submit(_get_hints)
            conn_rows = f_conn.result()
            df_rows = f_df.result()
            hint_rows = f_hints.result()

        connections = [self._dict_to_connection(d) for d in conn_rows]
        dataflows = [self._dict_to_dataflow(d) for d in df_rows]
        grouped = self._group_schema_hint_rows(hint_rows)
        return connections, dataflows, grouped

    def _group_schema_hint_rows(
        self,
        rows: List[Dict[str, Any]],
        *,
        fixed_connection_id: Optional[str] = None,
    ) -> Dict[Tuple[str, Optional[str], str], List[SchemaHint]]:
        """Group raw hint rows by ``(connection_id, schema_name, table_name)``.

        If *fixed_connection_id* is given, all rows are assumed to
        belong to that connection (used by per-connection prefetch);
        otherwise the connection id is read from each row.  Empty
        ``schema_name`` is normalised to ``None`` so lookups match
        ``BaseMetadataProvider._attach_schema_hints``.
        """
        grouped: Dict[Tuple[str, Optional[str], str], List[SchemaHint]] = {}
        for row in rows:
            cid = fixed_connection_id or row.get("connection_id")
            table = row.get("table_name")
            if not cid or not table:
                continue
            schema = row.get("schema_name") or None  # normalise '' -> None
            grouped.setdefault((cid, schema, table), []).append(
                self._dict_to_schema_hint(row)
            )
        return grouped

    @staticmethod
    def _dict_to_schema_hint(data: Dict[str, Any]) -> SchemaHint:
        """Map an API dict to a ``SchemaHint`` model."""
        return SchemaHint(
            column_name=data["column_name"],
            data_type=data["data_type"],
            format=data.get("format"),
            precision=data.get("precision"),
            scale=data.get("scale"),
            default_value=data.get("default_value"),
            ordinal_position=data.get("ordinal_position", 0),
            is_active=data.get("is_active", True),
        )

    # ------------------------------------------------------------------
    # Abstract method implementations — connections
    # ------------------------------------------------------------------

    def _fetch_connections(self, *, active_only: bool = True) -> List[Connection]:
        params: Dict[str, Any] = {}
        if active_only:
            params["active_only"] = "true"
        data = self._request("GET", "/connections", params=params, paginate=True)
        return [self._dict_to_connection(d) for d in data]

    def _fetch_connection_by_id(self, connection_id: str) -> Optional[Connection]:
        return self._safe_single_fetch(
            lambda: self._dict_to_connection(
                self._request("GET", f"/connections/{connection_id}")
            )
        )

    def _fetch_connection_by_name(self, name: str) -> Optional[Connection]:
        def _do() -> Optional[Connection]:
            data = self._request("GET", "/connections", params={"name": name}, paginate=True)
            return self._dict_to_connection(data[0]) if data else None
        return self._safe_single_fetch(_do)

    # ------------------------------------------------------------------
    # Abstract method implementations — dataflows
    # ------------------------------------------------------------------

    def _fetch_dataflows(
        self,
        *,
        stages: Optional[List[str]] = None,
        active_only: bool = True,
    ) -> List[DataFlow]:
        params: Dict[str, Any] = {}
        if active_only:
            params["active_only"] = "true"
        if stages is not None:
            params["stage"] = ",".join(stages)
        data = self._request("GET", "/dataflows", params=params, paginate=True)
        return [self._dict_to_dataflow(d) for d in data]

    def _fetch_dataflow_by_id(self, dataflow_id: str) -> Optional[DataFlow]:
        return self._safe_single_fetch(
            lambda: self._dict_to_dataflow(
                self._request("GET", f"/dataflows/{dataflow_id}")
            )
        )

    @staticmethod
    def _safe_single_fetch(fn: Callable[[], Optional[_T]]) -> Optional[_T]:
        """Run *fn* and swallow ``MetadataError`` into ``None``.

        Used by lookups that map "not found / HTTP 4xx" to ``None``
        instead of raising.
        """
        try:
            return fn()
        except MetadataError:
            return None

    # ------------------------------------------------------------------
    # Abstract method implementations — schema hints
    # ------------------------------------------------------------------

    def _fetch_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]:
        params: Dict[str, Any] = {
            "connection_id": connection_id,
            "table_name": table_name,
        }
        if schema_name is not None:
            params["schema_name"] = schema_name
        data = self._request("GET", "/schema-hints", params=params, paginate=True)
        return [self._dict_to_schema_hint(d) for d in data]

    # ------------------------------------------------------------------
    # Bulk schema-hint prefetch — avoid N+1 round trips
    # ------------------------------------------------------------------

    def _prefetch_schema_hints(self, dataflows: List[DataFlow]) -> None:
        """Bulk-load schema hints for all hint-requiring connections.

        Collects the distinct source ``connection_id`` values for
        dataflows whose source has ``use_schema_hint=True``, fetches
        all hints for each connection in a single paginated GET
        (concurrently across connections), then groups the results by
        ``(connection_id, schema_name, table_name)`` and writes them
        into the cache.  The subsequent per-dataflow attach loop then
        hits the cache instead of issuing one GET per dataflow.
        """
        if self._cache is None:
            return

        conn_ids: Set[str] = set()
        for df in dataflows:
            src_conn = df.source.connection
            if not src_conn.use_schema_hint:
                continue
            if df.source.table is None:
                continue
            if src_conn.connection_id:
                conn_ids.add(src_conn.connection_id)

        if not conn_ids:
            return

        def _fetch_for_conn(conn_id: str) -> List[Dict[str, Any]]:
            return self._request(
                "GET", "/schema-hints",
                params={"connection_id": conn_id},
                paginate=True,
            )

        results = self._fan_out(list(conn_ids), _fetch_for_conn)

        # Group per-connection and populate cache.
        for cid, rows in results.items():
            grouped = self._group_schema_hint_rows(rows, fixed_connection_id=cid)
            for (cid2, schema, table), hints in grouped.items():
                self._cache.set_schema_hints(cid2, schema, table, hints)

    # ------------------------------------------------------------------
    # Abstract method implementations — watermarks
    # ------------------------------------------------------------------

    def get_watermark(self, dataflow_id: str) -> Optional[str]:
        """Return the raw serialised watermark string for *dataflow_id*, or ``None``.

        The API may return ``current_value`` as a JSON string or as an already-parsed
        dict.  Either way, this method normalises the result to a JSON string so that
        ``WatermarkManager`` can deserialize it uniformly.
        """
        try:
            data = self._request("GET", f"/watermarks/{dataflow_id}")
            current = data.get("current_value")
            if current is None:
                return None
            if isinstance(current, dict):
                return json.dumps(current)
            return str(current)
        except MetadataError:
            return None

    def update_watermark(
        self,
        dataflow_id: str,
        watermark_value: str,
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        """Update the watermark via the API."""
        body: Dict[str, Any] = {"current_value": watermark_value}
        if job_id is not None:
            body["job_id"] = job_id
        if dataflow_run_id is not None:
            body["dataflow_run_id"] = dataflow_run_id
        try:
            self._request("PUT", f"/watermarks/{dataflow_id}", json_body=body)
        except MetadataError:
            raise
        except Exception as exc:
            raise WatermarkError(
                f"Failed to update watermark for dataflow {dataflow_id}"
            ) from exc

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the HTTP client and clear cache."""
        super().close()
        if hasattr(self, "_client") and self._client is not None:
            self._client.close()
