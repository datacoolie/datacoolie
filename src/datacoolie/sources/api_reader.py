"""API source reader.

Reads data from HTTP/REST APIs via ``httpx`` with support for
pagination, authentication, and rate limiting.
"""

from __future__ import annotations

import base64
import dataclasses
import functools
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Source
from datacoolie.engines.base import DF, BaseEngine
from datacoolie.logging.base import get_logger
from datacoolie.sources.base import BaseSourceReader

logger = get_logger(__name__)

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

try:
    import botocore.auth
    import botocore.awsrequest
    import botocore.credentials
    import botocore.session as _boto_session
    _HAS_BOTOCORE = True
except ImportError:
    _HAS_BOTOCORE = False

try:
    from dateutil.relativedelta import relativedelta as _relativedelta
    _HAS_DATEUTIL = True
except ImportError:
    _HAS_DATEUTIL = False

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
    _HAS_ZONEINFO = True
except ImportError:
    _ZoneInfo = None  # type: ignore[assignment,misc]
    _HAS_ZONEINFO = False

# Pagination strategies
_PAGINATION_OFFSET = "offset"
_PAGINATION_CURSOR = "cursor"
_PAGINATION_NEXT_LINK = "next_link"

# Auth strategies
_AUTH_BEARER = "bearer"
_AUTH_BASIC = "basic"
_AUTH_API_KEY = "api_key"
_AUTH_OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
_AUTH_AWS_SIGV4 = "aws_sigv4"

# OAuth2 token cache: (token_url, client_id) → (access_token, monotonic_expiry)
# Shared across all APIReader calls in the same process to avoid a round-trip per source.
_OAUTH2_TOKEN_CACHE: Dict[Tuple[str, str], Tuple[str, float]] = {}
_OAUTH2_TOKEN_CACHE_LOCK = threading.Lock()


@dataclasses.dataclass(frozen=True)
class _RangeCallConfig:
    """Immutable configuration bundle passed to each parallel range call.

    Frozen so it can be safely shared across threads without copying.
    """

    client: Any
    url: str
    method: str
    base_params: Dict[str, Any]
    base_body: Dict[str, Any]
    wm_mapping: Dict[str, str]
    wm_to_param: str
    wm_location: str
    wm_format: str
    overlap_mode: Optional[str]
    src_cfg: Dict[str, Any]


class _SigV4Auth(httpx.Auth if httpx else object):  # type: ignore[misc]
    """httpx Auth that signs every request with AWS Signature Version 4.

    Uses ``botocore`` so the standard credential chain (env vars,
    ``~/.aws/credentials``, EC2 instance profile, ECS task role, etc.)
    is respected automatically when ``aws_access_key_id`` is omitted.
    """

    def __init__(
        self,
        credentials: "botocore.credentials.Credentials",
        service: str,
        region: str,
    ) -> None:
        self._credentials = credentials
        self._service = service
        self._region = region

    def auth_flow(self, request: "httpx.Request"):  # type: ignore[override]
        aws_req = botocore.awsrequest.AWSRequest(
            method=request.method,
            url=str(request.url),
            data=request.content,
            headers=dict(request.headers),
        )
        signer = botocore.auth.SigV4Auth(self._credentials, self._service, self._region)
        signer.add_auth(aws_req)
        for key, value in aws_req.headers.items():
            request.headers[key] = value
        yield request


class APIReader(BaseSourceReader[DF]):
    """Source reader for HTTP/REST APIs.

    Fetches JSON data from paginated REST endpoints, converts the
    collected records to a DataFrame via the engine, and supports
    incremental reads through watermark filtering.

    API configuration is supplied through ``source.connection.configure``
    and ``source.configure``:

    Connection-level (``source.connection.configure``):
        - ``base_url`` (str): Base URL for the API (required).
        - ``auth_type`` (str): ``"bearer"``, ``"basic"``, ``"api_key"``, or
          ``"oauth2_client_credentials"``.
        - ``auth_token`` (str): Bearer token value (for ``auth_type="bearer"``).
        - ``username`` / ``password`` (str): Basic auth credentials.
        - ``api_key_header`` (str): Header name for API key auth.
        - ``api_key_value`` (str): API key value.
        - ``token_url`` (str): OAuth2 token endpoint (for ``oauth2_client_credentials``).
        - ``client_id`` (str): OAuth2 client ID.
        - ``client_secret`` (str): OAuth2 client secret — use ``secrets_ref`` so
          it is resolved from a secret store at runtime rather than stored in plain text.
        - ``scope`` (str): Optional space-separated OAuth2 scopes.
        - ``token_auth_method`` (str): How to send client credentials to the token
          endpoint. ``"client_secret_post"`` (default) puts ``client_id`` and
          ``client_secret`` in the POST body. ``"client_secret_basic"`` sends them
          as HTTP Basic Auth (required by Okta, Ping Identity, and some others).
        - ``token_request_body_format`` (str): ``"form"`` (default,
          ``application/x-www-form-urlencoded``) or ``"json"``
          (``application/json``) — some providers (e.g. GitHub Apps) expect JSON.
        - ``token_request_extras`` (dict): Extra fields forwarded to the token POST body.
        - ``watermark_to_param_timezone`` (str, optional): Default timezone for
          the ``"now"`` timestamp injected into ``watermark_to_param`` and for
          advancing watermarks of pushed-down columns.  Can be overridden per
          source via the source-level key of the same name (source wins).
          Accepts IANA names or UTC offset strings; defaults to ``"UTC"``.

    ``auth_type="aws_sigv4"`` (API Gateway IAM auth, direct AWS service endpoints):
        Signs every request with AWS Signature Version 4 via ``botocore``.
        When ``aws_access_key_id`` is omitted, the standard boto3 credential chain
        is used (env vars, ``~/.aws/credentials``, EC2/ECS instance role, etc.).
        - ``aws_region`` (str): AWS region, e.g. ``"us-east-1"``.
        - ``aws_service`` (str): AWS service name (default ``"execute-api"`` for
          API Gateway). Use ``"s3"``, ``"lambda"``, etc. for other endpoints.
        - ``aws_access_key_id`` (str, optional): Override access key — use
          ``secrets_ref`` rather than hard-coding.
        - ``aws_secret_access_key`` (str, optional): Override secret — use
          ``secrets_ref``.
        - ``aws_session_token`` (str, optional): Temporary session token for
          role-assumed or STS credentials.
        - ``default_headers`` (dict): Headers applied to every request.
        - ``timeout`` (int/float): Request timeout in seconds (default 30).

    Source-level (``source.configure``):
        - ``endpoint`` (str): API endpoint path appended to ``base_url``.
        - ``method`` (str): HTTP method (default ``"GET"``).
        - ``params`` (dict): Query parameters.
        - ``body`` (dict): Request body (for POST/PUT).
        - ``pagination_type`` (str): ``"offset"``, ``"cursor"``,
          or ``"next_link"``.
        - ``page_size`` (int): Number of records per page (default 100).
        - ``max_pages`` (int): Safety limit on pages fetched (default 1000).
        - ``data_path`` (str): Dot-separated path to records array in response
          JSON (e.g. ``"data.items"``). Defaults to root-level list.
        - ``next_link_path`` (str): Dot-separated path to the next-page URL.
        - ``cursor_path`` (str): Dot-separated path to the cursor token.
        - ``cursor_param`` (str): Query param name for cursor (default ``"cursor"``).
        - ``offset_param`` (str): Query param for offset (default ``"offset"``).
        - ``limit_param`` (str): Query param for page size (default ``"limit"``).
        - ``rate_limit_delay`` (float): Seconds to wait between requests (default 0).
        - ``max_retries`` (int): Maximum number of retries on HTTP 429 rate-limit
          responses (default ``10``).  When the ``Retry-After`` response header is
          present its value is used as the wait duration; otherwise exponential backoff
          applies: ``min(2 ** attempt, 30)`` seconds per retry
          (1 s, 2 s, 4 s, 8 s, 16 s, 30 s, 30 s, …), capped at 30 s.
          Raises :class:`~datacoolie.core.exceptions.SourceError` once all
          retries are exhausted.
        - ``total_path`` (str, optional): Dot-separated path to the total record count
          in the first-page response JSON (e.g. ``"meta.total"`` or
          ``"pagination.count"``).  Only applies when ``pagination_type="offset"``.
          When set, the reader fetches page 0 to discover the total, calculates the
          number of remaining pages, and dispatches them **concurrently** via
          :class:`~concurrent.futures.ThreadPoolExecutor`.  Without this key,
          offset pagination falls back to sequential page-by-page fetching (stopping
          when a page returns fewer records than ``page_size``).
        - ``offset_max_workers`` (int): Maximum number of parallel workers used when
          ``total_path`` is configured for concurrent offset fetching (default ``4``).
          Has no effect when ``total_path`` is absent.

    Incremental / watermark push-down (``source.configure``):
        Instead of filtering rows in memory after fetching, these keys inject the
        stored watermark value directly into the outgoing request so the API server
        returns only new records — analogous to a SQL ``WHERE col > last_value``.

        - ``watermark_param_mapping`` (dict): Maps each ``watermark_columns`` entry
          to its API parameter name, e.g.
          ``{"updated_at": "updated_since", "id": "after_id"}``.
          Only columns that are present in the stored watermark are injected.
          Pushed-down columns may be absent from the API response; they skip
          in-memory filtering and their new watermark is set to ``"now"``
          (see ``watermark_to_param_timezone``) rather than the column's max
          value in the dataframe.
        - ``watermark_to_param`` (str, optional): API parameter name to receive the
          current UTC timestamp as an upper bound ("to" side of the window).
          If omitted, no upper-bound parameter is sent.
        - ``watermark_param_location`` (str): Where to inject — ``"params"`` (default,
          URL query string) or ``"body"`` (JSON/form POST body).
        - ``watermark_param_format`` (str): How to serialise the watermark value:
          ``"iso"`` (default, e.g. ``"2024-01-15T12:00:00"``),
          ``"date"`` (``"2024-01-15"``),
          ``"timestamp"`` (Unix seconds as float string),
          ``"timestamp_ms"`` (Unix milliseconds as int string)
          ``"datetime"`` (``"2024-01-15 12:00:00"``),
          ``"datetime_ms"`` (``"2024-01-15 12:00:00.123"``).
        - ``watermark_to_param_timezone`` (str, optional): Timezone for the
          ``"now"`` timestamp injected into ``watermark_to_param`` and used
          when advancing the watermark of pushed-down columns.  Accepts IANA
          names (``"Asia/Ho_Chi_Minh"``, ``"America/New_York"``) or UTC
          offset strings (``"+07:00"``, ``"-05:30"``).  Defaults to ``"UTC"``.
          Source-level value takes precedence over the connection-level default.

    Range-split / parallel fetch (``source.configure``):
        Splits a large watermark window into equal-sized intervals and calls
        the API concurrently for each sub-range.  Requires ``watermark_to_param``
        to be set so each range's upper-bound can be passed to the API.

        - ``watermark_range_interval_unit`` (str): Interval unit for each sub-range
          — ``"hour"``, ``"day"``, ``"month"``, or ``"year"``.  When set, range
          splitting is enabled; otherwise a single API call is made (default).
        - ``watermark_range_interval_amount`` (int): Number of units per interval
          (default ``1``).  E.g. ``unit="hour", amount=3`` → 3-hour windows.
        - ``watermark_range_start`` (str): ISO-8601 datetime used as the initial
          lower bound when no stored watermark exists yet (first run).  Required
          when ``watermark_range_interval_unit`` is set and no watermark has been
          saved.
        - ``watermark_range_max_workers`` (int): Maximum parallel HTTP workers
          (default ``4``).
        - ``watermark_range_to_exclusive_offset`` (str, optional): Epsilon
          subtracted from each range's upper-bound **before sending it to the
          API**, to prevent duplicate rows when the API uses inclusive
          ``BETWEEN from AND to`` semantics.  The internal boundary that
          starts the next range is unchanged.  Values: ``None`` (default
          — no adjustment, correct for APIs with half-open ``[from, to)``
          semantics), ``"1ms"`` (1 millisecond), ``"1s"`` (1 second),
          ``"1day"`` (1 day; for date-precision parameters like
          ``"2025-01-15"``).
    """

    # ------------------------------------------------------------------
    # Core reading
    # ------------------------------------------------------------------

    def _read_internal(
        self,
        source: Source,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> Optional[DF]:
        action: dict = {"reader": type(self).__name__}
        conn_cfg = source.connection.configure
        src_cfg = source.configure

        base_url = conn_cfg.get("base_url", "").rstrip("/")
        endpoint = src_cfg.get("endpoint", "")
        url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url
        action["url"] = url
        self._set_source_action(action)

        if not base_url:
            raise SourceError(
                "APIReader requires 'base_url' in connection.configure",
                details={"connection": source.connection.name},
            )

        records = self._read_data(source, watermark=watermark)

        if not records:
            logger.info("APIReader: 0 records fetched — skipping. Table: %s (format: %s), URL: %s", source.full_table_name, source.connection.format, url)
            self._set_rows_read(0)
            self._set_new_watermark({})
            return None

        df = self._records_to_dataframe(records)

        # Identify which watermark columns are pushed down to the API
        # (the API filters them server-side so they may be absent from the response).
        _wm_mapping = src_cfg.get("watermark_param_mapping") or {}
        _pushed_cols = set(_wm_mapping)
        _non_pushed_cols = [c for c in (source.watermark_columns or []) if c not in _pushed_cols]

        if watermark and source.watermark_columns:
            # Apply in-memory filter only for columns not already pushed to the API.
            if _non_pushed_cols:
                df = self._apply_watermark_filter(df, _non_pushed_cols, watermark)

        # Compute count and max watermark only from columns present in the
        # dataframe (non-pushed).  Pushed-down columns advance to "now" separately.
        count, new_wm = self._calculate_count_and_new_watermark(
            df, _non_pushed_cols,
        )

        # Advance pushed-down columns to "now" — filtered server-side and may
        # not appear in the response dataframe at all.
        if _pushed_cols and source.watermark_columns:
            _to_tz = APIReader._resolve_timezone(
                src_cfg.get("watermark_to_param_timezone")
                or conn_cfg.get("watermark_to_param_timezone")
            )
            _to_now = datetime.now(tz=_to_tz)
            for _col in source.watermark_columns:
                if _col in _pushed_cols:
                    _prev = (watermark or {}).get(_col)
                    new_wm[_col] = _to_now.date() if type(_prev) is date else _to_now

        self._set_new_watermark(new_wm)
        self._set_rows_read(count)

        if count == 0:
            logger.info("APIReader: 0 rows after filtering — skipping. Table: %s (format: %s), URL: %s", source.full_table_name, source.connection.format, url)
            return None

        logger.info("APIReader: read %d rows from %s", count, url)
        return df

    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
        watermark: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all pages from the API and return collected records."""
        if httpx is None:
            raise SourceError(
                "APIReader requires 'httpx'. Install it with: pip install httpx",
            )

        conn_cfg = source.connection.configure
        src_cfg = source.configure
        if configure:
            src_cfg = {**src_cfg, **configure}

        base_url = conn_cfg.get("base_url", "").rstrip("/")
        endpoint = src_cfg.get("endpoint", "")
        url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url

        method = src_cfg.get("method", "GET").upper()
        params = dict(src_cfg.get("params", {}))
        body = dict(src_cfg.get("body") or {})
        timeout = float(conn_cfg.get("timeout", 30))

        # Watermark push-down config
        wm_mapping: Dict[str, str] = src_cfg.get("watermark_param_mapping") or {}
        wm_to_param: Optional[str] = src_cfg.get("watermark_to_param")
        wm_location: str = src_cfg.get("watermark_param_location", "params").lower()
        wm_format: str = src_cfg.get("watermark_param_format", "iso").lower()

        # Range-split config
        interval_unit: Optional[str] = src_cfg.get("watermark_range_interval_unit")
        interval_amount: int = int(src_cfg.get("watermark_range_interval_amount", 1))
        max_workers: int = int(src_cfg.get("watermark_range_max_workers", 4))

        # Shared timezone for the "now" used as wm_to_param upper-bound.
        # Source-level setting takes precedence over connection-level default.
        wm_to_tz = self._resolve_timezone(
            src_cfg.get("watermark_to_param_timezone")
            or conn_cfg.get("watermark_to_param_timezone")
        )

        if not interval_unit:
            # ----------------------------------------------------------------
            # Normal single-call path: inject stored watermark + upper bound
            # ----------------------------------------------------------------
            if watermark and wm_mapping:
                target = params if wm_location == "params" else body
                APIReader._inject_stored_watermark(target, wm_mapping, watermark, wm_format)

            if wm_to_param:
                to_val = datetime.now(tz=wm_to_tz)
                to_target = params if wm_location == "params" else body
                to_target[wm_to_param] = self._format_watermark_value(to_val, wm_format)

        headers = dict(conn_cfg.get("default_headers", {}))
        self._apply_auth(headers, conn_cfg)
        http_auth = self._get_http_auth(conn_cfg)

        # Warn when credentials are sent over plain HTTP
        auth_type = conn_cfg.get("auth_type", "")
        if auth_type and not url.lower().startswith("https"):
            logger.warning(
                "API credentials (auth_type=%s) sent over non-HTTPS URL: %s",
                auth_type,
                url,
            )

        with httpx.Client(timeout=timeout, headers=headers, auth=http_auth) as client:
            if interval_unit:
                # ------------------------------------------------------------
                # Range-split path: divide [from_dt, now) into sub-windows
                # and call the API concurrently for each window.
                # ------------------------------------------------------------
                if not wm_to_param:
                    raise SourceError(
                        "watermark_range_interval_unit requires 'watermark_to_param' "
                        "to be set so each range's upper bound can be sent to the API.",
                    )

                # Determine lower bound of the entire window
                from_dt: Optional[datetime] = APIReader._resolve_range_from_dt(
                    watermark, wm_mapping, src_cfg.get("watermark_range_start"),
                    tz=wm_to_tz,
                )

                if from_dt is None:
                    raise SourceError(
                        "watermark_range_interval_unit requires either a stored watermark "
                        "or 'watermark_range_start' in source.configure",
                    )

                to_dt = datetime.now(tz=wm_to_tz)
                # Normalise timezone so from_dt and to_dt are comparable
                if from_dt.tzinfo is None:
                    to_dt = to_dt.replace(tzinfo=None)

                ranges = self._build_watermark_ranges(from_dt, to_dt, interval_amount, interval_unit)
                if not ranges:
                    return []

                _overlap_mode = src_cfg.get("watermark_range_to_exclusive_offset")

                cfg = _RangeCallConfig(
                    client=client,
                    url=url,
                    method=method,
                    base_params=dict(params),
                    base_body=dict(body) if body else {},
                    wm_mapping=wm_mapping,
                    wm_to_param=wm_to_param,
                    wm_location=wm_location,
                    wm_format=wm_format,
                    overlap_mode=_overlap_mode,
                    src_cfg=src_cfg,
                )

                call = functools.partial(APIReader._execute_range_call, cfg)
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    batches = list(executor.map(call, ranges))

                return [rec for batch in batches for rec in batch]

            else:
                # Single-call path (watermark already injected above)
                return self._fetch_single_range(client, url, method, params, body or None, src_cfg)

    @staticmethod
    def _fetch_single_range(
        client: Any,
        url: str,
        method: str,
        params: Optional[Dict[str, Any]],
        body: Optional[Dict[str, Any]],
        src_cfg: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Fetch all pages for a single pre-configured request.

        Extracted as a static method so the range-split path can call it
        concurrently via :class:`ThreadPoolExecutor` without duplicating the
        pagination logic.

        Args:
            client:   Shared ``httpx.Client`` (thread-safe).
            url:      Full API URL for this range.
            method:   HTTP method (``"GET"``, ``"POST"``, …).
            params:   Query parameters with watermark values already injected.
            body:     Request body with watermark values already injected.
            src_cfg:  Source configure dict (for pagination settings).

        Returns:
            Flat list of record dicts collected across all pages.
        """
        pagination_type = src_cfg.get("pagination_type")

        # Concurrent offset path: fetch page 0 to get total, then remaining in parallel
        if pagination_type == _PAGINATION_OFFSET and src_cfg.get("total_path"):
            return APIReader._fetch_offset_concurrent(client, url, method, params, body, src_cfg)

        page_size = int(src_cfg.get("page_size", 100))
        max_pages = int(src_cfg.get("max_pages", 1000))
        data_path = src_cfg.get("data_path")
        rate_limit_delay = float(src_cfg.get("rate_limit_delay", 0))
        max_retries = int(src_cfg.get("max_retries", 10))

        # Work on local copies so callers' dicts are not mutated across pages
        local_params: Optional[Dict[str, Any]] = dict(params) if params is not None else {}
        local_body: Optional[Dict[str, Any]] = dict(body) if body is not None else {}

        current_url = url
        all_records: List[Dict[str, Any]] = []
        page = 0

        while page < max_pages:
            if pagination_type == _PAGINATION_OFFSET:
                offset_param = src_cfg.get("offset_param", "offset")
                limit_param = src_cfg.get("limit_param", "limit")
                local_params[offset_param] = page * page_size
                local_params[limit_param] = page_size

            response = APIReader._make_request(
                client, method, current_url,
                params=local_params,
                body=local_body or None,
                max_retries=max_retries,
            )
            data = response.json()

            records = APIReader._extract_records(data, data_path)
            if not records:
                break

            all_records.extend(records)

            if not pagination_type:
                break  # single-page fetch

            if pagination_type == _PAGINATION_NEXT_LINK:
                next_link_path = src_cfg.get("next_link_path", "next")
                next_url = APIReader._resolve_path(data, next_link_path)
                if not next_url:
                    break
                current_url = str(next_url)
                local_params = None  # next_link URL already contains all params

            elif pagination_type == _PAGINATION_CURSOR:
                cursor_path = src_cfg.get("cursor_path", "next_cursor")
                cursor_param = src_cfg.get("cursor_param", "cursor")
                cursor_value = APIReader._resolve_path(data, cursor_path)
                if not cursor_value:
                    break
                local_params[cursor_param] = str(cursor_value)

            elif pagination_type == _PAGINATION_OFFSET:
                if len(records) < page_size:
                    break  # last page

            page += 1

            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        return all_records

    @staticmethod
    def _fetch_offset_concurrent(
        client: Any,
        url: str,
        method: str,
        params: Optional[Dict[str, Any]],
        body: Optional[Dict[str, Any]],
        src_cfg: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Fetch all offset pages concurrently using a record total from the first page.

        Called automatically by :meth:`_fetch_single_range` when
        ``pagination_type="offset"`` and ``total_path`` is set.  The first page
        is fetched sequentially to learn the total record count; all remaining
        pages are then dispatched in parallel via :class:`ThreadPoolExecutor`.

        ``rate_limit_delay`` is not applied here — requests run concurrently
        so sequential throttling is not meaningful.

        Args:
            client:   Shared ``httpx.Client`` (thread-safe).
            url:      Full API URL.
            method:   HTTP method (``"GET"``, ``"POST"``, …).
            params:   Query parameters with watermark values already injected.
            body:     Request body with watermark values already injected.
            src_cfg:  Source configure dict.

        Returns:
            Flat list of record dicts from all pages, in page order
            (page 0, page 1, page 2, …).

        Raises:
            SourceError: When ``total_path`` resolves to a value that cannot be
                converted to an integer.
        """
        page_size = int(src_cfg.get("page_size", 100))
        max_pages = int(src_cfg.get("max_pages", 1000))
        data_path = src_cfg.get("data_path")
        total_path: str = src_cfg["total_path"]  # guaranteed set by caller
        offset_param: str = src_cfg.get("offset_param", "offset")
        limit_param: str = src_cfg.get("limit_param", "limit")
        max_workers = int(src_cfg.get("offset_max_workers", 4))
        max_retries = int(src_cfg.get("max_retries", 10))

        # --- Page 0: fetch first page and discover total record count ---
        p0_params = dict(params) if params is not None else {}
        p0_body = dict(body) if body is not None else {}
        p0_params[offset_param] = 0
        p0_params[limit_param] = page_size

        response = APIReader._make_request(
            client, method, url, params=p0_params, body=p0_body or None,
            max_retries=max_retries,
        )
        data = response.json()
        page0_records = APIReader._extract_records(data, data_path)

        if not page0_records:
            return []

        total_raw = APIReader._resolve_path(data, total_path)
        try:
            total = int(total_raw)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise SourceError(
                f"APIReader: total_path={total_path!r} resolved to {total_raw!r}, "
                f"which cannot be converted to int.",
                details={"total_path": total_path, "resolved": total_raw},
            ) from exc

        total_pages = min((total + page_size - 1) // page_size, max_pages)
        logger.debug(
            "APIReader concurrent offset: total=%d page_size=%d total_pages=%d workers=%d",
            total, page_size, total_pages, max_workers,
        )

        if total_pages <= 1:
            # Only one page worth of data — no further requests needed
            return list(page0_records)

        # --- Pages 1..(total_pages-1): fetch concurrently ---
        def fetch_page(page_num: int) -> List[Dict[str, Any]]:
            p_params = dict(params) if params is not None else {}
            p_body = dict(body) if body is not None else {}
            p_params[offset_param] = page_num * page_size
            p_params[limit_param] = page_size
            resp = APIReader._make_request(
                client, method, url, params=p_params, body=p_body or None,
                max_retries=max_retries,
            )
            return APIReader._extract_records(resp.json(), data_path)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            batches = list(executor.map(fetch_page, range(1, total_pages)))

        all_records: List[Dict[str, Any]] = list(page0_records)
        for batch in batches:
            all_records.extend(batch)
        return all_records

    # ------------------------------------------------------------------
    # Watermark helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _inject_stored_watermark(
        target: Dict[str, Any],
        wm_mapping: Dict[str, str],
        watermark: Dict[str, Any],
        wm_format: str,
    ) -> None:
        """Inject stored watermark values into *target* (params or body dict).

        For each ``{wm_col: api_param}`` pair in *wm_mapping*, looks up
        ``wm_col`` in *watermark* and, when a non-``None`` value is found,
        serialises it with :meth:`_format_watermark_value` and writes it into
        *target* under ``api_param``.  Mutates *target* in place.

        Only ``date``, ``datetime``, and ISO-8601 string values are accepted.
        Non-temporal types (e.g. integers) raise :class:`SourceError` because
        the serialisation formats (``iso``, ``timestamp``, etc.) are
        meaningless for non-temporal values.

        Raises:
            SourceError: When a pushed watermark value is not a ``date``,
                ``datetime``, or parseable ISO-8601 string.
        """
        for wm_col, api_param in wm_mapping.items():
            wm_val = watermark.get(wm_col)
            if wm_val is None:
                continue
            if not isinstance(wm_val, (date, datetime, str)):
                raise SourceError(
                    f"Watermark push-down only supports date/datetime values. "
                    f"Column {wm_col!r} has unsupported type {type(wm_val).__name__!r}.",
                    details={"column": wm_col, "type": type(wm_val).__name__, "api_param": api_param},
                )
            if isinstance(wm_val, str):
                try:
                    datetime.fromisoformat(wm_val)
                except ValueError as exc:
                    raise SourceError(
                        f"Watermark push-down column {wm_col!r} string value is not "
                        f"a valid ISO-8601 date/datetime: {wm_val!r}.",
                        details={"column": wm_col, "value": wm_val, "api_param": api_param},
                    ) from exc
            target[api_param] = APIReader._format_watermark_value(wm_val, wm_format)
            logger.debug(
                "APIReader watermark push-down: %s=%s (from %s)",
                api_param, target[api_param], wm_col,
            )

    @staticmethod
    def _execute_range_call(
        cfg: _RangeCallConfig,
        r_from_to: Tuple[datetime, datetime],
    ) -> List[Dict[str, Any]]:
        """Execute one sub-range API call using a shared :class:`_RangeCallConfig`.

        Used as the per-item callable in :meth:`concurrent.futures.Executor.map`.
        Each call gets its own copies of ``params`` and ``body`` so mutations
        across concurrent calls are isolated.

        Args:
            cfg:       Frozen configuration shared across all range calls.
            r_from_to: ``(range_start, range_end)`` for this sub-range.

        Returns:
            All records fetched for this sub-range across all pages.
        """
        r_from, r_to = r_from_to
        r_params = dict(cfg.base_params)
        r_body = dict(cfg.base_body)
        r_target = r_params if cfg.wm_location == "params" else r_body

        for api_param in cfg.wm_mapping.values():
            r_target[api_param] = APIReader._format_watermark_value(r_from, cfg.wm_format)

        r_to_sent = APIReader._adjust_range_to(r_to, cfg.overlap_mode)
        r_target[cfg.wm_to_param] = APIReader._format_watermark_value(r_to_sent, cfg.wm_format)

        first_api_param = next(iter(cfg.wm_mapping.values()), "from")
        logger.debug(
            "APIReader range call: %s=%s %s=%s",
            first_api_param, r_target.get(first_api_param),
            cfg.wm_to_param, r_target.get(cfg.wm_to_param),
        )

        return APIReader._fetch_single_range(
            cfg.client, cfg.url, cfg.method, r_params, r_body or None, cfg.src_cfg,
        )

    @staticmethod
    def _format_watermark_value(
        value: Union[datetime, date, str],
        fmt: str,
    ) -> str:
        """Serialise a watermark value to the string format the API expects.

        Args:
            value: A ``datetime`` or an ISO-8601 string (as stored by WatermarkManager).
            fmt:   ``"iso"`` | ``"datetime"`` | ``"datetime_ms"`` | ``"date"``
                   | ``"timestamp"`` | ``"timestamp_ms"``

        Returns:
            String representation ready to be set as a query param or body field.
        """
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                # Unparseable string — pass through unchanged
                return value

        if type(value) is date:  # exact check — datetime IS-A date, must come before datetime block
            if fmt == "date":
                return value.isoformat()
            value = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)

        if not isinstance(value, datetime):
            return str(value)

        if fmt == "date":
            return value.date().isoformat()
        if fmt == "timestamp":
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return str(value.timestamp())
        if fmt == "timestamp_ms":
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return str(int(value.timestamp() * 1000))
        if fmt == "datetime":
            # ISO datetime without timezone, second precision (e.g. "2024-01-15T12:30:45")
            return value.replace(tzinfo=None, microsecond=0).isoformat()
        if fmt == "datetime_ms":
            # ISO datetime without timezone, millisecond precision (e.g. "2024-01-15T12:30:45.123")
            naive = value.replace(tzinfo=None)
            return naive.strftime("%Y-%m-%dT%H:%M:%S.") + f"{naive.microsecond // 1000:03d}"
        # "iso" (default) — preserve whatever timezone state the datetime has
        return value.isoformat()

    @staticmethod
    def _resolve_timezone(tz_str: Optional[str]) -> Any:
        """Resolve a timezone string to a :class:`~datetime.tzinfo` object.

        Accepts IANA timezone names (e.g. ``"Asia/Ho_Chi_Minh"``, ``"UTC"``)
        when :mod:`zoneinfo` is available (Python 3.9+), and ``±HH:MM`` UTC
        offset strings (e.g. ``"+07:00"``, ``"-05:30"``) as a universal
        fallback.  Returns :data:`~datetime.timezone.utc` when *tz_str* is
        ``None`` or empty.

        Args:
            tz_str: Timezone name or UTC offset string, or ``None``.

        Returns:
            A :class:`~datetime.tzinfo` compatible with :func:`datetime.now`.

        Raises:
            SourceError: When *tz_str* is non-empty but cannot be parsed.
        """
        if not tz_str:
            return timezone.utc

        # Try IANA name via stdlib zoneinfo (Python 3.9+)
        if _HAS_ZONEINFO and _ZoneInfo is not None:
            try:
                return _ZoneInfo(tz_str)
            except Exception as exc:
                logger.debug("ZoneInfo lookup failed for '%s', trying offset: %s", tz_str, exc)

        # Try UTC offset format: ±HH:MM or ±HHMM
        m = re.match(r"^([+-])(\d{1,2}):?(\d{2})$", tz_str.strip())
        if m:
            sign = 1 if m.group(1) == "+" else -1
            hours = int(m.group(2))
            minutes = int(m.group(3))
            return timezone(timedelta(hours=sign * hours, minutes=sign * minutes))

        raise SourceError(
            f"Cannot resolve timezone {tz_str!r}. "
            "Use an IANA name (e.g. 'Asia/Ho_Chi_Minh') or a UTC offset (e.g. '+07:00').",
        )

    @staticmethod
    def _adjust_range_to(dt: datetime, mode: Optional[str]) -> datetime:
        """Subtract a small epsilon from *dt* before sending it to the API.

        Prevents duplicate rows at range split boundaries when the API uses
        inclusive ``BETWEEN from AND to`` semantics.  The adjusted value is
        only sent to the API; the internal boundary used to start the next
        sub-range in :meth:`_build_watermark_ranges` is unchanged so that
        adjacent ranges remain contiguous with no gaps.

        Args:
            dt:   Upper-bound datetime for a single sub-range.
            mode: ``"1ms"`` (1 millisecond), ``"1s"`` (1 second),
                  ``"1day"`` (1 day), or ``None`` (no adjustment, default).

        Returns:
            Adjusted :class:`~datetime.datetime`, or *dt* unchanged when
            *mode* is ``None`` or unrecognised.
        """
        if mode == "1ms":
            return dt - timedelta(milliseconds=1)
        if mode == "1s":
            return dt - timedelta(seconds=1)
        if mode == "1day":
            return dt - timedelta(days=1)
        return dt

    @staticmethod
    def _build_watermark_ranges(
        from_dt: datetime,
        to_dt: datetime,
        amount: int,
        unit: str,
    ) -> List[Tuple[datetime, datetime]]:
        """Split ``[from_dt, to_dt)`` into equal-sized intervals.

        Args:
            from_dt: Start of the overall window (inclusive).
            to_dt:   End of the overall window (exclusive).
            amount:  Number of ``unit`` per interval (e.g. ``3`` for 3 hours).
            unit:    One of ``"hour"``, ``"day"``, ``"month"``, ``"year"``.

        Returns:
            List of ``(range_start, range_end)`` tuples covering the window
            without gaps or overlaps.  The final range's end is clamped to
            ``to_dt``.  Returns an empty list if ``from_dt >= to_dt``.
        """
        if from_dt >= to_dt:
            return []

        if unit not in ("hour", "day", "month", "year"):
            raise SourceError(
                f"watermark_range_interval_unit must be one of "
                f"'hour', 'day', 'month', 'year' — got {unit!r}",
            )

        ranges: List[Tuple[datetime, datetime]] = []
        current = from_dt

        while current < to_dt:
            if unit in ("month", "year"):
                if _HAS_DATEUTIL:
                    kwargs = {"months": amount} if unit == "month" else {"years": amount}
                    next_dt = current + _relativedelta(**kwargs)
                else:
                    # Fallback approximation when dateutil is unavailable
                    days = 30 * amount if unit == "month" else 365 * amount
                    next_dt = current + timedelta(days=days)
            elif unit == "day":
                next_dt = current + timedelta(days=amount)
            else:  # "hour"
                next_dt = current + timedelta(hours=amount)

            end = min(next_dt, to_dt)
            ranges.append((current, end))
            current = next_dt  # full step — next range starts right after, clamping handled above

        return ranges

    @staticmethod
    def _resolve_range_from_dt(
        watermark: Optional[Dict[str, Any]],
        wm_mapping: Dict[str, str],
        range_start_str: Optional[str],
        tz: Any = timezone.utc,
    ) -> Optional[datetime]:
        """Determine the lower bound of a range-split window.

        Iterates *all* keys in *wm_mapping* and returns the first non-``None``
        watermark value as a :class:`~datetime.datetime`, coercing ISO-8601
        strings and ``date`` objects as needed.  Falls back to parsing
        *range_start_str* when no stored watermark value is found.

        Args:
            watermark:        Previously stored watermark dict, or ``None``.
            wm_mapping:       ``{wm_col: api_param}`` push-down mapping.
            range_start_str:  ISO-8601 fallback start for the first run.
            tz:               Timezone attached to naive datetime values parsed
                              from ISO strings or promoted from ``date`` objects.
                              Defaults to ``timezone.utc``.

        Returns:
            A ``datetime`` representing the lower bound, or ``None`` when
            neither the watermark nor *range_start_str* is available.
        """
        if watermark and wm_mapping:
            for col in wm_mapping:
                raw = watermark.get(col)
                if raw is None:
                    continue
                if isinstance(raw, str):
                    dt = datetime.fromisoformat(raw)
                    return dt if dt.tzinfo is not None else dt.replace(tzinfo=tz)
                if type(raw) is date:  # exact — datetime IS-A date
                    return datetime(raw.year, raw.month, raw.day, tzinfo=tz)
                return raw  # already a datetime

        if range_start_str:
            dt = datetime.fromisoformat(range_start_str)
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=tz)

        return None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_request(
        client: Any,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
        max_retries: int = 10,
    ) -> Any:
        """Execute a single HTTP request with retry handling for HTTP 429 responses.

        Retries up to *max_retries* times when the server returns HTTP 429.
        The wait duration per retry is determined by:

        * **``Retry-After`` header** — used verbatim when present.
        * **Exponential backoff** — ``min(2 ** attempt, 30)`` seconds when the
          header is absent (1 s, 2 s, 4 s, 8 s, 16 s, 30 s, …).

        Args:
            client:      Shared ``httpx.Client`` (thread-safe).
            method:      HTTP method string.
            url:         Request URL.
            params:      Query parameters.
            body:        JSON request body.
            max_retries: Maximum 429 retries before raising :class:`SourceError`
                         (default ``10``).
        """
        for attempt in range(max_retries + 1):
            try:
                response = client.request(method, url, params=params, json=body)
            except httpx.HTTPError as exc:
                msg = (
                    f"HTTP request failed after retry {attempt}: {exc}"
                    if attempt > 0
                    else f"HTTP request failed: {exc}"
                )
                raise SourceError(msg, details={"url": url, "method": method}) from exc

            if response.status_code != 429:
                break

            if attempt == max_retries:
                raise SourceError(
                    f"API returned HTTP 429 after {max_retries} retries",
                    details={"url": url, "retries": max_retries},
                )

            retry_after = response.headers.get("Retry-After")
            wait = float(retry_after) if retry_after is not None else min(2 ** attempt, 30)
            logger.warning(
                "Rate limited (429), attempt %d/%d. Waiting %.1fs before retry.",
                attempt + 1, max_retries, wait,
            )
            time.sleep(wait)

        if response.status_code >= 400:
            raise SourceError(
                f"API returned HTTP {response.status_code}",
                details={"url": url, "status": response.status_code, "body": response.text[:500]},
            )

        return response

    @staticmethod
    def _apply_auth(headers: Dict[str, str], conn_cfg: Dict[str, Any]) -> None:
        """Apply authentication to request headers."""
        auth_type = conn_cfg.get("auth_type", "").lower()

        if auth_type == _AUTH_BEARER:
            token = conn_cfg.get("auth_token", "")
            if token:
                headers["Authorization"] = f"Bearer {token}"

        elif auth_type == _AUTH_BASIC:
            username = conn_cfg.get("username", "")
            password = conn_cfg.get("password", "")
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"

        elif auth_type == _AUTH_API_KEY:
            key_header = conn_cfg.get("api_key_header", "X-API-Key")
            key_value = conn_cfg.get("api_key_value", "")
            if key_value:
                headers[key_header] = key_value

        elif auth_type == _AUTH_OAUTH2_CLIENT_CREDENTIALS:
            token = APIReader._fetch_oauth2_token(conn_cfg)
            if token:
                headers["Authorization"] = f"Bearer {token}"
        # aws_sigv4 is handled by _get_http_auth; nothing to set in headers here.

    @staticmethod
    def _get_http_auth(conn_cfg: Dict[str, Any]) -> Optional[Any]:
        """Return an httpx Auth handler for auth types that need per-request signing.

        Currently only ``aws_sigv4`` returns a handler; all other auth types
        are handled via plain headers in ``_apply_auth`` and return ``None``.
        """
        auth_type = conn_cfg.get("auth_type", "").lower()
        if auth_type != _AUTH_AWS_SIGV4:
            return None

        if not _HAS_BOTOCORE:
            raise SourceError(
                "auth_type='aws_sigv4' requires botocore. Install it with: pip install botocore"
            )

        region = conn_cfg.get("aws_region", "us-east-1")
        service = conn_cfg.get("aws_service", "execute-api")

        # Build a botocore session so the standard credential chain is honoured.
        # Explicit key overrides (e.g. from secrets_ref) take priority.
        session = _boto_session.Session()
        resolver = session.get_component("credential_provider")
        creds = resolver.load()

        key_id = conn_cfg.get("aws_access_key_id")
        secret = conn_cfg.get("aws_secret_access_key")
        token = conn_cfg.get("aws_session_token")

        if key_id and secret:
            creds = botocore.credentials.Credentials(
                access_key=key_id,
                secret_key=secret,
                token=token or None,
            )
        elif creds is None:
            raise SourceError(
                "auth_type='aws_sigv4': no AWS credentials found. "
                "Set aws_access_key_id/aws_secret_access_key in connection.configure or "
                "configure the environment (AWS_ACCESS_KEY_ID, instance profile, etc.)."
            )

        return _SigV4Auth(creds, service, region)

    @staticmethod
    def _fetch_oauth2_token(conn_cfg: Dict[str, Any]) -> str:
        """Fetch a short-lived Bearer token using the OAuth2 client credentials flow.

        Expected connection configure keys:
            - ``token_url`` (str, required): Token endpoint URL.
            - ``client_id`` (str, required): OAuth2 client ID.
            - ``client_secret`` (str, required): OAuth2 client secret — should be
              resolved from a secret reference before this is called.
            - ``scope`` (str, optional): Space-separated scopes.
            - ``token_auth_method`` (str, optional): ``"client_secret_post"`` (default)
              includes credentials in the POST body; ``"client_secret_basic"`` sends
              them via HTTP Basic Auth (required by Okta, Ping Identity, etc.).
            - ``token_request_body_format`` (str, optional): ``"form"`` (default,
              ``application/x-www-form-urlencoded``) or ``"json"``
              (``application/json``) — required by GitHub Apps and some others.
            - ``token_request_extras`` (dict, optional): Extra POST body fields
              forwarded verbatim to the token endpoint (e.g. ``audience`` for Auth0).

        The result is cached in-process keyed by ``(token_url, client_id)``.
        Subsequent calls with the same credentials return the cached token
        without a network round-trip until 30 seconds before expiry.

        Returns the ``access_token`` string.
        """
        token_url = conn_cfg.get("token_url", "")
        client_id = conn_cfg.get("client_id", "")
        client_secret = conn_cfg.get("client_secret", "")

        if not token_url:
            raise SourceError(
                "auth_type='oauth2_client_credentials' requires 'token_url' in connection.configure",
            )
        if not client_id or not client_secret:
            raise SourceError(
                "auth_type='oauth2_client_credentials' requires 'client_id' and "
                "'client_secret' in connection.configure",
            )

        cache_key = (token_url, client_id)
        with _OAUTH2_TOKEN_CACHE_LOCK:
            cached = _OAUTH2_TOKEN_CACHE.get(cache_key)
            if cached is not None:
                cached_token, expires_at = cached
                if time.monotonic() < expires_at:
                    logger.debug("OAuth2 token served from cache for %s", token_url)
                    return cached_token

        auth_method = conn_cfg.get("token_auth_method", "client_secret_post").lower()
        body_format = conn_cfg.get("token_request_body_format", "form").lower()

        payload: Dict[str, Any] = {"grant_type": "client_credentials"}
        scope = conn_cfg.get("scope", "")
        if scope:
            payload["scope"] = scope
        extras = conn_cfg.get("token_request_extras") or {}
        payload.update(extras)

        # client_secret_post: credentials go in the body (RFC 6749 §2.3.1, method 2)
        # client_secret_basic: credentials go in HTTP Basic Auth header (method 1)
        request_kwargs: Dict[str, Any] = {"timeout": 30}
        if auth_method == "client_secret_basic":
            request_kwargs["auth"] = (client_id, client_secret)
        else:  # client_secret_post (default)
            payload["client_id"] = client_id
            payload["client_secret"] = client_secret

        if body_format == "json":
            request_kwargs["json"] = payload
        else:  # form (default, application/x-www-form-urlencoded)
            request_kwargs["data"] = payload

        try:
            response = httpx.post(token_url, **request_kwargs)
        except httpx.HTTPError as exc:
            raise SourceError(
                f"OAuth2 token request failed: {exc}",
                details={"token_url": token_url},
            ) from exc

        if response.status_code >= 400:
            raise SourceError(
                f"OAuth2 token endpoint returned HTTP {response.status_code}",
                details={"token_url": token_url, "body": response.text[:500]},
            )

        resp_json = response.json()
        token = resp_json.get("access_token", "")
        if not token:
            raise SourceError(
                "OAuth2 token response did not contain 'access_token'",
                details={"token_url": token_url, "body": response.text[:500]},
            )

        expires_in = float(resp_json.get("expires_in", 3600))
        with _OAUTH2_TOKEN_CACHE_LOCK:
            _OAUTH2_TOKEN_CACHE[cache_key] = (token, time.monotonic() + expires_in - 30)
        logger.debug("OAuth2 token fetched and cached for %s (expires_in=%.0fs)", token_url, expires_in)
        return token

    # ------------------------------------------------------------------
    # Data extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_records(
        data: Any,
        data_path: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Extract the records list from the API response JSON.

        If *data_path* is provided (e.g. ``"data.items"``), drills into
        the nested response. Otherwise expects a top-level list.
        """
        target = data if data_path is None else APIReader._resolve_path(data, data_path)

        if isinstance(target, list):
            return target
        if isinstance(target, dict):
            return [target]
        return []

    @staticmethod
    def _resolve_path(data: Any, path: str) -> Any:
        """Resolve a dot-separated path into a nested dict/list."""
        current = data
        for key in path.split("."):
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list) and key.isdigit():
                idx = int(key)
                current = current[idx] if idx < len(current) else None
            else:
                return None
            if current is None:
                return None
        return current

    def _records_to_dataframe(self, records: List[Dict[str, Any]]) -> DF:
        """Convert a list of dicts to a DataFrame via the engine.

        Delegates to :meth:`BaseEngine.create_dataframe` which handles
        heterogeneous records (different keys per row) by filling missing
        fields with ``null`` and unioning all schemas.
        """
        try:
            return self._engine.create_dataframe(records)
        except Exception as exc:
            raise SourceError(
                f"Failed to convert API records to DataFrame: {exc}",
            ) from exc
