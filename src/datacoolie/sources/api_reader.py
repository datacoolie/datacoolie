"""Public API source reader facade.

The reader owns source configuration, DataFrame conversion, and runtime state.
HTTP/auth/pagination/watermark/record helpers live in the private ``_api``
package so the public component remains easy to discover.
"""

from __future__ import annotations

import functools
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models.source import Source
from datacoolie.core.secrets.provider import unwrap_configure
from datacoolie.engines.base import DF
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.sources.base import BaseSourceReader
from datacoolie.sources._api.auth import apply_auth, get_http_auth
from datacoolie.sources._api.pagination import (
    _RangeCallConfig,
    execute_range_call,
    fetch_single_range,
    validate_pagination_type,
)
from datacoolie.sources._api.ranges import (
    APIParamBinding,
    APIRangeBinding,
    NormalizedAPIRangeMapping,
    ReadRangeSpec,
    ResidualFilter,
    coerce_read_range,
    filter_records_by_ranges,
    format_bound,
    normalize_api_range_mapping,
    validate_operator_coverage,
)
from datacoolie.sources._api.transport import safe_url
from datacoolie.sources._api.watermark import (
    build_watermark_ranges,
    format_watermark_value,
    inject_stored_watermark,
    resolve_range_from_dt,
    resolve_timezone,
)
from datacoolie.utils.chunking import validate_iso_fraction_precision

logger = get_logger(__name__)
_DATETIME_TYPE = datetime


def validate_api_bounded_range(source: Source, column: str) -> None:
    """Validate the API-owned mapping needed for an exact bounded read.

    The selected field can be a selection-only field; it does not need to be
    listed in ``source.watermark_columns``. Persisted state remains limited to
    the authored watermark columns in the reader's runtime path.
    """

    mapping = normalize_api_range_mapping(source.configure)
    if not mapping.is_new:
        raise SourceError(
            "API bounded replay requires range_param_mapping; legacy "
            "watermark_param_mapping cannot express an independent [start, end) range."
        )

    binding = mapping.fields.get(column)
    if binding is None:
        raise SourceError(
            f"API replay chunk_column {column!r} must have a matching "
            "range_param_mapping field."
        )
    if binding.lower is None or binding.upper is None:
        raise SourceError(
            f"API replay chunk_column {column!r} requires both lower and upper "
            "range_param_mapping bindings."
        )

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]


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

        Bounded source ranges (``source.configure.range_param_mapping``):
            Each mapped field can declare separate lower and upper endpoint
            parameters, their operators, wire format, response column, and
            watermark meaning (``"observed_max"`` or ``"request_end"``).
            Replay may select a mapped field even when it is not listed in
            ``source.watermark_columns``; the mapping controls selection while
            only authored watermark columns are eligible for persisted state.
            A bounded replay requires both lower and upper bindings and uses
            the exact ``[start, end)`` range. The legacy
            ``watermark_param_mapping`` form remains for incremental reads but
            cannot express this independent bounded contract.

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

    def _resolve_watermark_operators(
        self,
        source: Source,
        start_operator: Optional[str],
        end_operator: Optional[str],
    ) -> tuple[str, str]:
        """Resolve omitted lower semantics from API watermark state kind."""

        resolved_end = end_operator if end_operator is not None else "<"
        mapping = normalize_api_range_mapping(source.configure)
        stored = getattr(self, "_pending_watermark_start", None)
        pending_end = getattr(self, "_pending_watermark_end", None)
        active_kinds = {
            binding.watermark_value
            for field, binding in mapping.fields.items()
            if (stored and stored.get(field) is not None)
            or (pending_end and pending_end.get(field) is not None)
        }
        if len(active_kinds) > 1:
            raise SourceError(
                "API active range fields cannot mix observed_max and request_end "
                "watermark semantics."
            )
        if start_operator is not None:
            if start_operator == ">" and active_kinds == {"request_end"}:
                raise SourceError(
                    "Explicit '>' lower operator is incompatible with API "
                    "request_end watermark state; use '>=' or omit it."
                )
            return super()._resolve_watermark_operators(
                source, start_operator, resolved_end
            )

        resolved_start = ">=" if active_kinds == {"request_end"} else ">"
        return super()._resolve_watermark_operators(
            source, resolved_start, resolved_end
        )

    def _validate_watermark_comparison(
        self,
        watermark: Optional[Dict[str, Any]],
        *,
        boundary: str = "watermark",
    ) -> None:
        """Validate raw new-binding ISO precision before look-back parsing.

        ``BaseSourceReader`` applies ``date_backward`` before the API binding
        compiler sees a stored lower bound.  Validate the caller-owned string
        first so ``datetime.fromisoformat`` cannot silently discard digits.
        The check is limited to new range mappings; legacy API formatting
        retains its established behavior.
        """

        super()._validate_watermark_comparison(watermark, boundary=boundary)
        source = getattr(self, "_active_source", None)
        if source is None or not watermark:
            return
        mapping = normalize_api_range_mapping(source.configure)
        if not mapping.is_new:
            return
        side = "upper" if "upper" in boundary else "lower"
        for field, binding in mapping.fields.items():
            value = watermark.get(field)
            if value is None or not isinstance(value, str):
                continue
            self._validate_iso_bound_precision(
                value,
                field=field,
                side=side,
                fmt=binding.format,
            )

    @staticmethod
    def _validate_iso_bound_precision(
        value: str,
        *,
        field: str,
        side: str,
        fmt: str,
    ) -> None:
        try:
            validate_iso_fraction_precision(value)
        except ValueError as exc:
            raise SourceError(
                f"API {fmt} range {side} bound for field {field!r} "
                f"cannot be represented without loss: {exc}",
                details={"field": field, "side": side, "format": fmt},
            ) from exc

    def _supports_read_range(self) -> bool:
        return True

    def _watermark_ordering_kinds(self, candidate: Dict[str, Any]) -> Dict[str, str]:
        """Authorize ordering from the API binding's declared wire format."""
        # Legacy API mappings still calculate observed maxima from response
        # columns that are not bound to request parameters. Preserve the
        # built-in typed-row contract for those fields; strings remain opaque.
        kinds: Dict[str, str] = self._typed_row_watermark_ordering_kinds(candidate)
        source = self._active_source
        if source is None:
            return kinds
        mapping = normalize_api_range_mapping(source.configure)
        for field, binding in mapping.fields.items():
            if field not in candidate or field not in (source.watermark_columns or []):
                continue
            if binding.format in {"integer", "int", "native_integer", "number"}:
                kinds[field] = "numeric"
            else:
                kinds[field] = "temporal"
        return kinds

    def _read_internal(
        self,
        source: Source,
        watermark_start: Optional[Dict[str, Any]] = None,
        *,
        watermark_end: Optional[Dict[str, Any]] = None,
        read_range: Any = None,
    ) -> Optional[DF]:
        action: dict = {"reader": type(self).__name__}
        conn_cfg = source.connection.configure
        src_cfg = source.configure

        # ``read_range`` is introduced by the source contract without making
        # API metadata responsible for importing the shared model.  Keep a
        # fallback for an older BaseSourceReader that stores it on the reader
        # while the public signature is being migrated by the coordinator.
        if read_range is None:
            read_range = getattr(self, "_read_range", None)
        normalized_mapping = normalize_api_range_mapping(src_cfg)
        normalized_read_range = coerce_read_range(read_range)
        self._set_runtime_watermark_kind(
            normalized_mapping,
            source=source,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            read_range=normalized_read_range,
        )
        if normalized_mapping.is_new or normalized_read_range is not None:
            return self._read_new_range(
                source,
                watermark_start,
                watermark_end=watermark_end,
                read_range=normalized_read_range,
                mapping=normalized_mapping,
            )

        base_url = conn_cfg.get("base_url", "").rstrip("/")
        endpoint = src_cfg.get("endpoint", "")
        url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url
        action["url"] = safe_url(url)
        self._set_source_action(action)

        if not base_url:
            raise SourceError(
                "APIReader requires 'base_url' in connection.configure",
                details={"connection": source.connection.name},
            )

        wm_mapping = src_cfg.get("watermark_param_mapping") or {}
        wm_to_param = src_cfg.get("watermark_to_param")
        request_watermark_end = dict(watermark_end or {})

        records = self._read_data(
            source,
            watermark_start=watermark_start,
            watermark_end=request_watermark_end or watermark_end,
        )

        # Legacy request-end resolution lives in the raw API path so explicit
        # active-field bounds are not replaced by a synthetic ``now`` value.
        # The context is also the source of truth when a shared upper
        # parameter was applied to all legacy mapped fields on an unbounded
        # incremental read.
        request_watermark_end = dict(
            getattr(self, "_api_read_context", {}).get(
                "effective_end", request_watermark_end
            )
        )

        if not records:
            logger.debug(
                "APIReader: 0 records fetched — skipping. Table: %s (format: %s), URL: %s",
                source.full_table_name,
                source.connection.format,
                safe_url(url),
            )
            self._set_rows_read(0)
            self._set_new_watermark({})
            return None

        df = self._records_to_dataframe(records)

        # Identify which watermark columns are pushed down to the API
        # (the API filters them server-side so they may be absent from the response).
        _wm_mapping = wm_mapping
        # A lower-bound mapping alone cannot justify advancing to the current
        # time: no request upper bound would have been applied. In that mode,
        # use any returned watermark column for row-derived progress instead.
        _pushed_cols = set(_wm_mapping) if wm_to_param else set()
        _non_pushed_cols = [c for c in (source.watermark_columns or []) if c not in _pushed_cols]

        if (watermark_start or watermark_end) and source.watermark_columns:
            # Apply in-memory filter only for columns not already pushed to the API.
            if _non_pushed_cols:
                df = self._apply_watermark_filter(df, _non_pushed_cols, watermark_start or {}, watermark_end)

        df = self._apply_filter_expression(df, source)

        # Compute count and max watermark only from columns present in the
        # dataframe (non-pushed). Pushed-down columns use the exact upper bound
        # sent with the request.
        count, new_wm = self._calculate_count_and_new_watermark(
            df, _non_pushed_cols,
        )

        # Advance pushed-down columns — they are filtered server-side and may
        # not appear in the response dataframe at all.
        # Use the same effective upper bound as the request so that
        # source_runtime.watermark_after matches what the driver will save.
        if count > 0 and _pushed_cols and source.watermark_columns:
            for _col in source.watermark_columns:
                if _col in _pushed_cols and request_watermark_end.get(_col) is not None:
                    new_wm[_col] = request_watermark_end[_col]

        self._set_new_watermark(new_wm)
        self._set_rows_read(count)

        if count == 0:
            if getattr(self, "_preserve_empty", False):
                logger.debug(
                    "APIReader: 0 rows after filtering — preserving typed empty frame. "
                    "Table: %s (format: %s), URL: %s",
                    source.full_table_name,
                    source.connection.format,
                    safe_url(url),
                )
                return df
            logger.debug(
                "APIReader: 0 rows after filtering — skipping. Table: %s (format: %s), URL: %s",
                source.full_table_name,
                source.connection.format,
                safe_url(url),
            )
            return None

        logger.debug("APIReader: read %d rows from %s", count, safe_url(url))
        return df

    def _read_new_range(
        self,
        source: Source,
        watermark_start: Optional[Dict[str, Any]],
        *,
        watermark_end: Optional[Dict[str, Any]],
        read_range: Optional[ReadRangeSpec],
        mapping: NormalizedAPIRangeMapping,
    ) -> Optional[DF]:
        """Read a new per-field range binding or an explicit source range."""
        self._set_runtime_watermark_kind(
            mapping,
            source=source,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            read_range=read_range,
        )
        self._api_read_context = {}
        records = self._read_data(
            source,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            read_range=read_range,
        )
        if not records:
            self._set_rows_read(0)
            self._set_new_watermark({})
            return None

        df = self._records_to_dataframe(records)
        df = self._apply_filter_expression(df, source)
        # A request-end field advances only from the endpoint's confirmed
        # covered end below. Never reinterpret a row maximum as a request-end
        # watermark when a first/unbounded read has no explicit covered end.
        request_end_fields = {
            field
            for field, binding in mapping.fields.items()
            if binding.watermark_value == "request_end"
        }
        observed_columns = [
            column
            for column in (source.watermark_columns or [])
            if column not in request_end_fields
        ]
        count, new_wm = self._calculate_count_and_new_watermark(
            df,
            observed_columns,
        )

        # ``request_end`` is a source contract, not an observed row maximum.
        # It advances only after pagination completed successfully and after
        # residual filtering has produced at least one row.  Empty reads are
        # handled above and intentionally do not fabricate progress.
        if count == 0:
            # A filtered or empty response does not prove request-end
            # coverage.  Leave the candidate empty even when a typed empty
            # frame is retained for replacement.
            self._set_new_watermark({})
            self._set_rows_read(0)
            if getattr(self, "_preserve_empty", False):
                return df
            return None

        for column, value in self._api_read_context.get("watermark_updates", {}).items():
            if column in (source.watermark_columns or []):
                new_wm[column] = value

        self._set_new_watermark(new_wm)
        self._set_rows_read(count)
        return df

    def _set_runtime_watermark_kind(
        self,
        mapping: NormalizedAPIRangeMapping,
        *,
        source: Source,
        watermark_start: Optional[Dict[str, Any]],
        watermark_end: Optional[Dict[str, Any]],
        read_range: Optional[ReadRangeSpec],
    ) -> None:
        """Expose the state interpretation used by replacement windows."""

        if read_range is not None:
            self._runtime_info.watermark_kind = None
            return
        active = [
            binding.watermark_value
            for field, binding in mapping.fields.items()
            if (watermark_start or {}).get(field) is not None
            or (watermark_end or {}).get(field) is not None
        ]
        # Columns tracked as ordinary watermarks but absent from the API
        # mapping still use observed-max semantics after the response is
        # materialized. Include them in the runtime meaning so a mapped
        # request-end field cannot silently combine with an unmapped observed
        # field under one shared continuation operator.
        active.extend(
            "observed_max"
            for field in (source.watermark_columns or [])
            if field not in mapping.fields
            and (
                (watermark_start or {}).get(field) is not None
                or (watermark_end or {}).get(field) is not None
            )
        )
        if not active:
            active = [binding.watermark_value for binding in mapping.fields.values()]
        self._runtime_info.watermark_kind = active[0] if len(set(active)) == 1 else None
    def _read_data(
        self,
        source: Source,
        configure: Optional[Dict[str, Any]] = None,
        watermark_start: Optional[Dict[str, Any]] = None,
        watermark_end: Optional[Dict[str, Any]] = None,
        *,
        read_range: Any = None,
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

        normalized_mapping = normalize_api_range_mapping(src_cfg)
        normalized_read_range = coerce_read_range(read_range)
        if normalized_read_range is None:
            normalized_read_range = coerce_read_range(getattr(self, "_read_range", None))
        if normalized_mapping.is_new or normalized_read_range is not None:
            return self._read_data_new(
                source,
                src_cfg,
                watermark_start=watermark_start,
                watermark_end=watermark_end,
                read_range=normalized_read_range,
                mapping=normalized_mapping,
            )

        # Runtime callers can bypass metadata schema validation. Reject a
        # typo before resolving credentials or constructing an HTTP client so
        # it cannot look like a completed incremental read.
        validate_pagination_type(src_cfg.get("pagination_type"))
        if src_cfg.get("next_link_bound_mode", "opaque") not in {
            "opaque",
            "repeat_query_bounds",
        }:
            raise SourceError(
                "Unsupported API next_link_bound_mode; expected 'opaque' or 'repeat_query_bounds'."
            )

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
        interval_amount = src_cfg.get("watermark_range_interval_amount", 1)
        max_workers: int = int(src_cfg.get("watermark_range_max_workers", 4))
        overlap_mode = src_cfg.get("watermark_range_to_exclusive_offset")
        if overlap_mode is not None and not interval_unit:
            raise SourceError(
                "watermark_range_to_exclusive_offset is supported only on the legacy incremental range split path."
            )

        # Shared timezone for the "now" used as wm_to_param upper-bound.
        # Source-level setting takes precedence over connection-level default.
        wm_to_tz = resolve_timezone(
            src_cfg.get("watermark_to_param_timezone")
            or conn_cfg.get("watermark_to_param_timezone")
        )

        request_watermark_end = dict(watermark_end or {})

        # Resolve one legacy request upper bound before opening the client.
        # A shared ``watermark_to_param`` cannot represent two different
        # explicit active upper values, and an explicit value always wins over
        # the default clock ceiling.
        if wm_to_param:
            request_watermark_end, legacy_to_value = self._resolve_legacy_request_end(
                wm_mapping,
                watermark_start=watermark_start,
                watermark_end=request_watermark_end,
                wm_tz=wm_to_tz,
            )
        else:
            legacy_to_value = None
        self._api_read_context = {
            "effective_end": dict(request_watermark_end),
            "legacy_to_value": legacy_to_value,
        }

        if not interval_unit:
            # ----------------------------------------------------------------
            # Normal single-call path: inject stored watermark + upper bound
            # ----------------------------------------------------------------
            if watermark_start and wm_mapping:
                target = params if wm_location == "params" else body
                inject_stored_watermark(target, wm_mapping, watermark_start, wm_format)

            if wm_to_param:
                to_val = legacy_to_value
                if to_val is None:
                    # ``_resolve_legacy_request_end`` supplies this whenever
                    # the shared upper parameter is configured.  Keep the
                    # fallback defensive for custom callers that pass a
                    # false-y mapping object.
                    to_val = datetime.now(tz=wm_to_tz)
                to_target = params if wm_location == "params" else body
                to_target[wm_to_param] = format_watermark_value(to_val, wm_format)

        headers = dict(conn_cfg.get("default_headers", {}))
        # Unwrap SecretStr values before passing to auth/HTTP libraries.
        auth_cfg = unwrap_configure(conn_cfg)
        apply_auth(headers, auth_cfg)
        http_auth = get_http_auth(auth_cfg)

        # Warn when credentials are sent over plain HTTP
        auth_type = conn_cfg.get("auth_type", "")
        if auth_type and not url.lower().startswith("https"):
            logger.warning(
                "API credentials (auth_type=%s) sent over non-HTTPS URL: %s",
                auth_type,
                safe_url(url),
            )

        with httpx.Client(
            timeout=timeout,
            headers=headers,
            auth=http_auth,
            follow_redirects=False,
        ) as client:
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
                from_dt: Optional[datetime] = resolve_range_from_dt(
                    watermark_start, wm_mapping, src_cfg.get("watermark_range_start"),
                    tz=wm_to_tz,
                )

                if from_dt is None:
                    raise SourceError(
                        "watermark_range_interval_unit requires either a stored watermark "
                        "or 'watermark_range_start' in source.configure",
                    )

                to_dt: Optional[datetime] = None
                # Cap to_dt at watermark_end when provided (replay)
                if request_watermark_end and wm_mapping:
                    _upper_col = next((c for c in wm_mapping if request_watermark_end.get(c) is not None), None)
                    if _upper_col:
                        _cap = request_watermark_end[_upper_col]
                        if isinstance(_cap, _DATETIME_TYPE):
                            to_dt = _cap
                        elif type(_cap) is date:
                            to_dt = datetime(_cap.year, _cap.month, _cap.day, tzinfo=wm_to_tz)
                        elif isinstance(_cap, str):
                            to_dt = datetime.fromisoformat(_cap)
                if to_dt is None:
                    to_dt = datetime.now(tz=wm_to_tz)
                # Normalise timezone so from_dt and to_dt are comparable
                if from_dt.tzinfo is None:
                    to_dt = to_dt.replace(tzinfo=None)

                ranges = build_watermark_ranges(from_dt, to_dt, interval_amount, interval_unit)
                if not ranges:
                    return []

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
                    overlap_mode=overlap_mode,
                    src_cfg=src_cfg,
                )

                call = functools.partial(execute_range_call, cfg)
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    batches = list(executor.map(call, ranges))

                return [rec for batch in batches for rec in batch]

            else:
                # Single-call path (watermark already injected above)
                request_cfg = dict(src_cfg)
                request_cfg["_active_query_bounds"] = {
                    **{
                        api_param: params[api_param]
                        for api_param in wm_mapping.values()
                        if api_param in params
                    },
                    **({wm_to_param: params[wm_to_param]} if wm_to_param and wm_to_param in params else {}),
                }
                if wm_location == "body":
                    request_cfg["_active_query_bounds"] = {}
                return fetch_single_range(client, url, method, params, body or None, request_cfg)

    def _read_data_new(
        self,
        source: Source,
        src_cfg: Dict[str, Any],
        *,
        watermark_start: Optional[Dict[str, Any]],
        watermark_end: Optional[Dict[str, Any]],
        read_range: Optional[ReadRangeSpec],
        mapping: NormalizedAPIRangeMapping,
    ) -> List[Dict[str, Any]]:
        """Compile and execute new API bindings, including residual filtering."""
        conn_cfg = source.connection.configure
        base_url = conn_cfg.get("base_url", "").rstrip("/")
        endpoint = src_cfg.get("endpoint", "")
        url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url
        method = src_cfg.get("method", "GET").upper()
        params = dict(src_cfg.get("params", {}))
        body = dict(src_cfg.get("body") or {})
        timeout = float(conn_cfg.get("timeout", 30))
        validate_pagination_type(src_cfg.get("pagination_type"))
        next_link_bound_mode = src_cfg.get("next_link_bound_mode", "opaque")
        if next_link_bound_mode not in {"opaque", "repeat_query_bounds"}:
            raise SourceError(
                "Unsupported API next_link_bound_mode; expected 'opaque' or 'repeat_query_bounds'."
            )

        # Validate the complete request contract before constructing the client
        # or dispatching a request.  This is especially important for mixed
        # observed-max/request-end state and bounded legacy migration errors.
        if read_range is not None and not mapping.is_new:
            raise SourceError(
                "A bounded read_range cannot use legacy watermark parameter mapping. "
                "Declare range_param_mapping with explicit endpoint operators and response_column."
            )
        if read_range is not None and (watermark_start or watermark_end):
            raise SourceError(
                "read_range is mutually exclusive with incremental watermark bounds."
            )
        if read_range is not None and src_cfg.get("watermark_range_interval_unit"):
            raise SourceError(
                "read_range cannot be combined with legacy watermark range splitting."
            )

        effective_start_operator = getattr(self, "_watermark_start_operator", None)
        effective_end_operator = getattr(self, "_watermark_end_operator", "<") or "<"
        active_fields = self._active_new_fields(
            mapping,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            read_range=read_range,
        )
        active_unmapped_fields = [
            field
            for field in (source.watermark_columns or [])
            if field not in mapping.fields
            and (
                (watermark_start or {}).get(field) is not None
                or (watermark_end or {}).get(field) is not None
            )
        ]
        kinds = {mapping.fields[field].watermark_value for field in active_fields}
        if active_unmapped_fields:
            kinds.add("observed_max")
        if len(kinds) > 1:
            raise SourceError(
                "API active range fields cannot mix observed_max and request_end watermark semantics.",
                details={
                    "fields": active_fields + active_unmapped_fields,
                    "watermark_values": sorted(kinds),
                },
            )

        wm_tz = resolve_timezone(
            src_cfg.get("watermark_to_param_timezone")
            or conn_cfg.get("watermark_to_param_timezone")
        )
        if src_cfg.get("watermark_range_interval_unit"):
            split_field = (
                active_fields[0]
                if active_fields
                else next(iter(mapping.fields), None)
            )
            split_kind = (
                mapping.fields[split_field].watermark_value
                if split_field is not None
                else None
            )
            split_start_operator = effective_start_operator
            if (
                not active_fields
                and split_kind == "request_end"
                and split_start_operator == ">"
            ):
                # A configured split start is the first request-end lower
                # boundary even when no stored watermark made the field
                # active during operator resolution.
                split_start_operator = ">="
            return self._read_data_new_split(
                source,
                src_cfg,
                mapping=mapping,
                watermark_start=watermark_start,
                watermark_end=watermark_end,
                active_fields=active_fields,
                wm_tz=wm_tz,
                start_operator=(
                    split_start_operator
                    or (">=" if split_kind == "request_end" else ">")
                ),
                end_operator=effective_end_operator,
            )
        params, body, query_bounds, residual_filters, watermark_updates = self._compile_new_request(
            mapping,
            src_cfg,
            params,
            body,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            read_range=read_range,
            active_fields=active_fields,
            start_operator=effective_start_operator,
            end_operator=effective_end_operator,
            wm_tz=wm_tz,
            tracked_fields=set(source.watermark_columns or []),
        )

        # Preserve the historical OR semantics for ordinary API watermark
        # columns that have no endpoint push-down mapping. They are filtered
        # from the materialized response together with mapped residuals; doing
        # this before response observation avoids turning the fields into an
        # accidental AND predicate.
        for field in active_unmapped_fields:
            residual_filters.append(
                ResidualFilter(
                    field=field,
                    response_column=field,
                    lower=(watermark_start or {}).get(field),
                    upper=(watermark_end or {}).get(field),
                    lower_operator=effective_start_operator or ">",
                    upper_operator=effective_end_operator,
                )
            )

        headers = dict(conn_cfg.get("default_headers", {}))
        auth_cfg = unwrap_configure(conn_cfg)
        apply_auth(headers, auth_cfg)
        http_auth = get_http_auth(auth_cfg)
        auth_type = conn_cfg.get("auth_type", "")
        if auth_type and not url.lower().startswith("https"):
            logger.warning(
                "API credentials (auth_type=%s) sent over non-HTTPS URL: %s",
                auth_type,
                safe_url(url),
            )

        request_cfg = dict(src_cfg)
        request_cfg["_active_query_bounds"] = query_bounds
        with httpx.Client(
            timeout=timeout,
            headers=headers,
            auth=http_auth,
            follow_redirects=False,
        ) as client:
            records = fetch_single_range(
                client,
                url,
                method,
                params,
                body or None,
                request_cfg,
            )

        # Filtering after pagination is deliberate: a continuation may omit
        # visible bounds or return a superset, and every page must receive the
        # same residual selection before state is observed.
        records = filter_records_by_ranges(records, residual_filters)
        self._api_read_context = {
            "watermark_updates": watermark_updates,
            "complete": True,
        }
        return records

    def _read_data_new_split(
        self,
        source: Source,
        src_cfg: Dict[str, Any],
        *,
        mapping: NormalizedAPIRangeMapping,
        watermark_start: Optional[Dict[str, Any]],
        watermark_end: Optional[Dict[str, Any]],
        active_fields: List[str],
        wm_tz: Any,
        start_operator: str,
        end_operator: str,
    ) -> List[Dict[str, Any]]:
        """Execute the legacy-style parallel split using new bindings.

        A split has one source selection column and keeps the exact
        ``[start, end)`` contract for every sub-call.  Per-field mappings are
        still used for wire names, locations and residual response filtering;
        the legacy exclusive-offset setting is intentionally rejected during
        normalization for this path.
        """
        if len(active_fields) > 1:
            raise SourceError(
                "API range splitting requires one active range field; multiple active fields have no shared interval contract."
            )
        if active_fields:
            field = active_fields[0]
        elif len(mapping.fields) == 1:
            field = next(iter(mapping.fields))
        else:
            raise SourceError(
                "API range splitting requires one active range field or a single configured binding."
            )

        binding = mapping.fields[field]
        if binding.lower is None or binding.upper is None:
            raise SourceError(
                f"API range splitting for {field!r} requires lower and upper bindings."
            )
        configured_start = src_cfg.get("watermark_range_start")
        if isinstance(configured_start, str):
            self._validate_iso_bound_precision(
                configured_start,
                field=field,
                side="range_start",
                fmt=binding.format,
            )
        preserve_date = type((watermark_start or {}).get(field)) is date or (
            isinstance(configured_start, str)
            and "T" not in configured_start
            and " " not in configured_start.strip()
        )
        from_dt = resolve_range_from_dt(
            watermark_start,
            {field: field},
            configured_start,
            tz=wm_tz,
        )
        if from_dt is None:
            raise SourceError(
                "watermark_range_interval_unit requires either a stored watermark "
                "or 'watermark_range_start' in source.configure"
            )
        if not isinstance(from_dt, _DATETIME_TYPE):
            raise SourceError(
                "API range splitting supports temporal bounds; use one-shot integer read_range for integer keys."
            )

        cap = (watermark_end or {}).get(field)
        generated_upper = cap is None
        if cap is None:
            to_dt = datetime.now(tz=wm_tz)
        elif isinstance(cap, _DATETIME_TYPE):
            to_dt = cap
        elif type(cap) is date:
            to_dt = datetime(cap.year, cap.month, cap.day, tzinfo=wm_tz)
        elif isinstance(cap, str):
            to_dt = datetime.fromisoformat(cap)
        else:
            raise SourceError(
                f"API range split upper bound for {field!r} must be a date, datetime, or ISO string."
            )
        if from_dt.tzinfo is None and to_dt.tzinfo is not None:
            to_dt = to_dt.replace(tzinfo=None)
        elif from_dt.tzinfo is not None and to_dt.tzinfo is None:
            to_dt = to_dt.replace(tzinfo=from_dt.tzinfo)

        # A synthetic ``now`` is a source-owned request ceiling.  Align only
        # that value to the endpoint's declared precision before range
        # generation; authored replay bounds and stored lower state remain
        # exact and are rejected by the compiler when the format cannot carry
        # their precision.
        if generated_upper:
            aligned_upper = self._align_generated_upper(
                to_dt,
                binding.format,
                preserve_date=preserve_date,
            )
            # Range construction operates on datetimes even when a
            # date-only lower bound controls the logical state type.  Keep
            # midnight and the resolved wall-time timezone for generation;
            # the candidate is converted back to ``date`` below.
            if type(aligned_upper) is date:
                to_dt = datetime(
                    aligned_upper.year,
                    aligned_upper.month,
                    aligned_upper.day,
                    tzinfo=to_dt.tzinfo,
                )
            else:
                to_dt = aligned_upper

        ranges = build_watermark_ranges(
            from_dt,
            to_dt,
            src_cfg.get("watermark_range_interval_amount", 1),
            src_cfg["watermark_range_interval_unit"],
        )
        if not ranges:
            self._api_read_context = {"watermark_updates": {}, "complete": True}
            return []

        conn_cfg = source.connection.configure
        base_url = conn_cfg.get("base_url", "").rstrip("/")
        endpoint = src_cfg.get("endpoint", "")
        url = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url
        method = src_cfg.get("method", "GET").upper()
        timeout = float(conn_cfg.get("timeout", 30))
        max_workers = int(src_cfg.get("watermark_range_max_workers", 4))
        if max_workers <= 0:
            raise SourceError("watermark_range_max_workers must be a positive integer.")

        compiled_calls: List[
            tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], List[ResidualFilter]]
        ] = []
        for index, (start, end) in enumerate(ranges):
            params = dict(src_cfg.get("params", {}))
            body = dict(src_cfg.get("body") or {})
            p, b, query_bounds, residual, _ = self._compile_bounded_request(
                mapping,
                params,
                body,
                ReadRangeSpec(
                    field,
                    start,
                    end,
                    start_operator if index == 0 else ">=",
                    end_operator,
                ),
                field,
                tracked_fields=set(source.watermark_columns or []),
            )
            compiled_calls.append((p, b, query_bounds, residual))

        headers = dict(conn_cfg.get("default_headers", {}))
        auth_cfg = unwrap_configure(conn_cfg)
        apply_auth(headers, auth_cfg)
        http_auth = get_http_auth(auth_cfg)
        auth_type = conn_cfg.get("auth_type", "")
        if auth_type and not url.lower().startswith("https"):
            logger.warning(
                "API credentials (auth_type=%s) sent over non-HTTPS URL: %s",
                auth_type,
                safe_url(url),
            )

        def fetch_call(
            call: tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], List[ResidualFilter]]
        ) -> List[Dict[str, Any]]:
            params, body, query_bounds, residual = call
            request_cfg = dict(src_cfg)
            request_cfg["_active_query_bounds"] = query_bounds
            records = fetch_single_range(
                client,
                url,
                method,
                params,
                body or None,
                request_cfg,
            )
            return filter_records_by_ranges(records, residual)

        with httpx.Client(
            timeout=timeout,
            headers=headers,
            auth=http_auth,
            follow_redirects=False,
        ) as client:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batches = list(executor.map(fetch_call, compiled_calls))

        records = [record for batch in batches for record in batch]
        update_value: Any = to_dt
        if generated_upper and preserve_date:
            update_value = to_dt.date()
        watermark_updates = (
            {field: update_value}
            if mapping.fields[field].watermark_value == "request_end"
            else {}
        )
        self._api_read_context = {
            "watermark_updates": watermark_updates,
            "complete": True,
        }
        return records

    @staticmethod
    def _active_new_fields(
        mapping: NormalizedAPIRangeMapping,
        *,
        watermark_start: Optional[Dict[str, Any]],
        watermark_end: Optional[Dict[str, Any]],
        read_range: Optional[ReadRangeSpec],
    ) -> List[str]:
        if read_range is not None:
            if read_range.column not in mapping.fields:
                raise SourceError(
                    f"API read_range column {read_range.column!r} has no range_param_mapping binding."
                )
            return [read_range.column]
        start = watermark_start or {}
        end = watermark_end or {}
        return [
            field
            for field in mapping.fields
            if start.get(field) is not None or end.get(field) is not None
        ]

    def _compile_new_request(
        self,
        mapping: NormalizedAPIRangeMapping,
        src_cfg: Dict[str, Any],
        params: Dict[str, Any],
        body: Dict[str, Any],
        *,
        watermark_start: Optional[Dict[str, Any]],
        watermark_end: Optional[Dict[str, Any]],
        read_range: Optional[ReadRangeSpec],
        active_fields: List[str],
        start_operator: Optional[str],
        end_operator: str,
        wm_tz: Any,
        tracked_fields: Optional[set[str]] = None,
    ) -> tuple[
        Dict[str, Any],
        Dict[str, Any],
        Dict[str, Any],
        List[ResidualFilter],
        Dict[str, Any],
    ]:
        if read_range is not None:
            return self._compile_bounded_request(
                mapping,
                params,
                body,
                read_range,
                active_fields[0],
                tracked_fields=tracked_fields,
            )

        lower_values: Dict[str, Any] = {}
        upper_values: Dict[str, Any] = {}
        start = watermark_start or {}
        end = watermark_end or {}
        active_kind = (
            mapping.fields[active_fields[0]].watermark_value
            if active_fields
            else None
        )
        now_value: Optional[datetime] = None
        for field in active_fields:
            binding = mapping.fields[field]
            if start.get(field) is not None:
                lower_values[field] = start[field]
            if end.get(field) is not None:
                upper_values[field] = end[field]
            elif active_kind == "request_end" and start.get(field) is not None and binding.upper is not None:
                if now_value is None:
                    now_value = datetime.now(tz=wm_tz)
                previous = start[field]
                upper_values[field] = self._align_generated_upper(
                    now_value,
                    binding.format,
                    preserve_date=type(previous) is date,
                )

        residual: List[ResidualFilter] = []
        updates: Dict[str, Any] = {}
        query_bounds: Dict[str, Any] = {}
        seen_request: Dict[tuple[str, str], tuple[str, str]] = {}
        for field in active_fields:
            binding = mapping.fields[field]
            self._validate_observed_max_response_column(
                binding,
                field,
                tracked=field in (tracked_fields or set()),
            )
            lower = lower_values.get(field)
            upper = upper_values.get(field)
            desired_lower = start_operator or (
                ">=" if binding.watermark_value == "request_end" else ">"
            )
            if lower is not None:
                if binding.lower is None:
                    raise SourceError(f"API binding for active field {field!r} has no lower parameter.")
                validate_operator_coverage(
                    requested_lower=desired_lower,
                    actual_lower=binding.lower.operator,
                    requested_upper=None,
                    actual_upper=None,
                    field=field,
                )
                wire = self._format_new_bound(
                    lower,
                    binding,
                    field=field,
                    side="lower",
                )
                self._put_bound(
                    params,
                    body,
                    binding.lower,
                    wire,
                    field=field,
                    side="lower",
                    seen=seen_request,
                )
                if binding.lower.location == "params":
                    query_bounds[binding.lower.name] = wire
            if upper is not None:
                if binding.upper is None:
                    raise SourceError(f"API binding for active field {field!r} has no upper parameter.")
                validate_operator_coverage(
                    requested_lower=None,
                    actual_lower=None,
                    requested_upper=end_operator,
                    actual_upper=binding.upper.operator,
                    field=field,
                )
                wire = self._format_new_bound(
                    upper,
                    binding,
                    field=field,
                    side="upper",
                )
                self._put_bound(
                    params,
                    body,
                    binding.upper,
                    wire,
                    field=field,
                    side="upper",
                    seen=seen_request,
                )
                if binding.upper.location == "params":
                    query_bounds[binding.upper.name] = wire
                if binding.watermark_value == "request_end":
                    updates[field] = upper

            if binding.watermark_value == "request_end" and binding.response_column is None:
                # Without a response field there is no residual predicate to
                # remove an endpoint superset.  Request-end advancement is
                # therefore safe only when both authored endpoint operators
                # exactly implement the effective request bounds.
                if lower is not None and binding.lower.operator != desired_lower:
                    raise SourceError(
                        f"Request-end API binding for {field!r} needs response_column "
                        "when its lower endpoint operator differs from the requested bound."
                    )
                if upper is not None and binding.upper.operator != end_operator:
                    raise SourceError(
                        f"Request-end API binding for {field!r} needs response_column "
                        "when its upper endpoint operator differs from the requested bound."
                    )
            if lower is not None or upper is not None:
                residual.append(
                    ResidualFilter(
                        field=field,
                        response_column=binding.response_column,
                        lower=lower,
                        upper=upper,
                        lower_operator=desired_lower,
                        upper_operator=end_operator,
                    )
                )

        return params, body, query_bounds, residual, updates

    def _compile_bounded_request(
        self,
        mapping: NormalizedAPIRangeMapping,
        params: Dict[str, Any],
        body: Dict[str, Any],
        read_range: ReadRangeSpec,
        field: str,
        *,
        tracked_fields: Optional[set[str]] = None,
    ) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], List[ResidualFilter], Dict[str, Any]]:
        binding = mapping.fields[field]
        if binding.lower is None or binding.upper is None:
            raise SourceError(
                f"Bounded API read for {field!r} requires explicit lower and upper bindings."
            )
        validate_operator_coverage(
            requested_lower=read_range.lower_operator,
            actual_lower=binding.lower.operator,
            requested_upper=read_range.upper_operator,
            actual_upper=binding.upper.operator,
            field=field,
        )
        self._validate_observed_max_response_column(
            binding,
            field,
            tracked=field in (tracked_fields or set()),
        )
        seen_request: Dict[tuple[str, str], tuple[str, str]] = {}
        lower_wire = self._format_new_bound(
            read_range.start,
            binding,
            field=field,
            side="lower",
        )
        upper_wire = self._format_new_bound(
            read_range.end,
            binding,
            field=field,
            side="upper",
        )
        self._put_bound(params, body, binding.lower, lower_wire, field=field, side="lower", seen=seen_request)
        self._put_bound(params, body, binding.upper, upper_wire, field=field, side="upper", seen=seen_request)
        query_bounds: Dict[str, Any] = {}
        if binding.lower.location == "params":
            query_bounds[binding.lower.name] = lower_wire
        if binding.upper.location == "params":
            query_bounds[binding.upper.name] = upper_wire

        if binding.response_column is None:
            if binding.lower.operator != read_range.lower_operator or binding.upper.operator != read_range.upper_operator:
                raise SourceError(
                    f"Bounded API read for {field!r} needs response_column for residual filtering."
                )
        residual = [
            ResidualFilter(
                field=field,
                response_column=binding.response_column,
                lower=read_range.start,
                upper=read_range.end,
                lower_operator=read_range.lower_operator,
                upper_operator=read_range.upper_operator,
            )
        ]
        watermark_updates = (
            {field: read_range.end}
            if binding.watermark_value == "request_end"
            else {}
        )
        return params, body, query_bounds, residual, watermark_updates

    @staticmethod
    def _align_generated_upper(
        value: datetime,
        fmt: str,
        *,
        preserve_date: bool = False,
    ) -> Any:
        """Align only a synthetic ``now`` ceiling to a declared wire unit."""

        if preserve_date:
            return value.date()
        normalized = str(fmt).lower()
        if normalized == "date":
            return value.replace(hour=0, minute=0, second=0, microsecond=0)
        if normalized == "datetime":
            return value.replace(microsecond=0)
        if normalized in {"datetime_ms", "timestamp_ms"}:
            return value.replace(microsecond=(value.microsecond // 1000) * 1000)
        return value

    @staticmethod
    def _validate_observed_max_response_column(
        binding: Any,
        field: str,
        *,
        tracked: bool,
    ) -> None:
        """Require a response field when tracked state uses observed maxima."""

        if tracked and binding.watermark_value == "observed_max" and binding.response_column is None:
            raise SourceError(
                f"Observed-max API binding for field {field!r} requires response_column.",
                details={
                    "field": field,
                    "watermark_value": binding.watermark_value,
                    "response_column": None,
                },
            )

    @staticmethod
    def _format_new_bound(
        value: Any,
        binding: APIRangeBinding,
        *,
        field: str,
        side: str,
    ) -> Any:
        """Compile a new-binding bound with actionable precision context."""

        try:
            return format_bound(value, binding.format)
        except SourceError as exc:
            details = {
                "field": field,
                "side": side,
                "format": binding.format,
            }
            details.update(getattr(exc, "details", {}))
            raise SourceError(
                f"API {side} bound for field {field!r} cannot be represented "
                f"losslessly with format {binding.format!r}: {exc.message}",
                details=details,
            ) from exc

    @staticmethod
    def _put_bound(
        params: Dict[str, Any],
        body: Dict[str, Any],
        binding: APIParamBinding,
        value: Any,
        *,
        field: str,
        side: str,
        seen: Dict[tuple[str, str], tuple[str, str]],
    ) -> None:
        target = params if binding.location == "params" else body
        key = (binding.location, binding.name)
        previous = seen.get(key)
        if previous is not None:
            raise SourceError(
                "API range request contains duplicate active parameter bindings.",
                details={"location": binding.location, "name": binding.name, "first": previous, "second": (field, side)},
            )
        if binding.name in target and target[binding.name] != value:
            raise SourceError(
                "API range bound conflicts with an authored request parameter.",
                details={"location": binding.location, "name": binding.name, "field": field, "side": side},
            )
        seen[key] = (field, side)
        target[binding.name] = value

    @staticmethod
    def _resolve_legacy_request_end(
        wm_mapping: Dict[str, str],
        *,
        watermark_start: Optional[Dict[str, Any]],
        watermark_end: Dict[str, Any],
        wm_tz: Any,
    ) -> tuple[Dict[str, Any], Any]:
        """Resolve the historical shared upper parameter without ambiguity."""
        explicit = [
            watermark_end[column]
            for column in wm_mapping
            if watermark_end.get(column) is not None
        ]
        if explicit:
            first = explicit[0]
            if any(value != first for value in explicit[1:]):
                raise SourceError(
                    "Legacy watermark_to_param cannot represent different active upper bounds.",
                    details={"columns": [column for column in wm_mapping if watermark_end.get(column) is not None]},
                )
            return dict(watermark_end), first

        now = datetime.now(tz=wm_tz)
        effective = dict(watermark_end)
        # An unbounded legacy incremental call historically applies the
        # shared ceiling to each mapped field.  Keep that behavior; explicit
        # partial bounds above never receive a synthetic value.
        first_value: Any = now
        for index, column in enumerate(wm_mapping):
            previous = (watermark_start or {}).get(column)
            effective[column] = now.date() if type(previous) is date else now
            if index == 0:
                first_value = effective[column]
        return effective, first_value
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


__all__ = ["APIReader"]
