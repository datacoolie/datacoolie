"""Public API source reader facade.

The reader owns source configuration, DataFrame conversion, and runtime state.
HTTP/auth/pagination/watermark/record helpers live in the private ``_api``
package so the public component remains easy to discover.
"""

from __future__ import annotations

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
    resolve_range_from_dt,
    resolve_timezone,
)
from datacoolie.utils.chunking import validate_iso_fraction_precision

logger = get_logger(__name__)
_DATETIME_TYPE = datetime
_RANGE_INTERVAL_UNITS = frozenset({"hour", "day", "month", "year"})


def validate_api_bounded_range(source: Source, column: str) -> None:
    """Validate the API-owned mapping needed for an exact bounded read.

    The selected field can be a selection-only field; it does not need to be
    listed in ``source.watermark_columns``. Persisted state remains limited to
    the authored watermark columns in the reader's runtime path.
    """

    mapping = normalize_api_range_mapping(source.configure)
    if not mapping.fields:
        raise SourceError(
            "API bounded replay requires range_param_mapping with canonical "
            "lower and upper bindings."
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
              generated request-end bounds and for advancing request-end watermarks.
              Can be overridden per source via the source-level key of the same name
              (source wins).
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

        Canonical watermark and bounded range push-down
        (``source.configure.range_param_mapping``):
            Each mapped field declares separate lower and upper endpoint
            parameters, their operators and locations, wire format, response
            column, and watermark meaning (``"observed_max"`` or
            ``"request_end"``).  Lower and upper bindings may use the shorthand
            parameter names or the explicit ``{"name": ..., "location": ...,
            "operator": ...}`` form.  Supported formats include ``"iso"``,
            ``"date"``, ``"timestamp"``, ``"timestamp_ms"``, ``"datetime"``,
            ``"datetime_ms"``, ``"integer"``, and ``"number"``.

            A bounded replay requires both lower and upper bindings and uses
            the exact authored operators.  Replay may select a mapped field
            even when it is not listed in ``source.watermark_columns``; the
            mapping controls selection while only authored watermark columns
            are eligible for persisted state.  Ordinary unmapped watermark
            columns continue to use response-side filtering.

            ``watermark_to_param_timezone`` (str, optional) selects the timezone
            used for generated request-end bounds and date/datetime alignment.
            It accepts IANA names or UTC offset strings and defaults to ``UTC``.

        Range-split / parallel fetch (``source.configure``):
            Splits a large watermark window into equal-sized intervals and calls
            the API concurrently for each sub-range.  Requires one canonical
            range mapping with lower and upper bindings.

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
            Each split request receives the exact contiguous interval boundary.
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
        stored = self._pending_watermark_start
        pending_end = self._pending_watermark_end
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
        """Validate canonical ISO precision before look-back parsing.

        ``BaseSourceReader`` applies ``date_backward`` before the API binding
        compiler sees a stored lower bound.  Validate the caller-owned string
        first so ``datetime.fromisoformat`` cannot silently discard digits.
        The check applies to every canonical range binding before the base
        reader can coerce a stored ISO value and discard fractional digits.
        """

        super()._validate_watermark_comparison(watermark, boundary=boundary)
        source = self._active_source
        if source is None or not watermark:
            return
        mapping = normalize_api_range_mapping(source.configure)
        if not mapping.fields:
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
        # Unmapped API watermark columns still calculate observed maxima from
        # response rows. Preserve the built-in typed-row contract for those
        # fields; strings remain opaque.
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

        # BaseSourceReader stores the per-instance range for the internal
        # handoff; keep the accessor as the source of truth when the caller
        # does not pass an explicit value.
        if read_range is None:
            read_range = self._get_read_range()
        normalized_mapping = normalize_api_range_mapping(src_cfg)
        normalized_read_range = coerce_read_range(read_range)
        self._set_runtime_watermark_kind(
            normalized_mapping,
            source=source,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            read_range=normalized_read_range,
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

        return self._read_new_range(
            source,
            watermark_start,
            watermark_end=watermark_end,
            read_range=normalized_read_range,
            mapping=normalized_mapping,
        )

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
        # No-mapping reads use the base watermark filter after materializing
        # rows so bounded replacement can preserve a typed empty frame.
        if not mapping.fields:
            df = self._apply_watermark_filter(
                df,
                list(source.watermark_columns or []),
                watermark_start or {},
                watermark_end,
            )
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
            if self._preserve_empty:
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
        src_cfg = source.configure
        if configure:
            src_cfg = {**src_cfg, **configure}

        normalized_mapping = normalize_api_range_mapping(src_cfg)
        normalized_read_range = coerce_read_range(read_range)
        if normalized_read_range is None:
            normalized_read_range = coerce_read_range(self._get_read_range())
        if httpx is None:
            raise SourceError(
                "APIReader requires 'httpx'. Install it with: pip install httpx",
            )
        return self._read_data_new(
            source,
            src_cfg,
            watermark_start=watermark_start,
            watermark_end=watermark_end,
            read_range=normalized_read_range,
            mapping=normalized_mapping,
        )

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
        """Compile and execute canonical API bindings, including residual filtering."""
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

        interval_unit_configured = "watermark_range_interval_unit" in src_cfg
        interval_unit = src_cfg.get("watermark_range_interval_unit")
        if interval_unit_configured:
            if not isinstance(interval_unit, str) or interval_unit not in _RANGE_INTERVAL_UNITS:
                raise SourceError(
                    "watermark_range_interval_unit must be one of 'hour', 'day', 'month', or 'year'."
                )
            if not mapping.fields:
                raise SourceError(
                    "API range splitting requires range_param_mapping with canonical "
                    "lower and upper bindings."
                )

        # Validate the complete request contract before constructing the client
        # or dispatching a request.
        if read_range is not None and not mapping.fields:
            raise SourceError(
                "A bounded read_range requires range_param_mapping with explicit "
                "endpoint operators and response_column."
            )
        if read_range is not None and (watermark_start or watermark_end):
            raise SourceError(
                "read_range is mutually exclusive with incremental watermark bounds."
            )
        if read_range is not None and interval_unit_configured:
            raise SourceError(
                "read_range cannot be combined with API range splitting."
            )

        effective_start_operator = self._watermark_start_operator
        effective_end_operator = self._watermark_end_operator
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
        if not read_range and (
            len(active_fields) > 1
            or (active_fields and active_unmapped_fields)
        ):
            raise SourceError(
                "API active watermark OR predicate cannot be represented by one "
                "bounded request; use one mapped field or response-side filtering."
            )

        wm_tz = resolve_timezone(
            src_cfg.get("watermark_to_param_timezone")
            or conn_cfg.get("watermark_to_param_timezone")
        )
        if interval_unit_configured:
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
        if mapping.fields:
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
        # Canonical mappings own raw residual filtering. With no mapping,
        # return the fetched rows so BaseSourceReader can materialize and
        # apply its ordinary watermark filter with preserve-empty semantics.
        if mapping.fields:
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
        """Execute a canonical parallel split using one range binding.

        A split has one source selection column and keeps the exact
        ``[start, end)`` contract for every sub-call.  Per-field mappings are
        still used for wire names, locations and residual response filtering;
        every sub-call receives the exact compiled interval bounds.
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

            if binding.response_column is None:
                # Without a response field there is no residual predicate to
                # remove an endpoint superset. The endpoint must implement
                # every active bound exactly.
                if lower is not None and binding.lower.operator != desired_lower:
                    raise SourceError(
                        f"API binding for {field!r} needs response_column "
                        "when its lower endpoint operator differs from the requested bound."
                    )
                if upper is not None and binding.upper.operator != end_operator:
                    raise SourceError(
                        f"API binding for {field!r} needs response_column "
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
        """Compile a canonical bound with actionable precision context."""

        try:
            return format_bound(value, binding.format)
        except SourceError as exc:
            details = {
                "field": field,
                "side": side,
                "format": binding.format,
            }
            details.update(exc.details)
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
