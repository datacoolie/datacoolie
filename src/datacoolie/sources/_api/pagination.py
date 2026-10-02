"""Pagination and range execution helpers for API sources."""

from __future__ import annotations

import dataclasses
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from datacoolie.core.exceptions import SourceError
from datacoolie.logging.runtime.manager import get_logger
from .records import extract_records, resolve_path
from .transport import make_request, safe_url
from .watermark import adjust_range_to, format_watermark_value

logger = get_logger(__name__)

_PAGINATION_OFFSET = "offset"
_PAGINATION_CURSOR = "cursor"
_PAGINATION_NEXT_LINK = "next_link"
_SUPPORTED_PAGINATION_TYPES = frozenset(
    {_PAGINATION_OFFSET, _PAGINATION_CURSOR, _PAGINATION_NEXT_LINK}
)
_NEXT_LINK_BOUND_MODES = frozenset({"opaque", "repeat_query_bounds"})


def validate_pagination_type(value: Any) -> Optional[str]:
    """Validate and return the configured API pagination mode."""
    if value is None:
        return None
    if not isinstance(value, str) or value not in _SUPPORTED_PAGINATION_TYPES:
        raise SourceError(
            "Unsupported API pagination_type; expected one of "
            "'offset', 'cursor', or 'next_link'.",
            details={"pagination_type": repr(value)},
        )
    return value


def _origin_key(url: str, *, allow_userinfo: bool = False) -> Tuple[str, str, int]:
    """Return a normalized HTTP origin or raise for an unsafe URL."""
    if not isinstance(url, str) or not url:
        raise SourceError(
            "API next_link must be a non-empty HTTP(S) URL.",
            details={"url": safe_url(url)},
        )

    try:
        parts = urlsplit(url)
        scheme = parts.scheme.lower()
        hostname = parts.hostname
        port = parts.port
    except (TypeError, ValueError) as exc:
        raise SourceError(
            "API next_link is not a valid HTTP(S) URL.",
            details={"url": safe_url(url)},
        ) from exc

    if scheme not in {"http", "https"} or not hostname:
        raise SourceError(
            "API next_link must use HTTP or HTTPS and include a host.",
            details={"url": safe_url(url)},
        )
    if not allow_userinfo and (parts.username is not None or parts.password is not None):
        raise SourceError(
            "API next_link must not contain URL userinfo.",
            details={"url": safe_url(url)},
        )

    try:
        normalized_host = hostname.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise SourceError(
            "API next_link host is not valid.",
            details={"url": safe_url(url)},
        ) from exc

    effective_port = port if port is not None else (443 if scheme == "https" else 80)
    return scheme, normalized_host, effective_port


def resolve_same_origin_next_url(
    current_url: str,
    next_link: Any,
    expected_origin: Tuple[str, str, int],
) -> str:
    """Resolve a server continuation and enforce the configured origin."""
    if not isinstance(next_link, str) or not next_link.strip():
        raise SourceError(
            "API next_link must be a non-empty HTTP(S) URL.",
            details={"url": safe_url(next_link)},
        )

    next_link = next_link.strip()
    try:
        resolved_url = urljoin(current_url, next_link)
    except (TypeError, ValueError) as exc:
        raise SourceError(
            "API next_link could not be resolved against the current URL.",
            details={"url": safe_url(next_link)},
        ) from exc

    actual_origin = _origin_key(resolved_url)
    if actual_origin != expected_origin:
        raise SourceError(
            "API next_link must stay on the configured HTTP(S) origin.",
            details={"url": safe_url(resolved_url)},
        )
    return resolved_url


def _repeat_query_bounds(
    next_url: str,
    active_bounds: Dict[str, Any],
) -> str:
    """Apply explicitly configured query bounds to a continuation URL.

    A server supplied value is retained when it exactly matches the active
    value.  A missing value is appended.  Repeated or conflicting active
    parameters are rejected before the continuation request is dispatched.
    ``opaque`` pagination never calls this helper, preserving signed/tokenized
    URLs byte-for-byte after origin resolution.
    """
    if not active_bounds:
        return next_url
    parts = urlsplit(next_url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    by_name: Dict[str, List[str]] = {}
    for name, value in pairs:
        by_name.setdefault(name, []).append(value)

    additions: List[Tuple[str, str]] = []
    for name, expected in active_bounds.items():
        # Values are formatted by the source compiler before reaching the
        # pagination helper.  ``str`` is therefore safe for integer and
        # temporal wire values and preserves the server-visible spelling.
        expected_text = str(expected)
        visible = by_name.get(name, [])
        if len(visible) > 1:
            raise SourceError(
                "API next_link repeats an active query bound; refusing ambiguous continuation.",
                details={"parameter": name},
            )
        if visible and visible[0] != expected_text:
            raise SourceError(
                "API next_link conflicts with the active query bound.",
                details={"parameter": name},
            )
        if not visible:
            additions.append((name, expected_text))

    if additions:
        pairs.extend(additions)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(pairs), parts.fragment))

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

def fetch_single_range(
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
    pagination_type = validate_pagination_type(src_cfg.get("pagination_type"))
    # Preserve the existing configured-base-URL behavior, including sources
    # whose base URL carries userinfo.  Continuation URLs themselves never
    # inherit that exception and are rejected by the default guard below.
    expected_origin = _origin_key(url, allow_userinfo=True)

    # Concurrent offset path: fetch page 0 to get total, then remaining in parallel
    if pagination_type == _PAGINATION_OFFSET and src_cfg.get("total_path"):
        return fetch_offset_concurrent(client, url, method, params, body, src_cfg)

    page_size = int(src_cfg.get("page_size", 100))
    max_pages = int(src_cfg.get("max_pages", 1000))
    if page_size <= 0 or max_pages <= 0:
        raise SourceError("API pagination page_size and max_pages must be positive integers.")
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

        response = make_request(
            client, method, current_url,
            params=local_params,
            body=local_body or None,
            max_retries=max_retries,
        )
        data = response.json()

        records = extract_records(data, data_path)
        all_records.extend(records)

        if not pagination_type:
            break  # single-page fetch

        if pagination_type == _PAGINATION_NEXT_LINK:
            next_link_path = src_cfg.get("next_link_path", "next")
            next_url = resolve_path(data, next_link_path)
            if not next_url:
                break
            if page + 1 >= max_pages:
                raise SourceError(
                    f"API pagination reached max_pages={max_pages} while a next link remained."
                )
            current_url = resolve_same_origin_next_url(
                current_url,
                next_url,
                expected_origin,
            )
            next_link_bound_mode = src_cfg.get("next_link_bound_mode", "opaque")
            if next_link_bound_mode not in _NEXT_LINK_BOUND_MODES:
                raise SourceError(
                    "Unsupported API next_link_bound_mode; expected 'opaque' or 'repeat_query_bounds'.",
                    details={"next_link_bound_mode": repr(next_link_bound_mode)},
                )
            if next_link_bound_mode == "repeat_query_bounds":
                active_query_bounds = src_cfg.get("_active_query_bounds")
                if active_query_bounds is None:
                    active_query_bounds = _infer_query_bounds(src_cfg, local_params)
                current_url = _repeat_query_bounds(
                    current_url,
                    active_query_bounds or {},
                )
            # The continuation URL owns its complete query string by default.
            # In repeat mode, bounds have been validated/added above, and the
            # URL still remains the sole source of query parameters.
            local_params = None

        elif pagination_type == _PAGINATION_CURSOR:
            cursor_path = src_cfg.get("cursor_path", "next_cursor")
            cursor_param = src_cfg.get("cursor_param", "cursor")
            cursor_value = resolve_path(data, cursor_path)
            if not cursor_value:
                break
            if page + 1 >= max_pages:
                raise SourceError(
                    f"API pagination reached max_pages={max_pages} while a cursor remained."
                )
            local_params[cursor_param] = str(cursor_value)

        elif pagination_type == _PAGINATION_OFFSET:
            if not records or len(records) < page_size:
                break  # last page
            if page + 1 >= max_pages:
                raise SourceError(
                    f"API offset pagination reached max_pages={max_pages} with a full page; "
                    "the end of the result was not confirmed."
                )

        page += 1

        if rate_limit_delay > 0:
            time.sleep(rate_limit_delay)

    return all_records

def fetch_offset_concurrent(
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
    validate_pagination_type(src_cfg.get("pagination_type"))
    page_size = int(src_cfg.get("page_size", 100))
    max_pages = int(src_cfg.get("max_pages", 1000))
    if page_size <= 0 or max_pages <= 0:
        raise SourceError("API pagination page_size and max_pages must be positive integers.")
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

    response = make_request(
        client, method, url, params=p0_params, body=p0_body or None,
        max_retries=max_retries,
    )
    data = response.json()
    page0_records = extract_records(data, data_path)

    total_raw = resolve_path(data, total_path)
    try:
        total = int(total_raw)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise SourceError(
            f"APIReader: total_path={total_path!r} resolved to {total_raw!r}, "
            f"which cannot be converted to int.",
            details={"total_path": total_path, "resolved": total_raw},
        ) from exc

    if isinstance(total_raw, bool) or total < 0 or (
        isinstance(total_raw, float) and not total_raw.is_integer()
    ):
        raise SourceError(
            f"APIReader: total_path={total_path!r} must resolve to a non-negative integer.",
            details={"total_path": total_path, "resolved": total_raw},
        )

    total_pages = (total + page_size - 1) // page_size
    if total_pages > max_pages:
        raise SourceError(
            f"APIReader: total_path={total_path!r} reports {total} records, requiring "
            f"{total_pages} pages, above max_pages={max_pages}.",
            details={"total_path": total_path, "total": total, "required_pages": total_pages, "max_pages": max_pages},
        )

    logger.debug(
        "APIReader concurrent offset: total=%d page_size=%d total_pages=%d workers=%d",
        total, page_size, total_pages, max_workers,
    )

    if total_pages <= 1:
        if len(page0_records) != total:
            raise SourceError(
                f"APIReader: fetched {len(page0_records)} records but total_path="
                f"{total_path!r} reports {total}.",
                details={"total_path": total_path, "total": total},
            )
        return list(page0_records)

    # --- Pages 1..(total_pages-1): fetch concurrently ---
    def fetch_page(page_num: int) -> List[Dict[str, Any]]:
        p_params = dict(params) if params is not None else {}
        p_body = dict(body) if body is not None else {}
        p_params[offset_param] = page_num * page_size
        p_params[limit_param] = page_size
        resp = make_request(
            client, method, url, params=p_params, body=p_body or None,
            max_retries=max_retries,
        )
        return extract_records(resp.json(), data_path)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        batches = list(executor.map(fetch_page, range(1, total_pages)))

    all_records: List[Dict[str, Any]] = list(page0_records)
    for batch in batches:
        all_records.extend(batch)
    if len(all_records) != total:
        raise SourceError(
            f"APIReader: fetched {len(all_records)} records but total_path="
            f"{total_path!r} reports {total}; pagination is incomplete or inconsistent.",
            details={"total_path": total_path, "total": total, "fetched": len(all_records)},
        )
    return all_records


def _infer_query_bounds(
    src_cfg: Dict[str, Any],
    params: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Infer active query bound values for direct pagination helper callers."""
    if not params:
        return {}
    names: List[str] = []
    raw_mapping = src_cfg.get("range_param_mapping")
    if isinstance(raw_mapping, dict):
        for item in raw_mapping.values():
            if not isinstance(item, dict):
                continue
            for side in ("lower", "upper"):
                bound = item.get(side)
                if isinstance(bound, str):
                    names.append(bound)
                elif isinstance(bound, dict) and bound.get("location", "params") == "params":
                    name = bound.get("name")
                    if isinstance(name, str):
                        names.append(name)
    legacy_mapping = src_cfg.get("watermark_param_mapping")
    if isinstance(legacy_mapping, dict):
        names.extend(name for name in legacy_mapping.values() if isinstance(name, str))
        upper = src_cfg.get("watermark_to_param")
        if isinstance(upper, str):
            names.append(upper)
    return {name: params[name] for name in names if name in params}

def execute_range_call(
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
        r_target[api_param] = format_watermark_value(r_from, cfg.wm_format)

    r_to_sent = adjust_range_to(r_to, cfg.overlap_mode)
    r_target[cfg.wm_to_param] = format_watermark_value(r_to_sent, cfg.wm_format)

    first_api_param = next(iter(cfg.wm_mapping.values()), "from")
    logger.debug(
        "APIReader range call: %s=%s %s=%s",
        first_api_param, r_target.get(first_api_param),
        cfg.wm_to_param, r_target.get(cfg.wm_to_param),
    )

    request_cfg = dict(cfg.src_cfg)
    request_cfg["_active_query_bounds"] = (
        {
            **{
                api_param: r_params[api_param]
                for api_param in cfg.wm_mapping.values()
                if cfg.wm_location == "params" and api_param in r_params
            },
            **(
                {cfg.wm_to_param: r_params[cfg.wm_to_param]}
                if cfg.wm_location == "params" and cfg.wm_to_param in r_params
                else {}
            ),
        }
        if cfg.wm_location == "params"
        else {}
    )
    return fetch_single_range(
        cfg.client, cfg.url, cfg.method, r_params, r_body or None, request_cfg,
    )

__all__ = [
    "_RangeCallConfig",
    "execute_range_call",
    "fetch_offset_concurrent",
    "fetch_single_range",
    "resolve_same_origin_next_url",
    "validate_pagination_type",
]
