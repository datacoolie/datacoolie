"""HTTP transport and URL-sanitization helpers for API sources."""

from __future__ import annotations

import time
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from datacoolie.core.exceptions import SourceError
from datacoolie.logging.runtime.manager import get_logger

logger = get_logger(__name__)

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

_SENSITIVE_URL_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "client_secret",
    "password",
    "secret",
    "signature",
    "sig",
    "token",
}

def safe_url(url: object) -> str:
    """Remove credentials and credential-bearing query values from a URL.

    The raw URL remains private to the HTTP client. This representation is for
    runtime action diagnostics, log messages, and exception details only.
    """
    if not isinstance(url, str) or not url:
        return "<empty-url>"
    try:
        parts = urlsplit(url)
        # Removing everything before the last ``@`` handles both userinfo and
        # password forms without attempting to parse or re-emit credentials.
        netloc = parts.netloc.rsplit("@", 1)[-1]
        query: list[tuple[str, str]] = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            normalised_key = key.lower().replace("-", "_")
            sensitive = (
                normalised_key in _SENSITIVE_URL_KEYS
                or normalised_key.endswith(
                    ("_token", "_secret", "_password", "_signature")
                )
            )
            query.append((key, "***" if sensitive else value))
        return urlunsplit(
            (parts.scheme, netloc, parts.path, urlencode(query), parts.fragment)
        )
    except Exception:
        return "<invalid-url>"

def make_request(
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
    request_error: Optional[SourceError] = None
    for attempt in range(max_retries + 1):
        try:
            response = client.request(method, url, params=params, json=body)
        except httpx.HTTPError as exc:
            msg = (
                f"HTTP request failed after retry {attempt} ({type(exc).__name__})"
                if attempt > 0
                else f"HTTP request failed ({type(exc).__name__})"
            )
            # Construct the safe error now, but raise it after leaving the
            # exception handler.  This avoids retaining a raw client
            # exception (which may contain a credential-bearing URL) in
            # the propagated exception context.
            request_error = SourceError(
                msg,
                details={"url": safe_url(url), "method": method},
            )
            break

        if response.status_code != 429:
            break

        if attempt == max_retries:
            raise SourceError(
                f"API returned HTTP 429 after {max_retries} retries",
                details={
                    "url": safe_url(url),
                    "method": method,
                    "retries": max_retries,
                },
            )

        retry_after = response.headers.get("Retry-After")
        wait = float(retry_after) if retry_after is not None else min(2 ** attempt, 30)
        logger.warning(
            "Rate limited (429), attempt %d/%d. Waiting %.1fs before retry.",
            attempt + 1, max_retries, wait,
        )
        time.sleep(wait)

    if request_error is not None:
        raise request_error

    if response.status_code >= 400:
        raise SourceError(
            f"API returned HTTP {response.status_code}",
            details={
                "url": safe_url(url),
                "method": method,
                "status": response.status_code,
            },
        )

    return response

__all__ = ["make_request", "safe_url"]
