"""Small, dependency-free helpers for classifying AWS SDK failures.

The AWS platform intentionally keeps boto3 optional at import time.  These
helpers therefore inspect the response shape exposed by botocore without
importing botocore (or requiring boto3) themselves.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


_NOT_FOUND_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "NotFound", "404"})
_PRECONDITION_CODES = frozenset({"PreconditionFailed", "412"})
_ENTITY_TOO_LARGE_CODES = frozenset({"EntityTooLarge", "TooLarge"})


def _response(exc: BaseException) -> Mapping[str, Any] | None:
    response = getattr(exc, "response", None)
    return response if isinstance(response, Mapping) else None


def error_code(exc: BaseException) -> str | None:
    """Return the SDK error code when one is available."""
    response = _response(exc)
    if response is not None:
        error = response.get("Error")
        if isinstance(error, Mapping):
            code = error.get("Code")
            if code is not None:
                return str(code)
        metadata = response.get("ResponseMetadata")
        if isinstance(metadata, Mapping):
            status = metadata.get("HTTPStatusCode")
            if status is not None:
                return str(status)

    code = getattr(exc, "code", None)
    return str(code) if code is not None else None


def http_status(exc: BaseException) -> int | None:
    """Return an HTTP status from a botocore-style error response."""
    response = _response(exc)
    if response is not None:
        metadata = response.get("ResponseMetadata")
        if isinstance(metadata, Mapping):
            status = metadata.get("HTTPStatusCode")
            try:
                return int(status) if status is not None else None
            except (TypeError, ValueError):
                return None

        error = response.get("Error")
        if isinstance(error, Mapping):
            status = error.get("HTTPStatusCode")
            try:
                return int(status) if status is not None else None
            except (TypeError, ValueError):
                return None

    return None


def is_not_found(exc: BaseException) -> bool:
    """Return whether *exc* is an explicit not-found result."""
    code = error_code(exc)
    return (
        (code in _NOT_FOUND_CODES)
        or http_status(exc) == 404
        or isinstance(exc, FileNotFoundError)
    )


def is_precondition_failed(exc: BaseException) -> bool:
    """Return whether *exc* represents an S3 conditional-write conflict."""
    code = error_code(exc)
    return code in _PRECONDITION_CODES or http_status(exc) == 412


def is_entity_too_large(exc: BaseException) -> bool:
    """Return whether a direct S3 copy exceeded the single-request limit."""
    code = error_code(exc)
    return code in _ENTITY_TOO_LARGE_CODES or http_status(exc) == 413


def is_throttled(exc: BaseException) -> bool:
    """Return whether an AWS response indicates throttling."""
    code = (error_code(exc) or "").lower()
    return (
        code
        in {
            "slowdown",
            "throttling",
            "throttlingexception",
            "requestlimitexceeded",
            "toomanyrequests",
            "429",
        }
        or http_status(exc) == 429
    )


def failure_detail(exc: BaseException) -> str:
    """Return a compact, non-secret error description for user-facing errors."""
    code = error_code(exc)
    return f"{code}: {exc}" if code else str(exc)
