"""Reject secret-bearing locators that would be exposed through process arguments."""
from __future__ import annotations

from urllib.parse import parse_qsl, unquote_plus, urlsplit

_SENSITIVE_KEY_PARTS = (
    "access_key",
    "api_key",
    "client_secret",
    "credential",
    "password",
    "signature",
    "token",
)

# Azure Blob/File/Data Lake SAS tokens use the short, exact ``sig`` query key.
# Keep this separate from the substring checks above: accepting an ordinary
# control such as ``api-version`` must not depend on a broad ``key``/``sig``
# blacklist.  Query parsers decode one percent-encoding layer, while callers
# sometimes pass a value that was encoded again by a wrapper; bounded repeated
# decoding catches those forms without treating arbitrary values as locators.
_SENSITIVE_EXACT_KEYS = frozenset({"sig"})


def _normalize_query_key(key: str) -> str:
    normalized = key
    for _ in range(3):
        decoded = unquote_plus(normalized)
        if decoded == normalized:
            break
        normalized = decoded
    return normalized.casefold().replace("-", "_")


def validate_nonsecret_locator(value: str, label: str) -> str:
    if "://" not in value:
        return value
    parts = urlsplit(value)
    if parts.username or parts.password:
        raise ValueError(f"{label} cannot contain URL user information; use environment identity")
    for key, _ in parse_qsl(parts.query, keep_blank_values=True):
        normalized = _normalize_query_key(key)
        if normalized in _SENSITIVE_EXACT_KEYS or any(
            part in normalized for part in _SENSITIVE_KEY_PARTS
        ):
            raise ValueError(
                f"{label} contains a secret-bearing query parameter; use environment identity"
            )
    return value
