"""Reject secret-bearing locators that would be exposed through process arguments."""
from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit

_SENSITIVE_KEY_PARTS = (
    "access_key",
    "api_key",
    "client_secret",
    "credential",
    "password",
    "signature",
    "token",
)


def validate_nonsecret_locator(value: str, label: str) -> str:
    if "://" not in value:
        return value
    parts = urlsplit(value)
    if parts.username or parts.password:
        raise ValueError(f"{label} cannot contain URL user information; use environment identity")
    for key, _ in parse_qsl(parts.query, keep_blank_values=True):
        normalized = key.lower().replace("-", "_")
        if any(part in normalized for part in _SENSITIVE_KEY_PARTS):
            raise ValueError(
                f"{label} contains a secret-bearing query parameter; use environment identity"
            )
    return value
