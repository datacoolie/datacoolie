"""Portable Unity Catalog Volume and native cloud path validation."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from typing import Literal
from urllib.parse import unquote, urlsplit

from datacoolie.core.exceptions import PlatformError

DatabricksPathKind = Literal["volume", "cloud"]

_VOLUME_PREFIX = "/Volumes/"
_VOLUME_ALIAS_PREFIX = "dbfs:/Volumes/"
_CLOUD_SCHEMES = frozenset({"abfss", "gs", "s3"})
_ENCODED_SEPARATOR_MARKERS = ("%2f", "%5c")


@dataclass(frozen=True, slots=True)
class DatabricksPath:
    """Validated path with one stable canonical representation."""

    kind: DatabricksPathKind
    canonical_path: str
    backend_path: str
    parts: tuple[str, ...]

    @property
    def is_volume(self) -> bool:
        return self.kind == "volume"

    @property
    def volume_identity(self) -> tuple[str, str, str] | None:
        """Return the Unity Catalog volume coordinate for Volume paths."""
        if not self.is_volume or len(self.parts) < 4:
            return None
        return self.parts[1], self.parts[2], self.parts[3]


def _validate_segment(segment: str) -> str:
    lowered = segment.lower()
    if any(marker in lowered for marker in _ENCODED_SEPARATOR_MARKERS):
        raise PlatformError(
            "Encoded path separators are not allowed in Databricks paths."
        )
    decoded = unquote(segment)
    if unquote(decoded) != decoded:
        raise PlatformError(
            "Double-encoded Databricks path components are not allowed."
        )
    if (
        not decoded
        or decoded in {".", ".."}
        or "/" in decoded
        or "\\" in decoded
        or any(ord(character) < 32 or ord(character) == 127 for character in decoded)
    ):
        raise PlatformError("Invalid Databricks path component.")
    return decoded


def _validate_parts(raw_path: str) -> tuple[str, ...]:
    if "\\" in raw_path or "//" in raw_path:
        raise PlatformError(
            "Databricks paths cannot contain backslashes or empty segments."
        )
    stripped = raw_path.strip("/")
    if not stripped:
        return ()
    return tuple(_validate_segment(part) for part in stripped.split("/"))


def parse_databricks_path(path: str, *, allow_cloud: bool) -> DatabricksPath:
    """Parse a portable Volume path or a native-only raw cloud URI."""
    if not isinstance(path, str) or not path:
        raise PlatformError("A Databricks path is required.")
    if path != path.strip():
        raise PlatformError("Databricks paths must not contain surrounding whitespace.")
    if "?" in path or "#" in path:
        raise PlatformError(
            "Query strings and fragments are not allowed in Databricks paths."
        )

    if path.startswith(_VOLUME_ALIAS_PREFIX):
        path = path.removeprefix("dbfs:")
    if path.startswith(_VOLUME_PREFIX):
        parts = _validate_parts(path)
        if len(parts) < 4 or parts[0] != "Volumes":
            raise PlatformError(
                "Unity Catalog paths must use /Volumes/<catalog>/<schema>/<volume>."
            )
        canonical = "/" + "/".join(parts)
        return DatabricksPath("volume", canonical, canonical, parts)

    try:
        parsed = urlsplit(path)
    except ValueError as exc:
        raise PlatformError("Malformed Databricks path.") from exc
    scheme = parsed.scheme.lower()
    if scheme == "dbfs":
        raise PlatformError(
            "DBFS root and mounts are not supported; use a Unity Catalog Volume."
        )
    if scheme not in _CLOUD_SCHEMES:
        raise PlatformError(
            "Databricks paths must use /Volumes/...; raw s3://, abfss://, and "
            "gs:// paths are supported only in native Databricks mode."
        )
    if not allow_cloud:
        raise PlatformError(
            "Raw cloud URIs are supported only with runtime='databricks'; "
            "external mode requires /Volumes/<catalog>/<schema>/<volume>/...."
        )
    if (
        parsed.query
        or parsed.fragment
        or parsed.password is not None
        or parsed.username is not None
        and scheme != "abfss"
    ):
        raise PlatformError(
            "Query strings, fragments, or user information are not allowed."
        )
    if not parsed.netloc:
        raise PlatformError("Raw cloud URIs require a storage authority.")
    parts = _validate_parts(parsed.path)
    canonical = path.rstrip("/") if parts else path
    return DatabricksPath("cloud", canonical, canonical, parts)


def ensure_mutable_path(path: DatabricksPath) -> None:
    """Reject mutation of managed Volume and raw storage roots."""
    if path.is_volume:
        if len(path.parts) <= 4:
            raise PlatformError(
                "Mutation of a Unity Catalog Volume root is not allowed."
            )
        return
    if not path.parts:
        raise PlatformError("Mutation of a cloud storage root is not allowed.")


def parent_path(path: DatabricksPath) -> str:
    """Return the canonical parent of a validated mutable path."""
    return posixpath.dirname(path.canonical_path.rstrip("/"))


def canonicalize_output_path(path: str) -> str:
    """Normalize a dbutils Volume alias to the portable POSIX contract."""
    if path.startswith(_VOLUME_ALIAS_PREFIX):
        return path.removeprefix("dbfs:").rstrip("/")
    if path.startswith(_VOLUME_PREFIX):
        return path.rstrip("/")
    return path.rstrip("/")
