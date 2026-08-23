"""Qualified OneLake and ADLS Gen2 path parsing for the Azure SDK backend."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from urllib.parse import quote, unquote, urlsplit

from datacoolie.core.exceptions import PlatformError

AzureDataLakeProvider = Literal["onelake", "adls"]

_ENCODED_SEPARATOR_MARKERS = ("%2f", "%5c")
_CANONICAL_SAFE = "!$&'()*+,;=:@-._~"
_FILESYSTEM_SAFE = "-._~"


@dataclass(frozen=True, slots=True)
class AzureDataLakePath:
    """SDK-ready components and canonical URI for one qualified cloud path."""

    provider: AzureDataLakeProvider
    account_url: str
    file_system: str
    path: str
    canonical_uri: str
    host: str

    def with_path(self, path: str) -> "AzureDataLakePath":
        """Return the same account/filesystem location with a different SDK path."""
        normalized = _decode_path(path, field="path")
        return _build_location(self.provider, self.host, self.file_system, normalized)


def _provider_for_host(host: str) -> AzureDataLakeProvider:
    if host == "onelake.dfs.fabric.microsoft.com" or host.endswith(
        "-onelake.dfs.fabric.microsoft.com"
    ):
        return "onelake"
    if host.endswith(".dfs.fabric.microsoft.com"):
        return "onelake"
    if host.endswith(".dfs.core.windows.net") and host != "dfs.core.windows.net":
        return "adls"
    raise PlatformError(
        "Unsupported Azure Data Lake host. Expected a OneLake DFS or ADLS Gen2 DFS host."
    )


def _decode_segment(value: str, *, field: str) -> str:
    lowered = value.lower()
    if any(marker in lowered for marker in _ENCODED_SEPARATOR_MARKERS):
        raise PlatformError(f"Encoded path separators are not allowed in Azure {field}.")
    decoded = unquote(value)
    if unquote(decoded) != decoded:
        raise PlatformError(f"Double-encoded Azure {field} components are not allowed.")
    if (
        not decoded
        or decoded in {".", ".."}
        or "/" in decoded
        or "\\" in decoded
        or any(ord(character) < 32 or ord(character) == 127 for character in decoded)
        or (field == "filesystem" and ("@" in decoded or ":" in decoded))
    ):
        raise PlatformError(f"Invalid Azure {field} component.")
    return decoded


def _decode_path(value: str, *, field: str) -> str:
    if value.startswith("//") or value.endswith("//"):
        raise PlatformError(f"Invalid Azure {field}; empty path segments are not allowed.")
    raw = value.strip("/")
    if not raw:
        return ""
    if "\\" in raw or "//" in raw:
        raise PlatformError(f"Invalid Azure {field}; empty or backslash segments are not allowed.")
    return "/".join(_decode_segment(segment, field=field) for segment in raw.split("/"))


def _encode_segment(value: str) -> str:
    return quote(value, safe=_CANONICAL_SAFE)


def _build_location(
    provider: AzureDataLakeProvider,
    host: str,
    file_system: str,
    path: str,
) -> AzureDataLakePath:
    encoded_fs = quote(file_system, safe=_FILESYSTEM_SAFE)
    encoded_path = "/".join(_encode_segment(segment) for segment in path.split("/")) if path else ""
    canonical = f"abfss://{encoded_fs}@{host}"
    if encoded_path:
        canonical = f"{canonical}/{encoded_path}"
    return AzureDataLakePath(
        provider=provider,
        account_url=f"https://{host}",
        file_system=file_system,
        path=path,
        canonical_uri=canonical,
        host=host,
    )


def parse_azure_datalake_path(uri: str) -> AzureDataLakePath:
    """Parse a qualified ABFS(S) or HTTPS OneLake/ADLS Gen2 URI."""
    if not isinstance(uri, str) or not uri.strip():
        raise PlatformError("A qualified OneLake or ADLS Gen2 path is required.")
    if uri != uri.strip():
        raise PlatformError("Azure paths must not contain leading or trailing whitespace.")

    try:
        parsed = urlsplit(uri)
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError as exc:
        raise PlatformError("Malformed Azure path authority.") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"abfs", "abfss", "https"}:
        raise PlatformError("External Fabric paths must use abfs://, abfss://, or https://.")
    if parsed.query or parsed.fragment:
        raise PlatformError("Query strings and fragments are not allowed in Azure paths.")
    if port is not None:
        raise PlatformError("Ports are not allowed in Azure paths.")

    if not host:
        raise PlatformError("Azure paths require a DFS host.")
    provider = _provider_for_host(host)

    if scheme in {"abfs", "abfss"}:
        if parsed.password is not None or parsed.username is None:
            raise PlatformError("ABFS paths must use <filesystem>@<dfs-host> authority syntax.")
        file_system = _decode_segment(parsed.username, field="filesystem")
        path = _decode_path(parsed.path, field="path")
    else:
        if parsed.username is not None or parsed.password is not None:
            raise PlatformError("User information is not allowed in Azure HTTPS paths.")
        raw_parts = parsed.path.strip("/").split("/") if parsed.path.strip("/") else []
        if not raw_parts:
            raise PlatformError("Azure HTTPS paths must include a workspace or filesystem.")
        file_system = _decode_segment(raw_parts[0], field="filesystem")
        path = _decode_path("/".join(raw_parts[1:]), field="path")

    return _build_location(provider, host, file_system, path)


def ensure_mutable_path(location: AzureDataLakePath) -> None:
    """Reject destructive operations against storage and OneLake managed roots."""
    parts = location.path.split("/") if location.path else []
    if not parts:
        raise PlatformError("Mutation of a workspace or filesystem root is not allowed.")
    if location.provider != "onelake":
        return
    if len(parts) < 3 or parts[1].lower() not in {"files", "tables"}:
        raise PlatformError(
            "OneLake mutations are allowed only below an item's Files or Tables root."
        )
