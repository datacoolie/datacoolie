"""DataCoolie metadata — providers for connections, dataflows, and schema hints."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datacoolie.metadata.base import BaseMetadataProvider, MetadataCache

if TYPE_CHECKING:
    from datacoolie.metadata.api_client import APIClient
    from datacoolie.metadata.database_provider import DatabaseProvider
    from datacoolie.metadata.file_provider import FileProvider


def __getattr__(name: str):
    if name == "DatabaseProvider":
        from datacoolie.metadata.database_provider import DatabaseProvider
        return DatabaseProvider
    if name == "APIClient":
        from datacoolie.metadata.api_client import APIClient
        return APIClient
    if name == "FileProvider":
        from datacoolie.metadata.file_provider import FileProvider
        return FileProvider
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "APIClient",
    "BaseMetadataProvider",
    "DatabaseProvider",
    "FileProvider",
    "MetadataCache",
]
