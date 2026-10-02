"""DataCoolie metadata — providers for connections, dataflows, and schema hints."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datacoolie.metadata.base import BaseMetadataProvider, MetadataCache
from datacoolie.metadata.contracts.context import MetadataProviderStartupContext
from datacoolie.metadata.resolution.query import QueryReference, classify_query

if TYPE_CHECKING:
    from datacoolie.metadata.api_provider import APIProvider
    from datacoolie.metadata.database_provider import DatabaseProvider
    from datacoolie.metadata.file_provider import FileProvider


def __getattr__(name: str):
    if name == "DatabaseProvider":
        from datacoolie.metadata.database_provider import DatabaseProvider
        return DatabaseProvider
    if name == "APIProvider":
        from datacoolie.metadata.api_provider import APIProvider
        return APIProvider
    if name == "FileProvider":
        from datacoolie.metadata.file_provider import FileProvider
        return FileProvider
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "APIProvider",
    "BaseMetadataProvider",
    "DatabaseProvider",
    "FileProvider",
    "MetadataCache",
    "MetadataProviderStartupContext",
    "QueryReference",
    "classify_query",
]
