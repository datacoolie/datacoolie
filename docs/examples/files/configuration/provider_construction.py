"""Construction-only examples for non-file metadata providers."""

from __future__ import annotations

import os

from datacoolie.metadata.api_provider import APIProvider
from datacoolie.metadata.database_provider import DatabaseProvider


def database_provider() -> DatabaseProvider:
    """Create a lazy SQLite/server database provider from environment config."""
    return DatabaseProvider(
        connection_string=os.environ.get("DATACOOLIE_METADATA_DB_URL", "sqlite:///metadata.db"),
        workspace_id=os.environ.get("DATACOOLIE_WORKSPACE_ID", "example-workspace"),
    )


def api_provider() -> APIProvider:
    """Create a lazy API provider without logging the API key."""
    return APIProvider(
        base_url=os.environ.get("DATACOOLIE_METADATA_API_URL", "https://metadata.example.invalid/v1"),
        api_key=os.environ.get("DATACOOLIE_METADATA_API_KEY", "set-me-before-use"),
        workspace_id=os.environ.get("DATACOOLIE_WORKSPACE_ID", "example-workspace"),
    )


__all__ = ["api_provider", "database_provider"]
