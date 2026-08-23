"""Lazy boto3 session and service-client helpers for AWSPlatform."""

from __future__ import annotations

from typing import Any

from datacoolie.core.exceptions import PlatformError


def create_session(region: str | None, profile: str | None) -> Any:
    """Create a boto3 Session without importing boto3 at module import time."""
    try:
        import boto3  # type: ignore[import-untyped]
    except ImportError as exc:
        raise PlatformError(
            "boto3 is not available. Install it with: pip install boto3"
        ) from exc

    session_kwargs: dict[str, Any] = {}
    if region:
        session_kwargs["region_name"] = region
    if profile:
        session_kwargs["profile_name"] = profile
    return boto3.Session(**session_kwargs)


def create_client(
    session: Any,
    service: str,
    *,
    storage_endpoint_url: str | None = None,
    **kwargs: Any,
) -> Any:
    """Create a service client, applying the platform endpoint to S3 only."""
    client_kwargs: dict[str, Any] = {**kwargs}
    if service == "s3" and storage_endpoint_url and "endpoint_url" not in client_kwargs:
        client_kwargs["endpoint_url"] = storage_endpoint_url
    return session.client(service, **client_kwargs)
