"""AWS Secrets Manager backend used by AWSPlatform."""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable

from datacoolie.core.exceptions import PlatformError


class SecretsBackend:
    """Fetch and decode values from AWS Secrets Manager."""

    def __init__(
        self, client_factory: Callable[..., Any], cache_ttl: int = 300
    ) -> None:
        self._client_factory = client_factory
        self._cache_ttl = cache_ttl
        self._payload_cache: dict[str, tuple[str, float]] = {}
        self._cache_lock = threading.RLock()

    def clear_cache(self) -> None:
        """Evict decoded source payloads."""
        with self._cache_lock:
            self._payload_cache.clear()

    def _payload(self, source: str) -> str:
        now = time.monotonic()
        if self._cache_ttl > 0:
            with self._cache_lock:
                cached = self._payload_cache.get(source)
                if cached is not None and now - cached[1] < self._cache_ttl:
                    return cached[0]

        # Serialize the miss path and check again so concurrent first access
        # performs one Secrets Manager request per source.
        with self._cache_lock:
            if self._cache_ttl > 0:
                cached = self._payload_cache.get(source)
                if (
                    cached is not None
                    and time.monotonic() - cached[1] < self._cache_ttl
                ):
                    return cached[0]
            sm_client = self._client_factory("secretsmanager")
            try:
                response = sm_client.get_secret_value(SecretId=source)
                secret_string: str = response.get("SecretString", "") or ""
                if not secret_string:
                    secret_binary = response.get("SecretBinary")
                    if secret_binary is not None:
                        if isinstance(secret_binary, bytes):
                            secret_string = secret_binary.decode("utf-8")
                        else:
                            secret_string = str(secret_binary)
            except Exception as exc:
                raise PlatformError(
                    f"Failed to fetch secret '{source}' from AWS Secrets Manager: {exc}"
                ) from exc

            if self._cache_ttl > 0:
                self._payload_cache[source] = (secret_string, time.monotonic())
            return secret_string

    def fetch_secret(self, key: str, source: str) -> str:
        if not source:
            raise PlatformError(
                "secret name (source) is required for AWSPlatform secret fetching. "
                "Pass it via secrets_ref as the outer key, e.g. "
                '{"prod/db/creds": ["key_1", "key_2"]}.'
            )
        secret_string = self._payload(source)
        try:
            parsed = json.loads(secret_string)
            if isinstance(parsed, dict):
                if key not in parsed:
                    raise PlatformError(
                        f"Key '{key}' not found in secret '{source}'. "
                        f"Available keys: {list(parsed.keys())}"
                    )
                return str(parsed[key])
        except (json.JSONDecodeError, TypeError):
            pass

        return secret_string
