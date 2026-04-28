"""Tests for the pluggable secret resolver system."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from datacoolie.core.exceptions import DataCoolieError
from datacoolie.core.secret_resolver import (
    BaseSecretResolver,
    EnvResolver,
    NativeProviderResolver,
    parse_source,
)
from datacoolie.core.secret_provider import (
    BaseSecretProvider,
    _resolved_values,
    _resolved_values_lock,
    get_registered_secret_values,
    resolve_secrets,
)


# ---------------------------------------------------------------------------
# Concrete test helpers
# ---------------------------------------------------------------------------


class InMemoryResolver(BaseSecretResolver):
    """Simple in-memory resolver for testing."""

    def __init__(self, secrets: dict[str, str], **_: object) -> None:
        self._secrets = secrets
        self.call_count = 0

    def resolve(self, key: str, source: str) -> str:
        self.call_count += 1
        full_key = f"{source}{key}" if source else key
        if full_key not in self._secrets:
            raise DataCoolieError(f"Resolver secret '{full_key}' not found")
        return self._secrets[full_key]


class InMemoryProvider(BaseSecretProvider):
    """Minimal provider for testing resolve_secrets."""

    def __init__(self, secrets: dict[str, str]) -> None:
        super().__init__(cache_ttl=0)
        self._secrets = secrets
        self.call_count = 0

    def _fetch_secret(self, key: str, source: str) -> str:
        self.call_count += 1
        if key not in self._secrets:
            raise DataCoolieError(f"Provider secret '{key}' not found")
        return self._secrets[key]


def _make_connection(secrets_ref=None, configure=None):
    conn = MagicMock()
    conn.name = "test-conn"
    conn.secrets_ref = secrets_ref
    conn.configure = configure or {}
    return conn


def _env_lookup(prefix: str) -> BaseSecretResolver | None:
    """Test lookup that only knows about 'env'."""
    if prefix == "env":
        return EnvResolver()
    return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_global_values():
    with _resolved_values_lock:
        _resolved_values.clear()
    yield
    with _resolved_values_lock:
        _resolved_values.clear()


# ---------------------------------------------------------------------------
# EnvResolver unit tests
# ---------------------------------------------------------------------------


class TestEnvResolver:
    def test_resolve_with_prefix(self, monkeypatch):
        monkeypatch.setenv("APP_DB_PASSWORD", "s3cret!")
        resolver = EnvResolver()
        assert resolver.resolve("DB_PASSWORD", "APP_") == "s3cret!"

    def test_resolve_without_prefix(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "hello")
        resolver = EnvResolver()
        assert resolver.resolve("MY_SECRET", "") == "hello"

    def test_missing_env_var_raises(self, monkeypatch):
        monkeypatch.delenv("DOES_NOT_EXIST", raising=False)
        resolver = EnvResolver()
        with pytest.raises(DataCoolieError, match="not set"):
            resolver.resolve("DOES_NOT_EXIST", "")


# ---------------------------------------------------------------------------
# NativeProviderResolver tests
# ---------------------------------------------------------------------------


class TestNativeProviderResolver:
    def test_delegates_to_provider(self):
        provider = InMemoryProvider(secrets={"my-key": "my-val"})
        resolver = NativeProviderResolver(provider)
        assert resolver.resolve("my-key", "") == "my-val"
        assert provider.call_count == 1

    def test_passes_source_through(self):
        provider = MagicMock()
        provider.get_secret.return_value = "resolved"
        resolver = NativeProviderResolver(provider)
        result = resolver.resolve("key", "scope")
        provider.get_secret.assert_called_once_with("key", "scope")
        assert result == "resolved"


# ---------------------------------------------------------------------------
# parse_source tests
# ---------------------------------------------------------------------------


class TestParseSource:
    def test_no_colon_returns_fallback(self):
        fallback = InMemoryResolver(secrets={})
        resolver, arg = parse_source("some-scope", fallback)
        assert resolver is fallback
        assert arg == "some-scope"

    def test_known_prefix_returns_plugin(self):
        env = EnvResolver()
        fallback = InMemoryResolver(secrets={})
        resolver, arg = parse_source("env:APP_", fallback, lambda p: env if p == "env" else None)
        assert resolver is env
        assert arg == "APP_"

    def test_unknown_prefix_returns_fallback(self):
        fallback = InMemoryResolver(secrets={})
        resolver, arg = parse_source("unknown:stuff", fallback, lambda _: None)
        assert resolver is fallback
        assert arg == "unknown:stuff"

    def test_no_lookup_returns_fallback(self):
        fallback = InMemoryResolver(secrets={})
        resolver, arg = parse_source("env:APP_", fallback, None)
        assert resolver is fallback
        assert arg == "env:APP_"

    def test_empty_arg_after_prefix(self):
        env = EnvResolver()
        fallback = InMemoryResolver(secrets={})
        resolver, arg = parse_source("env:", fallback, lambda p: env if p == "env" else None)
        assert resolver is env
        assert arg == ""


# ---------------------------------------------------------------------------
# BaseSecretResolver ABC tests
# ---------------------------------------------------------------------------


class TestBaseSecretResolver:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            BaseSecretResolver()  # type: ignore[abstract]

    def test_concrete_subclass_works(self):
        resolver = InMemoryResolver(secrets={"key1": "val1"})
        assert resolver.resolve("key1", "") == "val1"


# ---------------------------------------------------------------------------
# Resolver dispatch in resolve_secrets (integration)
# ---------------------------------------------------------------------------


class TestResolverDispatch:
    """Test that resolve_secrets dispatches to plugin resolvers for prefixed sources."""

    def test_env_prefix_dispatches_to_resolver(self, monkeypatch):
        """Source 'env:APP_' should dispatch to EnvResolver instead of provider."""
        monkeypatch.setenv("APP_DB_PASSWORD", "env-secret")

        provider = InMemoryProvider(secrets={})
        conn = _make_connection(
            secrets_ref={"env:APP_": ["password"]},
            configure={"password": "DB_PASSWORD"},
        )
        resolve_secrets(conn, provider, resolver_lookup=_env_lookup)

        assert conn.configure["password"] == "env-secret"
        assert provider.call_count == 0  # provider NOT called

    def test_no_prefix_uses_native_provider(self):
        """Source without ':' should use the platform provider."""
        provider = InMemoryProvider(secrets={"db-password": "native-secret"})
        conn = _make_connection(
            secrets_ref={"some-scope": ["password"]},
            configure={"password": "db-password"},
        )
        resolve_secrets(conn, provider, resolver_lookup=_env_lookup)

        assert conn.configure["password"] == "native-secret"
        assert provider.call_count == 1

    def test_unknown_prefix_uses_native_provider(self):
        """Source with ':' but unregistered prefix falls back to provider."""
        provider = InMemoryProvider(secrets={"key1": "native-val"})
        conn = _make_connection(
            secrets_ref={"unknown:stuff": ["field"]},
            configure={"field": "key1"},
        )
        resolve_secrets(conn, provider, resolver_lookup=_env_lookup)

        assert conn.configure["field"] == "native-val"
        assert provider.call_count == 1

    def test_mixed_sources(self, monkeypatch):
        """Mix of prefixed (env:) and native sources in one secrets_ref."""
        monkeypatch.setenv("MY_TOKEN", "env-tok")

        provider = InMemoryProvider(secrets={"vault-pw": "vault-secret"})
        conn = _make_connection(
            secrets_ref={
                "env:MY_": ["token"],
                "vault-scope": ["password"],
            },
            configure={"token": "TOKEN", "password": "vault-pw"},
        )
        resolve_secrets(conn, provider, resolver_lookup=_env_lookup)

        assert conn.configure["token"] == "env-tok"
        assert conn.configure["password"] == "vault-secret"

    def test_env_prefix_empty_source_part(self, monkeypatch):
        """Source 'env:' with empty source arg — key used as-is."""
        monkeypatch.setenv("MY_PASSWORD", "direct")

        provider = InMemoryProvider(secrets={})
        conn = _make_connection(
            secrets_ref={"env:": ["password"]},
            configure={"password": "MY_PASSWORD"},
        )
        resolve_secrets(conn, provider, resolver_lookup=_env_lookup)

        assert conn.configure["password"] == "direct"

    def test_resolver_failure_raises(self, monkeypatch):
        """Resolver failure is wrapped in DataCoolieError."""
        monkeypatch.delenv("NONEXISTENT", raising=False)

        provider = InMemoryProvider(secrets={})
        conn = _make_connection(
            secrets_ref={"env:": ["password"]},
            configure={"password": "NONEXISTENT"},
        )
        with pytest.raises(DataCoolieError, match="Failed to resolve secret"):
            resolve_secrets(conn, provider, resolver_lookup=_env_lookup)

    def test_resolved_values_registered_for_masking(self, monkeypatch):
        """Secrets resolved via plugin resolvers are still registered for log masking."""
        monkeypatch.setenv("SEC_KEY", "masked-val")

        provider = InMemoryProvider(secrets={})
        conn = _make_connection(
            secrets_ref={"env:SEC_": ["field"]},
            configure={"field": "KEY"},
        )
        resolve_secrets(conn, provider, resolver_lookup=_env_lookup)

        vals = get_registered_secret_values()
        assert "masked-val" in vals

    def test_no_lookup_still_works_with_native(self):
        """When resolver_lookup is None, everything goes through provider."""
        provider = InMemoryProvider(secrets={"pw": "native"})
        conn = _make_connection(
            secrets_ref={"scope": ["password"]},
            configure={"password": "pw"},
        )
        resolve_secrets(conn, provider)  # no resolver_lookup

        assert conn.configure["password"] == "native"
        assert provider.call_count == 1
