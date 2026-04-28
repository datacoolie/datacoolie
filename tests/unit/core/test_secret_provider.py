"""Tests for core secret_provider module — BaseSecretProvider, caching, and resolve_secrets."""

from __future__ import annotations

import threading
import time
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import DataCoolieError
from datacoolie.core.secret_provider import (
    BaseSecretProvider,
    _resolved_values,
    _resolved_values_lock,
    get_registered_secret_values,
    register_secret_values,
    resolve_secrets,
)


# ---------------------------------------------------------------------------
# Concrete test provider
# ---------------------------------------------------------------------------


class InMemorySecretProvider(BaseSecretProvider):
    """Simple in-memory provider for testing."""

    def __init__(self, secrets: Dict[str, str], **kwargs):
        super().__init__(**kwargs)
        self._secrets = secrets
        self.fetch_count = 0

    def _fetch_secret(self, key: str, source: str) -> str:
        self.fetch_count += 1
        if key not in self._secrets:
            raise DataCoolieError(f"Secret '{key}' not found")
        return self._secrets[key]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_global_values():
    """Clear the global resolved-values set before and after each test."""
    with _resolved_values_lock:
        _resolved_values.clear()
    yield
    with _resolved_values_lock:
        _resolved_values.clear()


@pytest.fixture()
def provider():
    return InMemorySecretProvider(
        secrets={"db-password": "s3cret!", "api-token": "tok123"},
        cache_ttl=300,
    )


@pytest.fixture()
def no_cache_provider():
    return InMemorySecretProvider(
        secrets={"db-password": "s3cret!"},
        cache_ttl=0,
    )


# ---------------------------------------------------------------------------
# BaseSecretProvider tests
# ---------------------------------------------------------------------------


class TestBaseSecretProvider:
    def test_get_secret_returns_value(self, provider):
        assert provider.get_secret("db-password") == "s3cret!"

    def test_get_secret_not_found_raises(self, provider):
        with pytest.raises(DataCoolieError, match="not found"):
            provider.get_secret("nonexistent")

    def test_get_secrets_returns_dict(self, provider):
        result = provider.get_secrets(["db-password", "api-token"])
        assert result == {"db-password": "s3cret!", "api-token": "tok123"}

    def test_cache_avoids_refetch(self, provider):
        provider.get_secret("db-password")
        provider.get_secret("db-password")
        assert provider.fetch_count == 1  # cached on second call

    def test_cache_disabled_always_fetches(self, no_cache_provider):
        no_cache_provider.get_secret("db-password")
        no_cache_provider.get_secret("db-password")
        assert no_cache_provider.fetch_count == 2

    def test_clear_cache_forces_refetch(self, provider):
        provider.get_secret("db-password")
        provider.clear_cache()
        provider.get_secret("db-password")
        assert provider.fetch_count == 2

    def test_cache_expires(self):
        p = InMemorySecretProvider(
            secrets={"k": "v"},
            cache_ttl=1,
        )
        p.get_secret("k")
        assert p.fetch_count == 1
        time.sleep(1.1)
        p.get_secret("k")
        assert p.fetch_count == 2

    def test_thread_safety(self, provider):
        """Multiple threads can safely fetch without errors."""
        results = []

        def fetch():
            results.append(provider.get_secret("db-password"))

        threads = [threading.Thread(target=fetch) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(r == "s3cret!" for r in results)
        assert len(results) == 10


# ---------------------------------------------------------------------------
# register / get_registered_secret_values
# ---------------------------------------------------------------------------


class TestSecretValueRegistry:
    def test_register_and_get(self):
        register_secret_values("abc", "def")
        vals = get_registered_secret_values()
        assert "abc" in vals
        assert "def" in vals

    def test_empty_strings_ignored(self):
        register_secret_values("", None)  # type: ignore[arg-type]
        assert get_registered_secret_values() == frozenset()


# ---------------------------------------------------------------------------
# resolve_secrets
# ---------------------------------------------------------------------------


class TestResolveSecrets:
    def _make_connection(self, secrets_ref=None, configure=None):
        """Create a mock Connection-like object."""
        conn = MagicMock()
        conn.name = "test-conn"
        conn.secrets_ref = secrets_ref
        conn.configure = configure or {}
        return conn

    def test_no_secrets_ref_is_noop(self, provider):
        conn = self._make_connection(secrets_ref=None)
        resolve_secrets(conn, provider)
        assert conn.configure == {}

    def test_empty_string_is_noop(self, provider):
        conn = self._make_connection(secrets_ref="")
        resolve_secrets(conn, provider)
        assert conn.configure == {}

    def test_dict_ref_multiple_fields(self, provider):
        ref = {"some-source": ["password", "auth_token"]}
        conn = self._make_connection(
            secrets_ref=ref,
            configure={"host": "localhost", "password": "db-password", "auth_token": "api-token"},
        )
        resolve_secrets(conn, provider)
        assert conn.configure["password"] == "s3cret!"
        assert conn.configure["auth_token"] == "tok123"
        assert conn.configure["host"] == "localhost"

    def test_dict_ref(self, provider):
        ref = {"some-source": ["password"]}
        conn = self._make_connection(secrets_ref=ref, configure={"password": "db-password"})
        resolve_secrets(conn, provider)
        assert conn.configure["password"] == "s3cret!"

    def test_non_dict_type_raises(self, provider):
        conn = self._make_connection(secrets_ref="not-allowed-string")
        with pytest.raises(DataCoolieError, match="must be a dict"):
            resolve_secrets(conn, provider)

    def test_invalid_type_raises(self, provider):
        conn = self._make_connection(secrets_ref=12345)
        with pytest.raises(DataCoolieError, match="must be a dict"):
            resolve_secrets(conn, provider)

    def test_fetch_failure_raises(self):
        provider = InMemorySecretProvider(secrets={})
        ref = {"some-source": ["password"]}
        conn = self._make_connection(secrets_ref=ref, configure={"password": "missing-key"})
        with pytest.raises(DataCoolieError, match="Failed to resolve secret"):
            resolve_secrets(conn, provider)

    def test_resolved_values_registered_for_masking(self, provider):
        ref = {"some-source": ["password"]}
        conn = self._make_connection(secrets_ref=ref, configure={"password": "db-password"})
        resolve_secrets(conn, provider)
        vals = get_registered_secret_values()
        assert "s3cret!" in vals

    def test_empty_dict_ref_is_noop(self, provider):
        conn = self._make_connection(secrets_ref={})
        resolve_secrets(conn, provider)
        assert conn.configure == {}

    def test_non_list_fields_value_raises(self, provider):
        conn = self._make_connection(secrets_ref={"source-a": "password"}, configure={"password": "db-password"})
        with pytest.raises(DataCoolieError, match="must be lists of field names"):
            resolve_secrets(conn, provider)

    def test_missing_configure_field_raises(self, provider):
        conn = self._make_connection(secrets_ref={"source-a": ["password"]}, configure={})
        with pytest.raises(DataCoolieError, match="missing from configure"):
            resolve_secrets(conn, provider)


# ---------------------------------------------------------------------------
# Phase F — connection.database refresh after secret resolution
# ---------------------------------------------------------------------------


class TestDatabaseFieldRefresh:
    """Verify connection.database is updated after secrets are resolved."""

    def _make_connection(self, secrets_ref=None, configure=None):
        conn = MagicMock()
        conn.name = "test-conn"
        conn.secrets_ref = secrets_ref
        conn.configure = configure or {}
        conn.database = configure.get("database") if configure else None
        conn.catalog = configure.get("catalog") if configure else None

        def _refresh():
            if "database" in conn.configure:
                conn.database = conn.configure["database"]
            if "catalog" in conn.configure:
                conn.catalog = conn.configure["catalog"]

        conn.refresh_from_configure = _refresh
        return conn

    def test_database_field_updated_after_resolve(self):
        provider = InMemorySecretProvider(secrets={"db-vault-key": "prod_db"})
        conn = self._make_connection(
            secrets_ref={"vault": ["database"]},
            configure={"database": "db-vault-key", "host": "localhost"},
        )
        resolve_secrets(conn, provider)
        assert conn.configure["database"] == "prod_db"
        assert conn.database == "prod_db"

    def test_catalog_field_updated_after_resolve(self):
        provider = InMemorySecretProvider(secrets={"cat-vault-key": "prod_catalog"})
        conn = self._make_connection(
            secrets_ref={"vault": ["catalog"]},
            configure={"catalog": "cat-vault-key"},
        )
        resolve_secrets(conn, provider)
        assert conn.configure["catalog"] == "prod_catalog"
        assert conn.catalog == "prod_catalog"

    def test_no_database_key_in_configure_skips_refresh(self):
        provider = InMemorySecretProvider(secrets={"pw": "s3cret"})
        conn = self._make_connection(
            secrets_ref={"vault": ["password"]},
            configure={"password": "pw"},
        )
        conn.database = "original_db"
        resolve_secrets(conn, provider)
        # database was not in secrets_ref, so it should remain unchanged
        assert conn.database == "original_db"
