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
    SecretStr,
    resolve_secrets,
    unwrap_configure,
    unwrap_secret,
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
# SecretStr tests
# ---------------------------------------------------------------------------


class TestSecretStr:
    def test_no_public_access_method(self):
        s = SecretStr("hunter2")
        assert not hasattr(s, "get_secret_value")
        # The only way to read the raw value is via unwrap_secret
        assert unwrap_secret(s) == "hunter2"

    def test_str_is_masked(self):
        s = SecretStr("hunter2")
        assert str(s) == "***"

    def test_repr_is_masked(self):
        s = SecretStr("hunter2")
        assert repr(s) == "SecretStr('***')"

    def test_format_is_masked(self):
        s = SecretStr("hunter2")
        assert f"pw={s}" == "pw=***"
        assert f"{s!s}" == "***"

    def test_print_is_masked(self, capsys):
        s = SecretStr("hunter2")
        print(s)  # noqa: T201
        assert capsys.readouterr().out.strip() == "***"

    def test_immutable(self):
        s = SecretStr("hunter2")
        with pytest.raises(AttributeError, match="immutable"):
            s._value = "changed"
        with pytest.raises(AttributeError, match="immutable"):
            del s._value

    def test_bool_true(self):
        assert bool(SecretStr("x"))

    def test_bool_false(self):
        assert not bool(SecretStr(""))

    def test_len(self):
        assert len(SecretStr("abc")) == 3

    def test_equality(self):
        a = SecretStr("same")
        b = SecretStr("same")
        assert a == b

    def test_inequality(self):
        assert SecretStr("a") != SecretStr("b")

    def test_not_equal_to_plain_str(self):
        assert SecretStr("x") != "x"

    def test_cannot_pickle(self):
        import pickle
        with pytest.raises(TypeError, match="Cannot pickle"):
            pickle.dumps(SecretStr("x"))

    def test_not_in_dict_repr(self):
        d = {"password": SecretStr("hunter2"), "host": "localhost"}
        text = str(d)
        assert "hunter2" not in text
        assert "***" in text

    def test_no_way_to_expose_via_common_operations(self):
        """Secret value never leaks through any standard Python operation."""
        s = SecretStr("hidden")
        # No public method to read
        assert not hasattr(s, "get_secret_value")
        # Cannot read via attribute access (immutable blocks setattr)
        with pytest.raises(AttributeError):
            s._value = "x"
        # str/repr/format all masked
        assert "hidden" not in str(s)
        assert "hidden" not in repr(s)
        assert "hidden" not in f"{s}"


class TestUnwrapHelpers:
    def test_unwrap_secret_with_secret_str(self):
        assert unwrap_secret(SecretStr("val")) == "val"

    def test_unwrap_secret_with_plain_value(self):
        assert unwrap_secret("plain") == "plain"
        assert unwrap_secret(42) == 42
        assert unwrap_secret(None) is None

    def test_unwrap_configure(self):
        cfg = {
            "host": "localhost",
            "password": SecretStr("s3cret"),
            "port": 5432,
        }
        result = unwrap_configure(cfg)
        assert result == {"host": "localhost", "password": "s3cret", "port": 5432}
        # Original is unchanged
        assert isinstance(cfg["password"], SecretStr)

    def test_unwrap_keeps_original_wrapped(self):
        """unwrap_configure does not mutate the original dict."""
        cfg = {"pw": SecretStr("secret"), "host": "localhost"}
        plain = unwrap_configure(cfg)
        assert plain == {"pw": "secret", "host": "localhost"}
        assert isinstance(cfg["pw"], SecretStr)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
        assert isinstance(conn.configure["password"], SecretStr)
        assert unwrap_secret(conn.configure["password"]) == "s3cret!"
        assert isinstance(conn.configure["auth_token"], SecretStr)
        assert unwrap_secret(conn.configure["auth_token"]) == "tok123"
        assert conn.configure["host"] == "localhost"  # non-secret, stays plain

    def test_dict_ref(self, provider):
        ref = {"some-source": ["password"]}
        conn = self._make_connection(secrets_ref=ref, configure={"password": "db-password"})
        resolve_secrets(conn, provider)
        assert isinstance(conn.configure["password"], SecretStr)
        assert unwrap_secret(conn.configure["password"]) == "s3cret!"

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
                conn.database = unwrap_secret(conn.configure["database"])
            if "catalog" in conn.configure:
                conn.catalog = unwrap_secret(conn.configure["catalog"])

        conn.refresh_from_configure = _refresh
        return conn

    def test_database_field_updated_after_resolve(self):
        provider = InMemorySecretProvider(secrets={"db-vault-key": "prod_db"})
        conn = self._make_connection(
            secrets_ref={"vault": ["database"]},
            configure={"database": "db-vault-key", "host": "localhost"},
        )
        resolve_secrets(conn, provider)
        assert isinstance(conn.configure["database"], SecretStr)
        assert unwrap_secret(conn.configure["database"]) == "prod_db"
        assert conn.database == "prod_db"  # unwrapped by refresh_from_configure

    def test_catalog_field_updated_after_resolve(self):
        provider = InMemorySecretProvider(secrets={"cat-vault-key": "prod_catalog"})
        conn = self._make_connection(
            secrets_ref={"vault": ["catalog"]},
            configure={"catalog": "cat-vault-key"},
        )
        resolve_secrets(conn, provider)
        assert isinstance(conn.configure["catalog"], SecretStr)
        assert unwrap_secret(conn.configure["catalog"]) == "prod_catalog"
        assert conn.catalog == "prod_catalog"  # unwrapped by refresh_from_configure

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
