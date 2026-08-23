from __future__ import annotations

from unittest.mock import MagicMock

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._aws.secrets import SecretsBackend


def test_payload_cache_fetches_json_source_once_for_multiple_fields() -> None:
    client = MagicMock()
    client.get_secret_value.return_value = {
        "SecretString": '{"username":"alice","password":"secret"}'
    }
    backend = SecretsBackend(lambda service: client, cache_ttl=300)

    assert backend.fetch_secret("username", "db") == "alice"
    assert backend.fetch_secret("password", "db") == "secret"
    client.get_secret_value.assert_called_once_with(SecretId="db")


def test_payload_cache_can_be_disabled_and_cleared() -> None:
    client = MagicMock()
    client.get_secret_value.side_effect = [
        {"SecretString": "one"},
        {"SecretString": "two"},
        {"SecretString": "three"},
    ]
    backend = SecretsBackend(lambda service: client, cache_ttl=0)
    assert backend.fetch_secret("key", "db") == "one"
    assert backend.fetch_secret("key", "db") == "two"

    cached = SecretsBackend(lambda service: client, cache_ttl=300)
    assert cached.fetch_secret("key", "db") == "three"
    cached.clear_cache()
    client.get_secret_value.side_effect = None
    client.get_secret_value.return_value = {"SecretString": "four"}
    assert cached.fetch_secret("key", "db") == "four"


def test_missing_json_field_is_a_platform_error() -> None:
    client = MagicMock()
    client.get_secret_value.return_value = {"SecretString": '{"present":"x"}'}
    backend = SecretsBackend(lambda service: client)
    try:
        backend.fetch_secret("missing", "db")
    except PlatformError as exc:
        assert "missing" in str(exc)
    else:
        raise AssertionError("missing JSON field did not fail")
