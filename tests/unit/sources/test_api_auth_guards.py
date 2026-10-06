"""Characterization guards for OAuth2 token caching and request isolation."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.sources._api import auth


def _response(token: str, *, expires_in: int = 3600, status_code: int = 200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": expires_in,
    }
    return response


@pytest.fixture(autouse=True)
def clear_oauth_cache():
    with auth._OAUTH2_TOKEN_CACHE_LOCK:
        auth._OAUTH2_TOKEN_CACHE.clear()
    yield
    with auth._OAUTH2_TOKEN_CACHE_LOCK:
        auth._OAUTH2_TOKEN_CACHE.clear()


def _config(token_url="https://api.example.test/oauth/token", client_id="client-a"):
    return {
        "token_url": token_url,
        "client_id": client_id,
        "client_secret": "secret-value",
        "scope": "read:orders",
    }


def test_oauth_cache_hit_then_controlled_expiry_fetches_again(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(auth.time, "monotonic", lambda: now[0])
    post = MagicMock(
        side_effect=[
            _response("token-one", expires_in=40),
            _response("token-two", expires_in=40),
        ]
    )
    monkeypatch.setattr(auth.httpx, "post", post)
    config = _config()

    assert auth.fetch_oauth2_token(config) == "token-one"
    now[0] = 109.0  # expires_at = 100 + 40 - 30 = 110
    assert auth.fetch_oauth2_token(config) == "token-one"
    assert post.call_count == 1
    now[0] = 110.0
    assert auth.fetch_oauth2_token(config) == "token-two"
    assert post.call_count == 2


def test_oauth_expired_token_is_not_reused_after_failed_fetch(monkeypatch):
    now = [10.0]
    monkeypatch.setattr(auth.time, "monotonic", lambda: now[0])
    post = MagicMock(
        side_effect=[
            _response("initial-token", expires_in=31),
            _response("ignored", status_code=503),
            _response("refreshed-token", expires_in=31),
        ]
    )
    monkeypatch.setattr(auth.httpx, "post", post)
    config = _config()

    assert auth.fetch_oauth2_token(config) == "initial-token"
    now[0] = 12.0  # expires_at = 11; the cache entry is expired
    with pytest.raises(SourceError, match="HTTP 503"):
        auth.fetch_oauth2_token(config)
    assert auth.fetch_oauth2_token(config) == "refreshed-token"
    assert post.call_count == 3


def test_oauth_cache_isolated_by_url_and_client_id(monkeypatch):
    post = MagicMock(
        side_effect=[
            _response("url-a"),
            _response("url-b"),
            _response("client-b"),
        ]
    )
    monkeypatch.setattr(auth.httpx, "post", post)
    first = _config("https://a.example.test/oauth/token", "client-a")
    second_url = _config("https://b.example.test/oauth/token", "client-a")
    second_client = _config("https://a.example.test/oauth/token", "client-b")

    assert auth.fetch_oauth2_token(first) == "url-a"
    assert auth.fetch_oauth2_token(second_url) == "url-b"
    assert auth.fetch_oauth2_token(second_client) == "client-b"
    assert auth.fetch_oauth2_token(first) == "url-a"
    assert auth.fetch_oauth2_token(second_url) == "url-b"
    assert auth.fetch_oauth2_token(second_client) == "client-b"
    assert post.call_count == 3


@pytest.mark.parametrize("body_format", ["form", "json"])
def test_oauth_request_declares_body_format_scope_and_extras(monkeypatch, body_format):
    post = MagicMock(return_value=_response("shape-token"))
    monkeypatch.setattr(auth.httpx, "post", post)
    config = _config()
    config.update(
        {
            "token_request_body_format": body_format,
            "token_request_extras": {"audience": "orders"},
        }
    )

    assert auth.fetch_oauth2_token(config) == "shape-token"
    kwargs = post.call_args.kwargs
    assert kwargs["timeout"] == 30
    payload_key = "data" if body_format == "form" else "json"
    payload = kwargs[payload_key]
    assert payload["grant_type"] == "client_credentials"
    assert payload["scope"] == "read:orders"
    assert payload["audience"] == "orders"
    assert payload["client_id"] == "client-a"
    assert payload["client_secret"] == "secret-value"
    assert ("json" not in kwargs) if body_format == "form" else ("data" not in kwargs)
