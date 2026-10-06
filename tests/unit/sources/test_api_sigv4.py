"""Focused contracts for API source SigV4 authentication."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from unittest.mock import MagicMock

import httpx
import pytest
from botocore.auth import S3SigV4Auth, SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials, ReadOnlyCredentials, RefreshableCredentials

from datacoolie.core.exceptions import SourceError
from datacoolie.sources._api import auth


def _run_auth(auth_handler, request: httpx.Request) -> httpx.Request:
    return next(auth_handler.auth_flow(request))


def _sdk_signed_headers(
    request: httpx.Request,
    credentials,
    service: str,
    region: str,
    signer_cls,
) -> dict[str, str]:
    aws_request = AWSRequest(
        method=request.method,
        url=str(request.url),
        data=request.content,
        headers=dict(request.headers),
    )
    signer_cls(credentials, service, region).add_auth(aws_request)
    return dict(aws_request.headers)


@pytest.mark.parametrize(
    ("token_config", "expected_token"),
    [
        ({}, None),
        ({"aws_session_token": ""}, None),
        ({"aws_session_token": "synthetic-session-token"}, "synthetic-session-token"),
    ],
    ids=["missing-token", "empty-token", "session-token"],
)
def test_explicit_complete_pair_precedes_session_and_preserves_token(
    monkeypatch, token_config, expected_token
):
    def fail_session():
        raise AssertionError("explicit credentials must not construct a session")

    monkeypatch.setattr(auth._boto_session, "Session", fail_session)
    config = {
        "auth_type": "aws_sigv4",
        "aws_access_key_id": "synthetic-access-key",
        "aws_secret_access_key": "synthetic-secret-key",
        **token_config,
    }

    handler = auth.get_http_auth(config)

    assert isinstance(handler, auth._SigV4Auth)
    assert handler._credentials.access_key == "synthetic-access-key"
    assert handler._credentials.secret_key == "synthetic-secret-key"
    assert handler._credentials.token == expected_token


@pytest.mark.parametrize(
    "partial_config",
    [
        {},
        {"aws_access_key_id": "key-only"},
        {"aws_secret_access_key": "secret-only"},
        {"aws_session_token": "token-only"},
    ],
    ids=["none", "key-only", "secret-only", "token-only"],
)
def test_partial_explicit_values_fall_back_to_the_provider_object(
    monkeypatch, partial_config
):
    provider_credentials = MagicMock(name="provider_credentials")
    session = MagicMock(name="session")
    session.get_credentials.return_value = provider_credentials
    monkeypatch.setattr(auth._boto_session, "Session", lambda: session)

    handler = auth.get_http_auth(
        {"auth_type": "aws_sigv4", **partial_config}
    )

    assert handler._credentials is provider_credentials
    session.get_credentials.assert_called_once_with()


def test_missing_provider_credentials_keep_the_existing_source_error(monkeypatch):
    session = MagicMock(name="session")
    session.get_credentials.return_value = None
    monkeypatch.setattr(auth._boto_session, "Session", lambda: session)

    with pytest.raises(SourceError, match="no AWS credentials found"):
        auth.get_http_auth({"auth_type": "aws_sigv4"})


def test_provider_failure_propagates_without_signing_or_http(monkeypatch):
    provider_error = RuntimeError("synthetic provider failure")
    session = MagicMock(name="session")
    session.get_credentials.side_effect = provider_error
    monkeypatch.setattr(auth._boto_session, "Session", lambda: session)

    with pytest.raises(RuntimeError, match="synthetic provider failure"):
        auth.get_http_auth({"auth_type": "aws_sigv4"})


def test_missing_botocore_keeps_the_dependency_error(monkeypatch):
    monkeypatch.setattr(auth, "_HAS_BOTOCORE", False)

    with pytest.raises(SourceError, match="requires botocore"):
        auth.get_http_auth({"auth_type": "aws_sigv4"})


def test_auth_flow_uses_a_frozen_snapshot_without_reading_live_properties(monkeypatch):
    fixed_now = datetime(2026, 10, 5, 12, 34, 56)
    monkeypatch.setattr(auth.botocore.auth, "get_current_datetime", lambda: fixed_now)

    class GenerationGuard:
        def __init__(self):
            self.freeze_calls = 0

        def get_frozen_credentials(self):
            self.freeze_calls += 1
            return ReadOnlyCredentials(
                "generation-access-key",
                "generation-secret-key",
                "generation-token",
            )

        @property
        def access_key(self):
            raise AssertionError("signing must not read live access_key")

        @property
        def secret_key(self):
            raise AssertionError("signing must not read live secret_key")

        @property
        def token(self):
            raise AssertionError("signing must not read live token")

    credentials = GenerationGuard()
    handler = auth._SigV4Auth(credentials, "execute-api", "us-east-1")
    request = httpx.Request(
        "GET",
        "https://api.example.test/orders?status=ready",
    )

    assert credentials.freeze_calls == 0
    _run_auth(handler, request)

    assert credentials.freeze_calls == 1
    assert request.headers["x-amz-security-token"] == "generation-token"


@pytest.mark.parametrize(
    ("method", "content"),
    [
        ("GET", b""),
        ("POST", b'{"order_id": "42"}'),
    ],
    ids=["get", "post"],
)
def test_generic_sigv4_matches_the_independent_sdk_oracle(
    monkeypatch, method, content
):
    fixed_now = datetime(2026, 10, 5, 12, 34, 56)
    monkeypatch.setattr(auth.botocore.auth, "get_current_datetime", lambda: fixed_now)
    credentials = Credentials(
        "synthetic-access-key",
        "synthetic-secret-key",
        "synthetic-session-token",
    )
    request_kwargs = {
        "headers": {
            "content-type": "application/json",
            "x-client-header": "synthetic-value",
        },
        "content": content,
    }
    request = httpx.Request(
        method,
        "https://service.example.test/orders?next=2&filter=ready",
        **request_kwargs,
    )
    oracle_request = httpx.Request(
        method,
        "https://service.example.test/orders?next=2&filter=ready",
        **request_kwargs,
    )

    _run_auth(auth._SigV4Auth(credentials, "custom-service", "eu-west-3"), request)
    expected = _sdk_signed_headers(
        oracle_request,
        credentials.get_frozen_credentials(),
        "custom-service",
        "eu-west-3",
        SigV4Auth,
    )

    for header in ("Authorization", "X-Amz-Date", "X-Amz-Security-Token"):
        assert request.headers[header] == expected[header]


def test_s3_sigv4_matches_the_sdk_oracle_and_preserves_object_path(monkeypatch):
    fixed_now = datetime(2026, 10, 5, 12, 34, 56)
    monkeypatch.setattr(auth.botocore.auth, "get_current_datetime", lambda: fixed_now)
    credentials = Credentials(
        "synthetic-access-key",
        "synthetic-secret-key",
        "synthetic-session-token",
    )
    url = (
        "https://bucket.example.test/root//folder with space/%E2%9C%93"
        "?prefix=a%2Fb&prefix=a%2Fb"
    )
    expected_transmitted_url = (
        "https://bucket.example.test/root//folder%20with%20space/%E2%9C%93"
        "?prefix=a%2Fb&prefix=a%2Fb"
    )
    expected_raw_path = (
        b"/root//folder%20with%20space/%E2%9C%93"
        b"?prefix=a%2Fb&prefix=a%2Fb"
    )
    expected_body_hash = hashlib.sha256(b"synthetic-body").hexdigest()
    request_kwargs = {
        "headers": {"content-type": "application/octet-stream"},
        "content": b"synthetic-body",
    }
    request = httpx.Request("POST", url, **request_kwargs)
    oracle_request = httpx.Request("POST", url, **request_kwargs)
    generic_request = httpx.Request("POST", url, **request_kwargs)
    transmitted_url_before = str(request.url)
    raw_path_before = request.url.raw_path
    assert transmitted_url_before == expected_transmitted_url
    assert raw_path_before == expected_raw_path

    _run_auth(auth._SigV4Auth(credentials, "s3", "eu-west-3"), request)
    expected = _sdk_signed_headers(
        oracle_request,
        credentials.get_frozen_credentials(),
        "s3",
        "eu-west-3",
        S3SigV4Auth,
    )
    generic_headers = _sdk_signed_headers(
        generic_request,
        credentials.get_frozen_credentials(),
        "s3",
        "eu-west-3",
        SigV4Auth,
    )

    for header in (
        "Authorization",
        "X-Amz-Date",
        "X-Amz-Security-Token",
        "X-Amz-Content-SHA256",
    ):
        assert request.headers[header] == expected[header]
    assert str(request.url) == expected_transmitted_url
    assert request.url.raw_path == expected_raw_path
    assert str(request.url) == transmitted_url_before
    assert request.url.raw_path == raw_path_before
    assert request.headers["X-Amz-Content-SHA256"] == expected_body_hash
    assert request.headers["Authorization"] != generic_headers["Authorization"]
    assert "X-Amz-Content-SHA256" not in generic_headers


def test_refreshable_provider_is_frozen_again_for_each_request(monkeypatch):
    clock = [datetime(2026, 10, 5, 12, 34, 56, tzinfo=timezone.utc)]
    refresh_generations = []

    def refresh_credentials():
        refresh_generations.append("generation-b")
        return {
            "access_key": "generation-b-access-key",
            "secret_key": "generation-b-secret-key",
            "token": "generation-b-token",
            "expiry_time": (clock[0] + timedelta(hours=1)).isoformat(),
        }

    credentials = RefreshableCredentials(
        access_key="generation-a-access-key",
        secret_key="generation-a-secret-key",
        token="generation-a-token",
        expiry_time=clock[0] + timedelta(seconds=5),
        refresh_using=refresh_credentials,
        method="synthetic-provider",
        time_fetcher=lambda: clock[0],
        advisory_timeout=0,
        mandatory_timeout=0,
    )

    def signing_time():
        return clock[0].replace(tzinfo=None)

    monkeypatch.setattr(auth.botocore.auth, "get_current_datetime", signing_time)
    handler = auth._SigV4Auth(credentials, "custom-service", "eu-west-3")

    first = httpx.Request("GET", "https://service.example.test/orders")
    first_expected = _sdk_signed_headers(
        httpx.Request("GET", "https://service.example.test/orders"),
        ReadOnlyCredentials(
            "generation-a-access-key",
            "generation-a-secret-key",
            "generation-a-token",
        ),
        "custom-service",
        "eu-west-3",
        SigV4Auth,
    )
    _run_auth(handler, first)
    assert first.headers["Authorization"] == first_expected["Authorization"]
    assert first.headers["X-Amz-Date"] == first_expected["X-Amz-Date"]
    assert first.headers["x-amz-security-token"] == first_expected[
        "X-Amz-Security-Token"
    ]

    clock[0] += timedelta(seconds=6)
    second = httpx.Request("GET", "https://service.example.test/orders")
    second_expected = _sdk_signed_headers(
        httpx.Request("GET", "https://service.example.test/orders"),
        ReadOnlyCredentials(
            "generation-b-access-key",
            "generation-b-secret-key",
            "generation-b-token",
        ),
        "custom-service",
        "eu-west-3",
        SigV4Auth,
    )
    _run_auth(handler, second)

    assert second.headers["Authorization"] == second_expected["Authorization"]
    assert second.headers["X-Amz-Date"] == second_expected["X-Amz-Date"]
    assert second.headers["x-amz-security-token"] == second_expected[
        "X-Amz-Security-Token"
    ]
    assert refresh_generations == ["generation-b"]
