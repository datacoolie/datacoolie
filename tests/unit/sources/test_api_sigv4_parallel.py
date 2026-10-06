"""Independent APIReader concurrency coverage for the general SigV4 contract."""

from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from threading import Barrier, BrokenBarrierError, Lock
from typing import Any

import botocore.auth
import botocore.awsrequest
import botocore.credentials
import httpx
import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.sources._api import auth
from datacoolie.sources.api_reader import APIReader
from tests.unit.sources.support import read_request


_API_READER_MODULE = importlib.import_module("datacoolie.sources.api_reader")
_FIXED_NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


class _GenerationProvider:
    """Provider that only exposes coherent frozen snapshots.

    The two parallel requests receive different generations.  Accessing live
    access/secret/token fields is deliberately forbidden: a signer must freeze
    once per request before it reads any credential component.
    """

    def __init__(self) -> None:
        self._calls = 0
        self._lock = Lock()
        self._first_two = Barrier(2, timeout=3)
        self.snapshots = (
            botocore.credentials.Credentials(
                access_key="AKIA-GENERATION-A",
                secret_key="secret-generation-a",
                token="session-generation-a",
            ),
            botocore.credentials.Credentials(
                access_key="AKIA-GENERATION-B",
                secret_key="secret-generation-b",
                token="session-generation-b",
            ),
        )

    @property
    def access_key(self) -> str:
        raise AssertionError("live access_key read bypassed get_frozen_credentials")

    @property
    def secret_key(self) -> str:
        raise AssertionError("live secret_key read bypassed get_frozen_credentials")

    @property
    def token(self) -> str:
        raise AssertionError("live token read bypassed get_frozen_credentials")

    def get_frozen_credentials(self) -> botocore.credentials.Credentials:
        with self._lock:
            call_number = self._calls
            self._calls += 1
        if call_number < 2:
            try:
                self._first_two.wait()
            except BrokenBarrierError as exc:
                raise AssertionError(
                    "parallel APIReader path did not request one snapshot per request"
                ) from exc
        return self.snapshots[call_number % len(self.snapshots)]


class _ControlledSession:
    def __init__(self, provider: _GenerationProvider) -> None:
        self._provider = provider

    def get_credentials(self) -> _GenerationProvider:
        return self._provider


class _ListEngine:
    def create_dataframe(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return records

    def count_rows(self, frame: list[dict[str, Any]]) -> int:
        return len(frame)


def _oracle_authorization(
    request: httpx.Request,
    credentials: botocore.credentials.Credentials,
) -> str:
    """Generate the expected Authorization header with botocore directly."""

    actual_authorization = request.headers["Authorization"]
    signed_headers = actual_authorization.split("SignedHeaders=", 1)[1].split(
        ",", 1
    )[0]
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() != "authorization"
    }
    # HTTPX may add Host at transport time.  Botocore independently derives it
    # when the auth flow signed before the transport inserted the header.
    if "host" not in signed_headers.split(";"):
        headers.pop("host", None)

    aws_request = botocore.awsrequest.AWSRequest(
        method=request.method,
        url=str(request.url),
        data=request.content,
        headers=headers,
    )
    botocore.auth.SigV4Auth(
        credentials,
        "execute-api",
        "eu-west-1",
    ).add_auth(aws_request)
    return aws_request.headers["Authorization"]


@pytest.mark.unit
def test_api_reader_parallel_ranges_sign_each_request_with_one_sdk_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise APIReader's actual ThreadPoolExecutor path with SDK parity."""

    provider = _GenerationProvider()
    monkeypatch.setattr(
        auth._boto_session,
        "Session",
        lambda: _ControlledSession(provider),
    )
    monkeypatch.setattr(
        botocore.auth,
        "get_current_datetime",
        lambda: _FIXED_NOW,
    )

    requests: list[httpx.Request] = []
    matched_generations: list[str] = []
    credentials_by_generation = {
        "generation-a": provider.snapshots[0],
        "generation-b": provider.snapshots[1],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert request.url.host == "api.example.test"
        assert request.url.path == "/events"
        assert request.headers["X-Amz-Date"] == "20260101T120000Z"
        assert request.headers["X-Amz-Security-Token"].startswith("session-generation-")

        payload = json.loads(request.content.decode("utf-8"))
        assert payload["marker"] == "parallel-sigv4"
        assert payload["to"]
        lower = request.url.params["from"]
        upper = payload["to"]
        lower_dt = datetime.fromisoformat(lower)
        upper_dt = datetime.fromisoformat(upper)
        assert lower_dt < upper_dt
        event_time = (lower_dt + (upper_dt - lower_dt) / 2).isoformat()

        matches = [
            name
            for name, credentials in credentials_by_generation.items()
            if _oracle_authorization(request, credentials)
            == request.headers["Authorization"]
        ]
        assert len(matches) == 1
        matched_generations.append(matches[0])
        return httpx.Response(
            200,
            json=[{"event_time": event_time, "upper_bound": upper}],
            request=request,
        )

    original_client = httpx.Client
    transport = httpx.MockTransport(handler)

    def client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    # Keep APIReader's production Client construction and only replace its
    # transport, so auth_flow and ThreadPoolExecutor remain in the exercised path.
    monkeypatch.setattr(_API_READER_MODULE.httpx, "Client", client_factory)

    source = Source(
        connection=Connection(
            name="parallel-sigv4",
            connection_type="api",
            format="api",
            configure={
                "base_url": "https://api.example.test",
                "auth_type": "aws_sigv4",
                "aws_region": "eu-west-1",
                "aws_service": "execute-api",
            },
        ),
        configure={
            "endpoint": "/events",
            "method": "POST",
            "body": {"marker": "parallel-sigv4"},
            "range_param_mapping": {
                "event_time": {
                    "lower": {"name": "from", "location": "params"},
                    "upper": {"name": "to", "location": "body"},
                    "format": "iso",
                    "response_column": "event_time",
                }
            },
            "watermark_range_start": "2026-01-01T00:00:00+00:00",
            "watermark_range_interval_unit": "hour",
            "watermark_range_interval_amount": 1,
            "watermark_range_max_workers": 2,
        },
    )

    result = APIReader(_ListEngine()).read(
        source,
        request=read_request(
            reference_time=datetime(2026, 1, 1, 2, tzinfo=timezone.utc)
        ),
    )

    assert result.rows_read == 2
    assert result.frame is not None
    assert len(result.frame) == 2
    assert len(requests) == 2
    assert provider._calls >= 2
    assert set(matched_generations) == {"generation-a", "generation-b"}
