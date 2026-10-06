"""Focused contracts for deterministic mock API auth and retry fixtures."""

from __future__ import annotations

import importlib.util
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def test_date_only_endpoint_exclusive_lower_and_case_trace(mock_api):
    client = mock_api.app.test_client()
    headers = {"X-Sim-Case-Token": "date-case"}
    first = client.get("/api/orders/date-only", headers=headers).get_json()
    assert [r["order_id"] for r in first["data"]] == [101, 102]
    second = client.get("/api/orders/date-only?modified_since=2024-01-02&include_late=true", headers=headers).get_json()
    assert second["data"] == [mock_api.DATE_ONLY_ROWS[2]]
    assert client.get("/api/orders/date-only?modified_since=2024-01-03&include_late=true", headers=headers).get_json()["data"] == []
    assert client.get("/api/orders/date-only?modified_since=2024-01-02T10:00:00", headers=headers).status_code == 400
    case_id = mock_api._safe_case_id("date-case")
    trace = client.get("/__sim/date-only/trace", query_string={"case_id": case_id}).get_json()["requests"]
    assert len(trace) == 4
    assert client.get("/__sim/date-only/trace?case_id=unrelated").get_json()["requests"] == []
    assert client.get("/__sim/date-only/trace", query_string={"case_id": case_id}).get_json()["requests"] == trace


def test_date_only_trace_is_bounded_per_case(mock_api):
    client = mock_api.app.test_client()
    headers = {"X-Sim-Case-Token": "bounded-date-case"}
    for _ in range(64):
        assert client.get("/api/orders/date-only", headers=headers).status_code == 200
    assert client.get("/api/orders/date-only", headers=headers).status_code == 429
    trace = client.get("/__sim/date-only/trace", query_string={"case_id": mock_api._safe_case_id("bounded-date-case")}).get_json()["requests"]
    assert len(trace) == 64


@pytest.fixture
def mock_api():
    pytest.importorskip("flask", reason="Mock API qualification requires Flask")
    path = Path(__file__).resolve().parents[2] / "usecase-sim/docker/mock_api_server.py"
    spec = importlib.util.spec_from_file_location("auth_contract_mock_api", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _configure_case(client, token: str, sequence: list[int]):
    response = client.post(
        "/__sim/rate-limit/config",
        json={"case_token": token, "sequence": sequence, "retry_after": "0"},
    )
    assert response.status_code == 200
    return response.get_json()


def test_rate_limit_case_tokens_are_isolated_and_repeatable(mock_api):
    client = mock_api.app.test_client()
    client.post("/__sim/rate-limit/reset")
    client.post("/__sim/trace/reset?run_id=auth-contract")
    alpha = "alpha-secret-case-token"
    beta = "beta-secret-case-token"
    alpha_config = _configure_case(client, alpha, [429, 200])
    beta_config = _configure_case(client, beta, [429, 200])

    def fetch(token):
        return client.get(
            "/api/orders/rate-limited",
            query_string={"case_token": token, "limit": 1},
        )

    assert alpha_config["case_id"] != beta_config["case_id"]
    assert fetch(alpha).status_code == 429
    assert fetch(beta).status_code == 429
    assert fetch(alpha).status_code == 200
    assert fetch(beta).status_code == 200
    assert fetch(alpha).get_json()["data"][0]["order_id"] == 1001

    # Reconfiguring one token resets only that token's sequence.
    _configure_case(client, alpha, [429, 200])
    assert fetch(alpha).status_code == 429
    assert fetch(beta).status_code == 200

    trace = client.get("/__sim/trace").get_json()["requests"]
    assert [item["status"] for item in trace] == [429, 429, 200, 200, 200, 429, 200]
    assert all(item["case_id"] in {alpha_config["case_id"], beta_config["case_id"]} for item in trace)
    assert alpha not in str(trace)
    assert beta not in str(trace)
    assert "case_token=%3Credacted%3E" in trace[0]["query"]

    # Trace reads are read-only and do not add request entries.
    assert client.get("/__sim/trace").get_json()["requests"] == trace


def test_rate_limit_case_tokens_are_isolated_under_concurrency(mock_api):
    setup = mock_api.app.test_client()
    setup.post("/__sim/rate-limit/reset")
    alpha = "concurrent-alpha-token"
    beta = "concurrent-beta-token"
    _configure_case(setup, alpha, [429, 200])
    _configure_case(setup, beta, [429, 200])

    def fetch(token):
        with mock_api.app.test_client() as client:
            response = client.get(
                "/api/orders/rate-limited",
                headers={"X-Sim-Case-Token": token},
            )
            return token, response.status_code, response.headers.get("Retry-After")

    with ThreadPoolExecutor(max_workers=4) as executor:
        first_round = list(executor.map(fetch, [alpha, beta, alpha, beta]))

    for token in (alpha, beta):
        statuses = [status for item_token, status, _ in first_round if item_token == token]
        assert sorted(statuses) == [200, 429]
    assert all(retry_after == "0" for _, status, retry_after in first_round if status == 429)


def test_oauth_form_and_json_requests_record_sanitized_shape(mock_api):
    client = mock_api.app.test_client()
    client.post("/__sim/oauth/trace/reset")

    form_response = client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": mock_api.MOCK_OAUTH2_CLIENT_ID,
            "client_secret": mock_api.MOCK_OAUTH2_CLIENT_SECRET,
            "scope": "read:orders",
        },
    )
    json_response = client.post(
        "/oauth/token",
        json={
            "grant_type": "client_credentials",
            "client_id": mock_api.MOCK_OAUTH2_CLIENT_ID,
            "client_secret": mock_api.MOCK_OAUTH2_CLIENT_SECRET,
            "scope": "read:orders",
            "audience": "datacoolie-orders",
        },
    )
    assert form_response.status_code == 200
    assert json_response.status_code == 200

    trace = client.get("/__sim/oauth/trace").get_json()["requests"]
    assert len(trace) == 2
    assert trace[0]["method"] == trace[1]["method"] == "POST"
    assert trace[0]["path"] == trace[1]["path"] == "/oauth/token"
    assert trace[0]["auth_method"] == trace[1]["auth_method"] == "client_secret_post"
    assert trace[0]["body_format"] == "form"
    assert trace[0]["content_type"] == "application/x-www-form-urlencoded"
    assert trace[1]["body_format"] == "json"
    assert trace[1]["content_type"] == "application/json"
    assert trace[0]["scope"] == trace[1]["scope"] == "read:orders"
    assert trace[1]["body"]["audience"] == "<present>"
    assert all("client_secret" not in item["body"] for item in trace)
    assert mock_api.MOCK_OAUTH2_CLIENT_SECRET not in str(trace)
    assert mock_api.MOCK_OAUTH2_ACCESS_TOKEN not in str(trace)


def test_legacy_rate_limit_route_still_has_first_429_then_success(mock_api):
    mock_api._rate_limit_triggered = False
    client = mock_api.app.test_client()
    first = client.get("/api/orders/rate-limited", query_string={"limit": 1})
    second = client.get("/api/orders/rate-limited", query_string={"limit": 1})
    assert first.status_code == 429
    assert first.headers["Retry-After"] == "0"
    assert second.status_code == 200
    assert second.get_json()["data"][0]["order_id"] == 1001
