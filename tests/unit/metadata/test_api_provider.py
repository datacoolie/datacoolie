"""Tests for APIClient — httpx-backed metadata provider.

Uses ``unittest.mock`` to patch httpx responses (no real HTTP traffic).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import MetadataError, WatermarkError


# ============================================================================
# Helpers — mock httpx module
# ============================================================================


class _FakeResponse:
    """Minimal httpx-like response object."""

    def __init__(
        self,
        status_code: int = 200,
        json_data: Any = None,
        text: str = "",
        headers: Dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text or json.dumps(json_data or {})
        self.headers = headers or {}

    def json(self) -> Any:
        return self._json_data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            _httpx = _import_httpx_for_tests()
            raise _httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=MagicMock(),
                response=self,
            )


def _import_httpx_for_tests() -> Any:
    """Import real httpx for exception types (required by tests)."""
    import httpx
    return httpx


def _build_api_client(
    fake_client: _FakeClient,
    **overrides: Any,
) -> Any:
    """Create APIClient wired to a fake httpx client for deterministic tests."""
    with patch("datacoolie.metadata.api_client._import_httpx") as mock_imp:
        mock_httpx = MagicMock()
        mock_httpx.Client.return_value = fake_client
        mock_httpx.HTTPStatusError = _import_httpx_for_tests().HTTPStatusError
        mock_httpx.HTTPError = _import_httpx_for_tests().HTTPError
        mock_imp.return_value = mock_httpx

        from datacoolie.metadata.api_client import APIClient

        cfg = {
            "base_url": "https://api.test.io",
            "api_key": "k",
            "workspace_id": "ws-1",
            "enable_cache": False,
            "max_retries": 0,
            "retry_backoff": 0.0,
        }
        cfg.update(overrides)

        client = APIClient(**cfg)
        client._client = fake_client
        return client


def _new_client_for_retries(
    responses: List[_FakeResponse],
    *,
    max_retries: int = 1,
    retry_backoff: float = 0.0,
) -> tuple[Any, _FakeClient]:
    """Create a retry-enabled APIClient and its backing fake client."""
    fake_client = _FakeClient(responses=responses)
    client = _build_api_client(
        fake_client,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
    )
    return client, fake_client


class _FakeClient:
    """Stand-in for ``httpx.Client`` that records calls."""

    def __init__(self, *, responses: List[_FakeResponse] | None = None, **kw: Any) -> None:
        self._responses = list(responses or [])
        self._call_log: List[Dict[str, Any]] = []
        self._idx = 0

    def request(self, method: str, url: str, **kw: Any) -> _FakeResponse:
        call = {"method": method, "url": url, **kw}
        self._call_log.append(call)
        if self._idx < len(self._responses):
            resp = self._responses[self._idx]
            self._idx += 1
            return resp
        return _FakeResponse(200, json_data={})

    def close(self) -> None:
        pass

    @property
    def calls(self) -> List[Dict[str, Any]]:
        return self._call_log


def _paginated(data: List[Dict[str, Any]], page: int = 1, total_pages: int = 1) -> Dict[str, Any]:
    """Build a paginated API response body."""
    return {
        "data": data,
        "pagination": {
            "page": page,
            "page_size": len(data),
            "total_items": len(data) * total_pages,
            "total_pages": total_pages,
        },
    }


def _connection_dict(
    name: str = "test_conn",
    connection_id: str = "c-1",
    **kw: Any,
) -> Dict[str, Any]:
    return {
        "connection_id": connection_id,
        "workspace_id": "ws-1",
        "name": name,
        "connection_type": "file",
        "format": "parquet",
        "configure": {},
        "is_active": True,
        **kw,
    }


def _dataflow_dict(
    name: str = "my_df",
    dataflow_id: str = "df-1",
    **kw: Any,
) -> Dict[str, Any]:
    return {
        "dataflow_id": dataflow_id,
        "workspace_id": "ws-1",
        "name": name,
        "stage": "bronze",
        "is_active": True,
        "source": {
            "connection": _connection_dict("src_conn", "c-src"),
            "schema_name": "raw",
            "table": "orders",
        },
        "destination": {
            "connection": _connection_dict("dest_conn", "c-dest"),
            "table": "dim_orders",
            "load_type": "overwrite",
        },
        **kw,
    }


def _schema_hint_dict(
    column_name: str = "col_a",
    data_type: str = "STRING",
    **kw: Any,
) -> Dict[str, Any]:
    return {"column_name": column_name, "data_type": data_type, **kw}


# ============================================================================
# Fixture — build an APIClient with a fake httpx Client
# ============================================================================


@pytest.fixture()
def make_client():
    """Factory that creates an APIClient with prepared mock responses."""

    def _factory(responses: List[_FakeResponse] | None = None):
        fake_client = _FakeClient(responses=responses or [])
        # Patch httpx.Client so the constructor uses our fake
        with patch("datacoolie.metadata.api_client._import_httpx") as mock_imp:
            mock_httpx = MagicMock()
            mock_httpx.Client.return_value = fake_client
            mock_httpx.HTTPStatusError = _import_httpx_for_tests().HTTPStatusError
            mock_httpx.HTTPError = _import_httpx_for_tests().HTTPError
            mock_imp.return_value = mock_httpx

            from datacoolie.metadata.api_client import APIClient

            client = APIClient(
                base_url="https://api.test.io",
                api_key="test-key",
                workspace_id="ws-1",
                enable_cache=False,
                max_retries=0,  # no retry for fast tests
            )
            # Replace the _client with our fake (in case construction path differs)
            client._client = fake_client
            return client, fake_client

    return _factory


# ============================================================================
# Connection tests
# ============================================================================


class TestAPIClientConnections:

    def test_fetch_connections(self, make_client) -> None:
        data = [_connection_dict("a"), _connection_dict("b", "c-2")]
        resp = _FakeResponse(200, _paginated(data))
        client, fake = make_client([resp])
        conns = client.get_connections()
        assert len(conns) == 2
        assert {c.name for c in conns} == {"a", "b"}

    def test_fetch_connections_empty(self, make_client) -> None:
        resp = _FakeResponse(200, _paginated([]))
        client, _ = make_client([resp])
        assert client.get_connections() == []

    def test_connection_by_id(self, make_client) -> None:
        resp = _FakeResponse(200, _connection_dict("my_conn", "c-42"))
        client, _ = make_client([resp])
        conn = client.get_connection_by_id("c-42")
        assert conn is not None
        assert conn.name == "my_conn"

    def test_connection_by_id_not_found(self, make_client) -> None:
        httpx = _import_httpx_for_tests()
        err_resp = _FakeResponse(404, json_data={"detail": "not found"})
        client, _ = make_client([err_resp])
        conn = client.get_connection_by_id("missing")
        assert conn is None

    def test_connection_by_name(self, make_client) -> None:
        data = [_connection_dict("target")]
        resp = _FakeResponse(200, _paginated(data))
        client, _ = make_client([resp])
        conn = client.get_connection_by_name("target")
        assert conn is not None
        assert conn.name == "target"

    def test_connection_by_name_empty(self, make_client) -> None:
        resp = _FakeResponse(200, _paginated([]))
        client, _ = make_client([resp])
        assert client.get_connection_by_name("nope") is None

    def test_fetch_connections_active_only_false_omits_param(self, make_client) -> None:
        resp = _FakeResponse(200, _paginated([_connection_dict("a")]))
        client, fake = make_client([resp])
        conns = client.get_connections(active_only=False)
        assert len(conns) == 1
        assert "active_only" not in fake.calls[0]["params"]


# ============================================================================
# Dataflow tests
# ============================================================================


class TestAPIClientDataflows:

    def test_fetch_dataflows(self, make_client) -> None:
        data = [_dataflow_dict()]
        resp = _FakeResponse(200, _paginated(data))
        client, _ = make_client([resp])
        dfs = client.get_dataflows(attach_schema_hints=False)
        assert len(dfs) == 1
        assert dfs[0].name == "my_df"

    def test_fetch_dataflows_stage_filter(self, make_client) -> None:
        data = [_dataflow_dict(stage="silver")]
        resp = _FakeResponse(200, _paginated(data))
        client, fake = make_client([resp])
        dfs = client.get_dataflows(stage="silver", attach_schema_hints=False)
        assert len(dfs) == 1
        # Verify stage param sent
        assert fake.calls[0]["params"]["stage"] == "silver"

    def test_fetch_dataflows_stage_list(self, make_client) -> None:
        data = [_dataflow_dict()]
        resp = _FakeResponse(200, _paginated(data))
        client, fake = make_client([resp])
        dfs = client.get_dataflows(stage=["bronze", "silver"], attach_schema_hints=False)
        assert fake.calls[0]["params"]["stage"] == "bronze,silver"

    def test_dataflow_by_id(self, make_client) -> None:
        resp = _FakeResponse(200, _dataflow_dict("target_df", "df-42"))
        client, _ = make_client([resp])
        df = client.get_dataflow_by_id("df-42", attach_schema_hints=False)
        assert df is not None
        assert df.name == "target_df"

    def test_dataflow_by_id_not_found(self, make_client) -> None:
        err_resp = _FakeResponse(404, json_data={"detail": "not found"})
        client, _ = make_client([err_resp])
        assert client.get_dataflow_by_id("missing", attach_schema_hints=False) is None

    def test_dataflow_source_linked(self, make_client) -> None:
        resp = _FakeResponse(200, _dataflow_dict())
        client, _ = make_client([resp])
        df = client.get_dataflow_by_id("df-1", attach_schema_hints=False)
        assert df is not None
        assert df.source.connection.name == "src_conn"

    def test_dataflow_destination_linked(self, make_client) -> None:
        resp = _FakeResponse(200, _dataflow_dict())
        client, _ = make_client([resp])
        df = client.get_dataflow_by_id("df-1", attach_schema_hints=False)
        assert df is not None
        assert df.destination.connection.name == "dest_conn"

    def test_fetch_dataflows_active_only_false_omits_param(self, make_client) -> None:
        resp = _FakeResponse(200, _paginated([_dataflow_dict()]))
        client, fake = make_client([resp])
        dfs = client.get_dataflows(active_only=False, attach_schema_hints=False)
        assert len(dfs) == 1
        assert "active_only" not in fake.calls[0]["params"]


# ============================================================================
# Schema Hints tests
# ============================================================================


class TestAPIClientSchemaHints:

    def test_fetch_schema_hints(self, make_client) -> None:
        data = [_schema_hint_dict("col_a", "STRING"), _schema_hint_dict("col_b", "INT")]
        resp = _FakeResponse(200, _paginated(data))
        client, _ = make_client([resp])
        hints = client.get_schema_hints("c-1", "my_table")
        assert len(hints) == 2
        assert hints[0].column_name == "col_a"

    def test_schema_hints_with_schema_filter(self, make_client) -> None:
        data = [_schema_hint_dict("col_a", "STRING")]
        resp = _FakeResponse(200, _paginated(data))
        client, fake = make_client([resp])
        client.get_schema_hints("c-1", "my_table", schema_name="dbo")
        assert fake.calls[0]["params"]["schema_name"] == "dbo"

    def test_schema_hints_empty(self, make_client) -> None:
        resp = _FakeResponse(200, _paginated([]))
        client, _ = make_client([resp])
        assert client.get_schema_hints("c-1", "no_table") == []


# ============================================================================
# Watermarks tests
# ============================================================================


class TestAPIClientWatermarks:

    def test_get_watermark(self, make_client) -> None:
        resp = _FakeResponse(200, {"current_value": '{"col": "2024-01-01"}'})
        client, _ = make_client([resp])
        wm = client.get_watermark("df-1")
        assert wm == '{"col": "2024-01-01"}'

    def test_get_watermark_none(self, make_client) -> None:
        resp = _FakeResponse(200, {"current_value": None})
        client, _ = make_client([resp])
        assert client.get_watermark("df-1") is None

    def test_get_watermark_not_found(self, make_client) -> None:
        err_resp = _FakeResponse(404, json_data={"detail": "not found"})
        client, _ = make_client([err_resp])
        assert client.get_watermark("missing") is None

    def test_get_watermark_dict_value(self, make_client) -> None:
        """current_value already a dict (not string)."""
        resp = _FakeResponse(200, {"current_value": {"col": "v1"}})
        client, _ = make_client([resp])
        wm = client.get_watermark("df-1")
        assert wm == '{"col": "v1"}'

    def test_update_watermark(self, make_client) -> None:
        resp = _FakeResponse(200, {"status": "ok"})
        client, fake = make_client([resp])
        client.update_watermark("df-1", '{"col": "new"}', job_id="j-1")
        assert len(fake.calls) == 1
        body = fake.calls[0]["json"]
        assert body["current_value"] == '{"col": "new"}'
        assert body["job_id"] == "j-1"


# ============================================================================
# Pagination tests
# ============================================================================


class TestAPIClientPagination:

    def test_multi_page_collection(self, make_client) -> None:
        page1 = _FakeResponse(200, _paginated([_connection_dict("a")], page=1, total_pages=2))
        page2 = _FakeResponse(200, _paginated([_connection_dict("b", "c-2")], page=2, total_pages=2))
        client, _ = make_client([page1, page2])
        conns = client.get_connections()
        assert len(conns) == 2
        assert {c.name for c in conns} == {"a", "b"}


# ============================================================================
# Lifecycle tests
# ============================================================================


class TestAPIClientLifecycle:

    def test_close(self, make_client) -> None:
        client, _ = make_client()
        client.close()  # should not raise

    def test_context_manager(self, make_client) -> None:
        client, _ = make_client([_FakeResponse(200, _paginated([]))])
        with client as c:
            c.get_connections()

    def test_close_with_none_client(self, make_client) -> None:
        client, _ = make_client([])
        client._client = None
        client.close()  # no-op branch


# ============================================================================
# Retry / error handling tests
# ============================================================================


class TestAPIClientRetryAndErrors:

    def test_retry_on_500(self, make_client) -> None:
        """Retryable status (500) is retried, then succeeds."""
        fail = _FakeResponse(500, json_data={"error": "internal"})
        ok = _FakeResponse(200, _connection_dict("ok"))

        client, fake_client = _new_client_for_retries([fail, ok], max_retries=1)
        conn = client.get_connection_by_id("c-1")
        assert conn is not None
        assert conn.name == "ok"
        assert len(fake_client.calls) == 2

    def test_retry_exhausted_raises_metadata_error(self, make_client) -> None:
        fail = _FakeResponse(503, json_data={"error": "unavailable"})

        client, _ = _new_client_for_retries([fail, fail], max_retries=1)
        with pytest.raises(MetadataError, match="API request failed"):
            client._request("GET", "/test")

    def test_http_error_no_retry(self, make_client) -> None:
        """Non-HTTP exception with max_retries=0 raises MetadataError."""
        httpx = _import_httpx_for_tests()

        class FailClient(_FakeClient):
            def request(self, method, url, **kw):
                raise httpx.ConnectError("Connection refused")

        fake_client = FailClient()
        client = _build_api_client(fake_client, max_retries=0)
        with pytest.raises(MetadataError, match="API request error"):
            client._request("GET", "/test")

    def test_backoff_respects_retry_after_header(self) -> None:
        """_backoff uses Retry-After header when present."""
        client = _build_api_client(_FakeClient(), retry_backoff=0.0)
        resp = _FakeResponse(429, json_data={}, headers={"Retry-After": "0.01"})
        with patch("datacoolie.metadata.api_client.time.sleep") as mock_sleep:
            client._backoff(0, resp)
            mock_sleep.assert_called_once()
            # Should use the Retry-After value (0.01) since it's > base delay (0.0)
            assert mock_sleep.call_args[0][0] >= 0.01

    def test_backoff_ignores_bad_retry_after(self) -> None:
        client = _build_api_client(_FakeClient(), retry_backoff=0.0)
        resp = _FakeResponse(429, json_data={}, headers={"Retry-After": "not-a-number"})
        with patch("datacoolie.metadata.api_client.time.sleep") as mock_sleep:
            client._backoff(0, resp)
            mock_sleep.assert_called_once()

    def test_update_watermark_metadata_error_propagates(self, make_client) -> None:
        err_resp = _FakeResponse(400, json_data={"error": "bad request"})
        client, _ = make_client([err_resp])
        with pytest.raises(MetadataError):
            client.update_watermark("df-1", '{"col": "v1"}')

    def test_watermark_invalid_json_raises(self, make_client) -> None:
        """get_watermark with non-JSON string returns the raw value."""
        resp = _FakeResponse(200, {"current_value": "not-json{"})
        client, _ = make_client([resp])
        wm = client.get_watermark("df-1")
        assert wm == "not-json{"

    def test_fetch_connection_by_name_metadata_error_returns_none(self, make_client) -> None:
        err_resp = _FakeResponse(500, json_data={"detail": "boom"})
        client, _ = make_client([err_resp])
        assert client.get_connection_by_name("x") is None

    def test_update_watermark_with_dataflow_run_id(self, make_client) -> None:
        ok = _FakeResponse(200, {"status": "ok"})
        client, fake = make_client([ok])
        client.update_watermark("df-1", '{"v": 1}', job_id="j-1", dataflow_run_id="r-1")
        body = fake.calls[0]["json"]
        assert body["job_id"] == "j-1"
        assert body["dataflow_run_id"] == "r-1"

    def test_update_watermark_wraps_unexpected_error(self, make_client) -> None:
        client, _ = make_client([])
        with patch.object(client, "_request", side_effect=RuntimeError("boom")):
            with pytest.raises(WatermarkError, match="Failed to update watermark"):
                client.update_watermark("df-1", '{"v": 1}')

    def test_request_http_status_retry_path(self, make_client) -> None:
        fail = _FakeResponse(503, json_data={"error": "unavailable"})
        ok = _FakeResponse(200, _paginated([_connection_dict("ok")]))

        client, _ = _new_client_for_retries([fail, ok], max_retries=1)

        with patch.object(client, "_backoff") as backoff:
            conns = client.get_connections()
            assert len(conns) == 1
            backoff.assert_called()

    def test_request_http_error_retry_path(self) -> None:
        httpx = _import_httpx_for_tests()

        class FlakyClient(_FakeClient):
            def __init__(self) -> None:
                super().__init__()
                self.calls_n = 0

            def request(self, method, url, **kw):  # type: ignore[override]
                self.calls_n += 1
                if self.calls_n == 1:
                    raise httpx.ConnectError("temporary")
                return _FakeResponse(200, {"current_value": "ok"})

        fake_client = FlakyClient()
        client = _build_api_client(fake_client, max_retries=1)

        with patch.object(client, "_backoff") as backoff:
            assert client.get_watermark("df-1") == "ok"
            backoff.assert_called_once()

    def test_request_loop_fallthrough_when_negative_retries(self, make_client) -> None:
        client, _ = make_client([])
        client._max_retries = -1
        with pytest.raises(MetadataError, match="failed after 0 attempts"):
            client._request("GET", "/x")

    def test_backoff_without_retry_after_header(self) -> None:
        client = _build_api_client(_FakeClient(), retry_backoff=0.01)

        with patch("datacoolie.metadata.api_client.time.sleep") as sleep:
            client._backoff(0, _FakeResponse(429, json_data={}, headers={}))
            sleep.assert_called_once_with(0.01)

    def test_backoff_without_response_uses_exponential_delay(self) -> None:
        client = _build_api_client(_FakeClient(), retry_backoff=0.5)

        with patch("datacoolie.metadata.api_client.time.sleep") as sleep:
            client._backoff(2, None)
            sleep.assert_called_once_with(2.0)

    def test_request_http_status_error_retry_branch(self) -> None:
        httpx = _import_httpx_for_tests()

        class WeirdResponse:
            status_code = 200
            text = "err"

            def json(self):
                return {"ok": True}

            def raise_for_status(self):
                err_resp = _FakeResponse(503, json_data={"error": "retry"})
                raise httpx.HTTPStatusError("HTTP 503", request=MagicMock(), response=err_resp)

        class FlakyClient(_FakeClient):
            def __init__(self):
                super().__init__()
                self.n = 0

            def request(self, method, url, **kw):  # type: ignore[override]
                self.n += 1
                if self.n == 1:
                    return WeirdResponse()
                return _FakeResponse(200, _paginated([_connection_dict("ok")]))

        fake_client = FlakyClient()
        client = _build_api_client(fake_client, max_retries=1)

        with patch.object(client, "_backoff") as backoff:
            out = client.get_connections()
            assert len(out) == 1
            backoff.assert_called_once()


# ============================================================================
# Import guard test
# ============================================================================


class TestAPIClientImportGuard:

    def test_missing_httpx_raises(self) -> None:
        """When httpx is not installed, a clear MetadataError is raised."""
        import builtins
        real_import = builtins.__import__

        def _block_httpx(name, *args, **kwargs):
            if name == "httpx":
                raise ImportError("No module named 'httpx'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_block_httpx):
            from datacoolie.metadata.api_client import _import_httpx
            with pytest.raises(MetadataError, match="httpx"):
                _import_httpx()


# ============================================================================
# base_url normalisation tests
# ============================================================================


class TestAPIClientBaseUrl:
    """Verify base_url is stored as-is (after stripping any trailing slash)."""

    def _make_client(self, base_url: str) -> Any:
        fake_client = _FakeClient()
        with patch("datacoolie.metadata.api_client._import_httpx") as mock_imp:
            mock_httpx = MagicMock()
            mock_httpx.Client.return_value = fake_client
            mock_httpx.HTTPStatusError = _import_httpx_for_tests().HTTPStatusError
            mock_httpx.HTTPError = _import_httpx_for_tests().HTTPError
            mock_imp.return_value = mock_httpx
            from datacoolie.metadata.api_client import APIClient
            client = APIClient(
                base_url=base_url,
                api_key="k",
                workspace_id="ws-1",
                enable_cache=False,
                max_retries=0,
            )
            client._client = fake_client
            return client

    def test_bare_host_stored_as_is(self) -> None:
        client = self._make_client("https://api.test.io")
        assert client._base_url == "https://api.test.io"

    def test_versioned_url_stored_as_is(self) -> None:
        client = self._make_client("https://api.test.io/api/v1")
        assert client._base_url == "https://api.test.io/api/v1"

    def test_trailing_slash_stripped(self) -> None:
        client = self._make_client("https://api.test.io/api/v1/")
        assert client._base_url == "https://api.test.io/api/v1"

    def test_ws_prefix_is_relative_path(self) -> None:
        client = self._make_client("https://api.test.io/api/v1")
        assert client._ws_prefix == "/workspaces/ws-1"

    def test_request_url_uses_ws_prefix(self) -> None:
        """URL passed to the HTTP client is /workspaces/{id}/{path} (no base_url)."""
        ok = _FakeResponse(200, _paginated([]))
        client = self._make_client("https://api.test.io/api/v1")
        client._client = _FakeClient(responses=[ok])
        client.get_connections()
        url = client._client.calls[0]["url"]
        assert url == "/workspaces/ws-1/connections"


# ============================================================================
# Phase D — TLS / insecure-connection warning
# ============================================================================


class TestTLSWarning:
    """Verify a warning is logged when base_url uses plain HTTP."""

    def test_http_url_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        with caplog.at_level(logging.WARNING):
            _build_api_client(_FakeClient(), base_url="http://insecure.test.io")
        assert any("insecure" in rec.message.lower() for rec in caplog.records)

    def test_https_url_no_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        with caplog.at_level(logging.WARNING):
            _build_api_client(_FakeClient(), base_url="https://secure.test.io")
        assert not any("insecure" in rec.message.lower() for rec in caplog.records)


# ============================================================================
# Phase E — Bounded pagination
# ============================================================================


class TestPaginationBound:
    """Verify pagination is capped at _MAX_PAGES."""

    def test_pagination_capped_at_max(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        # Return total_pages=1500 on every page — client should stop at 1000
        responses = [
            _FakeResponse(200, _paginated([_connection_dict(f"c-{i}", f"id-{i}")], page=i, total_pages=1500))
            for i in range(1, 1002)
        ]
        fake_client = _FakeClient(responses=responses)
        client = _build_api_client(fake_client)

        with caplog.at_level(logging.WARNING):
            conns = client.get_connections()

        # Should have fetched exactly 1000 pages
        assert len(fake_client.calls) == 1000
        assert len(conns) == 1000
        assert any("capped" in rec.message.lower() or "1000" in rec.message for rec in caplog.records)


class TestFanOut:
    """Cover _fan_out when workers > 1 (line 282)."""

    def test_fan_out_multiple_keys(self) -> None:
        """When len(keys) > 1, uses ThreadPoolExecutor."""
        client = _build_api_client(_FakeClient())

        def _fetcher(k: str) -> str:
            return f'result-{k}'

        # Force parallel path by calling directly
        results = client._fan_out(['a', 'b', 'c'], _fetcher)
        assert results == {'a': 'result-a', 'b': 'result-b', 'c': 'result-c'}

    def test_fan_out_single_key_serial(self) -> None:
        """Single key uses serial path."""
        client = _build_api_client(_FakeClient())

        results = client._fan_out(['only'], lambda k: f'v-{k}')
        assert results == {'only': 'v-only'}

    def test_fan_out_empty_returns_empty(self) -> None:
        """Empty keys returns empty dict without calling fetcher."""
        client = _build_api_client(_FakeClient())
        called = []
        results = client._fan_out([], lambda k: called.append(k) or 'x')
        assert results == {}
        assert called == []


class TestGroupSchemaHintRows:
    """Cover _group_schema_hint_rows (lines 427-437)."""

    def test_groups_by_connection_table(self) -> None:
        client = _build_api_client(_FakeClient())
        rows = [
            {'connection_id': 'c1', 'table_name': 'orders', 'schema_name': 'public', 'column_name': 'id', 'data_type': 'INT'},
            {'connection_id': 'c1', 'table_name': 'orders', 'schema_name': 'public', 'column_name': 'name', 'data_type': 'STRING'},
            {'connection_id': 'c2', 'table_name': 'items', 'schema_name': None, 'column_name': 'sku', 'data_type': 'STRING'},
        ]
        result = client._group_schema_hint_rows(rows)
        assert len(result[('c1', 'public', 'orders')]) == 2
        assert len(result[('c2', None, 'items')]) == 1

    def test_skips_rows_without_table(self) -> None:
        client = _build_api_client(_FakeClient())
        rows = [
            {'connection_id': 'c1', 'table_name': None, 'column_name': 'id', 'data_type': 'INT'},
        ]
        result = client._group_schema_hint_rows(rows)
        assert result == {}

    def test_empty_schema_name_normalised_to_none(self) -> None:
        client = _build_api_client(_FakeClient())
        rows = [
            {'connection_id': 'c1', 'table_name': 'tbl', 'schema_name': '', 'column_name': 'col', 'data_type': 'STRING'},
        ]
        result = client._group_schema_hint_rows(rows)
        # Empty schema -> None
        assert ('c1', None, 'tbl') in result


class TestAPIClientBulkLoad:
    """Cover lines 391-411: parallel _bulk_load via ThreadPoolExecutor."""

    def test_bulk_load_fetches_connections_dataflows_hints_in_parallel(self) -> None:
        conn_row = {
            'connection_id': 'c-1',
            'name': 'my_conn',
            'workspace_id': 'ws-1',
            'connection_type': 'lakehouse',
            'format': 'delta',
            'is_active': True,
        }
        df_row = {
            'dataflow_id': 'df-1',
            'name': 'my_df',
            'workspace_id': 'ws-1',
            'is_active': True,
            'source': {
                'table': 'src',
                'connection': {
                    'connection_id': 'c-1',
                    'name': 'my_conn',
                    'workspace_id': 'ws-1',
                    'connection_type': 'lakehouse',
                    'format': 'delta',
                    'is_active': True,
                },
            },
            'destination': {
                'table': 'dst',
                'load_type': 'overwrite',
                'connection': {
                    'connection_id': 'c-1',
                    'name': 'my_conn',
                    'workspace_id': 'ws-1',
                    'connection_type': 'lakehouse',
                    'format': 'delta',
                    'is_active': True,
                },
            },
        }
        # Each /connections, /dataflows, /schema-hints request returns one item
        responses = [
            _FakeResponse(200, json_data={'data': [conn_row], 'next_cursor': None}),
            _FakeResponse(200, json_data={'data': [df_row], 'next_cursor': None}),
            _FakeResponse(200, json_data={'data': [], 'next_cursor': None}),
        ]
        fake_client = _FakeClient(responses=responses)
        client = _build_api_client(fake_client, enable_cache=True)
        # prefetch_all triggers _bulk_load
        client.prefetch_all()
        conns = client.get_connections()
        assert len(conns) == 1
        assert conns[0].connection_id == 'c-1'


class TestAPIClientPrefetchSchemaHints:
    """Cover lines 548-577: _prefetch_schema_hints."""

    def test_prefetch_schema_hints_populates_cache(self) -> None:
        """Lines 548-577: fetches schema hints per connection and caches them."""
        from datacoolie.core.models import Connection, DataFlow, Destination, Source

        hint_row = {
            'connection_id': 'c-1',
            'table_name': 'orders',
            'schema_name': None,
            'column_name': 'id',
            'data_type': 'INT',
            'ordinal_position': 0,
            'is_active': True,
        }
        # Responses: hints request returns one hint
        responses = [
            _FakeResponse(200, json_data={'data': [hint_row], 'next_cursor': None}),
        ]
        fake_client = _FakeClient(responses=responses)
        client = _build_api_client(fake_client, enable_cache=True)
        # Inject a cache to make prefetch work
        from datacoolie.metadata.base import MetadataCache
        client._cache = MetadataCache()

        conn = Connection(
            connection_id='c-1', name='c-1',
            configure={'use_schema_hint': True}
        )
        df = DataFlow(
            dataflow_id='df-1',
            source=Source(connection=conn, table='orders'),
            destination=Destination(connection=conn, table='orders'),
        )
        client._prefetch_schema_hints([df])
        hints = client._cache.get_schema_hints('c-1', None, 'orders')
        assert hints is not None

    def test_prefetch_schema_hints_skips_when_no_cache(self) -> None:
        """Line 549: early return when cache is None."""
        from datacoolie.core.models import Connection, DataFlow, Destination, Source

        responses = []
        fake_client = _FakeClient(responses=responses)
        client = _build_api_client(fake_client, enable_cache=False)

        conn = Connection(
            connection_id='c-1', name='c-1',
            configure={'use_schema_hint': True}
        )
        df = DataFlow(
            dataflow_id='df-1',
            source=Source(connection=conn, table='orders'),
            destination=Destination(connection=conn, table='orders'),
        )
        # Should not make any requests
        client._prefetch_schema_hints([df])
        assert fake_client._idx == 0


class TestPrefetchSchemaHintsEarlyExits:
    """Cover lines 555, 557, 562: early exits in _prefetch_schema_hints."""

    def _make_client_with_cache(self) -> "APIClient":
        from unittest.mock import MagicMock
        client = _build_api_client(_FakeClient())
        # Provide a non-None cache so lines 548-549 don't short-circuit
        client._cache = MagicMock()
        return client

    def _make_dataflow(self, use_schema_hint=True, table=None) -> "DataFlow":
        from datacoolie.core.models import Connection, DataFlow, Destination, Source, Transform
        conn = Connection(
            connection_id='conn-1',
            name='test',
            configure={'use_schema_hint': use_schema_hint},
        )
        src = Source(connection=conn, table=table)
        dest = Destination(table='dest_tbl', connection=conn, configure={'catalog': 'cat'})
        return DataFlow.model_construct(
            dataflow_id='df-1', source=src, destination=dest,
            load_type='append', transform=Transform(),
        )

    def test_no_schema_hint_skips_connection(self) -> None:
        """Line 555: continue when use_schema_hint is False."""
        client = self._make_client_with_cache()
        df = self._make_dataflow(use_schema_hint=False, table='tbl')
        # conn_ids will be empty → returns on line 562
        client._prefetch_schema_hints([df])

    def test_no_table_skips_connection(self) -> None:
        """Line 557: continue when df.source.table is None."""
        client = self._make_client_with_cache()
        df = self._make_dataflow(use_schema_hint=True, table=None)
        # conn_ids will be empty → returns on line 562
        client._prefetch_schema_hints([df])

    def test_empty_conn_ids_returns_early(self) -> None:
        """Line 562: return when conn_ids is empty."""
        client = self._make_client_with_cache()
        # Empty list of dataflows → conn_ids stays empty
        client._prefetch_schema_hints([])
        # No exception means early return
