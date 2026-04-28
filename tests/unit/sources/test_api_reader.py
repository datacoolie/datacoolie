"""Tests for APIReader source."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Connection, Source
from datacoolie.sources.api_reader import APIReader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(
    conn_cfg: Optional[Dict[str, Any]] = None,
    src_cfg: Optional[Dict[str, Any]] = None,
    watermark_columns: Optional[List[str]] = None,
) -> Source:
    """Build a Source with API connection config."""
    return Source(
        connection=Connection(
            name="test-api",
            connection_type="api",
            format="api",
            configure=conn_cfg or {"base_url": "https://api.example.com"},
        ),
        configure=src_cfg or {},
        watermark_columns=watermark_columns or [],
    )


def _mock_response(
    data: Any,
    status_code: int = 200,
    headers: Optional[Dict[str, str]] = None,
) -> MagicMock:
    """Create a mock httpx response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    resp.text = json.dumps(data) if isinstance(data, (dict, list)) else str(data)
    resp.headers = headers or {}
    return resp


class FakeEngine:
    """Minimal engine mock for APIReader tests."""

    def create_dataframe(self, records):
        return list(records)  # return raw list for test assertions

    def count_rows(self, df):
        return len(df)

    def get_count_and_max_values(self, df, columns):
        count = len(df)
        maxes = {}
        for col in columns:
            vals = [r.get(col) for r in df if r.get(col) is not None]
            if vals:
                maxes[col] = max(vals)
        return count, maxes

    def apply_watermark_filter(self, df, columns, watermark):
        filtered = []
        for row in df:
            keep = False
            for col in columns:
                wm_val = watermark.get(col)
                if wm_val is not None and row.get(col) is not None and row[col] > wm_val:
                    keep = True
            if keep:
                filtered.append(row)
        return filtered


# ---------------------------------------------------------------------------
# Tests: Basic reading
# ---------------------------------------------------------------------------

class TestAPIReaderBasicRead:

    def test_single_page_read(self):
        """Simple GET returning a top-level JSON array."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(src_cfg={"endpoint": "/users"})
        records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response(records)

            df = reader.read(source)

        assert df is not None
        assert len(df) == 2
        assert df[0]["name"] == "Alice"

    def test_nested_data_path(self):
        """Extract records from nested response using data_path."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={"endpoint": "/v2/users", "data_path": "data.items"},
        )
        response_data = {
            "data": {
                "items": [{"id": 1}, {"id": 2}, {"id": 3}],
            },
            "meta": {"total": 3},
        }

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response(response_data)

            df = reader.read(source)

        assert df is not None
        assert len(df) == 3

    def test_empty_response_returns_none(self):
        """Empty array response returns None."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source()

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response([])

            df = reader.read(source)

        assert df is None

    def test_missing_base_url_raises(self):
        """Missing base_url raises SourceError."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(conn_cfg={"something": "else"})

        with pytest.raises(SourceError, match="base_url"):
            reader.read(source)


# ---------------------------------------------------------------------------
# Tests: Authentication
# ---------------------------------------------------------------------------

class TestAPIReaderAuth:

    def test_bearer_auth(self):
        """Bearer token is set in Authorization header."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            conn_cfg={
                "base_url": "https://api.example.com",
                "auth_type": "bearer",
                "auth_token": "my-secret-token",
            },
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response([{"id": 1}])

            reader.read(source)

        # Check that headers passed to Client include auth
        call_kwargs = mock_httpx.Client.call_args
        headers = call_kwargs[1].get("headers", {}) if call_kwargs[1] else {}
        assert headers.get("Authorization") == "Bearer my-secret-token"

    def test_basic_auth(self):
        """Basic auth encodes credentials in header."""
        import base64

        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            conn_cfg={
                "base_url": "https://api.example.com",
                "auth_type": "basic",
                "username": "user",
                "password": "pass",
            },
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response([{"id": 1}])

            reader.read(source)

        expected = base64.b64encode(b"user:pass").decode()
        call_kwargs = mock_httpx.Client.call_args
        headers = call_kwargs[1].get("headers", {}) if call_kwargs[1] else {}
        assert headers.get("Authorization") == f"Basic {expected}"

    def test_api_key_auth(self):
        """API key is set in custom header."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            conn_cfg={
                "base_url": "https://api.example.com",
                "auth_type": "api_key",
                "api_key_header": "X-Custom-Key",
                "api_key_value": "secret123",
            },
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response([{"id": 1}])

            reader.read(source)

        call_kwargs = mock_httpx.Client.call_args
        headers = call_kwargs[1].get("headers", {}) if call_kwargs[1] else {}
        assert headers.get("X-Custom-Key") == "secret123"


# ---------------------------------------------------------------------------
# Tests: Pagination
# ---------------------------------------------------------------------------

class TestAPIReaderPagination:

    def test_offset_pagination(self):
        """Offset pagination fetches multiple pages then stops."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "offset",
                "page_size": 2,
            },
        )

        page1 = [{"id": 1}, {"id": 2}]
        page2 = [{"id": 3}]  # fewer than page_size → last page

        call_count = 0

        def side_effect(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_response(page1)
            return _mock_response(page2)

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.side_effect = side_effect

            df = reader.read(source)

        assert df is not None
        assert len(df) == 3
        assert call_count == 2

    def test_cursor_pagination(self):
        """Cursor pagination follows cursor tokens."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "cursor",
                "data_path": "results",
                "cursor_path": "next_cursor",
                "cursor_param": "after",
            },
        )

        page1 = {"results": [{"id": 1}], "next_cursor": "abc123"}
        page2 = {"results": [{"id": 2}], "next_cursor": None}

        call_count = 0

        def side_effect(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_response(page1)
            return _mock_response(page2)

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.side_effect = side_effect

            df = reader.read(source)

        assert df is not None
        assert len(df) == 2
        assert call_count == 2

    def test_next_link_pagination(self):
        """Next-link pagination follows URLs from the response."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "next_link",
                "data_path": "data",
                "next_link_path": "paging.next",
            },
        )

        page1 = {
            "data": [{"id": 1}],
            "paging": {"next": "https://api.example.com/items?page=2"},
        }
        page2 = {
            "data": [{"id": 2}],
            "paging": {"next": None},
        }

        call_count = 0

        def side_effect(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_response(page1)
            return _mock_response(page2)

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.side_effect = side_effect

            df = reader.read(source)

        assert df is not None
        assert len(df) == 2
        assert call_count == 2

    def test_offset_pagination_max_pages_exits_loop(self):
        """Offset pagination should stop when max_pages is reached."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "offset",
                "page_size": 1,
                "max_pages": 1,
            },
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response([{"id": 1}])

            df = reader.read(source)

        assert df is not None
        assert len(df) == 1

    def test_unknown_pagination_type_advances_until_max_pages(self):
        """Unknown pagination type should still advance pages until max_pages."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "custom_mode",
                "max_pages": 1,
            },
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response([{"id": 1}])

            df = reader.read(source)

        assert df is not None
        assert len(df) == 1


# ---------------------------------------------------------------------------
# Tests: Error handling & rate limiting
# ---------------------------------------------------------------------------

class TestAPIReaderErrorHandling:

    def test_http_error_raises_source_error(self):
        """HTTP 500 raises SourceError."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(src_cfg={"endpoint": "/fail"})

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response(
                {"error": "internal"}, status_code=500,
            )

            with pytest.raises(SourceError, match="HTTP 500"):
                reader.read(source)

    def test_rate_limit_retry(self):
        """429 response with Retry-After triggers a retry."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(src_cfg={"endpoint": "/data"})

        rate_limited = _mock_response({}, status_code=429, headers={"Retry-After": "0.01"})
        success = _mock_response([{"id": 1}])

        call_count = 0

        def side_effect(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return rate_limited
            return success

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.side_effect = side_effect

            df = reader.read(source)

        assert df is not None
        assert call_count == 2

    def test_httpx_import_error(self):
        """Missing httpx raises informative SourceError."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(src_cfg={"endpoint": "/data"})

        with patch("datacoolie.sources.api_reader.httpx", None):
            with pytest.raises(SourceError, match="httpx"):
                reader.read(source)

    def test_http_error_before_retry_raises_source_error(self):
        """HTTP client errors are wrapped as SourceError on first request."""
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_httpx.HTTPError = RuntimeError
            client = MagicMock()
            client.request.side_effect = RuntimeError("network down")

            with pytest.raises(SourceError, match="HTTP request failed"):
                APIReader._make_request(client, "GET", "https://api.example.com/data")

    def test_http_error_after_retry_raises_source_error(self):
        """HTTP client errors on retry are wrapped with retry-specific message."""
        rate_limited = _mock_response({}, status_code=429, headers={"Retry-After": "0"})

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_httpx.HTTPError = RuntimeError
            client = MagicMock()
            client.request.side_effect = [rate_limited, RuntimeError("still down")]

            with pytest.raises(SourceError, match="after retry"):
                APIReader._make_request(client, "GET", "https://api.example.com/data")


# ---------------------------------------------------------------------------
# Tests: Watermark
# ---------------------------------------------------------------------------

class TestAPIReaderWatermark:

    def test_watermark_filtering(self):
        """Watermark filters out older records."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={"endpoint": "/events"},
            watermark_columns=["event_id"],
        )

        records = [
            {"event_id": 1, "name": "old"},
            {"event_id": 2, "name": "old2"},
            {"event_id": 3, "name": "new"},
        ]

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response(records)

            df = reader.read(source, watermark={"event_id": 2})

        assert df is not None
        assert len(df) == 1
        assert df[0]["event_id"] == 3

    def test_new_watermark_computed(self):
        """New watermark values are computed from the result."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={"endpoint": "/events"},
            watermark_columns=["event_id"],
        )

        records = [{"event_id": 10}, {"event_id": 20}, {"event_id": 5}]

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response(records)

            reader.read(source)

        wm = reader.get_new_watermark()
        assert wm["event_id"] == 20

    def test_watermark_filters_everything_returns_none(self):
        """When watermark excludes all rows, read returns None after filtering."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={"endpoint": "/events"},
            watermark_columns=["event_id"],
        )

        records = [{"event_id": 1}, {"event_id": 2}]

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response(records)

            df = reader.read(source, watermark={"event_id": 999})

        assert df is None


# ---------------------------------------------------------------------------
# Tests: Helper methods
# ---------------------------------------------------------------------------

class TestAPIReaderHelpers:

    def test_resolve_path_nested(self):
        """_resolve_path drills into nested dicts."""
        data = {"a": {"b": {"c": 42}}}
        assert APIReader._resolve_path(data, "a.b.c") == 42

    def test_resolve_path_missing(self):
        """_resolve_path returns None for missing keys."""
        assert APIReader._resolve_path({"a": 1}, "b.c") is None

    def test_resolve_path_list_index(self):
        """_resolve_path supports numeric list indices."""
        data = {"items": [{"id": 10}, {"id": 20}]}
        assert APIReader._resolve_path(data, "items.1.id") == 20

    def test_extract_records_top_level_list(self):
        """Top-level list returns records directly."""
        records = [{"id": 1}, {"id": 2}]
        assert APIReader._extract_records(records) == records

    def test_extract_records_single_dict(self):
        """Single dict is wrapped in a list."""
        record = {"id": 1}
        assert APIReader._extract_records(record) == [record]

    def test_extract_records_with_data_path(self):
        """Nested extraction via data_path."""
        data = {"response": {"items": [{"id": 1}]}}
        assert APIReader._extract_records(data, "response.items") == [{"id": 1}]

    def test_extract_records_missing_path(self):
        """Missing data_path returns empty list."""
        assert APIReader._extract_records({"a": 1}, "missing.path") == []

    def test_resolve_path_list_index_out_of_bounds(self):
        """Out-of-bounds list index returns None."""
        data = {"items": [{"id": 10}]}
        assert APIReader._resolve_path(data, "items.9.id") is None

    def test_resolve_path_list_with_non_numeric_key_returns_none(self):
        """Lists require numeric indices in path resolution."""
        data = {"items": [{"id": 10}]}
        assert APIReader._resolve_path(data, "items.foo") is None

    def test_apply_auth_bearer_without_token_no_header(self):
        """Bearer auth without token should not add Authorization header."""
        headers: Dict[str, str] = {}
        APIReader._apply_auth(headers, {"auth_type": "bearer"})
        assert "Authorization" not in headers

    def test_apply_auth_api_key_without_value_no_header(self):
        """API key auth without value should not add key header."""
        headers: Dict[str, str] = {}
        APIReader._apply_auth(headers, {"auth_type": "api_key", "api_key_header": "X-Key"})
        assert "X-Key" not in headers

    def test_records_to_dataframe_wraps_engine_error(self):
        """Engine dataframe conversion failures are wrapped as SourceError."""
        engine = FakeEngine()
        engine.create_dataframe = MagicMock(side_effect=RuntimeError("bad rows"))
        reader = APIReader(engine)
        with pytest.raises(SourceError, match="Failed to convert API records"):
            reader._records_to_dataframe([{"id": 1}])

    def test_read_data_merges_runtime_configure(self):
        """Runtime configure should override source.configure in _read_data."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(src_cfg={"endpoint": "/items", "method": "GET"})

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response([{"id": 1}])

            records = reader._read_data(source, configure={"method": "POST"})

        assert records == [{"id": 1}]
        assert mock_client.request.call_args[0][0] == "POST"

    def test_rate_limit_delay_calls_sleep(self):
        """rate_limit_delay should sleep between paginated requests."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "offset",
                "page_size": 1,
                "rate_limit_delay": 0.01,
            },
        )

        page1 = [{"id": 1}]
        page2 = []

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx, \
             patch("datacoolie.sources.api_reader.time.sleep") as mock_sleep:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.side_effect = [_mock_response(page1), _mock_response(page2)]

            df = reader.read(source)

        assert df is not None
        mock_sleep.assert_called()

    def test_import_fallback_sets_httpx_none(self):
        """Module import fallback sets httpx to None when import fails."""
        import builtins
        import importlib
        import sys

        module_name = "datacoolie.sources.api_reader"
        original_import = builtins.__import__
        original_module = sys.modules.get(module_name)

        def _patched_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "httpx":
                raise ImportError("mocked")
            return original_import(name, globals, locals, fromlist, level)

        try:
            builtins.__import__ = _patched_import
            sys.modules.pop(module_name, None)
            mod = importlib.import_module(module_name)
            assert mod.httpx is None
        finally:
            builtins.__import__ = original_import
            sys.modules.pop(module_name, None)
            if original_module is not None:
                sys.modules[module_name] = original_module


# ---------------------------------------------------------------------------
# Tests: Registry
# ---------------------------------------------------------------------------

class TestAPIReaderRegistry:

    def test_api_registered_in_source_registry(self):
        """APIReader is registered under 'api' key."""
        from datacoolie import source_registry

        assert source_registry.is_available("api")
        assert "api" in source_registry.list_plugins()


# ---------------------------------------------------------------------------
# Tests: Runtime info
# ---------------------------------------------------------------------------

class TestAPIReaderRuntimeInfo:

    def test_runtime_info_recorded(self):
        """Runtime info captures rows_read and source_action."""
        engine = FakeEngine()
        reader = APIReader(engine)
        source = _make_source(src_cfg={"endpoint": "/data"})

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            mock_client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            mock_client.request.return_value = _mock_response([{"id": 1}, {"id": 2}])

            reader.read(source)

        info = reader.get_runtime_info()
        assert info.rows_read == 2
        assert info.source_action["url"] == "https://api.example.com/data"
        assert info.status == "succeeded"
