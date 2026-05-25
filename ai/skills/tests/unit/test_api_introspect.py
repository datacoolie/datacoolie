"""Unit tests for _api_introspect._detect_pagination and _build_auth_headers."""
import base64
import sys
from pathlib import Path

import pytest

# conftest.py adds SCRIPTS to sys.path
from _api_introspect import _build_auth_headers, _detect_pagination
from _types import ApiParameter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _params(*names: str, required: bool = False) -> list[ApiParameter]:
    return [ApiParameter(name=n, location="query", param_type="string", required=required) for n in names]


# ---------------------------------------------------------------------------
# _detect_pagination
# ---------------------------------------------------------------------------

class TestDetectPagination:
    def test_cursor_param(self):
        assert _detect_pagination(_params("cursor", "limit"), None) == "cursor"

    def test_after_param(self):
        assert _detect_pagination(_params("after", "first"), None) == "cursor"

    def test_cursor_takes_priority_over_page(self):
        # cursor + page — cursor wins (more specific)
        assert _detect_pagination(_params("cursor", "page", "limit"), None) == "cursor"

    def test_response_next_cursor(self):
        schema = {"type": "object", "properties": {"data": {}, "next_cursor": {}}}
        assert _detect_pagination([], schema) == "cursor"

    def test_response_next_page_token(self):
        schema = {"type": "object", "properties": {"results": {}, "next_page_token": {}}}
        assert _detect_pagination([], schema) == "cursor"

    def test_offset_page_per_page(self):
        assert _detect_pagination(_params("page", "per_page"), None) == "offset"

    def test_offset_page_page_size(self):
        assert _detect_pagination(_params("page", "page_size"), None) == "offset"

    def test_offset_limit_offset(self):
        assert _detect_pagination(_params("limit", "offset"), None) == "offset"

    def test_offset_page_only(self):
        assert _detect_pagination(_params("page"), None) == "offset"

    def test_link_response_next(self):
        schema = {"type": "object", "properties": {"value": {}, "next": {}}}
        assert _detect_pagination([], schema) == "link"

    def test_no_pagination(self):
        assert _detect_pagination(_params("id", "filter"), None) is None

    def test_no_pagination_empty_params(self):
        assert _detect_pagination([], None) is None

    def test_no_pagination_empty_schema(self):
        assert _detect_pagination([], {"type": "object", "properties": {"data": {}}}) is None

    def test_limit_without_offset_no_pagination(self):
        # "limit" alone is NOT offset — needs both limit+offset or a page param
        assert _detect_pagination(_params("limit"), None) is None

    def test_offset_without_limit_no_pagination(self):
        # "offset" alone should not trigger offset pagination
        assert _detect_pagination(_params("offset"), None) is None

    def test_case_insensitive_param_names(self):
        # _detect_pagination lowercases param names — so mixed-case works
        assert _detect_pagination(_params("Page", "Per_Page"), None) == "offset"
        assert _detect_pagination(_params("Cursor", "Limit"), None) == "cursor"


# ---------------------------------------------------------------------------
# _build_auth_headers
# ---------------------------------------------------------------------------

class TestBuildAuthHeaders:
    def test_no_auth(self):
        headers = _build_auth_headers(None, None, "X-API-Key", None)
        assert "Authorization" not in headers
        assert "X-API-Key" not in headers
        assert headers.get("User-Agent") == "datacoolie-discover/1.0"

    def test_api_key_default_header(self):
        headers = _build_auth_headers("api_key", "testkey123", "X-API-Key", None)
        assert headers["X-API-Key"] == "testkey123"
        assert "Authorization" not in headers

    def test_api_key_custom_header(self):
        headers = _build_auth_headers("api_key", "mytoken", "X-Custom-Auth", None)
        assert headers["X-Custom-Auth"] == "mytoken"
        assert "X-API-Key" not in headers

    def test_bearer(self):
        headers = _build_auth_headers("bearer", None, "X-API-Key", "testtoken456")
        assert headers["Authorization"] == "Bearer testtoken456"

    def test_basic(self):
        headers = _build_auth_headers("basic", None, "X-API-Key", "testuser:testpass")
        expected = "Basic " + base64.b64encode(b"testuser:testpass").decode()
        assert headers["Authorization"] == expected

    def test_api_key_missing_value_no_header(self):
        # auth_mode set but no key — header must NOT be added (avoids empty key injection)
        headers = _build_auth_headers("api_key", None, "X-API-Key", None)
        assert "X-API-Key" not in headers

    def test_bearer_missing_token_no_header(self):
        headers = _build_auth_headers("bearer", None, "X-API-Key", None)
        assert "Authorization" not in headers

    def test_basic_missing_token_no_header(self):
        headers = _build_auth_headers("basic", None, "X-API-Key", None)
        assert "Authorization" not in headers

    def test_unknown_auth_mode_no_extra_headers(self):
        headers = _build_auth_headers("oauth2", None, "X-API-Key", "tok")
        # unknown mode — should not crash, no auth header added
        assert "Authorization" not in headers
