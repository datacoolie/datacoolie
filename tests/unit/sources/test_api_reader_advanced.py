"""Advanced APIReader tests focused on real reader behavior.

This suite complements test_api_reader.py with targeted scenarios for:
- pagination edge behavior
- retry/rate-limit mechanics
- timeout propagation
- response extraction variants
- error wrapping from request and conversion layers
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Connection, Source
from datacoolie.sources.api_reader import APIReader


class FakeEngine:
    """Minimal engine used by APIReader tests."""

    def create_dataframe(self, records):
        return list(records)

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


def _make_source(
    conn_cfg: Optional[Dict[str, Any]] = None,
    src_cfg: Optional[Dict[str, Any]] = None,
    watermark_columns: Optional[list[str]] = None,
) -> Source:
    return Source(
        connection=Connection(
            name="adv-api",
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
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    resp.text = json.dumps(data) if isinstance(data, (dict, list)) else str(data)
    resp.headers = headers or {}
    return resp


class TestAPIReaderAdvancedPagination:
    @pytest.mark.unit
    def test_cursor_pagination_stops_at_max_pages(self) -> None:
        """Cursor pagination must stop at max_pages even if cursor always exists."""
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "cursor",
                "cursor_path": "next_cursor",
                "cursor_param": "after",
                "data_path": "items",
                "max_pages": 2,
            },
        )

        payload = {"items": [{"id": 1}], "next_cursor": "same-cursor"}

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(payload)

            records = reader._read_data(source)

        assert records == [{"id": 1}, {"id": 1}]
        assert client.request.call_count == 2

    @pytest.mark.unit
    def test_next_link_pagination_replaces_params(self) -> None:
        """Next-link pagination should clear params and follow server-provided URL."""
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "params": {"q": "abc", "page": "1"},
                "pagination_type": "next_link",
                "data_path": "data",
                "next_link_path": "paging.next",
            },
        )

        page1 = {
            "data": [{"id": 1}],
            "paging": {"next": "https://api.example.com/items?page=2"},
        }
        page2 = {"data": [{"id": 2}], "paging": {"next": None}}

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.side_effect = [_mock_response(page1), _mock_response(page2)]

            records = reader._read_data(source)

        assert records == [{"id": 1}, {"id": 2}]
        # params=None means httpx will not append extra params to the next-link
        # URL which already contains all query parameters embedded in it.
        assert client.request.call_args_list[1].kwargs.get("params") is None


class TestAPIReaderAdvancedRateLimiting:
    @pytest.mark.unit
    def test_retry_after_header_is_respected(self) -> None:
        """429 with Retry-After should sleep for the specified duration."""
        reader = APIReader(FakeEngine())
        source = _make_source(src_cfg={"endpoint": "/data"})

        first = _mock_response({}, status_code=429, headers={"Retry-After": "0.01"})
        second = _mock_response([{"id": 1}], status_code=200)

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx, \
             patch("datacoolie.sources.api_reader.time.sleep") as sleep_mock:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.side_effect = [first, second]

            df = reader.read(source)

        assert df == [{"id": 1}]
        sleep_mock.assert_called_once_with(0.01)

    @pytest.mark.unit
    def test_retry_after_missing_uses_default_wait(self) -> None:
        """429 without Retry-After should fall back to default wait."""
        response_429 = _mock_response({}, status_code=429, headers={})
        response_ok = _mock_response([{"id": 1}], status_code=200)

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx, \
             patch("datacoolie.sources.api_reader.time.sleep") as sleep_mock:
            mock_httpx.HTTPError = Exception
            client = MagicMock()
            client.request.side_effect = [response_429, response_ok]

            result = APIReader._make_request(client, "GET", "https://api.example.com/data")

        assert result.status_code == 200
        # No Retry-After → exponential backoff: min(2**0, 30) = 1 second for first retry
        sleep_mock.assert_called_once_with(1)

    @pytest.mark.unit
    def test_exponential_backoff_grows_per_attempt(self) -> None:
        """Each successive 429 without Retry-After doubles the wait, capped at 30s."""
        # 4 x 429 then success — waits should be 1, 2, 4, 8
        responses = [_mock_response({}, status_code=429, headers={}) for _ in range(4)]
        responses.append(_mock_response([{"id": 1}], status_code=200))

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx, \
             patch("datacoolie.sources.api_reader.time.sleep") as sleep_mock:
            mock_httpx.HTTPError = Exception
            client = MagicMock()
            client.request.side_effect = responses

            result = APIReader._make_request(client, "GET", "https://api.example.com/data")

        assert result.status_code == 200
        assert sleep_mock.call_count == 4
        call_args = [c.args[0] for c in sleep_mock.call_args_list]
        assert call_args == [1, 2, 4, 8]  # min(2**0,30), min(2**1,30), ...

    @pytest.mark.unit
    def test_exponential_backoff_capped_at_30s(self) -> None:
        """Exponential backoff must not exceed 30 seconds."""
        # 7 x 429 → waits 1,2,4,8,16,30,30 (2**5=32 > 30 so capped)
        responses = [_mock_response({}, status_code=429, headers={}) for _ in range(7)]
        responses.append(_mock_response([{"id": 1}], status_code=200))

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx, \
             patch("datacoolie.sources.api_reader.time.sleep") as sleep_mock:
            mock_httpx.HTTPError = Exception
            client = MagicMock()
            client.request.side_effect = responses

            APIReader._make_request(client, "GET", "https://api.example.com/data")

        waits = [c.args[0] for c in sleep_mock.call_args_list]
        assert all(w <= 30 for w in waits), f"Some wait exceeded 30s: {waits}"
        assert waits[4] == 16  # attempt 4: min(2**4, 30) = 16
        assert waits[5] == 30  # attempt 5: min(2**5, 30) = 30 — first capped
        assert waits[6] == 30  # attempt 6: min(2**6, 30) = 30 — still capped

    @pytest.mark.unit
    def test_max_retries_exhausted_raises_source_error(self) -> None:
        """Persistent 429 beyond max_retries raises SourceError."""
        always_429 = _mock_response({}, status_code=429, headers={"Retry-After": "0"})

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx, \
             patch("datacoolie.sources.api_reader.time.sleep"):
            mock_httpx.HTTPError = Exception
            client = MagicMock()
            client.request.return_value = always_429

            with pytest.raises(SourceError, match="429 after 3 retries"):
                APIReader._make_request(
                    client, "GET", "https://api.example.com/data", max_retries=3
                )

        assert client.request.call_count == 4  # 1 original + 3 retries

    @pytest.mark.unit
    def test_max_retries_configurable_via_src_cfg(self) -> None:
        """max_retries from src_cfg is forwarded to each _make_request call."""
        always_429 = _mock_response({}, status_code=429, headers={"Retry-After": "0"})

        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={"endpoint": "/data", "max_retries": 2},
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx, \
             patch("datacoolie.sources.api_reader.time.sleep"):
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = always_429

            with pytest.raises(SourceError, match="429 after 2 retries"):
                reader._read_data(source)

        assert client.request.call_count == 3  # 1 original + 2 retries


class TestAPIReaderAdvancedTimeoutAndErrors:
    @pytest.mark.unit
    def test_timeout_setting_propagates_to_client(self) -> None:
        """Connection timeout should be propagated to httpx.Client."""
        reader = APIReader(FakeEngine())
        source = _make_source(
            conn_cfg={"base_url": "https://api.example.com", "timeout": 7},
            src_cfg={"endpoint": "/items"},
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"id": 1}])

            reader._read_data(source)

        assert mock_httpx.Client.call_args.kwargs["timeout"] == 7.0

    @pytest.mark.unit
    def test_non_2xx_response_raises_source_error(self) -> None:
        """Non-success responses should be wrapped in SourceError with status context."""
        bad = _mock_response({"error": "nope"}, status_code=500)

        with pytest.raises(SourceError, match="HTTP 500"):
            APIReader._make_request(MagicMock(request=MagicMock(return_value=bad)), "GET", "https://api.example.com/fail")


class TestAPIReaderAdvancedResponseShapes:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "payload,data_path,expected",
        [
            ([{"id": 1}], None, [{"id": 1}]),
            ({"id": 1}, None, [{"id": 1}]),
            ({"root": {"items": [{"id": 9}]}}, "root.items", [{"id": 9}]),
            ({"root": {"items": None}}, "root.items", []),
            ("not-a-json-object", None, []),
        ],
    )
    def test_extract_records_shapes(self, payload: Any, data_path: Optional[str], expected: list[dict]) -> None:
        assert APIReader._extract_records(payload, data_path) == expected

    @pytest.mark.unit
    def test_read_with_data_path_targeting_single_object(self) -> None:
        """Nested object data_path should still produce one-record dataframe."""
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={"endpoint": "/single", "data_path": "result"},
        )

        body = {"result": {"id": 11, "name": "alice"}}

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(body)

            df = reader.read(source)

        assert df is not None
        assert len(df) == 1
        assert df[0]["id"] == 11


class TestFormatWatermarkValueDate:
    """Tests for _format_watermark_value with date objects and datetime storage fix."""

    def test_format_date_iso(self) -> None:
        """date object with fmt='iso' returns ISO date string."""
        result = APIReader._format_watermark_value(date(2024, 1, 15), "iso")
        assert result == "2024-01-15T00:00:00+00:00"

    def test_format_date_date_fmt(self) -> None:
        """date object with fmt='date' returns bare date string without time."""
        result = APIReader._format_watermark_value(date(2024, 1, 15), "date")
        assert result == "2024-01-15"

    def test_format_date_timestamp(self) -> None:
        """date object with fmt='timestamp' returns Unix seconds — not the ISO string."""
        result = APIReader._format_watermark_value(date(2024, 1, 15), "timestamp")
        expected = str(datetime(2024, 1, 15, tzinfo=timezone.utc).timestamp())
        assert result == expected
        # Must not be the date ISO string
        assert result != "2024-01-15"

    def test_format_date_timestamp_ms(self) -> None:
        """date object with fmt='timestamp_ms' returns Unix milliseconds."""
        result = APIReader._format_watermark_value(date(2024, 1, 15), "timestamp_ms")
        expected = str(int(datetime(2024, 1, 15, tzinfo=timezone.utc).timestamp() * 1000))
        assert result == expected

    def test_pushed_col_watermark_stored_as_datetime(self) -> None:
        """Pushed-col watermark (from watermark_param_mapping) is stored as datetime, not str."""
        reader = APIReader(FakeEngine())
        source = _make_source(
            conn_cfg={"base_url": "http://example.com"},
            src_cfg={
                "endpoint": "/items",
                "watermark_param_mapping": {"modified_at": "modified_since"},
            },
            watermark_columns=["modified_at"],
        )

        records = [{"id": 1, "name": "foo"}]  # modified_at absent — server-side filtered

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(records)

            reader.read(source)

        new_wm = reader._new_watermark
        assert "modified_at" in new_wm
        # Must be stored as datetime — NOT as a bare ISO string
        assert isinstance(new_wm["modified_at"], datetime), (
            f"Expected datetime, got {type(new_wm['modified_at'])}: {new_wm['modified_at']!r}"
        )

    def test_pushed_col_watermark_preserves_date_type(self) -> None:
        """When previous watermark for a pushed col is date, new wm stays date."""
        reader = APIReader(FakeEngine())
        source = _make_source(
            conn_cfg={"base_url": "http://example.com"},
            src_cfg={
                "endpoint": "/items",
                "watermark_param_mapping": {"event_date": "since"},
            },
            watermark_columns=["event_date"],
        )

        records = [{"id": 1, "name": "foo"}]
        prev_watermark = {"event_date": date(2026, 3, 1)}  # date type, not datetime

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(records)

            reader.read(source, prev_watermark)

        new_wm = reader._new_watermark
        assert "event_date" in new_wm
        val = new_wm["event_date"]
        # Must stay date — WatermarkSerializer preserves type via __date__ sentinel
        assert type(val) is date, (
            f"Expected date, got {type(val)}: {val!r}"
        )


class TestOffsetConcurrentPagination:
    """Tests for _fetch_offset_concurrent — concurrent offset pagination via total_path."""

    @pytest.mark.unit
    def test_single_page_no_extra_requests(self) -> None:
        """When total <= page_size only one HTTP call is made."""
        client = MagicMock()
        client.request.return_value = _mock_response(
            {"data": [{"id": 1}, {"id": 2}], "total": 2}
        )

        src_cfg = {
            "pagination_type": "offset",
            "data_path": "data",
            "total_path": "total",
            "page_size": 100,
        }

        result = APIReader._fetch_offset_concurrent(client, "https://api.example.com/items", "GET", {}, {}, src_cfg)

        assert result == [{"id": 1}, {"id": 2}]
        assert client.request.call_count == 1

    @pytest.mark.unit
    def test_multiple_pages_fetched_concurrently(self) -> None:
        """With total=250 and page_size=100, pages 1 and 2 are fetched after page 0."""
        call_count = 0
        offsets_seen: list[int] = []

        def fake_request(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            params = kwargs.get("params") or {}
            offset = params.get("offset", 0)
            offsets_seen.append(offset)
            # Each page returns 100 records tagged with their starting id
            records = [{"id": offset + i} for i in range(100)]
            return _mock_response({"items": records, "total": 250})

        client = MagicMock()
        client.request.side_effect = fake_request

        src_cfg = {
            "pagination_type": "offset",
            "data_path": "items",
            "total_path": "total",
            "page_size": 100,
            "offset_max_workers": 2,
        }

        result = APIReader._fetch_offset_concurrent(
            client, "https://api.example.com/items", "GET", {}, {}, src_cfg
        )

        assert call_count == 3  # page 0 + page 1 + page 2
        assert sorted(offsets_seen) == [0, 100, 200]
        assert len(result) == 300  # 100 records x 3 pages

    @pytest.mark.unit
    def test_max_pages_cap_respected(self) -> None:
        """max_pages caps the number of concurrent pages even if total says more."""
        call_count = 0

        def fake_request(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            return _mock_response({"data": [{"id": call_count}], "total": 10000})

        client = MagicMock()
        client.request.side_effect = fake_request

        src_cfg = {
            "pagination_type": "offset",
            "data_path": "data",
            "total_path": "total",
            "page_size": 100,
            "max_pages": 2,  # allow at most 2 pages
        }

        result = APIReader._fetch_offset_concurrent(
            client, "https://api.example.com/items", "GET", {}, {}, src_cfg
        )

        assert call_count == 2  # page 0 + page 1
        assert len(result) == 2

    @pytest.mark.unit
    def test_empty_first_page_returns_empty(self) -> None:
        """Empty page 0 short-circuits without reading total."""
        client = MagicMock()
        client.request.return_value = _mock_response({"data": [], "total": 500})

        src_cfg = {
            "pagination_type": "offset",
            "data_path": "data",
            "total_path": "total",
            "page_size": 100,
        }

        result = APIReader._fetch_offset_concurrent(
            client, "https://api.example.com/items", "GET", {}, {}, src_cfg
        )

        assert result == []
        assert client.request.call_count == 1

    @pytest.mark.unit
    def test_bad_total_raises_source_error(self) -> None:
        """Non-integer total value raises SourceError with details."""
        client = MagicMock()
        client.request.return_value = _mock_response(
            {"data": [{"id": 1}], "total": "N/A"}
        )

        src_cfg = {
            "pagination_type": "offset",
            "data_path": "data",
            "total_path": "total",
            "page_size": 100,
        }

        with pytest.raises(SourceError, match="total_path="):
            APIReader._fetch_offset_concurrent(
                client, "https://api.example.com/items", "GET", {}, {}, src_cfg
            )

    @pytest.mark.unit
    def test_missing_total_raises_source_error(self) -> None:
        """None total (path not found) raises SourceError."""
        client = MagicMock()
        client.request.return_value = _mock_response(
            {"data": [{"id": 1}]}  # no "total" key at all
        )

        src_cfg = {
            "pagination_type": "offset",
            "data_path": "data",
            "total_path": "meta.total",  # path does not exist in response
            "page_size": 100,
        }

        with pytest.raises(SourceError, match="total_path="):
            APIReader._fetch_offset_concurrent(
                client, "https://api.example.com/items", "GET", {}, {}, src_cfg
            )

    @pytest.mark.unit
    def test_sequential_path_unchanged_without_total_path(self) -> None:
        """Without total_path configured, offset pagination stays sequential."""
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "offset",
                "data_path": "data",
                "page_size": 2,
            }
        )

        responses = [
            _mock_response({"data": [{"id": 1}, {"id": 2}]}),
            _mock_response({"data": [{"id": 3}]}),  # short page → stop
        ]

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.side_effect = responses

            records = reader._read_data(source)

        assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert client.request.call_count == 2
