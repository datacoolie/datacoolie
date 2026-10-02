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

import httpx
import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.sources.api_reader import APIReader
from datacoolie.sources.base import SourceReadRange
from datacoolie.sources._api.ranges import compare_values
from datacoolie.sources._api.pagination import (
    fetch_offset_concurrent,
    fetch_single_range,
    resolve_same_origin_next_url,
)
from datacoolie.sources._api.records import extract_records
from datacoolie.sources._api.transport import make_request
from datacoolie.sources._api.watermark import build_watermark_ranges, format_watermark_value


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

    def apply_watermark_filter(self, df, columns, watermark_start, *, start_operator=">", watermark_end=None, end_operator="<"):
        filtered = []
        for row in df:
            keep = False
            for col in columns:
                wm_val = watermark_start.get(col)
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


class _FrozenDateTime(datetime):
    """Deterministic clock that remains a datetime subclass when patched."""

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 9, 30, 10, 11, 12, 123456, tzinfo=tz)


class TestAPIReaderAdvancedPagination:
    @pytest.mark.unit
    def test_cursor_pagination_fails_at_cap_when_cursor_remains(self) -> None:
        """A page cap with a remaining cursor is incomplete, not successful."""
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

            with pytest.raises(SourceError, match="cursor remained"):
                reader._read_data(source)

        assert client.request.call_count == 2

    @pytest.mark.unit
    def test_cursor_pagination_continues_after_empty_intermediate_page(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "cursor",
                "cursor_path": "next_cursor",
                "cursor_param": "after",
                "data_path": "items",
            },
        )
        responses = [
            _mock_response({"items": [], "next_cursor": "continued"}),
            _mock_response({"items": [{"id": 1}], "next_cursor": None}),
        ]
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.side_effect = responses

            records = reader._read_data(source)

        assert records == [{"id": 1}]
        assert client.request.call_count == 2

    @pytest.mark.unit
    def test_next_link_error_on_later_page_discards_partial_records(self) -> None:
        """A failed continuation must not return the already fetched page."""
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(
                    200,
                    json={
                        "data": [{"id": 1}],
                        "paging": {"next": "/items?page=2"},
                    },
                    request=request,
                )
            return httpx.Response(503, json={"error": "temporary"}, request=request)

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(SourceError, match="HTTP 503"):
                fetch_single_range(
                    client,
                    "https://api.example.test/items",
                    "GET",
                    None,
                    None,
                    {
                        "pagination_type": "next_link",
                        "data_path": "data",
                        "next_link_path": "paging.next",
                    },
                )

        assert [request.url.path for request in requests] == ["/items", "/items"]
        assert requests[1].url.params["page"] == "2"

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

    @pytest.mark.unit
    def test_next_link_rejects_foreign_origin_before_dispatching_credentials(self) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200,
                json={
                    "data": [{"id": 1}],
                    "paging": {
                        "next": "https://evil.example.test/steal?token=synthetic-secret",
                    },
                },
                request=request,
            )

        source_cfg = {
            "pagination_type": "next_link",
            "data_path": "data",
            "next_link_path": "paging.next",
        }
        with httpx.Client(
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer synthetic-secret"},
        ) as client:
            with pytest.raises(SourceError, match=r"configured HTTP\(S\) origin") as exc_info:
                fetch_single_range(
                    client,
                    "https://api.example.test/items",
                    "GET",
                    None,
                    None,
                    source_cfg,
                )

        assert len(requests) == 1
        assert requests[0].headers["Authorization"] == "Bearer synthetic-secret"
        assert "synthetic-secret" not in str(exc_info.value)

    @pytest.mark.unit
    def test_next_link_same_origin_relative_url_keeps_client_auth(self) -> None:
        requests: list[httpx.Request] = []
        payloads = iter(
            [
                {
                    "data": [{"id": 1}],
                    "paging": {"next": "/items?page=2"},
                },
                {"data": [{"id": 2}], "paging": {"next": None}},
            ]
        )

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json=next(payloads), request=request)

        with httpx.Client(
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer synthetic-secret"},
        ) as client:
            records = fetch_single_range(
                client,
                "https://api.example.test/items",
                "GET",
                {"q": "original"},
                None,
                {
                    "pagination_type": "next_link",
                    "data_path": "data",
                    "next_link_path": "paging.next",
                },
            )

        assert records == [{"id": 1}, {"id": 2}]
        assert [str(request.url) for request in requests] == [
            "https://api.example.test/items?q=original",
            "https://api.example.test/items?page=2",
        ]
        assert all(request.headers["Authorization"] == "Bearer synthetic-secret" for request in requests)

    @pytest.mark.unit
    def test_next_link_repeat_query_bounds_preserves_matching_and_adds_missing(self) -> None:
        payloads = iter(
            [
                {
                    "data": [{"id": 1}],
                    "paging": {"next": "/items?from_id=2&token=signed"},
                },
                {"data": [{"id": 2}], "paging": {"next": None}},
            ]
        )
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json=next(payloads), request=request)

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            records = fetch_single_range(
                client,
                "https://api.example.test/items",
                "GET",
                {"from_id": 2, "to_id": 5},
                None,
                {
                    "pagination_type": "next_link",
                    "data_path": "data",
                    "next_link_path": "paging.next",
                    "next_link_bound_mode": "repeat_query_bounds",
                    "_active_query_bounds": {"from_id": 2, "to_id": 5},
                },
            )

        assert records == [{"id": 1}, {"id": 2}]
        assert str(requests[1].url) == (
            "https://api.example.test/items?from_id=2&token=signed&to_id=5"
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "query",
        ["from_id=9&token=signed", "from_id=2&from_id=2&token=signed"],
    )
    def test_next_link_repeat_query_bounds_rejects_conflict_or_duplicate(self, query: str) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200,
                json={"data": [{"id": 1}], "paging": {"next": f"/items?{query}"}},
                request=request,
            )

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(SourceError, match="(conflicts|repeats)"):
                fetch_single_range(
                    client,
                    "https://api.example.test/items",
                    "GET",
                    {"from_id": 2, "to_id": 5},
                    None,
                    {
                        "pagination_type": "next_link",
                        "data_path": "data",
                        "next_link_path": "paging.next",
                        "next_link_bound_mode": "repeat_query_bounds",
                        "_active_query_bounds": {"from_id": 2, "to_id": 5},
                    },
                )

        assert len(requests) == 1


class TestAPIReaderRangeBindings:
    @pytest.mark.unit
    def test_legacy_unmapped_typed_row_watermark_is_monotonic(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(watermark_columns=["id"])
        reader._active_source = source

        assert reader.merge_watermark({"id": 5}, {"id": 3}) == {"id": 5}
        assert reader.merge_watermark({"id": 5}, {"id": 7}) == {"id": 7}
        # A cursor-shaped string remains opaque even when it is authored as a
        # legacy watermark column.
        assert reader.merge_watermark({"id": "token-5"}, {"id": "token-3"}) == {
            "id": "token-3"
        }

    @pytest.mark.unit
    def test_residual_filter_accepts_datetime_response_for_date_bounds(self) -> None:
        assert compare_values(
            "2024-01-15T10:00:00",
            date(2024, 1, 15),
            ">=",
        )
        assert compare_values(
            "2024-01-15T10:00:00",
            date(2024, 1, 16),
            "<",
        )

    @pytest.mark.unit
    def test_bounded_range_keeps_integer_wire_types_and_filters_all_pages(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "pagination_type": "next_link",
                "data_path": "data",
                "next_link_path": "paging.next",
                "range_param_mapping": {
                    "source_id": {
                        "lower": {"location": "params", "name": "from_id", "operator": ">="},
                        "upper": {"location": "params", "name": "to_id", "operator": "<"},
                        "format": "integer",
                        "response_column": "id",
                    }
                },
            }
        )
        page1 = {
            "data": [{"id": 1}, {"id": 2}],
            "paging": {"next": "https://api.example.com/items?opaque=signed-token"},
        }
        page2 = {"data": [{"id": 4}, {"id": 5}], "paging": {"next": None}}

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.side_effect = [_mock_response(page1), _mock_response(page2)]

            result = reader.read(source, read_range=SourceReadRange("source_id", 2, 5))

        assert result == [{"id": 2}, {"id": 4}]
        first = client.request.call_args_list[0].kwargs
        assert first["params"]["from_id"] == 2
        assert first["params"]["to_id"] == 5
        assert str(client.request.call_args_list[1].args[1]) == (
            "https://api.example.com/items?opaque=signed-token"
        )
        assert client.request.call_args_list[1].kwargs.get("params") is None

    @pytest.mark.unit
    def test_new_mapping_can_bind_query_and_body_with_native_and_temporal_values(self) -> None:
        reader = APIReader(FakeEngine())
        updated_at = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
        updated_until = datetime(2026, 9, 27, 11, 0, tzinfo=timezone.utc)
        source = _make_source(
            src_cfg={
                "method": "POST",
                "params": {"tenant": "north"},
                "body": {"kind": "event"},
                "range_param_mapping": {
                    "event_id": {
                        "lower": {"location": "params", "name": "from_id", "operator": ">="},
                        "upper": {"location": "params", "name": "to_id", "operator": "<"},
                        "format": "integer",
                        "response_column": "event_id",
                    },
                    "updated_at": {
                        "lower": {"location": "body", "name": "from_time", "operator": ">="},
                        "upper": {"location": "body", "name": "to_time", "operator": "<"},
                        "format": "iso",
                        "response_column": "updated_at",
                    },
                },
            },
            watermark_columns=["event_id", "updated_at"],
        )
        rows = [
            {"event_id": 3, "updated_at": "2026-09-27T10:30:00+00:00"},
            {"event_id": 4, "updated_at": "2026-09-27T10:45:00+00:00"},
        ]

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(rows)

            result = reader.read(
                source,
                watermark_start={"event_id": 2, "updated_at": updated_at},
                watermark_end={"event_id": 4, "updated_at": updated_until},
            )

        assert result == rows
        request = client.request.call_args.kwargs
        assert request["params"] == {"tenant": "north", "from_id": 2, "to_id": 4}
        assert request["json"] == {
            "kind": "event",
            "from_time": updated_at.isoformat(),
            "to_time": updated_until.isoformat(),
        }
        assert reader.get_new_watermark() == {
            "event_id": 4,
            "updated_at": "2026-09-27T10:45:00+00:00",
        }

    @pytest.mark.unit
    def test_observed_max_explicit_inclusive_lower_keeps_boundary(self) -> None:
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "event_id": {
                        "lower": {"name": "from_id", "operator": ">="},
                        "upper": {"name": "to_id", "operator": "<"},
                        "format": "integer",
                        "response_column": "event_id",
                        "watermark_value": "observed_max",
                    }
                }
            },
            watermark_columns=["event_id"],
        )
        rows = [{"event_id": 5}, {"event_id": 6}]

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(rows)

            exclusive = APIReader(FakeEngine()).read(
                source,
                watermark_start={"event_id": 5},
            )
            inclusive = APIReader(FakeEngine()).read(
                source,
                watermark_start={"event_id": 5},
                watermark_start_operator=">=",
            )

        assert exclusive == [{"event_id": 6}]
        assert inclusive == rows

    @pytest.mark.unit
    def test_request_end_explicit_exclusive_lower_is_rejected_before_http(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "covered_at",
                        "watermark_value": "request_end",
                    }
                }
            },
            watermark_columns=["covered_at"],
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            with pytest.raises(SourceError, match="incompatible with API request_end"):
                reader.read(
                    source,
                    watermark_start={"covered_at": 5},
                    watermark_start_operator=">",
                )

        mock_httpx.Client.assert_not_called()

    @pytest.mark.unit
    def test_observed_max_requires_response_column_before_http(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "cursor": {
                        "lower": {"name": "from_cursor", "operator": ">="},
                        "upper": {"name": "to_cursor", "operator": "<"},
                        "format": "integer",
                        "response_column": None,
                        "watermark_value": "observed_max",
                    }
                }
            },
            watermark_columns=["cursor"],
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = httpx.Response(
                200,
                json=[{"cursor": 1}],
                request=httpx.Request("GET", "https://api.example.com"),
            )

            error = None
            result = None
            try:
                result = reader.read(source, read_range=SourceReadRange("cursor", 1, 3))
            except Exception as exc:  # pragma: no cover - assertion below reports the contract
                error = exc

        assert isinstance(
            error,
            SourceError,
        ), (
            "Expected bounded observed_max with response_column=None to fail before HTTP; "
            f"got result={result!r}, error={error!r}, "
            f"HTTP dispatches={client.request.call_count}"
        )
        assert "requires response_column" in str(error)
        assert client.request.call_count == 0

    @pytest.mark.unit
    def test_request_end_without_response_column_accepts_exact_endpoint(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": "integer",
                        "response_column": None,
                        "watermark_value": "request_end",
                    }
                }
            },
            watermark_columns=["covered_at"],
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"payload": "ok"}])

            result = reader.read(source, read_range=SourceReadRange("covered_at", 1, 3))

        assert result == [{"payload": "ok"}]
        assert client.request.call_args.kwargs["params"] == {
            "covered_from": 1,
            "covered_to": 3,
        }
        assert reader.get_new_watermark() == {"covered_at": 3}

    @pytest.mark.unit
    def test_missing_response_column_fails_without_advancing_state(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "cursor": {
                        "lower": {"name": "from_cursor", "operator": ">="},
                        "upper": {"name": "to_cursor", "operator": "<"},
                        "format": "integer",
                        "response_column": "cursor_value",
                    }
                }
            }
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"payload": "missing"}])

            with pytest.raises(SourceError, match="missing residual range field"):
                reader.read(source, read_range=SourceReadRange("cursor", 1, 3))

        assert reader.get_new_watermark() == {}

    @pytest.mark.unit
    def test_mixed_legacy_and_new_mapping_fails_before_http(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "event_id": {
                        "lower": {"name": "from_id"},
                        "upper": {"name": "to_id"},
                        "format": "integer",
                    }
                },
                "watermark_param_mapping": {"event_id": "legacy_from"},
            }
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            with pytest.raises(SourceError, match="cannot be combined"):
                reader.read(source, read_range=SourceReadRange("event_id", 1, 3))

        mock_httpx.Client.assert_not_called()

    @pytest.mark.unit
    def test_authored_parameter_conflict_fails_before_http(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "params": {"from_id": 99},
                "range_param_mapping": {
                    "event_id": {
                        "lower": {"name": "from_id"},
                        "upper": {"name": "to_id"},
                        "format": "integer",
                    }
                },
            }
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            with pytest.raises(SourceError, match="conflicts with an authored"):
                reader.read(source, read_range=SourceReadRange("event_id", 1, 3))

        mock_httpx.Client.assert_not_called()

    @pytest.mark.unit
    def test_bounded_request_end_range_exposes_covered_end_for_optional_save(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "covered_at",
                        "watermark_value": "request_end",
                    }
                }
            },
            watermark_columns=["covered_at"],
        )
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"covered_at": 3}])

            reader.read(source, read_range=SourceReadRange("covered_at", 2, 5))

        assert reader.get_new_watermark() == {"covered_at": 5}

    @pytest.mark.unit
    def test_independent_selection_column_uses_response_column_for_residual_filter(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "source_cursor": {
                        "lower": {"name": "cursor_from", "operator": ">="},
                        "upper": {"name": "cursor_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "event_id",
                    }
                }
            }
        )
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(
                [{"event_id": 9}, {"event_id": 10}, {"event_id": 12}]
            )

            result = reader.read(source, read_range=SourceReadRange("source_cursor", 10, 12))

        assert result == [{"event_id": 10}]
        assert client.request.call_args.kwargs["params"] == {
            "cursor_from": 10,
            "cursor_to": 12,
        }

    @pytest.mark.unit
    def test_independent_selection_only_persists_authored_watermark_columns(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "created_at": {
                        "lower": {"name": "created_from", "operator": ">="},
                        "upper": {"name": "created_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "created_at",
                        "watermark_value": "observed_max",
                    }
                }
            },
            watermark_columns=["updated_at"],
        )
        rows = [
            {"created_at": 10, "updated_at": 101},
            {"created_at": 11, "updated_at": 109},
        ]

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(rows)

            result = reader.read(
                source,
                read_range=SourceReadRange("created_at", 10, 12),
            )

        assert result == rows
        assert reader.get_new_watermark() == {"updated_at": 109}

    @pytest.mark.unit
    def test_unmapped_watermark_columns_keep_or_semantics_with_mapped_fields(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "updated_at": {
                        "lower": {"name": "updated_from", "operator": ">="},
                        "format": "integer",
                        "response_column": "updated_at",
                        "watermark_value": "observed_max",
                    }
                }
            },
            watermark_columns=["updated_at", "sequence_id"],
        )
        rows = [
            {"updated_at": 4, "sequence_id": 20},
            {"updated_at": 7, "sequence_id": 3},
            {"updated_at": 4, "sequence_id": 3},
        ]
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(rows)

            result = reader.read(
                source,
                watermark_start={"updated_at": 5, "sequence_id": 10},
            )

        assert result == [{"updated_at": 4, "sequence_id": 20}, {"updated_at": 7, "sequence_id": 3}]

    @pytest.mark.unit
    def test_explicit_active_upper_is_not_replaced_by_now(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "created_at": {
                        "lower": {"name": "created_from", "operator": ">="},
                        "upper": {"name": "created_to", "operator": "<"},
                        "format": "iso",
                        "response_column": "created_at",
                    }
                }
            },
            watermark_columns=["created_at"],
        )
        upper = datetime(2026, 9, 28, tzinfo=timezone.utc)
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(
                [{"created_at": "2026-09-27T00:00:00+00:00"}]
            )

            reader.read(
                source,
                watermark_start={"created_at": datetime(2026, 9, 26, tzinfo=timezone.utc)},
                watermark_end={"created_at": upper},
            )

        assert client.request.call_args.kwargs["params"]["created_to"] == upper.isoformat()

    @pytest.mark.parametrize(
        "wire_format,start,expected_wire,expected_state",
        [
            (
                "date",
                date(2026, 9, 29),
                "2026-09-30",
                date(2026, 9, 30),
            ),
            (
                "datetime",
                datetime(2026, 9, 29, tzinfo=timezone.utc),
                "2026-09-30T10:11:12",
                datetime(2026, 9, 30, 10, 11, 12, tzinfo=timezone.utc),
            ),
            (
                "datetime_ms",
                datetime(2026, 9, 29, tzinfo=timezone.utc),
                "2026-09-30T10:11:12.123",
                datetime(2026, 9, 30, 10, 11, 12, 123000, tzinfo=timezone.utc),
            ),
            (
                "timestamp",
                datetime(2026, 9, 29, tzinfo=timezone.utc),
                "1790763072.123456",
                datetime(2026, 9, 30, 10, 11, 12, 123456, tzinfo=timezone.utc),
            ),
            (
                "timestamp_ms",
                datetime(2026, 9, 29, tzinfo=timezone.utc),
                "1790763072123",
                datetime(2026, 9, 30, 10, 11, 12, 123000, tzinfo=timezone.utc),
            ),
        ],
        ids=["date", "datetime", "datetime-ms", "timestamp", "timestamp-ms"],
    )
    def test_generated_request_end_upper_aligns_request_and_candidate(
        self,
        wire_format: str,
        start: Any,
        expected_wire: str,
        expected_state: Any,
    ) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": wire_format,
                        "response_column": None,
                        "watermark_value": "request_end",
                    }
                }
            },
            watermark_columns=["covered_at"],
        )

        with patch("datacoolie.sources.api_reader.datetime", _FrozenDateTime), patch(
            "datacoolie.sources.api_reader.httpx"
        ) as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"payload": "ok"}])

            result = reader.read(source, watermark_start={"covered_at": start})

        assert result == [{"payload": "ok"}]
        assert client.request.call_args.kwargs["params"]["covered_to"] == expected_wire
        assert reader.get_new_watermark() == {"covered_at": expected_state}

    @pytest.mark.parametrize(
        "wire_format,start,expected_wire,expected_state",
        [
            (
                "date",
                date(2026, 9, 29),
                "2026-09-30",
                date(2026, 9, 30),
            ),
            (
                "datetime_ms",
                datetime(2026, 9, 29, tzinfo=timezone.utc),
                "2026-09-30T10:11:12.123",
                datetime(2026, 9, 30, 10, 11, 12, 123000, tzinfo=timezone.utc),
            ),
        ],
        ids=["split-date", "split-datetime-ms"],
    )
    def test_split_generated_request_end_uses_same_aligned_upper(
        self,
        wire_format: str,
        start: Any,
        expected_wire: str,
        expected_state: Any,
    ) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": wire_format,
                        "response_column": None,
                        "watermark_value": "request_end",
                    }
                },
                "watermark_range_interval_unit": "day",
                "watermark_range_interval_amount": 1,
                "watermark_range_max_workers": 1,
            },
            watermark_columns=["covered_at"],
        )

        with patch("datacoolie.sources.api_reader.datetime", _FrozenDateTime), patch(
            "datacoolie.sources.api_reader.httpx"
        ) as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"payload": "ok"}])

            result = reader.read(source, watermark_start={"covered_at": start})

        expected_rows = (
            [{"payload": "ok"}, {"payload": "ok"}]
            if wire_format == "datetime_ms"
            else [{"payload": "ok"}]
        )
        assert result == expected_rows
        assert client.request.call_args_list[-1].kwargs["params"]["covered_to"] == expected_wire
        assert reader.get_new_watermark() == {"covered_at": expected_state}

    @pytest.mark.parametrize(
        "wire_format,expected_wire",
        [
            ("iso", "2026-09-30T00:00:00+00:00"),
            ("datetime", "2026-09-30T00:00:00"),
            ("datetime_ms", "2026-09-30T00:00:00.000"),
        ],
        ids=["date-start-iso", "date-start-datetime", "date-start-datetime-ms"],
    )
    def test_split_date_only_start_aligns_generated_upper_and_state(
        self,
        wire_format: str,
        expected_wire: str,
    ) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": wire_format,
                        "response_column": None,
                        "watermark_value": "request_end",
                    }
                },
                "watermark_range_start": "2026-09-29",
                "watermark_range_interval_unit": "day",
                "watermark_range_interval_amount": 1,
                "watermark_range_max_workers": 1,
            },
            watermark_columns=["covered_at"],
        )

        with patch("datacoolie.sources.api_reader.datetime", _FrozenDateTime), patch(
            "datacoolie.sources.api_reader.httpx"
        ) as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"payload": "ok"}])

            result = reader.read(source)

        assert result == [{"payload": "ok"}]
        assert client.request.call_args.kwargs["params"] == {
            "covered_from": (
                "2026-09-29T00:00:00+00:00"
                if wire_format == "iso"
                else (
                    "2026-09-29T00:00:00.000"
                    if wire_format == "datetime_ms"
                    else "2026-09-29T00:00:00"
                )
            ),
            "covered_to": expected_wire,
        }
        assert reader.get_new_watermark() == {"covered_at": date(2026, 9, 30)}

    def test_new_mapping_rejects_lossy_raw_iso_lower_before_lookback(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "backward": {"days": 1},
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": "datetime_ms",
                        "response_column": "covered_at",
                        "watermark_value": "observed_max",
                    }
                },
            },
            watermark_columns=["covered_at"],
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            with pytest.raises(SourceError, match="microsecond|loss"):
                reader.read(
                    source,
                    watermark_start={
                        "covered_at": "2026-09-29T00:00:00.1230001+00:00"
                    },
                )

        mock_httpx.Client.assert_not_called()

    @pytest.mark.parametrize(
        "wire_format,expected_lower,expected_upper",
        [
            (
                "iso",
                "2026-09-29T00:00:00+07:00",
                "2026-09-30T00:00:00+07:00",
            ),
            ("timestamp", "1790614800", "1790701200"),
            ("timestamp_ms", "1790614800000", "1790701200000"),
        ],
        ids=["iso", "timestamp", "timestamp-ms"],
    )
    def test_split_date_only_start_preserves_configured_walltime_timezone(
        self,
        wire_format: str,
        expected_lower: str,
        expected_upper: str,
    ) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            conn_cfg={
                "base_url": "https://api.example.com",
                "watermark_to_param_timezone": "+07:00",
            },
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": wire_format,
                        "response_column": None,
                        "watermark_value": "request_end",
                    }
                },
                "watermark_range_start": "2026-09-29",
                "watermark_range_interval_unit": "day",
                "watermark_range_interval_amount": 1,
                "watermark_range_max_workers": 1,
            },
            watermark_columns=["covered_at"],
        )

        with patch("datacoolie.sources.api_reader.datetime", _FrozenDateTime), patch(
            "datacoolie.sources.api_reader.httpx"
        ) as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"payload": "ok"}])

            result = reader.read(source)

        assert result == [{"payload": "ok"}]
        assert client.request.call_args.kwargs["params"] == {
            "covered_from": expected_lower,
            "covered_to": expected_upper,
        }
        assert reader.get_new_watermark() == {"covered_at": date(2026, 9, 30)}

    @pytest.mark.unit
    def test_request_end_kind_from_explicit_upper_uses_inclusive_continuation(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "covered_at",
                        "watermark_value": "request_end",
                    }
                }
            },
            watermark_columns=["covered_at"],
        )

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"covered_at": 20}])

            reader.read(source, watermark_end={"covered_at": 25})

        assert reader.get_runtime_info().watermark_start_operator == ">="
        assert client.request.call_args.kwargs["params"]["covered_to"] == 25

    @pytest.mark.unit
    def test_mixed_watermark_kinds_fail_before_http(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "observed_at": {
                        "lower": {"name": "observed_from"},
                        "upper": {"name": "observed_to"},
                        "watermark_value": "observed_max",
                    },
                    "covered_at": {
                        "lower": {"name": "covered_from"},
                        "upper": {"name": "covered_to"},
                        "watermark_value": "request_end",
                    },
                }
            },
            watermark_columns=["observed_at", "covered_at"],
        )
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__enter__.return_value = client
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception

            with pytest.raises(SourceError, match="cannot mix observed_max and request_end"):
                reader.read(
                    source,
                    watermark_start={"observed_at": 1, "covered_at": 1},
                )

        client.request.assert_not_called()

    @pytest.mark.unit
    def test_request_end_requires_upper_binding(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from"},
                        "watermark_value": "request_end",
                    }
                },
            },
            watermark_columns=["covered_at"],
        )
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception

            with pytest.raises(SourceError, match="requires an upper parameter binding"):
                reader.read(source, watermark_start={"covered_at": 1})

        client.request.assert_not_called()

    @pytest.mark.unit
    def test_unbounded_request_end_read_does_not_use_row_max_as_state(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "covered_at",
                        "watermark_value": "request_end",
                    }
                }
            },
            watermark_columns=["covered_at"],
        )
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"covered_at": 11}])

            reader.read(source)

        assert reader.get_new_watermark() == {}

    @pytest.mark.unit
    def test_empty_or_residual_filtered_request_end_does_not_advance(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "covered_at",
                        "watermark_value": "request_end",
                    }
                }
            },
            watermark_columns=["covered_at"],
        )
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(
                [{"covered_at": 2}, {"covered_at": 9}]
            )

            result = reader.read(
                source,
                watermark_start={"covered_at": 10},
                watermark_end={"covered_at": 20},
            )

        assert result is None
        assert reader.get_new_watermark() == {}

    @pytest.mark.unit
    def test_bounded_legacy_mapping_requires_new_binding(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "watermark_param_mapping": {"id": "from_id"},
                "watermark_to_param": "to_id",
            }
        )
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            with pytest.raises(SourceError, match="cannot use legacy"):
                reader.read(source, read_range=SourceReadRange("id", 1, 2))
        client.request.assert_not_called()

    @pytest.mark.unit
    def test_legacy_explicit_active_upper_is_not_replaced_by_now(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "watermark_param_mapping": {
                    "created_at": "created_from",
                    "updated_at": "updated_from",
                },
                "watermark_to_param": "created_to",
            },
            watermark_columns=["created_at", "updated_at"],
        )
        upper = datetime(2026, 9, 28, tzinfo=timezone.utc)
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response(
                [{"created_at": "2026-09-27T00:00:00+00:00", "updated_at": "2026-09-27T00:00:00+00:00"}]
            )

            reader.read(
                source,
                watermark_start={
                    "created_at": datetime(2026, 9, 26, tzinfo=timezone.utc),
                    "updated_at": datetime(2026, 9, 26, tzinfo=timezone.utc),
                },
                watermark_end={"created_at": upper},
            )

        assert client.request.call_args.kwargs["params"]["created_to"] == upper.isoformat()
        assert "updated_at" not in reader.get_new_watermark()

    @pytest.mark.unit
    def test_new_mapping_rejects_legacy_range_offset(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "id": {
                        "lower": {"name": "from_id"},
                        "upper": {"name": "to_id"},
                        "format": "integer",
                    }
                },
                "watermark_range_to_exclusive_offset": "1s",
            }
        )
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            with pytest.raises(SourceError, match="supported only on the legacy"):
                reader.read(source, read_range=SourceReadRange("id", 1, 2))
        client.request.assert_not_called()

    @pytest.mark.unit
    def test_legacy_range_split_retains_exclusive_offset(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "watermark_param_mapping": {"created_at": "created_from"},
                "watermark_to_param": "created_to",
                "watermark_range_interval_unit": "day",
                "watermark_range_interval_amount": 1,
                "watermark_range_to_exclusive_offset": "1s",
            }
        )
        start = datetime(2026, 9, 26, tzinfo=timezone.utc)
        end = datetime(2026, 9, 27, tzinfo=timezone.utc)
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([])

            reader.read(source, watermark_start={"created_at": start}, watermark_end={"created_at": end})

        assert client.request.call_args.kwargs["params"] == {
            "created_from": start.isoformat(),
            "created_to": "2026-09-26T23:59:59+00:00",
        }

    @pytest.mark.unit
    def test_new_mapping_range_split_uses_per_field_bindings(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "range_param_mapping": {
                    "created_at": {
                        "lower": {"name": "created_from", "operator": ">="},
                        "upper": {"name": "created_to", "operator": "<"},
                        "format": "iso",
                    }
                },
                "watermark_range_interval_unit": "day",
                "watermark_range_interval_amount": 1,
                "watermark_range_max_workers": 1,
            }
        )
        start = datetime(2026, 9, 26, tzinfo=timezone.utc)
        end = datetime(2026, 9, 28, tzinfo=timezone.utc)
        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.side_effect = [_mock_response([]), _mock_response([])]

            reader.read(source, watermark_start={"created_at": start}, watermark_end={"created_at": end})

        assert client.request.call_count == 2
        sent = [call.kwargs["params"] for call in client.request.call_args_list]
        assert sent == [
            {
                "created_from": "2026-09-26T00:00:00+00:00",
                "created_to": "2026-09-27T00:00:00+00:00",
            },
            {
                "created_from": "2026-09-27T00:00:00+00:00",
                "created_to": "2026-09-28T00:00:00+00:00",
            },
        ]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "next_link",
        [
            "https://evil.example.test/items",
            "https://api.example.test:8443/items",
            "http://api.example.test/items",
            "https://user:pass@api.example.test/items",
            "javascript:alert(1)",
            "https://[invalid/items",
        ],
    )
    def test_next_link_rejects_unsafe_url_forms(self, next_link: str) -> None:
        with pytest.raises(SourceError):
            resolve_same_origin_next_url(
                "https://api.example.test/items",
                next_link,
                ("https", "api.example.test", 443),
            )


class TestAPIReaderAdvancedRateLimiting:
    @pytest.mark.unit
    def test_retry_after_header_is_respected(self) -> None:
        """429 with Retry-After should sleep for the specified duration."""
        reader = APIReader(FakeEngine())
        source = _make_source(src_cfg={"endpoint": "/data"})

        first = _mock_response({}, status_code=429, headers={"Retry-After": "0.01"})
        second = _mock_response([{"id": 1}], status_code=200)

        with patch("datacoolie.sources.api_reader.httpx") as mock_httpx, \
             patch("datacoolie.sources._api.transport.time.sleep") as sleep_mock:
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

        with patch("datacoolie.sources._api.transport.httpx") as mock_httpx, \
             patch("datacoolie.sources._api.transport.time.sleep") as sleep_mock:
            mock_httpx.HTTPError = Exception
            client = MagicMock()
            client.request.side_effect = [response_429, response_ok]

            result = make_request(client, "GET", "https://api.example.com/data")

        assert result.status_code == 200
        # No Retry-After → exponential backoff: min(2**0, 30) = 1 second for first retry
        sleep_mock.assert_called_once_with(1)

    @pytest.mark.unit
    def test_exponential_backoff_grows_per_attempt(self) -> None:
        """Each successive 429 without Retry-After doubles the wait, capped at 30s."""
        # 4 x 429 then success — waits should be 1, 2, 4, 8
        responses = [_mock_response({}, status_code=429, headers={}) for _ in range(4)]
        responses.append(_mock_response([{"id": 1}], status_code=200))

        with patch("datacoolie.sources._api.transport.httpx") as mock_httpx, \
             patch("datacoolie.sources._api.transport.time.sleep") as sleep_mock:
            mock_httpx.HTTPError = Exception
            client = MagicMock()
            client.request.side_effect = responses

            result = make_request(client, "GET", "https://api.example.com/data")

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

        with patch("datacoolie.sources._api.transport.httpx") as mock_httpx, \
             patch("datacoolie.sources._api.transport.time.sleep") as sleep_mock:
            mock_httpx.HTTPError = Exception
            client = MagicMock()
            client.request.side_effect = responses

            make_request(client, "GET", "https://api.example.com/data")

        waits = [c.args[0] for c in sleep_mock.call_args_list]
        assert all(w <= 30 for w in waits), f"Some wait exceeded 30s: {waits}"
        assert waits[4] == 16  # attempt 4: min(2**4, 30) = 16
        assert waits[5] == 30  # attempt 5: min(2**5, 30) = 30 — first capped
        assert waits[6] == 30  # attempt 6: min(2**6, 30) = 30 — still capped

    @pytest.mark.unit
    def test_max_retries_exhausted_raises_source_error(self) -> None:
        """Persistent 429 beyond max_retries raises SourceError."""
        always_429 = _mock_response({}, status_code=429, headers={"Retry-After": "0"})

        with patch("datacoolie.sources._api.transport.httpx") as mock_httpx, \
             patch("datacoolie.sources._api.transport.time.sleep"):
            mock_httpx.HTTPError = Exception
            client = MagicMock()
            client.request.return_value = always_429

            with pytest.raises(SourceError, match="429 after 3 retries"):
                make_request(
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
             patch("datacoolie.sources._api.transport.time.sleep"):
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
        assert mock_httpx.Client.call_args.kwargs["follow_redirects"] is False

    @pytest.mark.unit
    def test_non_2xx_response_raises_source_error(self) -> None:
        """Non-success responses should be wrapped in SourceError with status context."""
        bad = _mock_response({"error": "nope"}, status_code=500)

        with pytest.raises(SourceError, match="HTTP 500"):
            make_request(MagicMock(request=MagicMock(return_value=bad)), "GET", "https://api.example.com/fail")


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
        assert extract_records(payload, data_path) == expected

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
        result = format_watermark_value(date(2024, 1, 15), "iso")
        assert result == "2024-01-15T00:00:00+00:00"

    def test_format_date_date_fmt(self) -> None:
        """date object with fmt='date' returns bare date string without time."""
        result = format_watermark_value(date(2024, 1, 15), "date")
        assert result == "2024-01-15"

    def test_format_date_timestamp(self) -> None:
        """date object with fmt='timestamp' returns Unix seconds — not the ISO string."""
        result = format_watermark_value(date(2024, 1, 15), "timestamp")
        expected = str(datetime(2024, 1, 15, tzinfo=timezone.utc).timestamp())
        assert result == expected
        # Must not be the date ISO string
        assert result != "2024-01-15"

    def test_format_date_timestamp_ms(self) -> None:
        """date object with fmt='timestamp_ms' returns Unix milliseconds."""
        result = format_watermark_value(date(2024, 1, 15), "timestamp_ms")
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
                "watermark_to_param": "modified_before",
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
                "watermark_to_param": "until",
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

    @pytest.mark.unit
    def test_api_request_and_pushed_checkpoint_share_one_upper_bound(self) -> None:
        reader = APIReader(FakeEngine())
        source = _make_source(
            src_cfg={
                "endpoint": "/items",
                "watermark_param_mapping": {"modified_at": "modified_since"},
                "watermark_to_param": "modified_before",
            },
            watermark_columns=["modified_at"],
        )
        bound = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)

        with patch("datacoolie.sources.api_reader.datetime") as clock, \
             patch("datacoolie.sources.api_reader.httpx") as mock_httpx:
            clock.now.return_value = bound
            client = MagicMock()
            mock_httpx.Client.return_value.__enter__ = MagicMock(return_value=client)
            mock_httpx.Client.return_value.__exit__ = MagicMock(return_value=False)
            mock_httpx.HTTPError = Exception
            client.request.return_value = _mock_response([{"id": 1}])

            reader.read(source)

        assert client.request.call_args.kwargs["params"]["modified_before"] == bound.isoformat()
        assert reader.get_new_watermark()["modified_at"] == bound
        clock.now.assert_called_once()


@pytest.mark.parametrize("amount", [0, -1, 1.5, True])
def test_watermark_ranges_reject_non_positive_or_non_integer_amount(amount) -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 2, 1, tzinfo=timezone.utc)

    with pytest.raises(SourceError, match="positive integer"):
        build_watermark_ranges(start, end, amount, "day")


def test_watermark_ranges_accept_positive_integer_string() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 3, tzinfo=timezone.utc)

    assert build_watermark_ranges(start, end, "1", "day") == [
        (start, datetime(2026, 1, 2, tzinfo=timezone.utc)),
        (datetime(2026, 1, 2, tzinfo=timezone.utc), end),
    ]


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

        result = fetch_offset_concurrent(client, "https://api.example.com/items", "GET", {}, {}, src_cfg)

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
            # The final page contains the 50 rows remaining in total=250.
            count = 50 if offset == 200 else 100
            records = [{"id": offset + i} for i in range(count)]
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

        result = fetch_offset_concurrent(
            client, "https://api.example.com/items", "GET", {}, {}, src_cfg
        )

        assert call_count == 3  # page 0 + page 1 + page 2
        assert sorted(offsets_seen) == [0, 100, 200]
        assert len(result) == 250

    @pytest.mark.unit
    def test_max_pages_cap_fails_before_fetching_incomplete_total(self) -> None:
        """A known total over the budget fails instead of returning a partial set."""
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

        with pytest.raises(SourceError, match="above max_pages=2"):
            fetch_offset_concurrent(
                client, "https://api.example.com/items", "GET", {}, {}, src_cfg
            )

        assert call_count == 1

    @pytest.mark.unit
    def test_empty_first_page_with_positive_total_fails(self) -> None:
        """An empty page contradicting a positive total and must fail."""
        client = MagicMock()
        client.request.return_value = _mock_response({"data": [], "total": 500})

        src_cfg = {
            "pagination_type": "offset",
            "data_path": "data",
            "total_path": "total",
            "page_size": 100,
        }

        with pytest.raises(SourceError, match="fetched .* but total_path=.* reports 500"):
            fetch_offset_concurrent(
                client, "https://api.example.com/items", "GET", {}, {}, src_cfg
            )

        assert client.request.call_count == 5

    @pytest.mark.unit
    def test_zero_total_with_empty_first_page_is_complete(self) -> None:
        client = MagicMock()
        client.request.return_value = _mock_response({"data": [], "total": 0})
        src_cfg = {
            "pagination_type": "offset",
            "data_path": "data",
            "total_path": "total",
            "page_size": 100,
        }

        assert fetch_offset_concurrent(
            client, "https://api.example.com/items", "GET", {}, {}, src_cfg
        ) == []
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
            fetch_offset_concurrent(
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
            fetch_offset_concurrent(
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
