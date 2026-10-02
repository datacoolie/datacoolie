"""Canonical API range encoding must preserve, or reject, bound precision."""

from datetime import date, datetime, timezone

from unittest.mock import MagicMock, patch

import httpx
import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.sources._api.ranges import format_bound
from datacoolie.sources._api.watermark import format_watermark_value
from datacoolie.sources.api_reader import APIReader
from datacoolie.sources import SourceReadRange
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("value,wire_format,expected", [
    (2**100 + 1, "integer", 2**100 + 1),
    (date(2026, 1, 1), "date", "2026-01-01"),
    (datetime(2026, 1, 1, 12, 30), "datetime", "2026-01-01T12:30:00"),
    (datetime(2026, 1, 1, 12, 30, 0, 123000), "datetime_ms", "2026-01-01T12:30:00.123"),
    (datetime(2026, 1, 1, 12, 30, 0, 123456, tzinfo=timezone.utc), "iso",
     "2026-01-01T12:30:00.123456+00:00"),
], ids=["large-int", "date", "aligned-seconds", "aligned-ms", "iso-microseconds"])
def test_api_bound_encoding_preserves_supported_precision(value, wire_format, expected):
    assert format_bound(value, wire_format) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
            "-0.000001",
        ),
        (
            datetime(9999, 12, 31, 23, 59, 59, 123456, tzinfo=timezone.utc),
            "253402300799.123456",
        ),
    ],
    ids=["negative-fraction", "far-date-fraction"],
)
def test_timestamp_encoding_uses_exact_integer_epoch_arithmetic(value, expected):
    assert format_bound(value, "timestamp") == expected


@pytest.mark.parametrize(
    "value,wire_format",
    [
        (datetime(2026, 1, 1, 0, 0, 1), "date"),
        (datetime(2026, 1, 1, 0, 0, 0, 1), "datetime"),
        (datetime(2026, 1, 1, 0, 0, 0, 1001), "datetime_ms"),
        (datetime(2026, 1, 1, 0, 0, 0, 1001), "timestamp_ms"),
    ],
    ids=["date-non-midnight", "datetime-subsecond", "datetime-ms-sub-ms", "timestamp-ms-sub-ms"],
)
def test_direct_new_bound_encoding_rejects_lossy_precision(value, wire_format):
    with pytest.raises(SourceError, match="precision|loss"):
        format_bound(value, wire_format)


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            "2026-01-01T00:00:00.1234560+00:00",
            "2026-01-01T00:00:00.123456+00:00",
        ),
        ("2026-01-01T00:00:00.1230001+00:00", None),
        ("20260101T000000.1230001+0000", None),
        ("2026-01-01T00:00:00+07:00:00.1230001", None),
        ("2026-01-01T12.1230001", None),
        ("2026-01-01T12:30.1230001", None),
        ("2026-W01-4T12:30:00.1230001", None),
        ("2026W014T123000.1230001", None),
        ("2026-01-01T12:00:00+07.1230001", None),
        ("2026-01-01T12:00:00+07:30.1230001", None),
        ("2026-01-01T12:00:00+00:00:00.123456", None),
    ],
    ids=[
        "trailing-zero-is-lossless",
        "nonzero-submicrosecond-is-lossy",
        "basic-iso-is-lossy",
        "offset-fraction-is-lossy",
        "reduced-hour-is-lossy",
        "reduced-minute-is-lossy",
        "week-date-is-lossy",
        "basic-week-date-is-lossy",
        "reduced-offset-hour-is-lossy",
        "reduced-offset-minute-is-lossy",
        "zero-offset-fraction-is-lossy",
    ],
)
def test_iso_bound_precision_is_checked_before_datetime_parsing(value, expected):
    if expected is None:
        with pytest.raises(SourceError, match="microsecond|loss"):
            format_bound(value, "iso")
    else:
        assert format_bound(value, "iso") == expected


def test_legacy_watermark_formatter_keeps_existing_truncation_behavior():
    value = datetime(2026, 1, 1, 12, 30, 0, 123456, tzinfo=timezone.utc)

    assert format_watermark_value(value, "datetime") == "2026-01-01T12:30:00"
    assert format_watermark_value(value, "datetime_ms") == "2026-01-01T12:30:00.123"


@pytest.mark.parametrize("wire_format", ["date", "datetime", "datetime_ms", "timestamp_ms"])
def test_api_range_compiler_rejects_lossy_precision_before_http(wire_format):
    # The new binding compiler owns this rejection. The legacy formatter may
    # retain its established rounding behavior for legacy incremental routes.
    source = Source(
        connection=Connection(name="precision-api", connection_type="api", format="api",
                              configure={"base_url": "https://api.example.com"}),
        watermark_columns=["updated_at"],
        configure={"endpoint": "/items", "range_param_mapping": {
            "updated_at": {"lower": {"name": "since", "operator": ">="},
                           "upper": {"name": "until", "operator": "<"},
                           "format": wire_format, "response_column": "updated_at",
                           "watermark_value": "observed_max"},
        }},
    )
    engine = MagicMock()
    engine.create_dataframe.side_effect = lambda records: records
    engine.count_rows.side_effect = len
    client = MagicMock()
    client.request.return_value = httpx.Response(
        200, json=[], request=httpx.Request("GET", "https://api.example.com/items"),
    )
    with patch("datacoolie.sources.api_reader.httpx.Client") as client_factory:
        client_factory.return_value.__enter__.return_value = client
        with pytest.raises(SourceError, match="precision|loss|represent"):
            APIReader(engine).read(source, read_range=SourceReadRange(
                "updated_at", datetime(2026, 1, 1, 12, 30),
                datetime(2026, 1, 1, 12, 30, 0, 123456),
            ))
        client.request.assert_not_called()


@pytest.mark.parametrize("side", ["lower", "upper"])
def test_api_range_compiler_reports_lossy_bound_side_before_http(side):
    source = Source(
        connection=Connection(
            name="precision-api",
            connection_type="api",
            format="api",
            configure={"base_url": "https://api.example.com"},
        ),
        watermark_columns=["updated_at"],
        configure={
            "endpoint": "/items",
            "range_param_mapping": {
                "updated_at": {
                    "lower": {"name": "since", "operator": ">="},
                    "upper": {"name": "until", "operator": "<"},
                    "format": "datetime_ms",
                    "response_column": "updated_at",
                    "watermark_value": "observed_max",
                },
            },
        },
    )
    aligned = datetime(2026, 1, 1, 12, 30, 0, 124000)
    lossy = datetime(2026, 1, 1, 12, 30, 0, 123001)
    start, end = (
        (lossy, aligned)
        if side == "lower"
        else (datetime(2026, 1, 1, 12, 30, 0, 123000), lossy)
    )

    engine = MagicMock()
    engine.create_dataframe.side_effect = lambda records: records
    engine.count_rows.side_effect = len
    client = MagicMock()
    client.request.return_value = httpx.Response(
        200,
        json=[],
        request=httpx.Request("GET", "https://api.example.com/items"),
    )
    with patch("datacoolie.sources.api_reader.httpx.Client") as client_factory:
        client_factory.return_value.__enter__.return_value = client
        with pytest.raises(SourceError) as error:
            APIReader(engine).read(
                source,
                read_range=SourceReadRange("updated_at", start, end),
            )

    assert error.value.details == {
        "field": "updated_at",
        "side": side,
        "format": "datetime_ms",
    }
    client.request.assert_not_called()


@pytest.mark.parametrize("side", ["lower", "upper"])
def test_api_bounded_iso_fraction_precision_rejects_before_http(side):
    source = Source(
        connection=Connection(
            name="precision-api",
            connection_type="api",
            format="api",
            configure={"base_url": "https://api.example.com"},
        ),
        watermark_columns=["updated_at"],
        configure={
            "endpoint": "/items",
            "range_param_mapping": {
                "updated_at": {
                    "lower": {"name": "since", "operator": ">="},
                    "upper": {"name": "until", "operator": "<"},
                    "format": "iso",
                    "response_column": "updated_at",
                    "watermark_value": "observed_max",
                },
            },
        },
    )
    if side == "lower":
        start = "2026-01-01T00:00:00.1230001+00:00"
        end = "2026-01-01T00:00:00.1240000+00:00"
    else:
        start = "2026-01-01T00:00:00.1230000+00:00"
        end = "2026-01-01T00:00:00.1240001+00:00"

    engine = MagicMock()
    engine.create_dataframe.side_effect = lambda records: records
    engine.count_rows.side_effect = len
    with patch("datacoolie.sources.api_reader.httpx.Client") as client_factory:
        with pytest.raises(SourceError, match="microsecond|loss"):
            APIReader(engine).read(
                source,
                read_range=SourceReadRange("updated_at", start, end),
            )

    client_factory.assert_not_called()
