from datetime import date, datetime

from datacoolie.engines._polars import temporal


def test_iso_and_window_predicate_support_iceberg_quoting() -> None:
    assert temporal.to_iso8601(date(2026, 8, 20)) == "2026-08-20"
    assert temporal.to_iso8601("2026-08-20 10:11:12") == "2026-08-20T10:11:12"
    assert temporal.build_window_predicate({"ts": (1, 2)}, quote_char="") == (
        "ts > '1' AND ts <= '2'"
    )


def test_align_ms_boundaries_truncates_start_and_ceils_end() -> None:
    start, end = temporal.align_ms_boundaries(
        datetime(2026, 8, 20, 1, 2, 3, 123456),
        datetime(2026, 8, 20, 1, 2, 3, 999999),
    )
    assert start == datetime(2026, 8, 20, 1, 2, 3, 123000)
    assert end == datetime(2026, 8, 20, 1, 2, 4)
