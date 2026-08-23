from datetime import datetime

from datacoolie.engines._spark import temporal


def test_window_predicate_uses_spark_identifier_quoting() -> None:
    assert temporal.build_window_predicate({"ts": (1, 2)}) == (
        "`ts` > '1' AND `ts` <= '2'"
    )


def test_align_ms_boundaries_truncates_start_and_ceils_end() -> None:
    start, end = temporal.align_ms_boundaries(
        datetime(2026, 8, 20, 1, 2, 3, 123456),
        datetime(2026, 8, 20, 1, 2, 3, 999999),
    )
    assert start == datetime(2026, 8, 20, 1, 2, 3, 123000)
    assert end == datetime(2026, 8, 20, 1, 2, 4)
