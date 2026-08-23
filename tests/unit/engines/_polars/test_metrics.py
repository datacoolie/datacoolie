from datetime import datetime, timezone

import polars as pl

from datacoolie.engines._polars import metrics


def test_schema_and_row_metrics() -> None:
    frame = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}).lazy()

    assert metrics.count_rows(frame) == 2
    assert not metrics.is_empty(frame)
    assert metrics.get_columns(frame) == ["id", "name"]
    assert metrics.get_schema(frame) == {"id": "Int64", "name": "String"}
    assert metrics.get_hive_schema(frame) == {"id": "BIGINT", "name": "STRING"}


def test_combined_metric_preserves_utc_timezone() -> None:
    instant = datetime(2026, 8, 20, 10, 11, tzinfo=timezone.utc)
    frame = pl.DataFrame({"id": [1, 2], "instant": [instant, instant]}).lazy()

    count, values = metrics.get_count_and_max_values(frame, ["id", "instant"])

    assert count == 2
    assert values == {"id": 2, "instant": instant}
