"""Focused tests for stateless native Polars transformations."""

import polars as pl

from datacoolie.engines._polars.transforms import (
    add_column,
    apply_watermark_filter,
    deduplicate_by_rank,
    drop_columns,
    filter_rows,
    rename_columns,
    select_columns,
)


def test_watermark_filter_keeps_lazy_execution() -> None:
    frame = pl.DataFrame({"id": [1, 2, 3]}).lazy()
    result = apply_watermark_filter(
        frame, [("id", 1, 3)], start_operator=">", end_operator="<="
    )
    assert isinstance(result, pl.LazyFrame)
    assert result.collect()["id"].to_list() == [2, 3]


def test_rank_deduplication_keeps_ties() -> None:
    frame = pl.DataFrame({"key": ["a", "a", "a"], "rank": [1, 2, 2]}).lazy()
    result = deduplicate_by_rank(frame, ["key"], ["rank"], "desc").collect()
    assert result["rank"].to_list() == [2, 2]


def test_basic_column_operations_remain_lazy() -> None:
    frame = pl.DataFrame({"id": [1, 2], "drop_me": [3, 4]}).lazy()
    result = add_column(frame, "double_id", "id * 2")
    result = drop_columns(result, ["drop_me"])
    result = rename_columns(result, {"double_id": "value"})
    result = select_columns(result, ["id", "value"])
    result = filter_rows(result, "value > 2")

    assert isinstance(result, pl.LazyFrame)
    assert result.collect().to_dicts() == [{"id": 2, "value": 4}]
