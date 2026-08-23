"""Focused tests for Polars flat-file helpers."""

from datetime import datetime, timezone
from unittest.mock import patch

import polars as pl

from datacoolie.engines._polars.file_io import add_file_info_columns, make_file_name
from datacoolie.platforms.base import FileInfo


def test_overwrite_name_ignores_partition_suffixes() -> None:
    assert make_file_name("root/table/year=2026/04", "parquet", True) == "table.parquet"


def test_append_name_contains_utc_timestamp() -> None:
    with patch("datacoolie.engines._polars.file_io.datetime") as clock:
        clock.now.return_value.strftime.return_value = "20260820_101112"
        assert make_file_name("root/table", "csv", False) == "table_20260820_101112.csv"


def test_file_info_columns_join_metadata_by_normalized_path() -> None:
    modified = datetime(2026, 8, 20, tzinfo=timezone.utc)
    frame = pl.DataFrame({"__file_path": ["root/orders.parquet"]}).lazy()
    infos = [
        FileInfo(
            name="orders.parquet",
            path="root\\orders.parquet",
            modification_time=modified,
        )
    ]

    result = add_file_info_columns(frame, infos).collect().to_dicts()

    assert result == [
        {
            "__file_path": "root/orders.parquet",
            "__file_name": "orders.parquet",
            "__file_modification_time": modified,
        }
    ]
