"""Tests for FileReader including date folders and mtime watermarking."""

from __future__ import annotations

import re as re_mod
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from datacoolie.core.constants import Format
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Connection, Source
from datacoolie.sources.file_reader import FileReader, FileInfo, FileInfoColumn

from tests.unit.sources.support import (
    MockEngine,
    engine,
    file_source,
    file_source_csv,
    date_folder_source,
)


class TestFileReader:
    def test_read_parquet(self, engine: MockEngine, file_source: Source) -> None:
        engine.set_max_values({"event_time": "2024-06-01"})
        reader = FileReader(engine)
        result = reader.read(file_source)
        assert result is not None
        assert "__file_name" in result

    def test_read_csv(self, engine: MockEngine, file_source_csv: Source) -> None:
        reader = FileReader(engine)
        result = reader.read(file_source_csv)
        assert result is not None

    def test_read_no_path_raises(self, engine: MockEngine) -> None:
        conn = Connection(name="bad", connection_type="file", format="parquet", configure={})
        src = Source(connection=conn)
        reader = FileReader(engine)
        with pytest.raises(SourceError, match="requires source.path"):
            reader.read(src)

    def test_read_unsupported_format_raises(self, engine: MockEngine) -> None:
        conn = Connection(
            name="xml_conn",
            format="xml",
            configure={"base_path": "/data", "format": "xml"},
        )
        src = Source(connection=conn, table="test")
        reader = FileReader(engine)
        with pytest.raises(SourceError):
            reader.read(src)

    def test_read_zero_rows_returns_none(self, engine: MockEngine, file_source: Source) -> None:
        engine.set_data({}, 0)
        reader = FileReader(engine)
        result = reader.read(file_source)
        assert result is None

    def test_read_json(self, engine: MockEngine) -> None:
        conn = Connection(
            name="json_conn",
            connection_type="file",
            format=Format.JSON.value,
            configure={"base_path": "/data/raw"},
        )
        src = Source(connection=conn, schema_name="events", table="logs")
        reader = FileReader(engine)
        result = reader.read(src)
        assert result is not None

    def test_read_excel(self, engine: MockEngine) -> None:
        conn = Connection(
            name="excel_conn",
            connection_type="file",
            format=Format.EXCEL.value,
            configure={"base_path": "/data/raw"},
        )
        src = Source(connection=conn, schema_name="reports", table="sales")
        reader = FileReader(engine)
        result = reader.read(src)
        assert result is not None

    def test_hive_partitioning_sets_base_path_for_multi_paths(self, engine: MockEngine) -> None:
        conn = Connection(
            name="hive_conn",
            connection_type="file",
            format="parquet",
            configure={"base_path": "/data/raw", "use_hive_partitioning": True},
        )
        src = Source(connection=conn, schema_name="events", table="clicks")
        reader = FileReader(engine)
        engine.read = MagicMock(return_value=engine._data)
        reader._read_data(src, configure={"paths": ["/p1", "/p2"]})

        kwargs = engine.read.call_args.kwargs
        assert kwargs["options"]["use_hive_partitioning"] == "data/raw/events/clicks"

    def test_non_date_watermark_filter_is_applied(self, engine: MockEngine, file_source: Source) -> None:
        engine.set_max_values({"event_time": "2024-06-01"})
        reader = FileReader(engine)
        engine._filtered = False

        result = reader.read(file_source, watermark={"event_time": "2024-05-01"})
        assert result is not None
        assert engine._filtered is True


class TestFileReaderDateFolders:
    def test_build_date_regex(self) -> None:
        regex, groups = FileReader._build_date_regex("{year}/{month}/{day}")
        assert "year" in groups
        assert "month" in groups
        assert "day" in groups
        assert regex.startswith("^")
        assert regex.endswith("$")

    def test_build_date_regex_with_hour(self) -> None:
        regex, groups = FileReader._build_date_regex("{year}/{month}/{day}/{hour}")
        assert len(groups) == 4
        assert "hour" in groups

    def test_build_date_regex_keyed_format(self) -> None:
        regex, groups = FileReader._build_date_regex("year={year}/month={month}/day={day}")
        assert len(groups) == 3

    def test_date_groups_to_datetime(self) -> None:
        regex, groups = FileReader._build_date_regex("{year}/{month}/{day}")
        m = re_mod.match(regex, "2024/03/15")
        assert m is not None
        val = FileReader._date_groups_to_datetime(m, groups)
        assert val == datetime(2024, 3, 15, 0, 0, tzinfo=timezone.utc)

    def test_date_groups_to_datetime_with_hour(self) -> None:
        regex, groups = FileReader._build_date_regex("{year}/{month}/{day}/{hour}")
        m = re_mod.match(regex, "2024/03/15/08")
        assert m is not None
        val = FileReader._date_groups_to_datetime(m, groups)
        assert val == datetime(2024, 3, 15, 8, 0, tzinfo=timezone.utc)

    def test_get_max_date_folder_dt(self) -> None:
        paths = [
            "/data/events/raw/clicks/2024/01/01",
            "/data/events/raw/clicks/2024/03/15",
            "/data/events/raw/clicks/2024/02/10",
        ]
        max_dt = FileReader._get_max_date_folder_dt(paths, "{year}/{month}/{day}")
        assert max_dt == datetime(2024, 3, 15, 0, 0, tzinfo=timezone.utc)

    def test_get_max_date_folder_dt_no_match(self) -> None:
        paths = ["/data/events/no_match"]
        max_dt = FileReader._get_max_date_folder_dt(paths, "{year}/{month}/{day}")
        assert max_dt is None

    def test_read_with_date_folders_no_platform(
        self, engine: MockEngine, date_folder_source: Source
    ) -> None:
        """Without a platform, falls back to base path."""
        engine.set_max_values({"event_time": "2024-06-01"})
        reader = FileReader(engine)
        result = reader.read(date_folder_source)
        assert result is not None

    def test_read_with_date_folders_with_platform(
        self, engine: MockEngine, date_folder_source: Source
    ) -> None:
        """With a platform, discovers date folders via level-by-level listing."""
        engine.set_max_values({"event_time": "2024-06-01"})
        mock_platform = MagicMock()
        # Level-by-level listing: year → month → day
        mock_platform.list_folders.side_effect = [
            ["/data/events/raw/clicks/2024"],         # years
            ["/data/events/raw/clicks/2024/01",
             "/data/events/raw/clicks/2024/03"],      # months under 2024
            ["/data/events/raw/clicks/2024/01/01"],   # days under 2024/01
            ["/data/events/raw/clicks/2024/03/15"],   # days under 2024/03
        ]
        # list_files returns a file per leaf folder so _collect_file_infos
        # produces a non-empty result (non-Spark engines always collect).
        mock_platform.list_files.side_effect = [
            [FileInfo(name="a.parquet", path="/data/events/raw/clicks/2024/01/01/a.parquet",
                      modification_time=datetime(2024, 1, 1, tzinfo=timezone.utc), size=100)],
            [FileInfo(name="b.parquet", path="/data/events/raw/clicks/2024/03/15/b.parquet",
                      modification_time=datetime(2024, 3, 15, tzinfo=timezone.utc), size=200)],
        ]
        engine.set_platform(mock_platform)
        reader = FileReader(engine)
        result = reader.read(date_folder_source)
        assert result is not None

    def test_read_with_date_folders_empty(
        self, engine: MockEngine, date_folder_source: Source
    ) -> None:
        """With a platform returning no folders."""
        mock_platform = MagicMock()
        mock_platform.list_folders.return_value = []
        engine.set_platform(mock_platform)
        reader = FileReader(engine)
        result = reader.read(date_folder_source)
        assert result is None

    def test_get_date_folder_paths_without_platform_returns_base_path(self, engine: MockEngine) -> None:
        reader = FileReader(engine)
        paths = reader._get_date_folder_paths("/data/base", "{year}/{month}/{day}")
        assert paths == ["/data/base"]

    def test_get_date_folder_paths_invalid_pattern_returns_base_path(self, engine: MockEngine) -> None:
        reader = FileReader(engine)
        engine.set_platform(MagicMock())
        paths = reader._get_date_folder_paths("/data/base", "no_placeholders_here")
        assert paths == ["/data/base"]

    def test_get_date_folder_paths_handles_folder_list_errors(self, engine: MockEngine) -> None:
        platform = MagicMock()
        platform.list_folders.side_effect = RuntimeError("boom")
        engine.set_platform(platform)
        reader = FileReader(engine)
        paths = reader._get_date_folder_paths(
            "/data/base",
            "{year}/{month}",
            watermark_value=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        assert paths == []

    def test_get_date_folder_paths_prunes_old_values_on_boundary(self, engine: MockEngine) -> None:
        platform = MagicMock()
        platform.list_folders.side_effect = [
            ["/data/base/2023", "/data/base/2024"],
            ["/data/base/2024/01", "/data/base/2024/03"],
        ]
        engine.set_platform(platform)
        reader = FileReader(engine)

        paths = reader._get_date_folder_paths(
            "/data/base",
            "{year}/{month}",
            watermark_value=datetime(2024, 2, 1),
        )
        assert "/data/base/2024/03" in paths
        assert "/data/base/2024/01" not in paths

    def test_get_date_folder_paths_skips_non_matching_folder_names(self, engine: MockEngine) -> None:
        platform = MagicMock()
        platform.list_folders.side_effect = [
            ["/data/base/not-a-year", "/data/base/2024"],
            ["/data/base/2024/not-a-month"],
        ]
        engine.set_platform(platform)
        reader = FileReader(engine)

        paths = reader._get_date_folder_paths(
            "/data/base",
            "{year}/{month}",
            watermark_value=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        assert paths == []


# Helper functions for mtime tests
def _make_file_source(fmt: str = "parquet") -> Source:
    conn = Connection(
        name="mtime_conn",
        connection_type="file",
        format=fmt,
        configure={"base_path": "/data/events"},
    )
    return Source(
        connection=conn,
        schema_name="events",
        table="raw",
        watermark_columns=["__file_modification_time"],
    )


def _make_mock_platform(files: list) -> MagicMock:
    platform = MagicMock()
    platform.list_files.return_value = files
    return platform


class TestFileReaderMtimeFilter:
    """Tests for _collect_file_infos and mtime watermark filtering in FileReader."""

    @staticmethod
    def _engine_with_platform(platform: MagicMock) -> MockEngine:
        eng = MockEngine()
        eng.set_platform(platform)  # type: ignore[arg-type]
        return eng

    def test_read_filters_by_mtime_watermark(self) -> None:
        """Only files newer than the watermark are read; older ones are skipped."""
        files = [
            FileInfo(
                name="old.parquet",
                path="/data/events/old.parquet",
                modification_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                size=100,
            ),
            FileInfo(
                name="new1.parquet",
                path="/data/events/new1.parquet",
                modification_time=datetime(2024, 6, 15, tzinfo=timezone.utc),
                size=200,
            ),
            FileInfo(
                name="new2.parquet",
                path="/data/events/new2.parquet",
                modification_time=datetime(2024, 7, 1, tzinfo=timezone.utc),
                size=300,
            ),
        ]
        platform = _make_mock_platform(files)
        engine = self._engine_with_platform(platform)

        watermark = {FileInfoColumn.FILE_MODIFICATION_TIME: "2024-03-01T00:00:00+00:00"}
        reader = FileReader(engine)
        result = reader.read(_make_file_source(), watermark=watermark)

        assert result is not None
        platform.list_files.assert_called_once()

    def test_read_no_mtime_watermark_column_skips_filtering(self) -> None:
        """For non-Spark engines, list_files is always called (needed for add_file_info_columns).
        The watermark filter on __file_modification_time is NOT applied when the column
        is absent from watermark_columns — all found files are read."""
        files = [
            FileInfo(
                name="events.parquet",
                path="/data/events/events.parquet",
                modification_time=datetime(2024, 6, 1, tzinfo=timezone.utc),
                size=100,
            ),
        ]
        platform = _make_mock_platform(files)
        engine = self._engine_with_platform(platform)

        # Source has no mtime watermark column configured
        conn = Connection(
            name="no_mtime_conn",
            connection_type="file",
            format="parquet",
            configure={"base_path": "/data/events"},
        )
        source_no_mtime = Source(
            connection=conn,
            schema_name="events",
            table="raw",
            watermark_columns=["some_other_column"],
        )

        watermark = {"some_other_column": "2024-01-01"}
        reader = FileReader(engine)
        result = reader.read(source_no_mtime, watermark=watermark)

        assert result is not None
        # Non-Spark engines always call list_files to populate FileInfo metadata
        platform.list_files.assert_called_once()

    def test_read_mtime_filter_no_new_files(self) -> None:
        """When all files are at or before the watermark, read returns None."""
        files = [
            FileInfo(
                name="stale.parquet",
                path="/data/events/stale.parquet",
                modification_time=datetime(2023, 12, 1, tzinfo=timezone.utc),
                size=100,
            ),
        ]
        platform = _make_mock_platform(files)
        engine = self._engine_with_platform(platform)

        watermark = {FileInfoColumn.FILE_MODIFICATION_TIME: "2024-01-01T00:00:00+00:00"}
        reader = FileReader(engine)
        result = reader.read(_make_file_source(), watermark=watermark)

        assert result is None
        platform.list_files.assert_called_once()

    def test_collect_file_infos_handles_listing_errors(self) -> None:
        platform = MagicMock()
        platform.list_files.side_effect = RuntimeError("cannot list")
        engine = self._engine_with_platform(platform)
        reader = FileReader(engine)

        infos = reader._collect_file_infos(["/x"], "parquet", None)
        assert infos == []

    def test_collect_file_infos_skips_dirs_and_missing_mtime(self) -> None:
        platform = MagicMock()
        platform.list_files.return_value = [
            FileInfo(name="d", path="/d", modification_time=None, is_dir=True),
            FileInfo(name="n", path="/n", is_dir=False, modification_time=None),
        ]
        engine = self._engine_with_platform(platform)
        reader = FileReader(engine)

        infos = reader._collect_file_infos(["/x"], "parquet", None)
        assert infos == []

    def test_parse_date_watermark_naive_datetime_gets_utc(self) -> None:
        dt = FileReader._parse_date_watermark(datetime(2024, 1, 1, 12, 0, 0))
        assert dt is not None
        assert dt.tzinfo == timezone.utc

    def test_parse_date_watermark_unknown_type_returns_none(self) -> None:
        assert FileReader._parse_date_watermark(12345) is None

    def test_parse_date_levels_ignores_non_placeholder_segments(self) -> None:
        levels = FileReader._parse_date_levels("prefix/{year}/x={month}")
        assert levels[0][0] == "year"
        assert levels[1][0] == "month"

    def test_get_max_date_folder_dt_ignores_short_paths(self) -> None:
        paths = ["/short"]  
        max_dt = FileReader._get_max_date_folder_dt(paths, "{year}/{month}/{day}")
        assert max_dt is None
