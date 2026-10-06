"""Tests for FileReader including date folders and mtime watermarking."""

from __future__ import annotations

import re as re_mod
from datetime import datetime, timezone
from unittest.mock import MagicMock

import polars as pl
import pytest

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY, Format
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.sources.file_reader import FileReader, FileInfo, FileInfoColumn
from datacoolie.sources.contracts import SourceReadRange

from tests.unit.sources.support import (
    MockEngine,
    engine,
    file_source,
    file_source_csv,
    date_folder_source,
    read_request,
)


class TestFileReader:
    def test_read_parquet(self, engine: MockEngine, file_source: Source) -> None:
        engine.set_max_values({"event_time": "2024-06-01"})
        reader = FileReader(engine)
        result = reader.read(file_source, request=read_request())
        assert result is not None
        assert "__file_name" in result.frame

    def test_read_csv(self, engine: MockEngine, file_source_csv: Source) -> None:
        reader = FileReader(engine)
        result = reader.read(file_source_csv, request=read_request())
        assert result.frame is not None
        assert result.rows_read == 3
        assert result.frame["id"] == [1, 2, 3]
        assert result.frame["value"] == [10, 20, 30]

    def test_read_existing_flat_output_replaces_historical_file_info(
        self, tmp_path
    ) -> None:
        source_dir = tmp_path / "roundtrip"
        source_dir.mkdir()
        source_file = source_dir / "output.csv"
        pl.DataFrame(
            {
                "id": [1, 2],
                "payload": ["a", "b"],
                "__file_path": ["historical/a", "historical/b"],
                "__file_name": ["old-a", "old-b"],
                "__file_modification_time": ["2020-01-01", "2020-01-02"],
                "__created_at": ["created-a", "created-b"],
            }
        ).write_csv(source_file)

        platform = LocalPlatform()
        source = Source(
            connection=Connection(
                name="roundtrip_csv",
                connection_type="file",
                format=Format.CSV.value,
                configure={"base_path": str(tmp_path)},
            ),
            table="roundtrip",
        )
        result = FileReader(PolarsEngine(platform=platform)).read(
            source,
            request=read_request(),
        )

        assert result.frame is not None
        frame = result.frame.collect()
        assert frame.select("id", "payload", "__created_at").to_dicts() == [
            {"id": "1", "payload": "a", "__created_at": "created-a"},
            {"id": "2", "payload": "b", "__created_at": "created-b"},
        ]
        file_info = platform.list_files(str(source_dir), extension=".csv")[0]
        assert frame["__file_path"].to_list() == [file_info.path, file_info.path]
        assert frame["__file_name"].to_list() == [file_info.name, file_info.name]
        assert frame["__file_modification_time"].to_list() == [
            file_info.modification_time,
            file_info.modification_time,
        ]
        assert "__file_path_right" not in frame.columns

    def test_read_no_path_raises(self, engine: MockEngine) -> None:
        conn = Connection(name="bad", connection_type="file", format="parquet", configure={})
        src = Source(connection=conn)
        reader = FileReader(engine)
        with pytest.raises(SourceError, match="requires source.path"):
            reader.read(src, request=read_request())

    def test_read_unsupported_format_raises(self, engine: MockEngine) -> None:
        conn = Connection(
            name="xml_conn",
            format="xml",
            configure={"base_path": "/data", "format": "xml"},
        )
        src = Source(connection=conn, table="test")
        reader = FileReader(engine)
        with pytest.raises(SourceError):
            reader.read(src, request=read_request())

    def test_read_zero_rows_returns_none(self, engine: MockEngine, file_source: Source) -> None:
        engine.set_data({}, 0)
        reader = FileReader(engine)
        result = reader.read(file_source, request=read_request())
        assert result.frame is None

    def test_read_json(self, engine: MockEngine) -> None:
        conn = Connection(
            name="json_conn",
            connection_type="file",
            format=Format.JSON.value,
            configure={"base_path": "/data/raw"},
        )
        src = Source(connection=conn, schema_name="events", table="logs")
        reader = FileReader(engine)
        result = reader.read(src, request=read_request())
        assert result.frame is not None
        assert result.rows_read == 3
        assert result.frame["id"] == [1, 2, 3]
        assert result.frame["value"] == [10, 20, 30]

    def test_read_excel(self, engine: MockEngine) -> None:
        conn = Connection(
            name="excel_conn",
            connection_type="file",
            format=Format.EXCEL.value,
            configure={"base_path": "/data/raw"},
        )
        src = Source(connection=conn, schema_name="reports", table="sales")
        reader = FileReader(engine)
        result = reader.read(src, request=read_request())
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
        assert kwargs["options"]["use_hive_partitioning"] == "/data/raw/events/clicks"

    def test_non_date_watermark_filter_is_applied(self, engine: MockEngine, file_source: Source) -> None:
        engine.set_max_values({"event_time": "2024-06-01"})
        reader = FileReader(engine)
        engine._filtered = False

        result = reader.read(
            file_source,
            request=read_request(watermark_start={"event_time": "2024-05-01"}),
        )
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
        result = reader.read(date_folder_source, request=read_request())
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
        result = reader.read(date_folder_source, request=read_request())
        assert result is not None

    def test_read_with_date_folders_empty(
        self, engine: MockEngine, date_folder_source: Source
    ) -> None:
        """With a platform returning no folders."""
        mock_platform = MagicMock()
        mock_platform.list_folders.return_value = []
        engine.set_platform(mock_platform)
        reader = FileReader(engine)
        result = reader.read(date_folder_source, request=read_request())
        assert result.frame is None

    def test_read_with_date_folders_empty_does_not_report_old_state_as_observed(
        self, engine: MockEngine, date_folder_source: Source
    ) -> None:
        mock_platform = MagicMock()
        mock_platform.list_folders.return_value = []
        engine.set_platform(mock_platform)
        reader = FileReader(engine)
        previous = {DATE_FOLDER_PARTITION_KEY: "2024-03-15T00:00:00+00:00"}

        result = reader.read(
            date_folder_source,
            request=read_request(watermark_start=previous),
        )
        assert result.frame is None
        assert result.candidate is None

    def test_row_range_does_not_advance_internal_date_folder_key(
        self, engine: MockEngine, date_folder_source: Source
    ) -> None:
        """A row-column replay does not turn discovery scope into state."""
        platform = MagicMock()
        platform.list_folders.side_effect = [
            ["/data/events/2024"],
            ["/data/events/2024/03"],
            ["/data/events/2024/03/15"],
        ]
        platform.list_files.return_value = [
            FileInfo(
                name="orders.parquet",
                path="/data/events/2024/03/15/orders.parquet",
                modification_time=datetime(2024, 3, 15, tzinfo=timezone.utc),
                size=1,
            )
        ]
        engine.set_platform(platform)
        engine.set_max_values({"event_time": "2024-03-15"})

        reader = FileReader(engine)
        result = reader.read(
            date_folder_source,
            request=read_request(
                read_range=SourceReadRange(
                    "event_time", "2024-03-01", "2024-04-01"
                )
            ),
        )

        assert result is not None
        assert result.candidate is not None
        assert DATE_FOLDER_PARTITION_KEY not in result.candidate.values

    def test_get_date_folder_paths_without_platform_returns_base_path(self, engine: MockEngine) -> None:
        reader = FileReader(engine)
        paths = reader._get_date_folder_paths("/data/base", "{year}/{month}/{day}")
        assert paths == ["/data/base"]

    def test_get_date_folder_paths_invalid_pattern_returns_base_path(self, engine: MockEngine) -> None:
        reader = FileReader(engine)
        engine.set_platform(MagicMock())
        paths = reader._get_date_folder_paths("/data/base", "no_placeholders_here")
        assert paths == ["/data/base"]

    def test_get_date_folder_paths_propagates_folder_list_errors(self, engine: MockEngine) -> None:
        platform = MagicMock()
        platform.list_folders.side_effect = RuntimeError("boom")
        engine.set_platform(platform)
        reader = FileReader(engine)
        with pytest.raises(RuntimeError, match="boom"):
            reader._get_date_folder_paths(
                "/data/base",
                "{year}/{month}",
                watermark_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )

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
            watermark_start=datetime(2024, 2, 1),
        )
        assert "/data/base/2024/03" in paths
        assert "/data/base/2024/01" not in paths

    def test_get_date_folder_paths_keeps_each_inclusive_boundary_and_interior(
        self, engine: MockEngine
    ) -> None:
        platform = MagicMock()
        platform.list_folders.side_effect = [
            ["/data/base/2023", "/data/base/2024", "/data/base/2025"],
            ["/data/base/2024/01", "/data/base/2024/02"],
            [
                "/data/base/2024/01/14",
                "/data/base/2024/01/15",
                "/data/base/2024/01/16",
                "/data/base/2024/01/17",
                "/data/base/2024/01/18",
            ],
        ]
        engine.set_platform(platform)
        reader = FileReader(engine)

        paths = reader._get_date_folder_paths(
            "/data/base",
            "{year}/{month}/{day}",
            watermark_start=datetime(2024, 1, 15, tzinfo=timezone.utc),
            watermark_end=datetime(2024, 1, 17, tzinfo=timezone.utc),
        )

        assert paths == [
            "/data/base/2024/01/15",
            "/data/base/2024/01/16",
            "/data/base/2024/01/17",
        ]

    def test_get_date_folder_paths_returns_empty_when_matching_folders_are_outside_bounds(
        self, engine: MockEngine
    ) -> None:
        platform = MagicMock()
        platform.list_folders.side_effect = [
            ["/data/base/2024"],
            ["/data/base/2024/01"],
            ["/data/base/2024/01/14", "/data/base/2024/01/18"],
        ]
        engine.set_platform(platform)
        reader = FileReader(engine)

        paths = reader._get_date_folder_paths(
            "/data/base",
            "{year}/{month}/{day}",
            watermark_start=datetime(2024, 1, 15, tzinfo=timezone.utc),
            watermark_end=datetime(2024, 1, 17, tzinfo=timezone.utc),
        )

        assert paths == []

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
            watermark_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
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
        result = reader.read(
            _make_file_source(),
            request=read_request(watermark_start=watermark),
        )

        assert result is not None
        platform.list_files.assert_called_once()

    def test_bounded_mtime_range_is_independent_from_watermark_columns(self) -> None:
        files = [
            FileInfo(
                name="before.parquet",
                path="/data/events/before.parquet",
                modification_time=datetime(2023, 12, 31, tzinfo=timezone.utc),
                size=100,
            ),
            FileInfo(
                name="lower.parquet",
                path="/data/events/lower.parquet",
                modification_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                size=150,
            ),
            FileInfo(
                name="selected.parquet",
                path="/data/events/selected.parquet",
                modification_time=datetime(2024, 1, 15, tzinfo=timezone.utc),
                size=200,
            ),
            FileInfo(
                name="after.parquet",
                path="/data/events/after.parquet",
                modification_time=datetime(2024, 2, 1, tzinfo=timezone.utc),
                size=300,
            ),
        ]
        platform = _make_mock_platform(files)
        engine = self._engine_with_platform(platform)
        engine.read = MagicMock(return_value=engine._data)
        source = _make_file_source()
        source.watermark_columns = ["event_time"]

        reader = FileReader(engine)
        result = reader.read(
            source,
            request=read_request(
                read_range=SourceReadRange(
                    FileInfoColumn.FILE_MODIFICATION_TIME,
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 2, 1, tzinfo=timezone.utc),
                )
            ),
        )

        assert result is not None
        assert engine.read.call_args.kwargs["path"] == [
            "/data/events/lower.parquet", "/data/events/selected.parquet"
        ]
        assert platform.list_files.call_args.kwargs["extension"] == ".parquet"
        assert (
            result.candidate is None
            or FileInfoColumn.FILE_MODIFICATION_TIME not in result.candidate.values
        )

    def test_bounded_mtime_range_returns_empty_when_all_files_are_outside_bounds(self) -> None:
        files = [
            FileInfo(
                name="before.parquet",
                path="/data/events/before.parquet",
                modification_time=datetime(2023, 12, 31, tzinfo=timezone.utc),
                size=100,
            ),
            FileInfo(
                name="after.parquet",
                path="/data/events/after.parquet",
                modification_time=datetime(2024, 2, 1, tzinfo=timezone.utc),
                size=300,
            ),
        ]
        platform = _make_mock_platform(files)
        engine = self._engine_with_platform(platform)
        engine.read = MagicMock(return_value=engine._data)

        result = FileReader(engine).read(
            _make_file_source(),
            request=read_request(
                read_range=SourceReadRange(
                    FileInfoColumn.FILE_MODIFICATION_TIME,
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 2, 1, tzinfo=timezone.utc),
                )
            ),
        )

        assert result.frame is None
        assert result.rows_read == 0
        engine.read.assert_not_called()

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
        result = reader.read(
            source_no_mtime,
            request=read_request(watermark_start=watermark),
        )

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
        result = reader.read(
            _make_file_source(),
            request=read_request(watermark_start=watermark),
        )

        assert result.frame is None
        platform.list_files.assert_called_once()

    def test_collect_file_infos_propagates_listing_errors(self) -> None:
        platform = MagicMock()
        platform.list_files.side_effect = RuntimeError("cannot list")
        engine = self._engine_with_platform(platform)
        reader = FileReader(engine)

        with pytest.raises(RuntimeError, match="cannot list"):
            reader._collect_file_infos(["/x"], "parquet", None)

    def test_read_fails_when_discovered_file_cannot_be_read(self) -> None:
        platform = _make_mock_platform([
            FileInfo(
                name="gone.parquet",
                path="/data/events/gone.parquet",
                modification_time=datetime(2024, 6, 1, tzinfo=timezone.utc),
                size=100,
            ),
        ])
        engine = self._engine_with_platform(platform)
        engine.read = MagicMock(side_effect=RuntimeError("file disappeared after listing"))
        reader = FileReader(engine)

        with pytest.raises(SourceError, match="file disappeared after listing") as raised:
            reader.read(_make_file_source(), request=read_request())

        assert raised.value.runtime_info is not None
        assert raised.value.runtime_info.status == "failed"

    def test_read_fails_before_engine_read_when_folder_discovery_is_incomplete(
        self, engine: MockEngine, date_folder_source: Source
    ) -> None:
        platform = MagicMock()
        platform.list_folders.side_effect = [
            ["/data/events/raw/clicks/2024"],
            RuntimeError("month listing failed"),
        ]
        engine.set_platform(platform)
        engine.read = MagicMock()
        reader = FileReader(engine)

        with pytest.raises(SourceError, match="month listing failed"):
            reader.read(date_folder_source, request=read_request())

        engine.read.assert_not_called()

    def test_collect_file_infos_retains_file_with_missing_mtime_without_watermark(self) -> None:
        platform = MagicMock()
        platform.list_files.return_value = [
            FileInfo(name="d", path="/d", modification_time=None, is_dir=True),
            FileInfo(name="n", path="/n", is_dir=False, modification_time=None),
        ]
        engine = self._engine_with_platform(platform)
        reader = FileReader(engine)

        infos = reader._collect_file_infos(["/x"], "parquet", None)
        assert [info.path for info in infos] == ["/n"]

    def test_collect_file_infos_fails_when_mtime_watermark_lacks_metadata(self) -> None:
        platform = MagicMock()
        platform.list_files.return_value = [
            FileInfo(name="n", path="/n", is_dir=False, modification_time=None),
        ]
        engine = self._engine_with_platform(platform)
        reader = FileReader(engine)

        with pytest.raises(SourceError, match="modification time is required"):
            reader._collect_file_infos(
                ["/x"],
                "parquet",
                None,
                require_modification_time=True,
            )

    def test_read_without_mtime_watermark_keeps_file_missing_metadata(self) -> None:
        files = [
            FileInfo(
                name="unknown.parquet",
                path="/data/events/unknown.parquet",
                modification_time=None,
            ),
            FileInfo(
                name="known.parquet",
                path="/data/events/known.parquet",
                modification_time=datetime(2024, 6, 1, tzinfo=timezone.utc),
            ),
        ]
        platform = _make_mock_platform(files)
        engine = self._engine_with_platform(platform)
        engine.read = MagicMock(return_value=engine._data)
        source = Source(
            connection=Connection(
                name="no_mtime_conn",
                connection_type="file",
                format="parquet",
                configure={"base_path": "/data/events"},
            ),
            schema_name="events",
            table="raw",
            watermark_columns=["event_time"],
        )

        reader = FileReader(engine)
        assert reader.read(source, request=read_request()) is not None
        assert engine.read.call_args.kwargs["path"] == [
            "/data/events/known.parquet",
            "/data/events/unknown.parquet",
        ]

    def test_read_with_mtime_watermark_fails_before_engine_read_for_missing_metadata(self) -> None:
        files = [
            FileInfo(
                name="unknown.parquet",
                path="/data/events/unknown.parquet",
                modification_time=None,
            ),
            FileInfo(
                name="known.parquet",
                path="/data/events/known.parquet",
                modification_time=datetime(2024, 6, 1, tzinfo=timezone.utc),
            ),
        ]
        platform = _make_mock_platform(files)
        engine = self._engine_with_platform(platform)
        engine.read = MagicMock(return_value=engine._data)
        reader = FileReader(engine)

        with pytest.raises(SourceError, match="modification time is required"):
            reader.read(_make_file_source(), request=read_request())

        engine.read.assert_not_called()

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
