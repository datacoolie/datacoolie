"""Focused tests for Polars flat-file helpers."""

from datetime import datetime, timezone
from unittest.mock import patch

import polars as pl
import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._polars.file_io import (
    add_file_info_columns,
    make_file_name,
    read_avro_files,
    read_json_files,
    scan_csv,
    scan_jsonl,
    scan_parquet,
    write_flat_eager,
    write_flat_sink,
)
from datacoolie.platforms.base import FileInfo
from datacoolie.platforms.local_platform import LocalPlatform


def test_overwrite_name_ignores_partition_suffixes() -> None:
    assert make_file_name("root/table/year=2026/04", "parquet", True) == "table.parquet"


def test_append_name_contains_utc_timestamp() -> None:
    with patch("datacoolie.engines._polars.file_io.datetime") as clock:
        clock.now.return_value.strftime.return_value = "20260820_101112"
        name = make_file_name("root/table", "csv", False)
        assert name.startswith("table_20260820_101112_")
        assert name.endswith(".csv")


def test_append_names_are_unique_within_the_same_second() -> None:
    with patch("datacoolie.engines._polars.file_io.datetime") as clock:
        clock.now.return_value.strftime.return_value = "20260820_101112"
        first = make_file_name("root/table", "parquet", False)
        second = make_file_name("root/table", "parquet", False)
    assert first != second


def test_overwrite_deletes_only_resolved_date_folder() -> None:
    class DeleteRecorder:
        def __init__(self) -> None:
            self.deleted: list[tuple[str, bool]] = []

        def delete_folder(self, path: str, *, recursive: bool = False) -> None:
            self.deleted.append((path, recursive))

    platform = DeleteRecorder()
    frame = pl.DataFrame({"id": [1]}).lazy()
    resolved = "root/table/2026/09/25"

    with patch.object(pl.LazyFrame, "sink_parquet"):
        write_flat_sink(
            frame,
            resolved,
            "overwrite",
            "parquet",
            None,
            {},
            platform=platform,
            storage_options={},
        )

    assert platform.deleted == [(resolved, True)]


def test_overwrite_keeps_previous_date_folder_snapshot(tmp_path) -> None:
    base = tmp_path / "table"
    previous = base / "2026" / "09" / "24"
    current = base / "2026" / "09" / "25"
    previous.mkdir(parents=True)
    (previous / "snapshot.parquet").write_bytes(b"old")

    frame = pl.DataFrame({"id": [1], "value": ["current"]}).lazy()
    write_flat_sink(
        frame,
        str(current),
        "overwrite",
        "parquet",
        None,
        {},
        platform=LocalPlatform(),
        storage_options={},
    )

    assert (previous / "snapshot.parquet").read_bytes() == b"old"
    assert (current / "table.parquet").exists()


def test_sink_overwrite_stops_when_delete_fails() -> None:
    class DeleteFailure:
        def delete_folder(self, path: str, *, recursive: bool = False) -> None:
            raise PermissionError(f"denied: {path}")

    frame = pl.DataFrame({"id": [1]}).lazy()
    with patch.object(pl.LazyFrame, "sink_parquet") as sink:
        with pytest.raises(PermissionError, match="denied"):
            write_flat_sink(
                frame,
                "/tmp/out",
                "overwrite",
                "parquet",
                None,
                {},
                platform=DeleteFailure(),
                storage_options={},
            )

    sink.assert_not_called()


def test_eager_overwrite_stops_when_delete_fails() -> None:
    class DeleteFailure:
        def __init__(self) -> None:
            self.created = False
            self.written = False

        def delete_folder(self, path: str, *, recursive: bool = False) -> None:
            raise TimeoutError(f"timeout: {path}")

        def create_folder(self, path: str) -> None:
            self.created = True

        def write_bytes(self, path: str, content: bytes, *, overwrite: bool = False) -> None:
            self.written = True

    platform = DeleteFailure()
    with pytest.raises(TimeoutError, match="timeout"):
        write_flat_eager(
            pl.DataFrame({"id": [1]}).lazy(),
            "/tmp/out",
            "overwrite",
            "json",
            {},
            platform=platform,
        )

    assert platform.created is False
    assert platform.written is False


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


def _historical_file_info_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2],
            "payload": ["a", "b"],
            "__file_path": ["historical/a", "historical/b"],
            "__file_name": ["old-a", "old-b"],
            "__file_modification_time": ["2020-01-01", "2020-01-02"],
            "__created_at": ["created-a", "created-b"],
        }
    )


def _assert_current_file_info(
    frame: pl.LazyFrame,
    path: str,
    name: str,
    modified: datetime,
    expected_ids: tuple[object, object] = (1, 2),
) -> None:
    rows = add_file_info_columns(
        frame,
        [FileInfo(name=name, path=path, modification_time=modified)],
    ).collect()
    assert rows.select("id", "payload", "__created_at").to_dicts() == [
        {"id": expected_ids[0], "payload": "a", "__created_at": "created-a"},
        {"id": expected_ids[1], "payload": "b", "__created_at": "created-b"},
    ]
    assert rows["__file_path"].to_list() == [path.replace("\\", "/")] * 2
    assert rows["__file_name"].to_list() == [name] * 2
    assert rows["__file_modification_time"].to_list() == [modified] * 2
    assert len(rows.columns) == len(set(rows.columns))
    assert not any(column.endswith("_right") for column in rows.columns)


@pytest.mark.parametrize(
    ("suffix", "writer", "reader"),
    [
        ("csv", lambda frame, path: frame.write_csv(path), lambda path: scan_csv(str(path), {}, {})),
        (
            "parquet",
            lambda frame, path: frame.write_parquet(path),
            lambda path: scan_parquet(str(path), {}, {}),
        ),
        (
            "jsonl",
            lambda frame, path: frame.write_ndjson(path),
            lambda path: scan_jsonl(str(path), {}, {}),
        ),
    ],
)
def test_lazy_file_read_replaces_historical_file_info_columns(
    tmp_path, suffix, writer, reader
) -> None:
    path = tmp_path / f"input.{suffix}"
    writer(_historical_file_info_frame(), path)
    modified = datetime(2026, 8, 20, tzinfo=timezone.utc)

    _assert_current_file_info(
        reader(path),
        str(path),
        path.name,
        modified,
        expected_ids=["1", "2"] if suffix == "csv" else [1, 2],
    )


@pytest.mark.parametrize("suffix", ["json", "avro"])
def test_eager_file_read_replaces_historical_file_info_columns(tmp_path, suffix) -> None:
    path = tmp_path / f"input.{suffix}"
    frame = _historical_file_info_frame()
    if suffix == "json":
        frame.write_json(path)
        result = read_json_files([str(path)], LocalPlatform(), {})
    else:
        frame.write_avro(path)
        result = read_avro_files([str(path)], LocalPlatform(), {})

    modified = datetime(2026, 8, 20, tzinfo=timezone.utc)
    _assert_current_file_info(result, str(path), path.name, modified)


def test_file_info_replacement_supports_multiple_paths_and_preserves_system_columns(
    tmp_path,
) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _historical_file_info_frame().head(1).write_parquet(first)
    _historical_file_info_frame().tail(1).write_parquet(second)
    first_modified = datetime(2026, 8, 20, tzinfo=timezone.utc)
    second_modified = datetime(2026, 8, 21, tzinfo=timezone.utc)

    result = add_file_info_columns(
        scan_parquet([str(first), str(second)], {}, {}),
        [
            FileInfo(name=first.name, path=str(first), modification_time=first_modified),
            FileInfo(name=second.name, path=str(second), modification_time=second_modified),
        ],
    ).collect()

    assert result.select("id", "payload", "__created_at").to_dicts() == [
        {"id": 1, "payload": "a", "__created_at": "created-a"},
        {"id": 2, "payload": "b", "__created_at": "created-b"},
    ]
    assert result["__file_name"].to_list() == [first.name, second.name]
    assert result["__file_modification_time"].to_list() == [
        first_modified,
        second_modified,
    ]


def test_csv_options_and_schema_hints_survive_file_info_replacement(tmp_path) -> None:
    path = tmp_path / "input.csv"
    path.write_text(
        "id;payload;__file_path;__file_name;__file_modification_time;__created_at\n"
        "1;a;historical/a;old-a;2020-01-01;created-a\n",
        encoding="utf-8",
    )

    result = add_file_info_columns(
        scan_csv(
            str(path),
            {
                "header": True,
                "sep": ";",
                "inferSchema": True,
                "schema_overrides": {"id": pl.Int64},
            },
            {},
        ),
        [
            FileInfo(
                name=path.name,
                path=str(path),
                modification_time=datetime(2026, 8, 20, tzinfo=timezone.utc),
            )
        ],
    ).collect()

    assert result.schema["id"] == pl.Int64
    assert result.select("id", "payload", "__created_at").to_dicts() == [
        {"id": 1, "payload": "a", "__created_at": "created-a"}
    ]


def test_csv_read_maps_canonical_options_without_mutating_input() -> None:
    options = {
        "header": "false",
        "sep": ";",
        "quote": "|",
        "inferSchema": "true",
    }
    with (
        patch("datacoolie.engines._polars.file_io.pl.scan_csv") as scan,
        patch(
            "datacoolie.engines._polars.file_io._scan_file_path_column",
            return_value="__scan_path",
        ),
    ):
        scan.return_value = pl.DataFrame(
            {"id": [1], "__scan_path": ["/tmp/input.csv"]}
        ).lazy()
        scan_csv("/tmp/input.csv", options, {})

    assert options == {
        "header": "false",
        "sep": ";",
        "quote": "|",
        "inferSchema": "true",
    }
    kwargs = scan.call_args.kwargs
    assert kwargs["include_file_paths"] == "__scan_path"
    assert kwargs["has_header"] is False
    assert kwargs["separator"] == ";"
    assert kwargs["quote_char"] == "|"
    assert kwargs["infer_schema"] is True
    assert "header" not in kwargs and "sep" not in kwargs


def test_csv_alias_conflict_is_rejected() -> None:
    with pytest.raises(EngineError, match="header.*has_header"):
        scan_csv("/tmp/input.csv", {"header": True, "has_header": False}, {})


def test_csv_write_validates_aliases_before_overwrite_delete() -> None:
    class DeleteRecorder:
        def __init__(self) -> None:
            self.deleted = False

        def delete_folder(self, *_args, **_kwargs) -> None:
            self.deleted = True

    platform = DeleteRecorder()
    with pytest.raises(EngineError, match="header.*include_header"):
        write_flat_sink(
            pl.DataFrame({"id": [1]}).lazy(),
            "/tmp/out",
            "overwrite",
            "csv",
            None,
            {"header": True, "include_header": False},
            platform=platform,
            storage_options={},
        )
    assert platform.deleted is False


def test_csv_write_maps_header_and_separator() -> None:
    frame = pl.DataFrame({"id": [1]}).lazy()
    with patch.object(pl.LazyFrame, "sink_csv") as sink:
        write_flat_sink(
            frame,
            "/tmp/out",
            "append",
            "csv",
            None,
            {"header": False, "sep": ";"},
            platform=None,
            storage_options={},
        )
    kwargs = sink.call_args.kwargs
    assert kwargs["include_header"] is False
    assert kwargs["separator"] == ";"
    assert "header" not in kwargs and "sep" not in kwargs
