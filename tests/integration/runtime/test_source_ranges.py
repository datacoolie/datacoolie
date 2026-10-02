"""Native local source-range checks through readers and the Driver.

Each test reads the same temporary source directly and through a replay Driver
with an explicit ``SourceReadRange``.  The direct result is the oracle for the
Driver output, so the assertions cover actual source rows and the wiring that
delivers the range to the reader.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.core.models.source import Source
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.watermark.watermark_manager import WatermarkManager
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.sources import SourceReadRange
from datacoolie.watermark.base import BaseWatermarkManager
from datacoolie.sources.database_reader import DatabaseReader
from datacoolie.sources.delta_reader import DeltaReader
from datacoolie.sources.file_reader import FileReader
from datacoolie.sources.iceberg_reader import IcebergReader
from datacoolie.sources.python_function_reader import PythonFunctionReader


pytestmark = pytest.mark.integration


_FUNCTION_ROWS = [
    {"id": 0, "value": "zero"},
    {"id": 1, "value": "one"},
    {"id": 2, "value": "two"},
    {"id": 3, "value": "three"},
    {"id": 4, "value": "four"},
]
_FUNCTION_CALLS: list[dict[str, Any]] = []


@pytest.fixture
def persisted_manager(tmp_path: Path):
    """Use the concrete codec/provider and close the owned provider."""
    metadata = tmp_path / "state-metadata.json"
    metadata.write_text(json.dumps({"connections": [], "dataflows": []}), encoding="utf-8")
    provider = FileProvider(
        config_path=str(metadata), platform=LocalPlatform(),
        watermark_base_path=str(tmp_path / "state"),
    )
    provider.initialize()
    try:
        yield WatermarkManager(provider)
    finally:
        provider.close()


def _reloaded_state(tmp_path: Path, dataflow_id: str) -> dict[str, Any] | None:
    provider = FileProvider(
        config_path=str(tmp_path / "state-metadata.json"), platform=LocalPlatform(),
        watermark_base_path=str(tmp_path / "state"),
    )
    provider.initialize()
    try:
        return WatermarkManager(provider).get_watermark(dataflow_id)
    finally:
        provider.close()


def _range_function_loader(
    *,
    engine: PolarsEngine,
    source: Source,
    watermark_start: dict[str, Any] | None = None,
    watermark_end: dict[str, Any] | None = None,
    read_range: SourceReadRange | None = None,
):
    """Return a native Polars frame and record the bounded-call contract."""

    _FUNCTION_CALLS.append(
        {
            "watermark_start": watermark_start,
            "watermark_end": watermark_end,
            "read_range": read_range,
        }
    )
    rows = _FUNCTION_ROWS
    if read_range is not None:
        rows = [
            row
            for row in rows
            if (
                row[read_range.column] >= read_range.start
                and row[read_range.column] < read_range.end
            )
        ]
    return engine.create_dataframe(rows)


def _source_connection(
    *,
    name: str,
    connection_type: str,
    fmt: str,
    base_path: Path | None = None,
    **configure: Any,
) -> Connection:
    options = dict(configure)
    if base_path is not None:
        options["base_path"] = str(base_path)
    return Connection(
        name=name,
        connection_type=connection_type,
        format=fmt,
        configure=options,
    )


def _selected_rows(frame: pl.LazyFrame, columns: list[str]) -> list[dict[str, Any]]:
    return frame.select(columns).collect().sort(columns[0]).to_dicts()


def _driver_rows(
    source: Source,
    engine: PolarsEngine,
    platform: LocalPlatform,
    tmp_path: Path,
    read_range: SourceReadRange,
    *,
    dataflow_id: str,
    chunk_interval: dict[str, int] | None = None,
    save_watermark: bool = False,
    watermark_manager: BaseWatermarkManager | None = None,
    output_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    output_root = tmp_path / f"driver-output-{dataflow_id}"
    destination = Destination(
        connection=_source_connection(
            name=f"destination-{dataflow_id}",
            connection_type="file",
            fmt="parquet",
            base_path=output_root,
        ),
        table="selected",
        load_type="append",
    )
    dataflow = DataFlow(
        dataflow_id=dataflow_id,
        stage="source-ranges",
        source=source,
        destination=destination,
    )
    source_snapshot = source.model_dump()
    dataflow_snapshot = dataflow.model_dump()
    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=None,
        watermark_manager=watermark_manager,
        config=DataCoolieRunConfig(
            max_workers=1,
            retry_count=0,
            retry_delay=0,
        ),
    ) as driver:
        result = driver.run_replay(
            dataflow,
            ReplayConfig(
                start=read_range.start,
                end=read_range.end,
                chunk_column=read_range.column,
                chunk_interval=chunk_interval,
                save_watermark=save_watermark,
            ),
        )
    assert source.model_dump() == source_snapshot
    assert dataflow.model_dump() == dataflow_snapshot
    assert (result.total, result.succeeded, result.failed) == (1, 1, 0), result.errors
    output_files = sorted((output_root / "selected").glob("*.parquet"))
    assert output_files
    output = pl.concat([pl.read_parquet(path) for path in output_files])
    if output_columns is not None:
        output = output.select(output_columns)
    return output.sort(read_range.column).to_dicts()


def test_parquet_source_range_matches_driver(tmp_path: Path, persisted_manager: WatermarkManager) -> None:
    input_root = tmp_path / "parquet-input"
    table_root = input_root / "events"
    table_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "updated_seq": [100, 101, 102, 103, 104],
            "active": [0, 1, 1, 0, 1],
            "value": ["zero", "one", "two", "three", "four"],
        }
    ).write_parquet(table_root / "part.parquet")

    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    source = Source(
        connection=_source_connection(
            name="parquet-source",
            connection_type="file",
            fmt="parquet",
            base_path=input_root,
        ),
        table="events",
        watermark_columns=["updated_seq"],
        filter_expression="active = 1",
    )
    read_range = SourceReadRange("id", 1, 4)
    manager = persisted_manager

    direct = FileReader(engine).read(source, read_range=read_range)
    assert direct is not None
    expected = _selected_rows(direct, ["id", "updated_seq", "value"])
    actual = _driver_rows(
        source,
        engine,
        platform,
        tmp_path,
        read_range,
        dataflow_id="parquet-range",
        save_watermark=True,
        watermark_manager=manager,
        output_columns=["id", "updated_seq", "value"],
    )

    assert expected == [
        {"id": 1, "updated_seq": 101, "value": "one"},
        {"id": 2, "updated_seq": 102, "value": "two"},
    ]
    assert actual == expected
    assert _reloaded_state(tmp_path, "parquet-range") == {"updated_seq": 102}


def test_delta_source_range_matches_driver(tmp_path: Path, persisted_manager: WatermarkManager) -> None:
    input_root = tmp_path / "delta-input"
    input_root.mkdir()
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    engine.write_to_path(
        pl.DataFrame(
            {
                "id": [0, 1, 2, 3, 4],
                "updated_seq": [100, 101, 102, 103, 104],
                "active": [0, 1, 1, 0, 1],
                "value": ["zero", "one", "two", "three", "four"],
            }
        ).lazy(),
        str(input_root / "events"),
        mode="overwrite",
        fmt="delta",
    )
    source = Source(
        connection=_source_connection(
            name="delta-source",
            connection_type="lakehouse",
            fmt="delta",
            base_path=input_root,
        ),
        table="events",
        watermark_columns=["updated_seq"],
        filter_expression="active = 1",
    )
    read_range = SourceReadRange("id", 1, 4)
    manager = persisted_manager

    direct = DeltaReader(engine).read(source, read_range=read_range)
    assert direct is not None
    expected = _selected_rows(direct, ["id", "updated_seq", "value"])
    actual = _driver_rows(
        source,
        engine,
        platform,
        tmp_path,
        read_range,
        dataflow_id="delta-range",
        save_watermark=True,
        watermark_manager=manager,
        output_columns=["id", "updated_seq", "value"],
    )

    assert expected == [
        {"id": 1, "updated_seq": 101, "value": "one"},
        {"id": 2, "updated_seq": 102, "value": "two"},
    ]
    assert actual == expected
    assert _reloaded_state(tmp_path, "delta-range") == {"updated_seq": 102}


@pytest.mark.parametrize("source_format", ["parquet", "delta"])
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (datetime(2024, 1, 2), "2024-01-04T00:00:00"),
        ("2024-01-02T00:00:00", datetime(2024, 1, 4)),
        (date(2024, 1, 2), "2024-01-04"),
        ("2024-01-02", date(2024, 1, 4)),
    ],
    ids=["native-timestamp-iso", "iso-native-timestamp", "native-date-iso", "iso-native-date"],
)
@pytest.mark.parametrize(
    "chunk_interval",
    [None, {"days": 1}],
    ids=["one-shot", "daily-chunks"],
)
def test_temporal_native_iso_bounds_match_direct_and_driver(
    tmp_path: Path,
    source_format: str,
    start: date | datetime | str,
    end: date | datetime | str,
    chunk_interval: dict[str, int] | None,
) -> None:
    """Native and ISO temporal bounds keep exact rows through replay chunks."""

    input_root = tmp_path / f"{source_format}-temporal-input"
    input_root.mkdir()
    value_type = date if type(start) is date or type(end) is date else datetime
    event_times = [value_type(2024, 1, day) for day in range(1, 6)]
    frame = pl.DataFrame(
        {
            "id": list(range(5)),
            "event_time": event_times,
            "value": ["before", "lower", "middle", "upper", "after"],
        }
    )
    source_table = input_root / "events"
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    if source_format == "parquet":
        source_table.mkdir()
        frame.write_parquet(source_table / "part.parquet")
        reader_type = FileReader
        connection_type = "file"
    else:
        engine.write_to_path(
            frame.lazy(),
            str(source_table),
            mode="overwrite",
            fmt="delta",
        )
        reader_type = DeltaReader
        connection_type = "lakehouse"

    source = Source(
        connection=_source_connection(
            name=f"{source_format}-temporal-source",
            connection_type=connection_type,
            fmt=source_format,
            base_path=input_root,
        ),
        table="events",
        watermark_columns=["event_time"],
    )
    read_range = SourceReadRange("event_time", start, end)

    direct = reader_type(engine).read(source, read_range=read_range)
    assert direct is not None
    expected = _selected_rows(direct, ["id", "event_time", "value"])
    assert expected == [
        {"id": 1, "event_time": value_type(2024, 1, 2), "value": "lower"},
        {"id": 2, "event_time": value_type(2024, 1, 3), "value": "middle"},
    ]

    actual = _driver_rows(
        source,
        engine,
        platform,
        tmp_path,
        read_range,
        dataflow_id=f"{source_format}-temporal-range",
        chunk_interval=chunk_interval,
        output_columns=["id", "event_time", "value"],
    )
    assert actual == expected


@pytest.mark.parametrize("source_mode", ["table", "query"], ids=["table", "query"])
def test_sqlite_source_range_matches_driver(
    tmp_path: Path, source_mode: str
) -> None:
    database_path = tmp_path / "source.sqlite"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("CREATE TABLE events (id INTEGER, value TEXT, active INTEGER)")
        connection.executemany(
            "INSERT INTO events VALUES (?, ?, ?)",
            [
                (0, "zero", 0),
                (1, "one", 0),
                (2, "two", 1),
                (3, "three", 1),
                (4, "four", 0),
            ],
        )
        connection.commit()
    finally:
        connection.close()

    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    source_options = dict(
        connection=_source_connection(
            name=f"sqlite-source-{source_mode}",
            connection_type="database",
            fmt="sql",
            database_type="sqlite",
            database_read_engine="native",
            database=str(database_path),
        ),
        watermark_columns=["id"],
        filter_expression="active = 1",
    )
    if source_mode == "table":
        source = Source(table="events", **source_options)
    else:
        source = Source(
            query="SELECT id, value, active FROM events",
            **source_options,
        )
    read_range = SourceReadRange("id", 1, 4)

    direct = DatabaseReader(engine).read(source, read_range=read_range)
    assert direct is not None
    expected = _selected_rows(direct, ["id", "value"])
    actual = _driver_rows(
        source, engine, platform, tmp_path, read_range, dataflow_id="sqlite-range"
    )

    assert expected == [{"id": 2, "value": "two"}, {"id": 3, "value": "three"}]
    assert [row["id"] for row in actual] == [row["id"] for row in expected]
    assert [row["value"] for row in actual] == [row["value"] for row in expected]


def test_sqlite_incremental_filter_excludes_inactive_watermark_branch(
    tmp_path: Path,
) -> None:
    """A source filter must apply to every branch of a native SQL watermark OR."""

    database_path = tmp_path / "incremental.sqlite"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "CREATE TABLE events (id INTEGER, updated_at TEXT, active INTEGER)"
        )
        connection.executemany(
            "INSERT INTO events VALUES (?, ?, ?)",
            [
                (1, "2024-01-02", 0),
                (2, "2024-01-02", 1),
                (0, "2024-01-03", 1),
            ],
        )
        connection.commit()
    finally:
        connection.close()

    engine = PolarsEngine(platform=LocalPlatform())
    source = Source(
        connection=_source_connection(
            name="sqlite-incremental",
            connection_type="database",
            fmt="sql",
            database_type="sqlite",
            database_read_engine="native",
            database=str(database_path),
        ),
        table="events",
        watermark_columns=["id", "updated_at"],
        filter_expression="active = 1",
    )
    result = DatabaseReader(engine).read(
        source,
        watermark_start={"id": 1, "updated_at": "2024-01-01"},
    )

    assert result is not None
    assert result.collect().sort("id").select("id").to_series().to_list() == [0, 2]


def test_python_function_source_range_matches_driver(tmp_path: Path) -> None:
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    source = Source(
        connection=_source_connection(
            name="function-source",
            connection_type="function",
            fmt="function",
        ),
        table="events",
        python_function=f"{__name__}._range_function_loader",
        watermark_columns=["id"],
    )
    read_range = SourceReadRange("id", 1, 4)

    _FUNCTION_CALLS.clear()
    direct = PythonFunctionReader(engine).read(source, read_range=read_range)
    assert direct is not None
    expected = _selected_rows(direct, ["id", "value"])
    assert _FUNCTION_CALLS[-1] == {
        "watermark_start": None,
        "watermark_end": None,
        "read_range": read_range,
    }

    _FUNCTION_CALLS.clear()
    actual = _driver_rows(
        source, engine, platform, tmp_path, read_range, dataflow_id="function-range"
    )
    assert _FUNCTION_CALLS == [
        {
            "watermark_start": None,
            "watermark_end": None,
            "read_range": read_range,
        }
    ]
    assert [row["id"] for row in actual] == [row["id"] for row in expected]
    assert [row["value"] for row in actual] == [row["value"] for row in expected]


def test_decimal_delta_source_range_preserves_precision_through_driver(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "decimal-input"
    input_root.mkdir()
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    amounts = [
        Decimal("0.123456"),
        Decimal("1.234567"),
        Decimal("2.345678"),
        Decimal("3.456789"),
    ]
    frame = pl.DataFrame(
        {
            "id": [0, 1, 2, 3],
            "amount": pl.Series(amounts, dtype=pl.Decimal(18, 6)),
        }
    ).lazy()
    engine.write_to_path(
        frame,
        str(input_root / "events"),
        mode="overwrite",
        fmt="delta",
    )
    source = Source(
        connection=_source_connection(
            name="decimal-delta-source",
            connection_type="lakehouse",
            fmt="delta",
            base_path=input_root,
        ),
        table="events",
        watermark_columns=["amount"],
    )
    read_range = SourceReadRange(
        "amount", Decimal("1.234567"), Decimal("3.456789")
    )

    direct = DeltaReader(engine).read(source, read_range=read_range)
    assert direct is not None
    direct_frame = direct.collect().sort("amount")
    assert direct_frame.schema["amount"] == pl.Decimal(18, 6)
    expected = [
        {"id": 1, "amount": Decimal("1.234567")},
        {"id": 2, "amount": Decimal("2.345678")},
    ]
    assert direct_frame.select(["id", "amount"]).to_dicts() == expected

    actual = _driver_rows(
        source, engine, platform, tmp_path, read_range, dataflow_id="decimal-range"
    )
    assert [
        {"id": row["id"], "amount": row["amount"]} for row in actual
    ] == expected


def _windows_file_uri(path: Path) -> str:
    uri = path.as_uri()
    if os.name == "nt":
        return "file://" + uri.removeprefix("file:///")
    return uri


def test_local_pyiceberg_sql_catalog_source_range_matches_driver(tmp_path: Path, persisted_manager: WatermarkManager) -> None:
    load_catalog = pytest.importorskip("pyiceberg.catalog").load_catalog
    warehouse = tmp_path / "iceberg-warehouse"
    warehouse.mkdir()
    catalog_path = tmp_path / "iceberg-catalog.sqlite"
    catalog = load_catalog(
        "local-source-ranges",
        type="sql",
        uri=f"sqlite:///{catalog_path.as_posix()}",
        warehouse=_windows_file_uri(warehouse),
    )
    try:
        catalog.create_namespace_if_not_exists("source_ranges")
        frame = pl.DataFrame(
            {
                "id": [0, 1, 2, 3],
                "updated_seq": [100, 101, 102, 103],
                "value": ["zero", "one", "two", "three"],
            }
        )
        table = catalog.create_table("source_ranges.events", schema=frame.to_arrow().schema)
        table.append(frame.to_arrow())

        platform = LocalPlatform()
        engine = PolarsEngine(platform=platform, iceberg_catalog=catalog)
        source = Source(
            connection=Connection(
                name="iceberg-source",
                connection_type="lakehouse",
                format="iceberg",
                catalog="local",
            ),
            schema_name="source_ranges",
            table="events",
            watermark_columns=["updated_seq"],
        )
        read_range = SourceReadRange("id", 1, 3)
        manager = persisted_manager

        direct = IcebergReader(engine).read(source, read_range=read_range)
        assert direct is not None
        expected = _selected_rows(direct, ["id", "updated_seq", "value"])
        actual = _driver_rows(
            source,
            engine,
            platform,
            tmp_path,
            read_range,
            dataflow_id="iceberg-range",
            save_watermark=True,
            watermark_manager=manager,
            output_columns=["id", "updated_seq", "value"],
        )
        assert expected == [
            {"id": 1, "updated_seq": 101, "value": "one"},
            {"id": 2, "updated_seq": 102, "value": "two"},
        ]
        assert actual == expected
        assert _reloaded_state(tmp_path, "iceberg-range") == {"updated_seq": 102}
    finally:
        catalog.engine.dispose()
