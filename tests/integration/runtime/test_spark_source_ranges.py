"""Host Spark source-range and native Delta replacement qualification.

These cases use the test host's Spark and Delta runtime with local files only.
The session is owned by this module and is shared serially so the JVM remains
bounded while the direct-reader and Driver paths use the same source oracle.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.core.models.source import Source
from datacoolie.core.constants import FileInfoColumn
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.sources import SourceReadRange
from datacoolie.sources.delta_reader import DeltaReader
from datacoolie.sources.file_reader import FileReader
from datacoolie.sources.iceberg_reader import IcebergReader


pytestmark = [
    pytest.mark.integration,
    pytest.mark.spark,
    pytest.mark.runtime_qualification,
    pytest.mark.xdist_group("spark"),
]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    """Create and stop a local Delta-enabled Spark session for this module."""

    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from delta import configure_spark_with_delta_pip
    import pyspark
    from pyspark.sql import SparkSession

    if shutil.which("java") is None:
        pytest.skip("host Java runtime is unavailable for Spark qualification")

    iceberg_jars = sorted(
        (Path(pyspark.__file__).parent / "jars").glob("*iceberg*.jar")
    )
    iceberg_warehouse = tmp_path_factory.mktemp("spark-iceberg-warehouse")

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("datacoolie-amendment3-source-ranges")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
    )
    if iceberg_jars:
        builder = (
            builder.config("spark.jars", ",".join(str(path) for path in iceberg_jars))
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", str(iceberg_warehouse))
        )
    previous_spark_ip = os.environ.get("SPARK_LOCAL_IP")
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    session = None
    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        yield session
    finally:
        if session is not None:
            session.stop()
        if previous_spark_ip is None:
            os.environ.pop("SPARK_LOCAL_IP", None)
        else:
            os.environ["SPARK_LOCAL_IP"] = previous_spark_ip


def _engine(spark):
    from datacoolie.engines.spark_engine import SparkEngine

    return SparkEngine(spark_session=spark, platform=LocalPlatform())


def _connection(
    name: str,
    connection_type: str,
    fmt: str,
    base_path: Path,
) -> Connection:
    return Connection(
        name=name,
        connection_type=connection_type,
        format=fmt,
        configure={"base_path": str(base_path)},
    )


def _run_range_driver(
    *,
    spark,
    source: Source,
    read_range: SourceReadRange,
    output_root: Path,
    dataflow_id: str,
    select_columns: list[str] | None = None,
) -> list[tuple[Any, ...]]:
    """Read one explicit range through Driver and return the persisted rows."""

    platform = LocalPlatform()
    from datacoolie.engines.spark_engine import SparkEngine

    engine = SparkEngine(spark_session=spark, platform=platform)
    destination = Destination(
        connection=_connection(
            f"destination-{dataflow_id}", "file", "parquet", output_root
        ),
        table="selected",
        load_type="append",
    )
    dataflow = DataFlow(
        dataflow_id=dataflow_id,
        stage="spark-source-ranges",
        source=source,
        destination=destination,
    )
    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=None,
        config=DataCoolieRunConfig(max_workers=1, retry_count=0, retry_delay=0),
    ) as driver:
        result = driver.run_replay(
            dataflow,
            ReplayConfig(
                start=read_range.start,
                end=read_range.end,
                chunk_column=read_range.column,
                save_watermark=False,
            ),
        )
    assert (result.total, result.succeeded, result.failed) == (1, 1, 0), result.errors
    frame = spark.read.parquet(str(output_root / "selected"))
    selected = select_columns or [*source.watermark_columns, "value"]
    return [
        tuple(row)
        for row in frame.select(*selected)
        .orderBy(source.watermark_columns[0])
        .collect()
    ]


def _write_metadata(
    path: Path,
    input_root: Path,
    output_root: Path,
    *,
    transform: dict[str, object] | None = None,
) -> None:
    path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "lakehouse",
                        "format": "delta",
                        "configure": {"base_path": str(input_root)},
                    },
                    {
                        "name": "destination",
                        "connection_type": "lakehouse",
                        "format": "delta",
                        "configure": {"base_path": str(output_root)},
                    },
                ],
                "dataflows": [
                    {
                        "name": "replace-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": ["modified_at"],
                            "configure": {"backward_days": 1},
                        },
                        "destination": {
                            "connection_name": "destination",
                            "table": "events",
                            "load_type": "merge_overwrite",
                            "configure": {"replace_by_watermark": True},
                        },
                        "transform": transform
                        or {"rename_columns": {"modified_at": "Window-Start"}},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _run_replay(
    *,
    spark,
    metadata_path: Path,
    state_root: Path,
    log_root: Path,
    job_id: str,
):
    from datacoolie.engines.spark_engine import SparkEngine
    from datacoolie.metadata.file_provider import FileProvider

    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata_path), platform=platform)
    with DataCoolieDriver(
        engine=SparkEngine(spark_session=spark, platform=platform),
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(state_root),
        log_base_path=str(log_root),
        config=DataCoolieRunConfig(job_id=job_id, retry_count=0, retry_delay=0),
    ) as driver:
        dataflow = provider.get_dataflows(stage="bronze")[0]
        return driver.run_replay(
            dataflow,
            ReplayConfig(
                start=1,
                end=10,
                chunk_column="modified_at",
                save_watermark=True,
            ),
            column_name_mode="snake",
        )


def _write_delta(engine, path: Path, rows: list[tuple[Any, ...]], schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = engine.spark.createDataFrame(rows, schema=schema)
    engine.write_to_path(frame, str(path), mode="overwrite", fmt="delta")


def _delta_rows(spark, path: Path) -> list[dict[str, Any]]:
    return [
        row.asDict()
        for row in spark.read.format("delta")
        .load(str(path))
        .select("id", "window_start", "value")
        .orderBy("window_start")
        .collect()
    ]


def test_spark_delta_source_range_matches_direct_and_driver(
    spark, tmp_path: Path
) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    input_root = tmp_path / "delta-input"
    source_path = input_root / "events"
    engine = _engine(spark)
    schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )
    _write_delta(
        engine,
        source_path,
        [(0, "zero"), (1, "one"), (2, "two"), (3, "three"), (4, "four")],
        schema,
    )
    source = Source(
        connection=_connection("delta-source", "lakehouse", "delta", input_root),
        table="events",
        watermark_columns=["id"],
    )
    read_range = SourceReadRange("id", 1, 4)

    direct = DeltaReader(engine).read(source, read_range=read_range)
    assert direct is not None
    expected = [
        (row["id"], row["value"])
        for row in direct.select("id", "value").orderBy("id").collect()
    ]
    actual = _run_range_driver(
        spark=spark,
        source=source,
        read_range=read_range,
        output_root=tmp_path / "driver-output",
        dataflow_id="spark-delta-range",
    )
    assert expected == [(1, "one"), (2, "two"), (3, "three")]
    assert actual == expected


def test_spark_delta_temporal_source_range_selects_exact_rows(
    spark, tmp_path: Path
) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    input_root = tmp_path / "temporal-input"
    source_path = input_root / "events"
    engine = _engine(spark)
    schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("event_time", TimestampType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )
    lower = datetime(2024, 1, 1, tzinfo=timezone.utc)
    middle = datetime(2024, 1, 2, tzinfo=timezone.utc)
    upper = datetime(2024, 1, 3, tzinfo=timezone.utc)
    after = datetime(2024, 1, 4, tzinfo=timezone.utc)
    _write_delta(
        engine,
        source_path,
        [(0, lower, "lower"), (1, middle, "middle"), (2, upper, "upper"), (3, after, "after")],
        schema,
    )
    source = Source(
        connection=_connection("temporal-source", "lakehouse", "delta", input_root),
        table="events",
        watermark_columns=["event_time"],
    )
    read_range = SourceReadRange("event_time", lower, upper)

    direct = DeltaReader(engine).read(source, read_range=read_range)
    assert direct is not None
    expected = [
        row["id"] for row in direct.select("id").orderBy("id").collect()
    ]
    assert expected == [0, 1]
    actual = _run_range_driver(
        spark=spark,
        source=source,
        read_range=read_range,
        output_root=tmp_path / "driver-output",
        dataflow_id="spark-temporal-range",
        select_columns=["id", "event_time", "value"],
    )
    assert [row[0] for row in actual] == expected


def test_spark_parquet_mtime_range_uses_exact_file_boundaries(
    spark, tmp_path: Path
) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    input_root = tmp_path / "parquet-input"
    table_root = input_root / "events"
    table_root.mkdir(parents=True)
    schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )
    stamps = [1_700_000_000, 1_700_003_600, 1_700_007_200, 1_700_010_800]
    for row_id, stamp in enumerate(stamps):
        before = set(table_root.glob("*.parquet"))
        (
            spark.createDataFrame([(row_id, f"row-{row_id}")], schema=schema)
            .coalesce(1)
            .write.mode("append")
            .parquet(str(table_root))
        )
        created = sorted(set(table_root.glob("*.parquet")) - before)
        assert len(created) == 1
        os.utime(created[0], (stamp, stamp))

    platform = LocalPlatform()
    engine = _engine(spark)
    source = Source(
        connection=_connection("mtime-source", "file", "parquet", input_root),
        table="events",
        watermark_columns=[],
    )
    read_range = SourceReadRange(
        FileInfoColumn.FILE_MODIFICATION_TIME.value,
        datetime.fromtimestamp(stamps[0], tz=timezone.utc),
        datetime.fromtimestamp(stamps[2], tz=timezone.utc),
    )
    direct = FileReader(engine).read(source, read_range=read_range)
    assert direct is not None
    assert [row["id"] for row in direct.select("id").orderBy("id").collect()] == [0, 1]
    assert platform.list_files(str(table_root), extension=".parquet")


def test_spark_iceberg_source_range_when_native_runtime_is_available(
    spark,
) -> None:
    """Exercise IcebergReader only when a native Spark Iceberg JAR is present."""

    import pyspark

    iceberg_jars = sorted(
        (Path(pyspark.__file__).parent / "jars").glob("*iceberg*.jar")
    )
    if not iceberg_jars:
        pytest.skip(
            "host Spark Iceberg source-read is unqualified: no Iceberg runtime JAR "
            "is available in the PySpark distribution"
        )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.source_ranges")
    spark.sql("DROP TABLE IF EXISTS local.source_ranges.events")
    spark.createDataFrame(
        [(0, "zero"), (1, "one"), (2, "two"), (3, "three")],
        schema="id BIGINT, value STRING",
    ).writeTo("local.source_ranges.events").using("iceberg").create()

    source = Source(
        connection=Connection(
            name="iceberg-source",
            connection_type="lakehouse",
            format="iceberg",
            catalog="local",
        ),
        schema_name="source_ranges",
        table="events",
        watermark_columns=["id"],
    )
    engine = _engine(spark)
    direct = IcebergReader(engine).read(
        source,
        read_range=SourceReadRange("id", 1, 3),
    )
    assert direct is not None
    assert [row["id"] for row in direct.select("id").orderBy("id").collect()] == [1, 2]


def test_spark_driver_delta_replace_preserves_requested_tail_and_rerun(
    spark, tmp_path: Path
) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    source_path = input_root / "events"
    target_path = output_root / "events"
    engine = _engine(spark)
    source_schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("modified_at", LongType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )
    target_schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("window_start", LongType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )
    _write_delta(engine, source_path, [(2, 2, "new-2"), (3, 3, "new-3")], source_schema)
    _write_delta(
        engine,
        target_path,
        [
            (100, 0, "before"),
            (101, 1, "stale-1"),
            (102, 4, "stale-4"),
            (103, 9, "stale-9"),
            (104, 10, "after"),
        ],
        target_schema,
    )
    metadata_path = tmp_path / "metadata.json"
    _write_metadata(metadata_path, input_root, output_root)
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"

    first = _run_replay(
        spark=spark,
        metadata_path=metadata_path,
        state_root=state_root,
        log_root=log_root,
        job_id="spark-replace-first",
    )
    assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
    expected = [
        {"id": 100, "window_start": 0, "value": "before"},
        {"id": 2, "window_start": 2, "value": "new-2"},
        {"id": 3, "window_start": 3, "value": "new-3"},
        {"id": 104, "window_start": 10, "value": "after"},
    ]
    assert _delta_rows(spark, target_path) == expected
    checkpoints = sorted(state_root.rglob("watermark_value.json"))
    assert len(checkpoints) == 1
    assert json.loads(checkpoints[0].read_text(encoding="utf-8")) == {"modified_at": 3}

    second = _run_replay(
        spark=spark,
        metadata_path=metadata_path,
        state_root=state_root,
        log_root=log_root,
        job_id="spark-replace-rerun",
    )
    assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
    assert _delta_rows(spark, target_path) == expected


def test_spark_driver_delta_typed_empty_replaces_requested_window(
    spark, tmp_path: Path
) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    source_path = input_root / "events"
    target_path = output_root / "events"
    engine = _engine(spark)
    source_schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("modified_at", LongType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )
    target_schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("window_start", LongType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )
    _write_delta(engine, source_path, [(200, 0, "before"), (201, 10, "after")], source_schema)
    _write_delta(
        engine,
        target_path,
        [
            (100, 0, "before"),
            (101, 2, "stale-2"),
            (102, 8, "stale-8"),
            (103, 10, "after"),
        ],
        target_schema,
    )
    metadata_path = tmp_path / "metadata.json"
    _write_metadata(metadata_path, input_root, output_root)
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"

    first = _run_replay(
        spark=spark,
        metadata_path=metadata_path,
        state_root=state_root,
        log_root=log_root,
        job_id="spark-empty-first",
    )
    assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
    expected = [
        {"id": 100, "window_start": 0, "value": "before"},
        {"id": 103, "window_start": 10, "value": "after"},
    ]
    assert _delta_rows(spark, target_path) == expected
    state_files = list(state_root.rglob("watermark_value.json"))
    assert not state_files, [
        (path.as_posix(), path.read_text(encoding="utf-8")) for path in state_files
    ]

    second = _run_replay(
        spark=spark,
        metadata_path=metadata_path,
        state_root=state_root,
        log_root=log_root,
        job_id="spark-empty-rerun",
    )
    assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
    assert _delta_rows(spark, target_path) == expected
