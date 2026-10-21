"""Native JSON framing, discovery and Driver checkpoint qualification."""

from datetime import datetime, timezone
import json
import os
from unittest.mock import patch

import polars as pl
import pytest

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY, FileInfoColumn
from datacoolie.core.exceptions import ConfigurationError, SourceError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.sources.file_reader import FileReader
from datacoolie.sources.contracts import SourceReadRequest, SourceReadRange

pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]
ROWS = [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}]


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory):
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("json-framing-qualification")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config(
            "spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("json-warehouse"))
        )
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(params=["polars", pytest.param("spark", marks=pytest.mark.spark)])
def native_engine(request):
    if request.param == "polars":
        return PolarsEngine(platform=LocalPlatform())
    from datacoolie.engines.spark_engine import SparkEngine

    return SparkEngine(
        spark_session=request.getfixturevalue("spark_session"), platform=LocalPlatform()
    )


def values(frame):
    if isinstance(frame, pl.LazyFrame):
        result = frame.select("id", "name").collect()
        assert result.schema == {"id": pl.Int64, "name": pl.String}
        return result.sort("id").to_dicts()
    assert frame.select("id", "name").dtypes == [("id", "bigint"), ("name", "string")]
    return [row.asDict() for row in frame.select("id", "name").orderBy("id").collect()]


@pytest.mark.parametrize("suffix", [".json", ".jsonl"])
@pytest.mark.parametrize(
    "fmt,option",
    [
        ("json", {}),
        ("json", {"multiLine": True}),
        ("json", {"multiLine": " TRUE "}),
        ("json", {"multiLine": False}),
        ("json", {"multiLine": "false"}),
        ("jsonl", {}),
        ("jsonl", {"multiLine": False}),
        ("jsonl", {"multiLine": " false "}),
    ],
)
def test_native_framing_explicit_suffix_and_generic_read(
    native_engine, tmp_path, suffix, fmt, option
):
    document = fmt == "json" and option.get("multiLine", True) not in (False, "false")
    path = tmp_path / ("records" + suffix)
    path.write_text(
        json.dumps(ROWS, indent=2) if document else "\n".join(map(json.dumps, ROWS)),
        encoding="utf-8",
    )
    before = dict(option)
    assert values(native_engine.read(fmt=fmt, path=str(path), options=option)) == ROWS
    assert (
        values(getattr(native_engine, "read_" + fmt)(str(path), options=option)) == ROWS
    )
    assert option == before


@pytest.mark.parametrize(
    "fmt,value", [("jsonl", True), ("json", None), ("json", 0), ("json", "bad")]
)
def test_invalid_framing_fails_before_listing_even_on_empty_input(
    native_engine, tmp_path, fmt, value
):
    empty = tmp_path / "empty"
    empty.mkdir()
    with patch.object(native_engine.platform, "list_files") as listing:
        with pytest.raises(ConfigurationError):
            getattr(native_engine, "read_" + fmt)(str(empty), {"multiLine": value})
        conn = Connection(
            name="input",
            connection_type="file",
            format=fmt,
            configure={"base_path": str(tmp_path)},
        )
        source = Source(
            connection=conn,
            table="empty",
            configure={"read_options": {"multiLine": value}},
        )
        with pytest.raises(SourceError):
            FileReader(native_engine).read(
                source,
                request=SourceReadRequest(reference_time=datetime.now(timezone.utc)),
            )
        listing.assert_not_called()


def test_polars_directory_suffixes_overlaps_override_and_native_glob(tmp_path):
    root = tmp_path / "input"
    root.mkdir()
    for suffix, row in zip([".json", ".jsonl", ".custom"], [1, 2, 3]):
        (root / ("part" + suffix)).write_text(
            json.dumps({"id": row, "name": str(row)}), encoding="utf-8"
        )
    (root / "ignore.txt").write_text("invalid", encoding="utf-8")
    for engine in [PolarsEngine(), PolarsEngine(platform=LocalPlatform())]:
        result = engine.read_jsonl([str(root), str(root / "part.json")]).collect()
        assert sorted(result["id"].to_list()) == [1, 2]
        assert engine.read_jsonl([str(root / "part.json")] * 2).collect().height == 1
        assert (
            engine.read_json([str(root / "part.json")] * 2, {"multiLine": False})
            .collect()
            .height
            == 1
        )
        assert engine.read_json(str(root), {"multiLine": False}).collect()[
            "id"
        ].to_list() == [1]
    plain = PolarsEngine()
    assert plain.read_jsonl(str(root / "*.json*")).collect().height == 2
    plain.FORMAT_EXTENSIONS = {**plain.FORMAT_EXTENSIONS, "jsonl": ".custom"}
    assert plain.read_jsonl(str(root)).collect()["id"].to_list() == [3]
    attached = PolarsEngine(platform=LocalPlatform())
    attached.FORMAT_EXTENSIONS = plain.FORMAT_EXTENSIONS
    assert [fi.name for fi in attached.list_format_files(str(root), "jsonl")] == [
        "part.custom"
    ]


def test_polars_native_options_remain_without_platform():
    engine = PolarsEngine(storage_options={"token": "test"})
    options = {"multiLine": False, "schema_overrides": {"id": pl.Int64}}
    with patch(
        "polars.scan_ndjson", return_value=pl.DataFrame({"id": [1]}).lazy()
    ) as scan:
        engine.read_jsonl("s3://bucket/*.json", options)
    assert scan.call_args.args[0] == "s3://bucket/*.json"
    assert scan.call_args.kwargs["storage_options"] == {"token": "test"}
    assert scan.call_args.kwargs["schema_overrides"] == {"id": pl.Int64}
    assert "multiLine" not in scan.call_args.kwargs
    assert options["multiLine"] is False


def test_repeated_jsonl_roots_are_listed_once(tmp_path):
    (tmp_path / "part.json").write_text(json.dumps(ROWS[0]), encoding="utf-8")
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    with patch.object(platform, "list_files", wraps=platform.list_files) as listing:
        assert engine.read_jsonl([str(tmp_path)] * 2).collect().height == 1
        listing.assert_called_once()
    with patch.object(platform, "list_files", wraps=platform.list_files) as listing:
        infos = FileReader(engine)._collect_file_infos(
            [str(tmp_path)] * 2, "jsonl", None
        )
        assert len(infos) == 1
        listing.assert_called_once()


@pytest.mark.spark
def test_spark_native_paths_partitions_and_filters_preserved(spark_session, tmp_path):
    from datacoolie.engines.spark_engine import SparkEngine

    root = tmp_path / "partitioned"
    for day in [1, 2]:
        leaf = root / f"day={day}"
        leaf.mkdir(parents=True)
        (leaf / "part.json").write_text(
            json.dumps({"id": day, "name": str(day)}), encoding="utf-8"
        )
        (leaf / "skip.txt").write_text("broken", encoding="utf-8")
    for platform in [None, LocalPlatform()]:
        engine = SparkEngine(spark_session=spark_session, platform=platform)
        opts = {"pathGlobFilter": "*.json", "basePath": str(root), "mode": "FAILFAST"}
        result = engine.read_jsonl(str(root / "day=*"), opts)
        assert result.select("day").distinct().count() == 2
        assert result.count() == 2
        recursive = engine.read_jsonl(
            str(root), {"recursiveFileLookup": "true", "pathGlobFilter": "*.json"}
        )
        assert recursive.count() == 2
        assert "day" not in recursive.columns


def test_malformed_document_does_not_fallback_to_lines(native_engine, tmp_path):
    path = tmp_path / "document.json"
    path.write_text(json.dumps(ROWS, indent=2), encoding="utf-8")
    options = {"multiLine": False}
    if isinstance(native_engine, PolarsEngine):
        error = pl.exceptions.ComputeError
    else:
        from py4j.protocol import Py4JJavaError

        error = Py4JJavaError
        options["mode"] = "FAILFAST"
    with pytest.raises(error):
        frame = native_engine.read_json(str(path), options)
        values(frame)
    assert values(native_engine.read_json(str(path))) == ROWS


def test_polars_document_parser_does_not_retry_lines(tmp_path):
    path = tmp_path / "lines.json"
    path.write_text("\n".join(map(json.dumps, ROWS)), encoding="utf-8")
    engine = PolarsEngine(platform=LocalPlatform())
    with pytest.raises(pl.exceptions.ComputeError):
        engine.read_json(str(path)).collect()
    assert values(engine.read_json(str(path), {"multiLine": False})) == ROWS


@pytest.mark.spark
def test_spark_document_parser_keeps_native_trailing_record_behavior(
    spark_session, tmp_path
):
    from datacoolie.engines.spark_engine import SparkEngine

    path = tmp_path / "lines.json"
    path.write_text("\n".join(map(json.dumps, ROWS)), encoding="utf-8")
    engine = SparkEngine(spark_session=spark_session)
    # Spark's document parser accepts its first object even in FAILFAST mode.
    # Framing selection must not introduce retries or strengthen that policy.
    assert values(engine.read_json(str(path), {"mode": "FAILFAST"})) == ROWS[:1]
    assert values(engine.read_json(str(path), {"multiLine": False})) == ROWS


def test_driver_dual_suffix_incremental_failure_and_state(native_engine, tmp_path):
    inp = tmp_path / "input" / "events"
    inp.mkdir(parents=True)
    output, state, logs = [tmp_path / name for name in ["output", "state", "logs"]]
    metadata = tmp_path / "metadata.json"
    body = {
        "connections": [
            {
                "name": "src",
                "connection_type": "file",
                "format": "jsonl",
                "configure": {"base_path": str(inp.parent)},
            },
            {
                "name": "dst",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": str(output)},
            },
        ],
        "dataflows": [
            {
                "name": "json-events",
                "stage": "bronze",
                "source": {
                    "connection_name": "src",
                    "table": "events",
                    "watermark_columns": ["__file_modification_time"],
                },
                "destination": {
                    "connection_name": "dst",
                    "table": "events",
                    "load_type": "append",
                },
            }
        ],
    }
    metadata.write_text(json.dumps(body), encoding="utf-8")

    def run():
        platform = native_engine.platform
        provider = FileProvider(config_path=str(metadata), platform=platform)
        with DataCoolieDriver(
            engine=native_engine,
            platform=platform,
            metadata_provider=provider,
            state_base_path=str(state),
            log_base_path=str(logs),
            config=DataCoolieRunConfig(retry_count=0),
        ) as driver:
            return driver.run(stage="bronze")

    def ids():
        return sorted(
            pl.read_parquet(list((output / "events").glob("*.parquet")))["id"].to_list()
        )

    first = inp / "first.json"
    first.write_text(json.dumps(ROWS[0]), encoding="utf-8")
    os.utime(first, (1700000000, 1700000000))
    result = run()
    assert result.succeeded == 1, result.errors
    assert ids() == [1]
    second = inp / "second.jsonl"
    second.write_text(json.dumps(ROWS[1]), encoding="utf-8")
    os.utime(second, (1700003600, 1700003600))
    result = run()
    assert result.succeeded == 1, result.errors
    assert ids() == [1, 2]
    checkpoints = list(state.rglob("watermark_value.json"))
    assert len(checkpoints) == 1
    checkpoint = checkpoints[0].read_bytes()
    assert run().skipped == 1
    assert ids() == [1, 2]
    assert checkpoints[0].read_bytes() == checkpoint
    body["connections"][0]["configure"]["read_options"] = {"multiLine": True}
    metadata.write_text(json.dumps(body), encoding="utf-8")
    with (
        patch.object(native_engine, "read", wraps=native_engine.read) as read,
        patch.object(native_engine, "write", wraps=native_engine.write) as write,
    ):
        result = run()
        assert result.failed == 1
        read.assert_not_called()
        write.assert_not_called()
    assert ids() == [1, 2]
    assert checkpoints[0].read_bytes() == checkpoint


def test_source_mtime_range_hive_and_precedence(native_engine, tmp_path):
    root = tmp_path / "events"
    for number, suffix in [(1, ".json"), (2, ".jsonl")]:
        leaf = root / f"day={number}"
        leaf.mkdir(parents=True)
        path = leaf / ("part" + suffix)
        path.write_text(
            json.dumps({"id": number, "name": str(number)}), encoding="utf-8"
        )
        os.utime(path, (1700000000 + number * 3600, 1700000000 + number * 3600))
    conn = Connection(
        name="input",
        connection_type="file",
        format="jsonl",
        configure={
            "base_path": str(tmp_path),
            "use_hive_partitioning": True,
            "read_options": {"multiLine": True},
        },
    )
    source = Source(
        connection=conn,
        table="events",
        configure={"read_options": {"multiLine": False}},
    )
    request = SourceReadRequest(
        reference_time=datetime.now(timezone.utc),
        read_range=SourceReadRange(
            column=FileInfoColumn.FILE_MODIFICATION_TIME,
            start=datetime.fromtimestamp(1700003600, timezone.utc),
            end=datetime.fromtimestamp(1700007200, timezone.utc),
            lower_operator=">=",
            upper_operator="<",
        ),
    )
    result = FileReader(native_engine).read(source, request=request)
    assert result.rows_read == 1
    assert values(result.frame) == [{"id": 1, "name": "1"}]
    assert conn.read_options["multiLine"] is True


def test_source_date_folders_selected_paths_and_metadata(native_engine, tmp_path):
    paths = []
    for day, suffix in [(1, ".json"), (2, ".jsonl")]:
        leaf = tmp_path / "events" / "2024" / "03" / f"{day:02}"
        leaf.mkdir(parents=True)
        path = leaf / ("part" + suffix)
        path.write_text(json.dumps({"id": day, "name": str(day)}), encoding="utf-8")
        os.utime(path, (1700000000 + day * 3600, 1700000000 + day * 3600))
        paths.append(native_engine.platform.get_file_info(str(path)).path)
    conn = Connection(
        name="dated",
        connection_type="file",
        format="jsonl",
        configure={
            "base_path": str(tmp_path),
            "date_folder_partitions": "{year}/{month}/{day}",
        },
    )
    source = Source(
        connection=conn,
        table="events",
        watermark_columns=[FileInfoColumn.FILE_MODIFICATION_TIME],
    )
    result = FileReader(native_engine).read(
        source,
        request=SourceReadRequest(
            reference_time=datetime.now(timezone.utc),
            watermark_start={DATE_FOLDER_PARTITION_KEY: "2024-03-02T00:00:00+00:00"},
        ),
    )
    assert values(result.frame) == [{"id": 2, "name": "2"}]
    assert (
        result.candidate.values[DATE_FOLDER_PARTITION_KEY]
        == "2024-03-02T00:00:00+00:00"
    )
    if isinstance(result.frame, pl.LazyFrame):
        info = result.frame.select("__file_path", "__file_name").collect().to_dicts()
    else:
        info = [
            row.asDict()
            for row in result.frame.select("__file_path", "__file_name").collect()
        ]
        # Spark retains native input_file_name URI representation.
        assert info[0]["__file_path"].startswith("file:/")
        info[0]["__file_path"] = info[0]["__file_path"].removeprefix("file:/")
    assert info == [{"__file_path": paths[1], "__file_name": "part.jsonl"}]


@pytest.mark.parametrize(
    "fmt,options", [("json", {}), ("json", {"multiLine": False}), ("jsonl", {})]
)
def test_native_single_object(native_engine, tmp_path, fmt, options):
    path = tmp_path / "one.json"
    path.write_text(
        json.dumps(ROWS[0], indent=2)
        if fmt == "json" and not options
        else json.dumps(ROWS[0]),
        encoding="utf-8",
    )
    assert (
        values(native_engine.read(fmt=fmt, path=str(path), options=options)) == ROWS[:1]
    )
