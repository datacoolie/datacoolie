import pytest
from unittest.mock import MagicMock

from datacoolie.engines._spark import file_io
from datacoolie.core.exceptions import ConfigurationError, EngineError
from pyspark.sql.types import StructField, StructType, TimestampType


@pytest.mark.parametrize("multiline,value,expected", [
    (True, None, "true"), (True, False, "false"),
    (True, " TRUE ", "true"), (False, None, "false"), (False, "false", "false"),
])
def test_json_framing_keeps_native_paths_and_options(multiline, value, expected):
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader
    options = {"pathGlobFilter": "*.json", "basePath": "/root", "mode": "FAILFAST"}
    if value is not None:
        options["multiLine"] = value
    before = dict(options)
    paths = ["/root/day=*/part*.json", "/root/other.jsonl"]
    file_io.read_json(spark, paths, options, multiline=multiline)
    reader.load.assert_called_once_with(paths)
    actual = dict(call.args for call in reader.option.call_args_list)
    assert actual == {**options, "multiLine": expected}
    assert options == before


@pytest.mark.parametrize("multiline,value", [(False, True), (True, None), (True, 0), (True, "invalid")])
def test_json_invalid_options_fail_before_native_reader(multiline, value):
    spark = MagicMock()
    with pytest.raises(ConfigurationError):
        file_io.read_json(spark, "/empty", {"multiLine": value}, multiline=multiline)
    spark.read.format.assert_not_called()


def test_read_parquet_translates_hive_partition_option_without_mutating_input() -> None:
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader
    options = {"use_hive_partitioning": "/landing", "mergeSchema": "false"}

    file_io.read_parquet(spark, "/landing/day=1", options)

    assert options == {"use_hive_partitioning": "/landing", "mergeSchema": "false"}
    assert [call.args for call in reader.option.call_args_list] == [
        ("mergeSchema", "false"),
        ("basePath", "/landing"),
    ]


def test_write_jsonl_uses_json_and_caller_options_override_defaults() -> None:
    df = MagicMock()
    writer = df.write.format.return_value.mode.return_value
    writer.partitionBy.return_value = writer
    writer.option.return_value = writer

    file_io.write_to_path(
        df, "/out", "append", "jsonl", ["day"], {"mergeSchema": "false"}
    )

    df.write.format.assert_called_once_with("json")
    writer.partitionBy.assert_called_once_with("day")
    writer.option.assert_called_once_with("mergeSchema", "false")
    writer.save.assert_called_once_with("/out")


def test_write_full_load_uses_overwrite_schema_default() -> None:
    df = MagicMock()
    writer = df.write.format.return_value.mode.return_value
    writer.option.return_value = writer

    file_io.write_to_path(df, "/out", "full_load", "jsonl", None, None)

    writer.option.assert_called_once_with("overwriteSchema", "true")


def test_write_rejects_conflicting_schema_controls_before_writer() -> None:
    df = MagicMock()
    with pytest.raises(EngineError, match="mergeSchema.*overwriteSchema"):
        file_io.write_to_path(
            df,
            "/out",
            "append",
            "jsonl",
            None,
            {"mergeSchema": "true", "overwriteSchema": "false"},
        )
    df.write.format.assert_not_called()


def test_write_rejects_int96_for_instant_timestamps() -> None:
    df = MagicMock()
    df.schema = StructType([StructField("created_at", TimestampType(), True)])
    df.sparkSession.conf.get.return_value = "INT96"

    with pytest.raises(EngineError, match="TIMESTAMP_MICROS"):
        file_io.write_to_path(df, "/out", "overwrite", "parquet", None, None)
