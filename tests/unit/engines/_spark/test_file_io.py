from unittest.mock import MagicMock

from datacoolie.engines._spark import file_io


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
