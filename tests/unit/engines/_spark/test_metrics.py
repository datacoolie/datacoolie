from unittest.mock import MagicMock, patch

from pyspark.sql.types import LongType, StringType, StructField

from datacoolie.engines._spark import metrics


def test_count_and_empty_delegate_to_dataframe_actions() -> None:
    df = MagicMock()
    df.count.return_value = 2
    df.isEmpty.return_value = False

    assert metrics.count_rows(df) == 2
    assert not metrics.is_empty(df)


def test_schema_helpers_use_native_spark_types() -> None:
    df = MagicMock()
    df.columns = ["id", "name"]
    df.schema.fields = [
        StructField("id", LongType()),
        StructField("name", StringType()),
    ]

    assert metrics.get_columns(df) == ["id", "name"]
    assert metrics.get_schema(df) == {"id": "LongType()", "name": "StringType()"}
    assert metrics.get_hive_schema(df) == {"id": "BIGINT", "name": "STRING"}


def test_max_values_uses_one_aggregation_action() -> None:
    df = MagicMock()
    aggregate = df.agg.return_value
    row = aggregate.collect.return_value[0]
    row.asDict.return_value = {"id": 9}
    maximum = MagicMock()

    with patch.object(metrics.sf, "max", return_value=maximum):
        assert metrics.get_max_values(df, ["id"]) == {"id": 9}

    maximum.alias.assert_called_once_with("id")
    df.agg.assert_called_once()
    aggregate.collect.assert_called_once_with()
