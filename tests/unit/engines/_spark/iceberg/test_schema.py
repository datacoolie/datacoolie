from unittest.mock import MagicMock

from datacoolie.engines._spark.iceberg import schema


def test_prepare_table_fetches_schema_once_when_no_evolution_or_partitions() -> None:
    spark = MagicMock()
    spark.table.return_value.schema.fields = []
    df = MagicMock()
    df.schema.fields = []

    schema.prepare_table(spark, df, "catalog.namespace.table", None)

    spark.table.assert_called_once_with("catalog.namespace.table")
    spark.sql.assert_not_called()
