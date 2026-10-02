"""Cross-engine Parquet qualification for Spark instant and NTZ timestamps."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest


pytestmark = [
    pytest.mark.integration,
    pytest.mark.datatype_qualification,
    pytest.mark.spark,
]


def test_spark_and_polars_share_parquet_timestamp_contract(tmp_path) -> None:
    """Verify both read directions with an independently defined contract.

    The Spark SQL literal carries an explicit UTC offset.  This avoids using
    a Python ``datetime`` conversion whose naive/aware handling depends on the
    host timezone and would test PySpark's bridge rather than Parquet.
    """

    pytest.importorskip("polars")
    pytest.importorskip("pyspark")

    import polars as pl
    from pyspark.sql import SparkSession

    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.engines.spark_engine import SparkEngine

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("datacoolie-parquet-cross-engine-qualification")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )
    try:
        spark_engine = SparkEngine(spark_session=spark)
        polars_engine = PolarsEngine()
        expected_instant = datetime(2024, 1, 15, 3, 30, 45, 123456, tzinfo=timezone.utc)
        expected_wall_clock = datetime(2024, 1, 15, 10, 30, 45, 123456)

        spark_path = tmp_path / "spark-write"
        spark_frame = spark.sql(
            """
            SELECT
                1 AS id,
                CAST(12.30 AS DECIMAL(10,2)) AS amount,
                timestamp '2024-01-15 03:30:45.123456+00:00' AS instant,
                timestamp_ntz '2024-01-15 10:30:45.123456' AS wall_clock
            """
        )
        spark_engine.write_to_path(
            spark_frame, str(spark_path), mode="overwrite", fmt="parquet"
        )

        polars_frame = polars_engine.read_parquet(str(spark_path)).collect()
        if "__file_path" in polars_frame.columns:
            polars_frame = polars_frame.drop("__file_path")
        assert polars_frame.schema == {
            "id": pl.Int32,
            "amount": pl.Decimal(10, 2),
            "instant": pl.Datetime("us", "UTC"),
            "wall_clock": pl.Datetime("us"),
        }
        assert polars_frame.row(0) == (
            1,
            Decimal("12.30"),
            expected_instant,
            expected_wall_clock,
        )

        polars_path = tmp_path / "polars-write"
        polars_frame_for_write = pl.DataFrame(
            {
                "id": [1],
                "amount": pl.Series([Decimal("12.30")], dtype=pl.Decimal(10, 2)),
                "instant": pl.Series(
                    [expected_instant], dtype=pl.Datetime("us", "UTC")
                ),
                "wall_clock": pl.Series(
                    [expected_wall_clock], dtype=pl.Datetime("us")
                ),
            }
        ).lazy()
        polars_engine.write_to_path(
            polars_frame_for_write,
            str(polars_path),
            mode="overwrite",
            fmt="parquet",
        )

        spark_back = spark.read.parquet(str(polars_path))
        fields = {field.name: field.dataType for field in spark_back.schema.fields}
        from pyspark.sql import types as spark_types

        assert fields == {
            "id": spark_types.LongType(),
            "amount": spark_types.DecimalType(10, 2),
            "instant": spark_types.TimestampType(),
            "wall_clock": spark_types.TimestampNTZType(),
        }
        values = spark_back.selectExpr(
            "CAST(id AS BIGINT) AS id",
            "CAST(amount AS STRING) AS amount",
            "unix_micros(instant) AS instant_us",
            "date_format(wall_clock, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS wall_clock",
        ).first()
        assert values.id == 1
        assert values.amount == "12.30"
        assert values.instant_us == int(expected_instant.timestamp() * 1_000_000)
        assert values.wall_clock == expected_wall_clock.strftime("%Y-%m-%d %H:%M:%S.%f")
    finally:
        spark.stop()
