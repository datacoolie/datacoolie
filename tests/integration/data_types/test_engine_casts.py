"""Qualification cells for the shared native Spark/Polars cast contract."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.transform import SchemaHint
from datacoolie.core.models.source import Source
from datacoolie.core.models.transform import Transform
from datacoolie.transformers.schema_converter import SchemaConverter


pytestmark = pytest.mark.datatype_qualification


def _cast_dataflow(
    *,
    destination_format: str = "parquet",
    schema_hints: list[SchemaHint] | None = None,
) -> DataFlow:
    source = Connection(
        name="qualification-source",
        connection_type="file",
        format="parquet",
        configure={"use_schema_hint": True, "schema_hint_type_system": "spark_sql"},
    )
    destination = Connection(
        name="qualification-output",
        connection_type=(
            "lakehouse" if destination_format in {"delta", "iceberg"} else "file"
        ),
        format=destination_format,
        configure={"base_path": ".scratch/datatype-qualification"},
    )
    return DataFlow(
        source=Source(connection=source, table="input"),
        destination=Destination(connection=destination, table="output"),
        transform=Transform(
            schema_hints=schema_hints
            or [
                SchemaHint(column_name="amount", data_type="decimal(10,2)"),
                SchemaHint(column_name="wall_clock", data_type="timestamp_ntz"),
            ],
        ),
    )


def test_polars_matches_shared_decimal_and_ntz_contract() -> None:
    polars = pytest.importorskip("polars")
    from datacoolie.engines.polars_engine import PolarsEngine

    frame = polars.DataFrame(
        {
            "amount": polars.Series(
                [Decimal("12.30"), None], dtype=polars.Decimal(10, 2)
            ),
            "wall_clock": polars.Series(
                [datetime(2024, 1, 15, 10, 30, 45), None],
                dtype=polars.Datetime("us"),
            ),
        }
    ).lazy()
    engine = PolarsEngine()
    converted = SchemaConverter(engine).transform(frame, _cast_dataflow())

    schema = converted.collect_schema()
    assert schema["amount"] == polars.Decimal(10, 2)
    assert schema["wall_clock"] == polars.Datetime("us")
    values = converted.collect()
    assert str(values["amount"].to_list()[0]) == "12.30"
    assert values["wall_clock"].to_list()[0] == datetime(2024, 1, 15, 10, 30, 45)


def test_polars_keeps_logical_integer_at_cast_boundary() -> None:
    polars = pytest.importorskip("polars")
    from datacoolie.engines.polars_engine import PolarsEngine

    frame = polars.DataFrame(
        {"value": polars.Series([-128, 127], dtype=polars.Int8)}
    ).lazy()
    dataflow = _cast_dataflow(
        destination_format="iceberg",
        schema_hints=[SchemaHint(column_name="value", data_type="tinyint")],
    )

    converted = SchemaConverter(PolarsEngine()).transform(frame, dataflow)

    assert converted.collect_schema()["value"] == polars.Int8
    assert converted.collect()["value"].to_list() == [-128, 127]


@pytest.mark.spark
@pytest.mark.xdist_group("spark")
def test_spark_matches_shared_decimal_and_ntz_contract() -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession, types as spark_types

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("datacoolie-datatype-qualification")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        frame = spark.createDataFrame(
            [
                (Decimal("12.30"), datetime(2024, 1, 15, 10, 30, 45)),
                (None, None),
            ],
            schema=spark_types.StructType(
                [
                    spark_types.StructField(
                        "amount", spark_types.DecimalType(10, 2), nullable=True
                    ),
                    spark_types.StructField(
                        "wall_clock", spark_types.TimestampNTZType(), nullable=True
                    ),
                ]
            ),
        )
        from datacoolie.engines.spark_engine import SparkEngine

        engine = SparkEngine(spark_session=spark)
        converted = SchemaConverter(engine).transform(frame, _cast_dataflow())
        fields = {field.name: field.dataType for field in converted.schema.fields}
        assert fields["amount"] == spark_types.DecimalType(10, 2)
        assert fields["wall_clock"] == spark_types.TimestampNTZType()
        assert str(converted.filter("amount IS NOT NULL").first()["amount"]) == "12.30"

        small_integer = spark.createDataFrame(
            [(-128,), (127,)],
            schema=spark_types.StructType(
                [spark_types.StructField("value", spark_types.ByteType())]
            ),
        )
        promoted = SchemaConverter(engine).transform(
            small_integer,
            _cast_dataflow(
                destination_format="iceberg",
                schema_hints=[SchemaHint(column_name="value", data_type="tinyint")],
            ),
        )
        assert promoted.schema["value"].dataType == spark_types.ByteType()

        from datacoolie.engines._spark import type_mapping

        iceberg_output = type_mapping.normalize_output_frame(
            small_integer, "iceberg"
        )
        parquet_output = type_mapping.normalize_output_frame(
            small_integer, "parquet"
        )
        assert iceberg_output.schema["value"].dataType == spark_types.IntegerType()
        assert parquet_output.schema["value"].dataType == spark_types.ByteType()
    finally:
        spark.stop()
