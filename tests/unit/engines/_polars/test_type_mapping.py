"""Tests for private Polars type conversion rules."""

import polars as pl

from datacoolie.engines._polars.type_mapping import (
    build_cast_expr,
    polars_type_to_hive,
    to_chrono_format,
)


def test_scalar_and_unsigned_hive_types() -> None:
    expected = {
        pl.Int64(): "BIGINT",
        pl.Int32(): "INT",
        pl.Int16(): "SMALLINT",
        pl.Int8(): "TINYINT",
        pl.UInt8(): "SMALLINT",
        pl.UInt16(): "INT",
        pl.UInt32(): "BIGINT",
        pl.UInt64(): "BIGINT",
        pl.Float32(): "FLOAT",
        pl.Float64(): "DOUBLE",
        pl.Boolean(): "BOOLEAN",
        pl.String(): "STRING",
        pl.Binary(): "BINARY",
        pl.Date(): "DATE",
    }
    assert {dtype: polars_type_to_hive(dtype) for dtype in expected} == expected


def test_nested_and_temporal_hive_types() -> None:
    assert polars_type_to_hive(pl.Datetime("us", "UTC")) == "TIMESTAMP"
    assert polars_type_to_hive(pl.Datetime("us")) == "TIMESTAMP"
    assert polars_type_to_hive(pl.Decimal(10, 2)) == "DECIMAL(10,2)"
    assert polars_type_to_hive(pl.Duration("us")) == "STRING"
    assert polars_type_to_hive(pl.Time()) == "STRING"
    assert polars_type_to_hive(pl.Null()) == "STRING"
    assert polars_type_to_hive(pl.List(pl.List(pl.String()))) == (
        "ARRAY<ARRAY<STRING>>"
    )
    dtype = pl.Struct({"a": pl.Int64(), "b": pl.String()})
    assert polars_type_to_hive(dtype) == "STRUCT<a:BIGINT,b:STRING>"


def test_chrono_format_conversion() -> None:
    assert to_chrono_format("yyyy-MM-dd HH:mm:ss") == "%Y-%m-%d %H:%M:%S"
    assert to_chrono_format("%Y/%m/%d") == "%Y/%m/%d"


def test_cast_expression_supports_decimal_and_unknown_type() -> None:
    frame = pl.DataFrame({"value": ["1.25"]}).lazy()
    decimal_expr = build_cast_expr("value", "DECIMAL(10,2)", pl.String(), None)
    assert decimal_expr is not None
    assert frame.select(decimal_expr.alias("value")).collect().schema["value"] == (
        pl.Decimal(10, 2)
    )
    assert build_cast_expr("value", "not-a-type", pl.String(), None) is None
