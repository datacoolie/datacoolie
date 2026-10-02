"""Tests for private Polars type conversion rules."""

from datetime import date, datetime

import polars as pl
import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.engines._polars.type_mapping import (
    build_cast_expr,
    normalize_output_frame,
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


def test_cast_expression_supports_decimal_and_rejects_unknown_type() -> None:
    frame = pl.DataFrame({"value": ["1.25"]}).lazy()
    decimal_expr = build_cast_expr("value", "DECIMAL(10,2)", pl.String(), None)
    assert decimal_expr is not None
    assert frame.select(decimal_expr.alias("value")).collect().schema["value"] == (
        pl.Decimal(10, 2)
    )
    with pytest.raises(ConfigurationError, match="Unsupported Spark SQL"):
        build_cast_expr("value", "not-a-type", pl.String(), None)


def test_cast_expression_uses_shared_float_widths() -> None:
    frame = pl.DataFrame({"value": ["1.25"]})
    float_frame = frame.select(
        build_cast_expr("value", "float", pl.String(), None).alias("value")
    )
    double_frame = frame.select(
        build_cast_expr("value", "double", pl.String(), None).alias("value")
    )

    assert float_frame.schema["value"] == pl.Float32
    assert double_frame.schema["value"] == pl.Float64


def test_cast_expression_resolves_source_dialect_without_canonical_strings() -> None:
    frame = pl.DataFrame({"value": ["255"]}).lazy()
    expr = build_cast_expr(
        "value",
        "tinyint unsigned",
        pl.String(),
        None,
        type_system="mysql",
    )
    assert frame.select(expr.alias("value")).collect().schema["value"] == pl.Int16


def test_typed_temporal_values_do_not_use_string_format_parser() -> None:
    date_frame = pl.DataFrame({"value": [date(2024, 1, 2)]}).lazy()
    date_expr = build_cast_expr("value", "date", pl.Date(), "yyyy-MM-dd")
    assert date_frame.select(date_expr.alias("value")).collect().schema["value"] == pl.Date

    timestamp_frame = pl.DataFrame(
        {"value": [datetime(2024, 1, 2, 3, 4, 5)]}
    ).lazy()
    timestamp_expr = build_cast_expr(
        "value", "timestamp_ntz", pl.Datetime("us"), "yyyy-MM-dd HH:mm:ss"
    )
    assert timestamp_frame.select(timestamp_expr.alias("value")).collect().schema[
        "value"
    ] == pl.Datetime("us")


def test_mysql_year_preserves_integer_values() -> None:
    frame = pl.DataFrame({"value": [2024, 0, None, 1999]}).lazy()
    expr = build_cast_expr(
        "value", "YEAR", pl.Int64(), None, type_system="mysql"
    )
    result = frame.select(expr.alias("value")).collect()
    assert result.schema["value"] == pl.Int16
    assert result["value"].to_list() == [2024, 0, None, 1999]


def test_output_normalization_keeps_parquet_and_delta_signed_widths() -> None:
    frame = pl.DataFrame(
        {"tiny": pl.Series([-1, 1], dtype=pl.Int8)}
    ).lazy()

    for output_format in ("parquet", "delta"):
        normalized = normalize_output_frame(frame, output_format)
        assert normalized.collect_schema()["tiny"] == pl.Int8


def test_output_normalization_promotes_iceberg_signed_widths() -> None:
    frame = pl.DataFrame(
        {
            "tiny": pl.Series([-1, 1], dtype=pl.Int8),
            "small": pl.Series([-1, 1], dtype=pl.Int16),
        }
    ).lazy()

    normalized = normalize_output_frame(frame, "iceberg")

    assert normalized.collect_schema()["tiny"] == pl.Int32
    assert normalized.collect_schema()["small"] == pl.Int32


def test_output_normalization_preserves_unsigned_ranges() -> None:
    frame = pl.DataFrame(
        {
            "u8": pl.Series([255], dtype=pl.UInt8),
            "u16": pl.Series([65535], dtype=pl.UInt16),
            "u64": pl.Series([2**64 - 1], dtype=pl.UInt64),
        }
    ).lazy()

    normalized = normalize_output_frame(frame, "parquet").collect()

    assert normalized.schema["u8"] == pl.Int16
    assert normalized.schema["u16"] == pl.Int32
    assert normalized.schema["u64"] == pl.Decimal(20, 0)
    assert normalized["u64"].to_list() == [18446744073709551615]


def test_output_normalization_rejects_unbounded_int128() -> None:
    if not hasattr(pl, "Int128"):
        pytest.skip("This Polars version has no Int128 dtype")
    frame = pl.DataFrame({"value": pl.Series([1], dtype=pl.Int128)}).lazy()
    with pytest.raises(ConfigurationError, match="Int128"):
        normalize_output_frame(frame, "parquet")


def test_output_normalization_covers_later_derived_narrow_columns() -> None:
    frame = (
        pl.DataFrame({"base": pl.Series([-1, 1], dtype=pl.Int8)})
        .lazy()
        .with_columns((pl.col("base") * 1).cast(pl.Int8).alias("derived"))
    )

    normalized = normalize_output_frame(frame, "iceberg").collect()

    assert normalized.schema["base"] == pl.Int32
    assert normalized.schema["derived"] == pl.Int32
    assert normalized.to_dict(as_series=False) == {
        "base": [-1, 1],
        "derived": [-1, 1],
    }
