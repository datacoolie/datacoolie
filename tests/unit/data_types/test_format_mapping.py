"""Pure persisted-format mapping tests."""

from __future__ import annotations

import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.engines.data_types.formats import output_type_for_format


def test_parquet_and_delta_keep_logical_integer_width() -> None:
    assert output_type_for_format("tinyint", "parquet") == "tinyint"
    assert output_type_for_format("tinyint", "delta") == "tinyint"


def test_iceberg_promotes_small_integrals() -> None:
    assert (
        output_type_for_format(
            "smallint", "iceberg"
        )
        == "int"
    )
    assert (
        output_type_for_format(
            "bigint", "iceberg"
        )
        == "bigint"
    )


def test_iceberg_keeps_unsigned_range_safe_after_promotion() -> None:
    assert (
        output_type_for_format(
            "smallint",
            "iceberg",
        )
        == "int"
    )
    assert (
        output_type_for_format(
            "decimal(20,0)",
            "iceberg",
        )
        == "decimal(20,0)"
    )


def test_format_mapping_rejects_unknown_format() -> None:
    with pytest.raises(ConfigurationError, match="Unsupported datatype output format"):
        output_type_for_format("decimal(10,2)", "csv")
