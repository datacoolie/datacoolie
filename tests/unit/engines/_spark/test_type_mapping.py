import pytest

from datacoolie.core.exceptions import ConfigurationError
from pyspark.sql import types as T

from datacoolie.engines._spark import type_mapping


def test_resolve_parameterized_alias() -> None:
    resolved = type_mapping.resolve_type("decimal(18,2)")
    assert resolved.precision == 18
    assert resolved.scale == 2


def test_resolve_float_aliases_keeps_declared_width() -> None:
    assert type_mapping.resolve_type("float").bit_width == 32
    assert type_mapping.resolve_type("double").bit_width == 64


def test_unknown_type_is_not_resolved() -> None:
    with pytest.raises(ConfigurationError, match="Unsupported Spark SQL"):
        type_mapping.resolve_type("geography")


def test_recursive_hive_type_mapping() -> None:
    dtype = T.StructType(
        [
            T.StructField("ids", T.ArrayType(T.LongType())),
            T.StructField("labels", T.MapType(T.StringType(), T.StringType())),
        ]
    )

    assert (
        type_mapping.spark_type_to_hive(dtype)
        == "STRUCT<ids:ARRAY<BIGINT>,labels:MAP<STRING,STRING>>"
    )
