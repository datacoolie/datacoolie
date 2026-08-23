from pyspark.sql import types as T

from datacoolie.engines._spark import type_mapping


def test_resolve_parameterized_alias() -> None:
    assert type_mapping.resolve_type("NUMERIC(18,2)") == "decimal(18,2)"


def test_unknown_type_is_not_resolved() -> None:
    assert type_mapping.resolve_type("geography") is None


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
