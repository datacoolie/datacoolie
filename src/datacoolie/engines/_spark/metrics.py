"""Spark metric and schema inspection operations."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf

from datacoolie.engines._spark.type_mapping import spark_type_to_hive


def count_rows(df: DataFrame) -> int:
    return df.count()


def is_empty(df: DataFrame) -> bool:
    return df.isEmpty()


def get_columns(df: DataFrame) -> List[str]:
    return df.columns  # type: ignore[return-value]


def get_schema(df: DataFrame) -> Dict[str, str]:
    return {field.name: str(field.dataType) for field in df.schema.fields}


def get_hive_schema(df: DataFrame) -> Dict[str, str]:
    return {
        field.name: spark_type_to_hive(field.dataType) for field in df.schema.fields
    }


def get_max_values(df: DataFrame, columns: List[str]) -> Dict[str, Any]:
    return (
        df.agg(*[sf.max(column).alias(column) for column in columns])
        .collect()[0]
        .asDict()
    )


def get_count_and_max_values(
    df: DataFrame,
    columns: List[str],
) -> Tuple[int, Dict[str, Any]]:
    expressions = [sf.count(sf.lit(1)).alias("__row_count")]
    expressions.extend(sf.max(column).alias(column) for column in columns)
    values = df.agg(*expressions).collect()[0].asDict()
    count = values.pop("__row_count", 0)
    return count, values
