"""Spark Iceberg schema evolution and partition-spec operations."""

from __future__ import annotations

from typing import List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import types as T

from datacoolie.core.constants import TRAILING_COLUMNS
from datacoolie.logging.base import get_logger

logger = get_logger(__name__)


def align_df_to_table_schema(
    spark: SparkSession, df: DataFrame, table_name: str
) -> DataFrame:
    table_fields = spark.table(table_name).schema.fields
    df_columns = {column.lower(): column for column in df.columns}
    expressions = [
        sf.col(f"`{df_columns[field.name.lower()]}`").alias(field.name)
        if field.name.lower() in df_columns
        else sf.lit(None).cast(field.dataType).alias(field.name)
        for field in table_fields
    ]
    return df.select(*expressions)


def evolve_schema(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    existing_column_names: Optional[List[str]] = None,
) -> List[T.StructField]:
    if existing_column_names is None:
        existing_column_names = [
            field.name for field in spark.table(table_name).schema.fields
        ]
    existing_lower = {column.lower() for column in existing_column_names}
    new_fields = [
        field for field in df.schema.fields if field.name.lower() not in existing_lower
    ]
    if not new_fields:
        return []
    definitions = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}" for field in new_fields
    )
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({definitions})")
    logger.info(
        "Evolved Iceberg schema for %s: added %s",
        table_name,
        [field.name for field in new_fields],
    )
    return new_fields


def reorder_trailing_columns(
    spark: SparkSession,
    table_name: str,
    current_columns: Optional[List[str]] = None,
) -> bool:
    if current_columns is None:
        current_columns = [
            field.name for field in spark.table(table_name).schema.fields
        ]
    trailing_lower = {column.lower() for column in TRAILING_COLUMNS}
    by_lower = {column.lower(): column for column in current_columns}
    leading = [
        column for column in current_columns if column.lower() not in trailing_lower
    ]
    trailing = [
        by_lower[column.lower()]
        for column in TRAILING_COLUMNS
        if column.lower() in by_lower
    ]
    if not trailing or current_columns == leading + trailing:
        return False
    previous = leading[-1] if leading else None
    for column in trailing:
        if previous is None:
            spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN `{column}` FIRST")
        else:
            spark.sql(
                f"ALTER TABLE {table_name} ALTER COLUMN `{column}` AFTER `{previous}`"
            )
        previous = column
    logger.debug("Reordered trailing columns for %s", table_name)
    return True


def existing_identity_partitions(spark: SparkSession, table_name: str) -> set[str]:
    try:
        rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()
    except Exception:  # noqa: BLE001
        logger.debug(
            "Could not parse partition spec for %s; will attempt ADD PARTITION FIELD unconditionally",
            table_name,
        )
        return set()
    existing: set[str] = set()
    in_partitioning = False
    for row in rows:
        column_name = (row[0] or "").strip()
        if "# partition" in column_name.lower():
            in_partitioning = True
            continue
        if not in_partitioning:
            continue
        if column_name.startswith("#"):
            continue
        if not column_name:
            break
        existing.add(column_name.lower())
    return existing


def ensure_partition_spec(
    spark: SparkSession,
    table_name: str,
    partition_columns: Sequence[str],
) -> None:
    existing = existing_identity_partitions(spark, table_name)
    for column in partition_columns:
        if column.lower() in existing:
            continue
        spark.sql(f"ALTER TABLE {table_name} ADD PARTITION FIELD `{column}`")
        logger.info("Added identity partition field `%s` to %s", column, table_name)


def prepare_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    partition_columns: Optional[Sequence[str]],
) -> None:
    current_columns = [field.name for field in spark.table(table_name).schema.fields]
    new_fields = evolve_schema(spark, df, table_name, current_columns)
    if new_fields:
        reorder_trailing_columns(
            spark, table_name, current_columns + [field.name for field in new_fields]
        )
    if partition_columns:
        ensure_partition_spec(spark, table_name, partition_columns)
