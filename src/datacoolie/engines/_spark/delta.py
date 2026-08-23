"""Path-oriented Spark Delta operations and Delta maintenance."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf

from datacoolie.core.constants import Format, LoadType, SCD2Column, SystemColumn
from datacoolie.core.exceptions import EngineError
from datacoolie.engines._spark import file_io, runtime
from datacoolie.engines._spark import temporal


def delta_table() -> type:
    from delta import DeltaTable  # noqa: PLC0415

    return DeltaTable


def merge_to_path(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: List[str],
    fmt: str,
) -> None:
    if fmt.lower() != Format.DELTA.value:
        raise EngineError(f"merge_to_path only supports delta format, got {fmt!r}")
    condition = " AND ".join(
        f"target.`{column}` = source.`{column}`" for column in merge_keys
    )
    excluded = set(merge_keys)
    excluded.add(SystemColumn.CREATED_AT)
    updates = {
        f"`{column}`": f"source.`{column}`"
        for column in df.columns
        if column not in excluded
    }
    inserts = {f"`{column}`": f"source.`{column}`" for column in df.columns}
    try:
        (
            delta_table()
            .forPath(spark, path)
            .alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdate(set=updates)
            .whenNotMatchedInsert(values=inserts)
            .execute()
        )
    except Exception as exc:
        raise EngineError(
            f"Merge failed — target path does not exist or is not a {fmt} table: {path}"
        ) from exc


def merge_overwrite_to_path(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: List[str],
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
) -> None:
    if fmt.lower() != Format.DELTA.value:
        raise EngineError(
            f"merge_overwrite_to_path only supports delta format, got {fmt!r}"
        )
    df = runtime.safe_cache(df)
    try:
        keys = df.select(merge_keys).dropDuplicates()
        condition = " AND ".join(
            f"target.`{column}` = source.`{column}`" for column in merge_keys
        )
        (
            delta_table()
            .forPath(spark, path)
            .alias("target")
            .merge(keys.alias("source"), condition)
            .whenMatchedDelete()
            .execute()
        )
        file_io.write_to_path(
            df, path, LoadType.APPEND.value, fmt, partition_columns, options
        )
    finally:
        runtime.safe_unpersist(df)


def delete_by_window_path(
    path: str, window: Dict[str, tuple], fmt: str, spark: SparkSession
) -> None:
    if fmt.lower() != Format.DELTA.value:
        raise EngineError(
            f"delete_by_window_path only supports delta format, got {fmt!r}"
        )
    delta_table().forPath(spark, path).delete(temporal.build_window_predicate(window))


def scd2_to_path(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: List[str],
    fmt: str,
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
) -> None:
    if fmt.lower() != Format.DELTA.value:
        raise EngineError(f"scd2_to_path only supports Delta, got {fmt!r}")
    key_condition = " AND ".join(
        f"target.`{key}` = source.`{key}`" for key in merge_keys
    )
    condition = f"{key_condition} AND target.`{SCD2Column.IS_CURRENT.value}` = true"
    updates = {
        f"`{SCD2Column.VALID_TO.value}`": f"source.`{SCD2Column.VALID_FROM.value}`",
        f"`{SCD2Column.IS_CURRENT.value}`": "false",
    }
    late_guard = (
        f"source.`{SCD2Column.VALID_FROM.value}` > "
        f"target.`{SCD2Column.VALID_FROM.value}`"
    )
    try:
        (
            delta_table()
            .forPath(spark, path)
            .alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdate(condition=late_guard, set=updates)
            .execute()
        )
    except Exception as exc:
        raise EngineError(
            f"SCD2 merge failed — target path does not exist or is not a Delta table: {path}"
        ) from exc
    file_io.write_to_path(
        df, path, LoadType.APPEND.value, fmt, partition_columns, options
    )


def table_exists_by_path(spark: SparkSession, platform: Any, path: str) -> bool:
    try:
        if platform:
            return platform.folder_exists(f"{path.rstrip('/')}/_delta_log")
        spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
        return True
    except Exception:  # noqa: BLE001
        return False


def get_history(
    spark: SparkSession,
    identifier: str,
    limit: int,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    *,
    is_path: bool,
) -> List[Dict[str, Any]]:
    try:
        history = (
            delta_table().forPath(spark, identifier).history(limit)
            if is_path
            else delta_table().forName(spark, identifier).history(limit)
        )
    except Exception:  # noqa: BLE001
        return []
    start, end = temporal.align_ms_boundaries(start_time, end_time)
    if start is not None:
        history = history.where(sf.col("timestamp") >= start)
    if end is not None:
        history = history.where(sf.col("timestamp") <= end)
    return [row.asDict() for row in history.collect()]


def compact_by_path(spark: SparkSession, path: str) -> None:
    delta_table().forPath(spark, path).optimize().executeCompaction()


def compact_by_name(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"OPTIMIZE {table_name}")


def cleanup_by_path(spark: SparkSession, path: str, retention_hours: int) -> None:
    delta_table().forPath(spark, path).vacuum(float(retention_hours))


def cleanup_by_name(spark: SparkSession, table_name: str, retention_hours: int) -> None:
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")


def generate_symlink_manifest(spark: SparkSession, path: str) -> None:
    delta_table().forPath(spark, path).generate("symlink_format_manifest")
