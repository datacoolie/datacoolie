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
from datacoolie.engines.contracts.windows import WindowSpec


def delta_table() -> type:
    from delta import DeltaTable  # noqa: PLC0415

    return DeltaTable


def _merge_parts(
    merge_keys: List[str], options: Optional[Dict[str, str]] = None
) -> tuple[str, str, str]:
    """Resolve the portable merge aliases/predicate for Spark Delta.

    ``merge_options`` is intentionally consumed here, at the MERGE boundary;
    writer options are passed separately to the later append operation.
    """
    merged = dict(options or {})
    source_alias = merged.get("source_alias", "source")
    target_alias = merged.get("target_alias", "target")
    predicate = merged.get("predicate")
    if not isinstance(source_alias, str) or not isinstance(target_alias, str):
        raise EngineError("Spark Delta merge aliases must be strings")
    if not source_alias.strip() or not target_alias.strip():
        raise EngineError("Spark Delta merge aliases must be non-empty strings")
    if predicate is None:
        predicate = " AND ".join(
            f"{target_alias}.`{column}` = {source_alias}.`{column}`"
            for column in merge_keys
        )
    elif not isinstance(predicate, str):
        raise EngineError("Spark Delta merge predicate must be a string")
    elif not predicate.strip():
        raise EngineError("Spark Delta merge predicate must be non-empty")
    return source_alias, target_alias, predicate


def _validate_write_options(
    options: Optional[Dict[str, str]], *, mode: str
) -> Dict[str, str]:
    """Share the canonical Spark writer validation at compound write boundaries."""

    return file_io.validate_write_options(options, mode=mode)


def merge_to_path(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: List[str],
    fmt: str,
    options: Optional[Dict[str, str]] = None,
) -> None:
    if fmt.lower() != Format.DELTA.value:
        raise EngineError(f"merge_to_path only supports delta format, got {fmt!r}")
    file_io.require_portable_timestamp_output(df, fmt)
    source_alias, target_alias, condition = _merge_parts(merge_keys, options)
    excluded = set(merge_keys)
    excluded.add(SystemColumn.CREATED_AT)
    updates = {
        f"`{column}`": f"{source_alias}.`{column}`"
        for column in df.columns
        if column not in excluded
    }
    inserts = {
        f"`{column}`": f"{source_alias}.`{column}`" for column in df.columns
    }
    try:
        (
            delta_table()
            .forPath(spark, path)
            .alias(target_alias)
            .merge(df.alias(source_alias), condition)
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
    write_options: Optional[Dict[str, str]] = None,
) -> None:
    if fmt.lower() != Format.DELTA.value:
        raise EngineError(
            f"merge_overwrite_to_path only supports delta format, got {fmt!r}"
        )
    file_io.require_portable_timestamp_output(df, fmt)
    source_alias, target_alias, condition = _merge_parts(merge_keys, options)
    validated_write_options = _validate_write_options(
        write_options, mode=LoadType.APPEND.value
    )
    df = runtime.safe_cache(df)
    try:
        keys = df.select(merge_keys).dropDuplicates()
        (
            delta_table()
            .forPath(spark, path)
            .alias(target_alias)
            .merge(keys.alias(source_alias), condition)
            .whenMatchedDelete()
            .execute()
        )
        file_io.write_to_path(
            df,
            path,
            LoadType.APPEND.value,
            fmt,
            partition_columns,
            validated_write_options,
        )
    finally:
        runtime.safe_unpersist(df)


def delete_by_window_path(
    path: str, window: WindowSpec, fmt: str, spark: SparkSession
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
    write_options: Optional[Dict[str, str]] = None,
) -> None:
    if fmt.lower() != Format.DELTA.value:
        raise EngineError(f"scd2_to_path only supports Delta, got {fmt!r}")
    file_io.require_portable_timestamp_output(df, fmt)
    validated_write_options = _validate_write_options(
        write_options, mode=LoadType.APPEND.value
    )
    source_alias, target_alias, key_condition = _merge_parts(merge_keys, options)
    condition = (
        f"({key_condition}) AND {target_alias}.`{SCD2Column.IS_CURRENT.value}` = true"
    )
    updates = {
        f"`{SCD2Column.VALID_TO.value}`": (
            f"{source_alias}.`{SCD2Column.VALID_FROM.value}`"
        ),
        f"`{SCD2Column.IS_CURRENT.value}`": "false",
    }
    late_guard = (
        f"{source_alias}.`{SCD2Column.VALID_FROM.value}` > "
        f"{target_alias}.`{SCD2Column.VALID_FROM.value}`"
    )
    try:
        (
            delta_table()
            .forPath(spark, path)
            .alias(target_alias)
            .merge(df.alias(source_alias), condition)
            .whenMatchedUpdate(condition=late_guard, set=updates)
            .execute()
        )
    except Exception as exc:
        raise EngineError(
            f"SCD2 merge failed — target path does not exist or is not a Delta table: {path}"
        ) from exc
    file_io.write_to_path(
        df,
        path,
        LoadType.APPEND.value,
        fmt,
        partition_columns,
        validated_write_options,
    )


def table_exists_by_path(spark: SparkSession, platform: Any, path: str) -> bool:
    if platform is not None and not platform.folder_exists(
        f"{path.rstrip('/')}/_delta_log"
    ):
        if file_io.path_is_absent_or_empty(spark, platform, path):
            return False
        raise EngineError(
            f"Spark Delta target exists but has no _delta_log: {path}"
        )
    if file_io.path_is_absent_or_empty(spark, platform, path):
        return False
    try:
        spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
    except Exception as exc:  # noqa: BLE001
        error_class = getattr(exc, "getErrorClass", lambda: None)()
        if error_class == "DELTA_MISSING_DELTA_TABLE":
            raise EngineError(
                f"Spark Delta target exists but is not a Delta table: {path}"
            ) from exc
        raise
    return True


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
