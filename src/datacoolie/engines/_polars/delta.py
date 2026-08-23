"""Path-oriented Delta Lake operations for Polars."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from datacoolie.core.constants import LoadType, SCD2Column, SystemColumn
from datacoolie.core.exceptions import EngineError
from datacoolie.logging.base import get_logger
from datacoolie.platforms.base import BasePlatform

logger = get_logger(__name__)

_SINK_DELTA_AVAILABLE = hasattr(pl.LazyFrame, "sink_delta")
_CHECKPOINT_INTERVAL = 10


def scan_delta(
    path: str,
    options: Optional[Dict[str, Any]],
    storage_options: Dict[str, Any],
) -> pl.LazyFrame:
    merged = dict(options or {})
    if storage_options:
        merged["storage_options"] = storage_options
    return pl.scan_delta(path, **merged)


def sink_or_write_delta(
    df: pl.LazyFrame, path: str, **kwargs: Any
) -> Any:
    """Use the lazy Delta sink when available, otherwise collect and write."""
    if _SINK_DELTA_AVAILABLE:
        return df.sink_delta(path, **kwargs)
    return df.collect().write_delta(path, **kwargs)


def post_commit(
    path: str, delta_table_cls: type, storage_options: Dict[str, Any]
) -> None:
    """Create periodic checkpoints and clean expired Delta metadata."""
    try:
        table = delta_table_cls(path, storage_options=storage_options or None)
        version = table.version()
        if version % _CHECKPOINT_INTERVAL == 0 and version > 0:
            table.create_checkpoint()
        table.cleanup_metadata()
    except Exception:  # noqa: BLE001
        logger.debug(
            "Delta post-commit maintenance skipped for %s", path, exc_info=True
        )


def write_path(
    df: pl.LazyFrame,
    path: str,
    mode: str,
    partition_columns: Optional[List[str]],
    options: Dict[str, Any],
    *,
    storage_options: Dict[str, Any],
    delta_table_cls: type,
) -> None:
    if mode in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value):
        delta_mode, schema_mode = "overwrite", "overwrite"
    elif mode == LoadType.APPEND.value:
        delta_mode, schema_mode = "append", "merge"
    else:
        raise EngineError(f"PolarsEngine._write_delta_path: unsupported mode {mode!r}")

    write_options: Dict[str, Any] = {}
    if storage_options:
        write_options["storage_options"] = storage_options
    delta_write_options = dict(options)
    if partition_columns:
        delta_write_options["partition_by"] = partition_columns
    delta_write_options.setdefault("schema_mode", schema_mode)
    write_options["delta_write_options"] = delta_write_options
    sink_or_write_delta(df, path, mode=delta_mode, **write_options)
    post_commit(path, delta_table_cls, storage_options)


def build_merge_options(
    merge_keys: List[str], options: Optional[Dict[str, str]] = None
) -> Tuple[Dict[str, Any], str, str]:
    merged: Dict[str, Any] = dict(options or {})
    merged.setdefault("source_alias", "source")
    merged.setdefault("target_alias", "target")
    source_alias = merged["source_alias"]
    target_alias = merged["target_alias"]
    merged["predicate"] = " AND ".join(
        f"{target_alias}.`{column}` = {source_alias}.`{column}`"
        for column in merge_keys
    )
    return merged, source_alias, target_alias


def raise_if_target_missing(exc: Exception, path: str, operation: str) -> None:
    name = type(exc).__name__
    message = str(exc).lower()
    if "NotFound" in name or "not found" in message or "does not exist" in message:
        raise EngineError(
            f"{operation} failed — target path does not exist or is not a Delta table: {path}"
        ) from exc


def merge_path(
    df: pl.LazyFrame,
    path: str,
    actual_columns: List[str],
    merge_keys: List[str],
    options: Optional[Dict[str, str]],
    *,
    storage_options: Dict[str, Any],
    delta_table_cls: type,
) -> None:
    merge_options, source_alias, _ = build_merge_options(merge_keys, options)
    write_options: Dict[str, Any] = {}
    if storage_options:
        write_options["storage_options"] = storage_options
    excluded = set(merge_keys) | {SystemColumn.CREATED_AT}
    updates = {
        column: f"{source_alias}.`{column}`"
        for column in actual_columns
        if column not in excluded
    }
    try:
        (
            sink_or_write_delta(
                df,
                path,
                mode="merge",
                delta_merge_options=merge_options,
                **write_options,
            )
            .when_matched_update(updates=updates)
            .when_not_matched_insert_all()
            .execute()
        )
    except Exception as exc:
        raise_if_target_missing(exc, path, "Merge")
        raise
    post_commit(path, delta_table_cls, storage_options)


def merge_overwrite_path(
    df: pl.LazyFrame,
    path: str,
    merge_keys: List[str],
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
    *,
    storage_options: Dict[str, Any],
    delta_table_cls: type,
) -> None:
    key_frame = df.select(merge_keys).unique()
    merge_options, _, _ = build_merge_options(merge_keys, options)
    write_options: Dict[str, Any] = {}
    if storage_options:
        write_options["storage_options"] = storage_options
    try:
        (
            sink_or_write_delta(
                key_frame,
                path,
                mode="merge",
                delta_merge_options=merge_options,
                **write_options,
            )
            .when_matched_delete()
            .execute()
        )
    except Exception as exc:
        raise_if_target_missing(exc, path, "Merge")
        raise
    write_path(
        df,
        path,
        LoadType.APPEND.value,
        partition_columns,
        dict(options or {}),
        storage_options=storage_options,
        delta_table_cls=delta_table_cls,
    )


def scd2_path(
    df: pl.LazyFrame,
    path: str,
    merge_keys: List[str],
    partition_columns: Optional[List[str]],
    options: Optional[Dict[str, str]],
    *,
    storage_options: Dict[str, Any],
    delta_table_cls: type,
) -> None:
    merge_options, source_alias, target_alias = build_merge_options(
        merge_keys, options
    )
    merge_options["predicate"] = (
        f"{merge_options['predicate']}"
        f" AND {target_alias}.`{SCD2Column.IS_CURRENT.value}` = true"
    )
    write_options: Dict[str, Any] = {}
    if storage_options:
        write_options["storage_options"] = storage_options
    updates = {
        SCD2Column.VALID_TO.value: (
            f"{source_alias}.`{SCD2Column.VALID_FROM.value}`"
        ),
        SCD2Column.IS_CURRENT.value: "false",
    }
    late_guard = (
        f"{source_alias}.`{SCD2Column.VALID_FROM.value}` > "
        f"{target_alias}.`{SCD2Column.VALID_FROM.value}`"
    )
    try:
        (
            sink_or_write_delta(
                df,
                path,
                mode="merge",
                delta_merge_options=merge_options,
                **write_options,
            )
            .when_matched_update(updates=updates, predicate=late_guard)
            .execute()
        )
    except Exception as exc:
        raise_if_target_missing(exc, path, "SCD2 merge")
        raise
    write_path(
        df,
        path,
        LoadType.APPEND.value,
        partition_columns,
        dict(options or {}),
        storage_options=storage_options,
        delta_table_cls=delta_table_cls,
    )


def delete_by_window_path(
    path: str,
    predicate: str,
    *,
    storage_options: Dict[str, Any],
    delta_table_cls: type,
) -> None:
    try:
        table = delta_table_cls(path, storage_options=storage_options or None)
        table.delete(predicate)
    except Exception as exc:
        raise_if_target_missing(exc, path, "DeleteByWindow")
        raise


def generate_manifest(
    path: str, *, storage_options: Dict[str, Any], delta_table_cls: type
) -> None:
    logger.debug("PolarsEngine: generating symlink manifest for %s", path)
    delta_table_cls(path, storage_options=storage_options or None).generate()


def table_exists(
    path: str,
    *,
    platform: Optional[BasePlatform],
    storage_options: Dict[str, Any],
    delta_table_cls: type,
) -> bool:
    try:
        if platform is not None:
            return platform.folder_exists(f"{path.rstrip('/')}/_delta_log")
        return delta_table_cls.is_deltatable(
            path, storage_options=storage_options or None
        )
    except Exception:  # noqa: BLE001
        return False


def history(
    path: str,
    limit: int,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    *,
    storage_options: Dict[str, Any],
    delta_table_cls: type,
) -> List[Dict[str, Any]]:
    try:
        entries = delta_table_cls(
            path, storage_options=storage_options or None
        ).history(limit)
    except Exception:  # noqa: BLE001
        return []
    result: List[Dict[str, Any]] = []
    for entry in entries:
        timestamp = entry.get("timestamp")
        if timestamp is not None and isinstance(timestamp, (int, float)):
            entry = {
                **entry,
                "timestamp": datetime.fromtimestamp(
                    timestamp / 1000, tz=timezone.utc
                ),
            }
        timestamp_value = entry.get("timestamp")
        if start_time is not None and timestamp_value is not None:
            if timestamp_value <= start_time:
                continue
        if end_time is not None and timestamp_value is not None:
            if timestamp_value >= end_time:
                continue
        result.append(entry)
    return result


def compact(
    path: str, *, storage_options: Dict[str, Any], delta_table_cls: type
) -> None:
    table = delta_table_cls(path, storage_options=storage_options or None)
    table.optimize.compact()


def cleanup(
    path: str,
    retention_hours: int,
    *,
    storage_options: Dict[str, Any],
    delta_table_cls: type,
) -> None:
    table = delta_table_cls(path, storage_options=storage_options or None)
    table.vacuum(
        retention_hours=retention_hours, enforce_retention_duration=False
    )
