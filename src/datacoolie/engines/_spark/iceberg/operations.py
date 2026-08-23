"""Spark Iceberg history, existence, and maintenance operations."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

from datacoolie.engines._spark.temporal import align_ms_boundaries


def get_history(
    spark: SparkSession,
    identifier: str,
    limit: int,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    *,
    is_path: bool,
) -> List[dict[str, Any]]:
    try:
        snapshots = (
            spark.read.format("iceberg").load(f"{identifier}#snapshots")
            if is_path
            else spark.sql(f"SELECT * FROM {identifier}.snapshots")
        )
        start, end = align_ms_boundaries(start_time, end_time)
        if start is not None:
            snapshots = snapshots.where(sf.col("committed_at") >= start)
        if end is not None:
            snapshots = snapshots.where(sf.col("committed_at") <= end)
        rows = (
            snapshots.select(
                sf.col("snapshot_id"),
                sf.col("parent_id"),
                sf.col("committed_at").alias("timestamp"),
                sf.col("operation"),
                sf.col("manifest_list"),
                sf.col("summary"),
            )
            .orderBy(sf.col("timestamp").desc())
            .limit(limit)
            .collect()
        )
        return [row.asDict() for row in rows]
    except Exception:  # noqa: BLE001
        return []


def table_exists_by_path(spark: SparkSession, platform: Any, path: str) -> bool:
    try:
        if platform:
            return platform.folder_exists(f"{path.rstrip('/')}/metadata")
        spark.read.format("iceberg").load(f"{path}#metadata_log_entries")
        return True
    except Exception:  # noqa: BLE001
        return False


def extract_catalog(table_name: str) -> str:
    return table_name.split(".")[0]


def compact_by_name(
    spark: SparkSession, table_name: str, options: Optional[dict[str, Any]]
) -> None:
    opts = options or {}
    catalog = extract_catalog(table_name)
    if opts.get("rewrite_data_files", True):
        spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{table_name}')")
    if opts.get("rewrite_position_delete_files", True):
        spark.sql(
            f"CALL {catalog}.system.rewrite_position_delete_files(table => '{table_name}')"
        )
    if opts.get("rewrite_manifests", True):
        spark.sql(f"CALL {catalog}.system.rewrite_manifests(table => '{table_name}')")


def cleanup_by_name(
    spark: SparkSession,
    table_name: str,
    retention_hours: int,
    options: Optional[dict[str, Any]],
) -> None:
    opts = options or {}
    timestamp = datetime.now(tz=timezone.utc) - timedelta(hours=retention_hours)
    timestamp_text = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    catalog = extract_catalog(table_name)
    if opts.get("expire_snapshots", True):
        spark.sql(
            f"CALL {catalog}.system.expire_snapshots("
            f"table => '{table_name}', older_than => TIMESTAMP '{timestamp_text}')"
        )
    if opts.get("remove_orphan_files", True):
        spark.sql(
            f"CALL {catalog}.system.remove_orphan_files("
            f"table => '{table_name}', older_than => TIMESTAMP '{timestamp_text}')"
        )
