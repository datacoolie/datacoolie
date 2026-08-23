"""Catalog-oriented Iceberg operations used by :class:`PolarsEngine`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import reduce
from typing import Any, Dict, List, Optional

import polars as pl

from datacoolie.core.constants import LoadType, SCD2Column
from datacoolie.core.exceptions import EngineError
from datacoolie.engines._polars.iceberg import schema as iceberg_schema
from datacoolie.logging.base import get_logger

logger = get_logger(__name__)


def read_table(
    table_name: str,
    *,
    catalog: Any,
    storage_options: Dict[str, str],
) -> pl.LazyFrame:
    ice_table = catalog.load_table(iceberg_schema.table_id(table_name))
    kwargs: Dict[str, Any] = {}
    if storage_options:
        kwargs["storage_options"] = storage_options
    return pl.scan_iceberg(ice_table, **kwargs)


def write_table(
    df: pl.LazyFrame,
    table_name: str,
    mode: str,
    partition_columns: Optional[List[str]],
    *,
    catalog: Any,
) -> None:
    from pyiceberg.exceptions import (  # noqa: PLC0415
        NoSuchTableError,
        TableAlreadyExistsError,
    )

    pyice_id = iceberg_schema.table_id(table_name)
    arrow_table = df.collect().to_arrow()
    table_created = False
    try:
        ice_table = catalog.load_table(pyice_id)
    except NoSuchTableError:
        namespace = pyice_id.rsplit(".", 1)[0] if "." in pyice_id else "default"
        catalog.create_namespace_if_not_exists(namespace)
        try:
            ice_table = catalog.create_table(pyice_id, schema=arrow_table.schema)
            table_created = True
        except TableAlreadyExistsError:
            ice_table = catalog.load_table(pyice_id)

    mode_lower = mode.lower()
    if mode_lower in (LoadType.OVERWRITE.value, LoadType.FULL_LOAD.value):
        iceberg_mode = "overwrite"
    elif mode_lower == LoadType.APPEND.value:
        iceberg_mode = "append"
    else:
        raise EngineError(f"PolarsEngine: unsupported Iceberg write mode {mode!r}")

    if not table_created:
        ice_table, arrow_table = iceberg_schema.align_and_evolve(
            ice_table, arrow_table, pyice_id, catalog=catalog
        )
    elif "schema.name-mapping.default" not in (ice_table.properties or {}):
        ice_table = iceberg_schema.refresh_name_mapping(
            ice_table, pyice_id, catalog=catalog
        )
    if partition_columns:
        ice_table = iceberg_schema.ensure_partition_spec(
            ice_table, partition_columns, pyice_id, catalog=catalog
        )

    aligned = iceberg_schema.align_arrow_to_table(arrow_table, ice_table)
    if iceberg_mode == "overwrite":
        ice_table.overwrite(aligned)
    else:
        ice_table.append(aligned)


def build_key_filter(collected: pl.DataFrame, merge_keys: List[str]) -> Any:
    from pyiceberg.expressions import And, EqualTo, In, Or  # noqa: PLC0415

    if len(merge_keys) == 1:
        key = merge_keys[0]
        return In(key, tuple(collected.get_column(key).unique().to_list()))
    conditions = []
    for row in collected.select(merge_keys).unique().iter_rows(named=True):
        parts = [EqualTo(key, row[key]) for key in merge_keys]
        conditions.append(reduce(lambda left, right: And(left, right), parts))
    return reduce(lambda left, right: Or(left, right), conditions)


def transactional_key_overwrite(
    plan: iceberg_schema.IcebergWritePlan,
    merge_keys: List[str],
) -> None:
    from pyiceberg.table.name_mapping import create_mapping_from_schema  # noqa: PLC0415

    key_filter = build_key_filter(plan.collected, merge_keys)
    with plan.ice_table.transaction() as transaction:
        iceberg_schema.stage_merge_schema(transaction, plan)
        iceberg_schema.stage_merge_partitions(
            transaction, plan.missing_partition_columns
        )
        if "schema.name-mapping.default" not in (plan.ice_table.properties or {}):
            mapping = create_mapping_from_schema(transaction.table_metadata.schema())
            transaction.set_properties(
                {"schema.name-mapping.default": mapping.model_dump_json()}
            )
        aligned = iceberg_schema.align_arrow_to_schema(
            plan.arrow_table, transaction.table_metadata.schema().as_arrow()
        )
        transaction.overwrite(aligned, overwrite_filter=key_filter)


def merge_table(
    df: pl.LazyFrame,
    table_name: str,
    merge_keys: List[str],
    partition_columns: Optional[List[str]],
    *,
    catalog: Any,
) -> None:
    plan = iceberg_schema.inspect_write_target(
        df, table_name, partition_columns, catalog=catalog
    )
    snapshot = plan.ice_table.current_snapshot()
    strategy = (
        "transactional_key_overwrite"
        if plan.requires_transactional_write
        else "native_upsert"
    )
    logger.info(
        "Iceberg merge strategy=%s table=%s current_schema_id=%s "
        "snapshot_schema_id=%s added_columns=%s reorder_required=%s "
        "missing_partitions=%s",
        strategy,
        plan.pyice_id,
        plan.ice_table.metadata.current_schema_id,
        snapshot.schema_id if snapshot is not None else None,
        [field.name for field in plan.new_fields],
        plan.schema_reorder_required,
        list(plan.missing_partition_columns),
    )
    if plan.requires_transactional_write:
        transactional_key_overwrite(plan, merge_keys)
        return
    plan.ice_table.upsert(
        iceberg_schema.align_arrow_to_table(plan.arrow_table, plan.ice_table),
        join_cols=merge_keys,
    )


def merge_overwrite_table(
    df: pl.LazyFrame,
    table_name: str,
    merge_keys: List[str],
    partition_columns: Optional[List[str]],
    *,
    catalog: Any,
) -> None:
    plan = iceberg_schema.inspect_write_target(
        df, table_name, partition_columns, catalog=catalog
    )
    ice_table, arrow_table = iceberg_schema.apply_write_metadata(
        plan, catalog=catalog
    )
    ice_table.overwrite(
        iceberg_schema.align_arrow_to_table(arrow_table, ice_table),
        overwrite_filter=build_key_filter(plan.collected, merge_keys),
    )


def scd2_table(
    df: pl.LazyFrame,
    table_name: str,
    merge_keys: List[str],
    partition_columns: Optional[List[str]],
    *,
    catalog: Any,
) -> None:
    plan = iceberg_schema.inspect_write_target(
        df, table_name, partition_columns, catalog=catalog
    )
    ice_table, arrow_table = iceberg_schema.apply_write_metadata(
        plan, catalog=catalog
    )
    collected = plan.collected
    valid_from = SCD2Column.VALID_FROM.value
    valid_to = SCD2Column.VALID_TO.value
    is_current = SCD2Column.IS_CURRENT.value

    from pyiceberg.expressions import And, EqualTo  # noqa: PLC0415

    current_filter = And(
        build_key_filter(collected, merge_keys), EqualTo(is_current, True)
    )
    target_arrow = ice_table.scan(row_filter=current_filter).to_arrow()
    if len(target_arrow) > 0:
        target_df = pl.from_arrow(target_arrow)
        source_valid_from = collected.select(
            merge_keys + [pl.col(valid_from).alias("__src_vf")]
        ).unique(subset=merge_keys)
        target_dtype = target_df[valid_to].dtype
        source_dtype = source_valid_from.schema["__src_vf"]
        if source_dtype == target_dtype:
            valid_to_expr = pl.col("__src_vf")
        elif source_dtype == pl.Utf8 and isinstance(target_dtype, pl.Datetime):
            valid_to_expr = pl.col("__src_vf").str.to_datetime(
                time_unit=target_dtype.time_unit,
                time_zone=target_dtype.time_zone,
            )
        else:
            valid_to_expr = pl.col("__src_vf").cast(target_dtype)
        closed = (
            target_df.join(source_valid_from, on=merge_keys)
            .filter(pl.col("__src_vf") > pl.col(valid_from))
            .with_columns(
                valid_to_expr.alias(valid_to), pl.lit(False).alias(is_current)
            )
            .drop("__src_vf")
        )
        if len(closed) > 0:
            ice_table.overwrite(
                iceberg_schema.align_arrow_to_table(closed.to_arrow(), ice_table),
                overwrite_filter=build_key_filter(
                    closed, merge_keys + [valid_from]
                ),
            )
    ice_table.append(iceberg_schema.align_arrow_to_table(arrow_table, ice_table))


def delete_by_window(table_name: str, predicate: str, *, catalog: Any) -> None:
    catalog.load_table(iceberg_schema.table_id(table_name)).delete(
        delete_filter=predicate
    )


def table_exists(table_name: str, *, catalog: Any) -> bool:
    try:
        return catalog.table_exists(iceberg_schema.table_id(table_name))
    except Exception as exc:  # noqa: BLE001
        logger.debug("table_exists check failed, assuming absent: %s", exc)
        return False


def history(
    table_name: str,
    limit: int,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    *,
    catalog: Any,
) -> List[Dict[str, Any]]:
    try:
        ice_table = catalog.load_table(iceberg_schema.table_id(table_name))
        snapshots = []
        for snapshot in ice_table.snapshots() or []:
            timestamp = datetime.fromtimestamp(
                snapshot.timestamp_ms / 1000, tz=timezone.utc
            )
            if start_time is not None and timestamp <= start_time:
                continue
            if end_time is not None and timestamp >= end_time:
                continue
            snapshots.append(snapshot)
    except Exception:  # noqa: BLE001
        return []

    result: List[Dict[str, Any]] = []
    for snapshot in sorted(
        snapshots, key=lambda item: item.timestamp_ms, reverse=True
    ):
        value = snapshot.dict()
        summary = value.get("summary")
        result.append(
            {
                "snapshot_id": value.get("snapshot_id"),
                "parent_id": value.get("parent_snapshot_id"),
                "timestamp": datetime.fromtimestamp(
                    value.get("timestamp_ms") / 1000, tz=timezone.utc
                ),
                "operation": summary.get("operation") if summary else None,
                "manifest_list": value.get("manifest_list"),
                "summary": dict(summary) if summary else None,
            }
        )
    return result[:limit]


def expire_snapshots(ice_table: Any, retention_hours: int) -> None:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=retention_hours)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    current_id = getattr(ice_table.metadata, "current_snapshot_id", None)
    expired = [
        snapshot
        for snapshot in (ice_table.snapshots() or [])
        if getattr(snapshot, "timestamp_ms", 0) < cutoff_ms
        and snapshot.snapshot_id != current_id
    ]
    if not expired:
        logger.debug(
            "PolarsEngine cleanup_by_name: no snapshots older than %dh; "
            "skipping expire_snapshots",
            retention_hours,
        )
        return
    try:
        ice_table.maintenance.expire_snapshots().older_than(cutoff).commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "PolarsEngine cleanup_by_name: expire_snapshots skipped (%s)", exc
        )


def cleanup(
    table_name: str,
    retention_hours: int,
    options: Dict[str, Any],
    *,
    catalog: Any,
) -> None:
    ice_table = catalog.load_table(iceberg_schema.table_id(table_name))
    if options.get("expire_snapshots", True):
        expire_snapshots(ice_table, retention_hours)
    if options.get("remove_orphan_files", True):
        logger.warning(
            "PolarsEngine cleanup_by_name: pyiceberg does not support "
            "remove_orphan_files; skipping"
        )
