"""Iceberg schema alignment and write-planning helpers.

This module owns no catalog or engine state. Catalog references are passed
explicitly only when a metadata mutation must be followed by a reload.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

import polars as pl

from datacoolie.core.constants import TRAILING_COLUMNS


@dataclass(frozen=True)
class IcebergWritePlan:
    """Inspected Iceberg write state; constructing it never mutates the table."""

    ice_table: Any
    collected: pl.DataFrame
    arrow_table: Any
    pyice_id: str
    new_fields: Tuple[Any, ...]
    expected_column_order: Tuple[str, ...]
    schema_reorder_required: bool
    snapshot_schema_stale: bool
    missing_partition_columns: Tuple[str, ...]

    @property
    def requires_transactional_write(self) -> bool:
        return bool(
            self.new_fields
            or self.schema_reorder_required
            or self.snapshot_schema_stale
            or self.missing_partition_columns
        )


def table_id(table_name: str) -> str:
    """Normalize a qualified SQL name for a pyiceberg catalog."""
    parts = [part.strip().strip("`") for part in table_name.split(".")]
    if len(parts) >= 3:
        return ".".join(parts[1:])
    if len(parts) == 2:
        return f"{parts[0]}.{parts[1]}"
    return parts[0]


def align_arrow_to_schema(arrow_table: Any, target_schema: Any) -> Any:
    """Backfill, case-align, and order Arrow columns for an Iceberg schema."""
    import pyarrow as pa  # noqa: PLC0415

    target_names = target_schema.names
    if arrow_table.column_names == target_names:
        return arrow_table

    arrow_col_lower = {name.lower(): name for name in arrow_table.column_names}
    missing = [
        field for field in target_schema if field.name.lower() not in arrow_col_lower
    ]
    for field in missing:
        arrow_table = arrow_table.append_column(
            field,
            pa.nulls(arrow_table.num_rows, type=field.type),
        )

    ice_name_by_lower = {name.lower(): name for name in target_names}
    renamed = [
        ice_name_by_lower.get(column.lower(), column)
        for column in arrow_table.column_names
    ]
    if renamed != arrow_table.column_names:
        arrow_table = arrow_table.rename_columns(renamed)
    return arrow_table.select(target_names)


def align_arrow_to_table(arrow_table: Any, ice_table: Any) -> Any:
    return align_arrow_to_schema(arrow_table, ice_table.schema().as_arrow())


def align_arrow_casing(arrow_table: Any, ice_table: Any) -> Any:
    """Rename case-insensitive matches to the existing Iceberg casing."""
    ice_names = [field.name for field in ice_table.schema().fields]
    ice_exact = set(ice_names)
    ice_by_lower = {name.lower(): name for name in ice_names}
    renamed = [
        ice_by_lower.get(name.lower(), name) if name not in ice_exact else name
        for name in arrow_table.column_names
    ]
    if renamed == arrow_table.column_names:
        return arrow_table
    return arrow_table.rename_columns(renamed)


def evolve_schema(ice_table: Any, arrow_schema: Any) -> bool:
    """Union genuinely new Arrow fields into an Iceberg schema."""
    import pyarrow as pa  # noqa: PLC0415

    target_names = {field.name for field in ice_table.schema().fields}
    new_fields = [field for field in arrow_schema if field.name not in target_names]
    if not new_fields:
        return False
    with ice_table.update_schema() as update:
        update.union_by_name(pa.schema(new_fields))
    return True


def expected_column_order(column_names: List[str]) -> Tuple[str, ...]:
    trailing_lower = {column.lower() for column in TRAILING_COLUMNS}
    column_by_lower = {column.lower(): column for column in column_names}
    leading = [
        column for column in column_names if column.lower() not in trailing_lower
    ]
    trailing = [
        column_by_lower[column.lower()]
        for column in TRAILING_COLUMNS
        if column.lower() in column_by_lower
    ]
    return tuple(leading + trailing)


def reorder_trailing_columns(ice_table: Any) -> bool:
    current = [field.name for field in ice_table.schema().fields]
    expected = list(expected_column_order(current))
    if current == expected:
        return False
    with ice_table.update_schema() as update:
        leading = [column for column in expected if column not in TRAILING_COLUMNS]
        previous = leading[-1] if leading else None
        for column in (column for column in TRAILING_COLUMNS if column in current):
            if previous is None:
                update.move_first(column)
            else:
                update.move_after(column, previous)
            previous = column
    return True


def snapshot_schema_is_stale(ice_table: Any) -> bool:
    snapshot = ice_table.current_snapshot()
    if snapshot is None or snapshot.schema_id is None:
        return False
    return snapshot.schema_id != ice_table.metadata.current_schema_id


def missing_identity_partitions(
    ice_table: Any,
    partition_columns: Optional[List[str]],
) -> Tuple[str, ...]:
    if not partition_columns:
        return ()
    from pyiceberg.transforms import IdentityTransform  # noqa: PLC0415

    schema = ice_table.schema()
    existing: set[str] = set()
    for field in ice_table.spec().fields:
        if isinstance(field.transform, IdentityTransform):
            try:
                existing.add(schema.find_field(field.source_id).name.lower())
            except Exception:  # noqa: BLE001
                pass
    return tuple(
        column for column in partition_columns if column.lower() not in existing
    )


def inspect_write_target(
    df: pl.LazyFrame,
    table_name: str,
    partition_columns: Optional[List[str]],
    *,
    catalog: Any,
) -> IcebergWritePlan:
    """Materialize and inspect a write target without mutating it."""
    pyice_id = table_id(table_name)
    ice_table = catalog.load_table(pyice_id)
    collected = df.collect()
    arrow_table = align_arrow_casing(collected.to_arrow(), ice_table)
    current_names = [field.name for field in ice_table.schema().fields]
    current_name_set = set(current_names)
    new_fields = tuple(
        field for field in arrow_table.schema if field.name not in current_name_set
    )
    post_evolution_names = current_names + [field.name for field in new_fields]
    expected_order = expected_column_order(post_evolution_names)
    return IcebergWritePlan(
        ice_table=ice_table,
        collected=collected,
        arrow_table=arrow_table,
        pyice_id=pyice_id,
        new_fields=new_fields,
        expected_column_order=expected_order,
        schema_reorder_required=tuple(post_evolution_names) != expected_order,
        snapshot_schema_stale=snapshot_schema_is_stale(ice_table),
        missing_partition_columns=missing_identity_partitions(
            ice_table, partition_columns
        ),
    )


def refresh_name_mapping(ice_table: Any, pyice_id: str, *, catalog: Any) -> Any:
    from pyiceberg.table.name_mapping import create_mapping_from_schema  # noqa: PLC0415

    mapping = create_mapping_from_schema(ice_table.schema())
    with ice_table.transaction() as transaction:
        transaction.set_properties(
            {"schema.name-mapping.default": mapping.model_dump_json()}
        )
    return catalog.load_table(pyice_id)


def align_and_evolve(
    ice_table: Any,
    arrow_table: Any,
    pyice_id: str,
    *,
    catalog: Any,
) -> Tuple[Any, Any]:
    arrow_table = align_arrow_casing(arrow_table, ice_table)
    schema_evolved = evolve_schema(ice_table, arrow_table.schema)
    if schema_evolved:
        ice_table = catalog.load_table(pyice_id)
    reordered = reorder_trailing_columns(ice_table)
    if schema_evolved or reordered:
        if reordered:
            ice_table = catalog.load_table(pyice_id)
        ice_table = refresh_name_mapping(ice_table, pyice_id, catalog=catalog)
    return ice_table, arrow_table


def ensure_partition_spec(
    ice_table: Any,
    partition_columns: List[str],
    pyice_id: str,
    *,
    catalog: Any,
) -> Any:
    missing = missing_identity_partitions(ice_table, partition_columns)
    if not missing:
        return ice_table
    with ice_table.update_spec() as update_spec:
        for column in missing:
            update_spec.add_identity(column)
    return catalog.load_table(pyice_id)


def apply_write_metadata(
    plan: IcebergWritePlan,
    *,
    catalog: Any,
) -> Tuple[Any, Any]:
    ice_table, arrow_table = align_and_evolve(
        plan.ice_table, plan.arrow_table, plan.pyice_id, catalog=catalog
    )
    if plan.missing_partition_columns:
        ice_table = ensure_partition_spec(
            ice_table,
            list(plan.missing_partition_columns),
            plan.pyice_id,
            catalog=catalog,
        )
    return ice_table, arrow_table


def stage_merge_schema(transaction: Any, plan: IcebergWritePlan) -> None:
    if not plan.new_fields and not plan.schema_reorder_required:
        return
    import pyarrow as pa  # noqa: PLC0415

    trailing_lower = {column.lower() for column in TRAILING_COLUMNS}
    leading = [
        column
        for column in plan.expected_column_order
        if column.lower() not in trailing_lower
    ]
    trailing = [
        column
        for column in plan.expected_column_order
        if column.lower() in trailing_lower
    ]
    with transaction.update_schema() as update:
        if plan.new_fields:
            update.union_by_name(pa.schema(plan.new_fields))
        if plan.schema_reorder_required:
            previous = leading[-1] if leading else None
            for column in trailing:
                if previous is None:
                    update.move_first(column)
                else:
                    update.move_after(column, previous)
                previous = column


def stage_merge_partitions(
    transaction: Any,
    missing_partition_columns: Tuple[str, ...],
) -> None:
    if not missing_partition_columns:
        return
    with transaction.update_spec() as update_spec:
        for column in missing_partition_columns:
            update_spec.add_identity(column)
