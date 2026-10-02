"""Independent persisted-output observations for datatype qualification."""

from __future__ import annotations

import base64
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.parquet as pq


GENERATED_SYSTEM_FIELDS = frozenset(
    {
        "__created_at",
        "__updated_at",
        "__updated_by",
        "__dataflow_run_id",
        "__file_name",
        "__file_path",
        "__file_modification_time",
        "__valid_from",
        "__valid_to",
        "__is_current",
    }
)


@dataclass(frozen=True, slots=True)
class FrameObservation:
    """A stable, JSON-serialisable observation of one persisted dataset."""

    case_id: str
    output_format: str
    engine: str
    path: str
    fields: tuple[dict[str, Any], ...]
    row_count: int
    rows: tuple[dict[str, Any], ...]
    runtime: Mapping[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def business_rows(self) -> tuple[dict[str, Any], ...]:
        """Rows with only the explicitly allowlisted runtime-generated fields removed."""

        return tuple(
            {
                key: value
                for key, value in row.items()
                if key not in GENERATED_SYSTEM_FIELDS
            }
            for row in self.rows
        )


def _type_descriptor(field: pa.Field) -> dict[str, Any]:
    dtype = field.type
    if pa.types.is_decimal(dtype):
        return {
            "name": field.name,
            "kind": "decimal",
            "precision": dtype.precision,
            "scale": dtype.scale,
            "nullable": field.nullable,
        }
    if pa.types.is_timestamp(dtype):
        return {
            "name": field.name,
            "kind": "timestamp",
            "unit": dtype.unit,
            "timezone": dtype.tz,
            "nullable": field.nullable,
        }
    if pa.types.is_date32(dtype) or pa.types.is_date64(dtype):
        return {
            "name": field.name,
            "kind": "date",
            "unit": "day" if pa.types.is_date32(dtype) else "millisecond",
            "nullable": field.nullable,
        }
    if (
        pa.types.is_string(dtype)
        or pa.types.is_large_string(dtype)
        or getattr(pa.types, "is_string_view", lambda _value: False)(dtype)
    ):
        kind = "string"
    elif (
        pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or getattr(pa.types, "is_binary_view", lambda _value: False)(dtype)
    ):
        kind = "binary"
    else:
        kind = str(dtype)
    return {"name": field.name, "kind": kind, "nullable": field.nullable}


def _normalise_value(value: Any) -> Any:
    """Make Arrow scalar values deterministic without losing numeric meaning."""

    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat()}
    if isinstance(value, (date, time)):
        return {type(value).__name__: value.isoformat()}
    if isinstance(value, bytes):
        return {"bytes_base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Mapping):
        return {
            str(key): _normalise_value(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_normalise_value(item) for item in value]
    return str(value)


def _row_sort_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    def stable(value: Any) -> tuple[str, str]:
        return ("1" if value is None else "0", str(value))

    for name in ("id", "identifier", "key"):
        if name in row:
            return stable(row[name])
    row_id = next(
        (name for name in row if name == "row_id" or name.endswith("_row_id")),
        None,
    )
    if row_id is not None:
        return stable(row[row_id])
    return tuple(f"{key}={row[key]!r}" for key in sorted(row))


def _validate_stable_keys(rows: tuple[dict[str, Any], ...]) -> None:
    """Reject missing or duplicate values for a declared stable row key."""

    if not rows:
        return
    key_name = next(
        (
            name
            for name in ("id", "identifier", "key")
            if name in rows[0]
        ),
        None,
    )
    if key_name is None:
        key_name = next(
            (
                name
                for name in rows[0]
                if name == "row_id" or name.endswith("_row_id")
            ),
            None,
        )
    if key_name is None:
        return
    seen: set[str] = set()
    for row in rows:
        value = row.get(key_name)
        if value is None:
            raise AssertionError(f"stable key {key_name!r} is null")
        marker = json.dumps(value, sort_keys=True, default=str)
        if marker in seen:
            raise AssertionError(f"duplicate stable key {key_name!r}: {value!r}")
        seen.add(marker)


def observe_parquet_dataset(
    root: str | Path,
    *,
    table_name: str,
    case_id: str,
    engine: str,
    output_format: str = "parquet",
    runtime: Mapping[str, str] | None = None,
) -> FrameObservation:
    """Read a Parquet dataset and capture schema plus values without recasting."""

    dataset_root = Path(root) / table_name
    files = sorted(dataset_root.rglob("*.parquet"))
    if not files:
        raise AssertionError(f"{table_name}: no Parquet output under {dataset_root}")
    table = pq.read_table([str(path) for path in files])
    return observe_arrow_table(
        table,
        case_id=case_id,
        output_format=output_format,
        engine=engine,
        path=str(dataset_root),
        runtime=runtime,
    )


def observe_delta_table(
    root: str | Path,
    *,
    table_name: str,
    case_id: str,
    engine: str,
    output_format: str = "delta",
    runtime: Mapping[str, str] | None = None,
) -> FrameObservation:
    """Read a Delta table through the native Delta reader.

    Reading the Parquet data files below a Delta directory would validate only
    the physical files and could miss Delta transaction/schema metadata.  This
    observer deliberately opens the table through ``deltalake.DeltaTable`` so
    the persisted-format qualification exercises the Delta reader boundary.
    """

    if output_format != "delta":
        raise ValueError(f"observe_delta_table only supports delta, got {output_format!r}")
    from deltalake import DeltaTable

    table_root = Path(root) / table_name
    if not (table_root / "_delta_log").is_dir():
        raise AssertionError(f"{table_name}: no Delta transaction log under {table_root}")
    table = DeltaTable(str(table_root)).to_pyarrow_table()
    return observe_arrow_table(
        table,
        case_id=case_id,
        output_format="delta",
        engine=engine,
        path=str(table_root),
        runtime=runtime,
    )


def observe_arrow_table(
    table: pa.Table,
    *,
    case_id: str,
    output_format: str,
    engine: str,
    path: str,
    runtime: Mapping[str, str] | None = None,
) -> FrameObservation:
    """Observe an in-memory Arrow table returned by a catalog reader."""

    rows = tuple(
        {
            key: _normalise_value(value)
            for key, value in row.items()
        }
        for row in sorted(table.to_pylist(), key=_row_sort_key)
    )
    _validate_stable_keys(rows)
    return FrameObservation(
        case_id=case_id,
        output_format=output_format,
        engine=engine,
        path=path,
        fields=tuple(_type_descriptor(field) for field in table.schema),
        row_count=table.num_rows,
        rows=rows,
        runtime=runtime,
    )


def observe_iceberg_table(
    catalog: object,
    table_name: str,
    *,
    case_id: str,
    engine: str,
    output_format: str = "iceberg",
    runtime: Mapping[str, str] | None = None,
) -> FrameObservation:
    """Observe a catalog table without converting its values or schema."""

    table = catalog.load_table(table_name)  # type: ignore[attr-defined]
    arrow_table = table.scan().to_arrow()  # type: ignore[attr-defined]
    return observe_arrow_table(
        arrow_table,
        case_id=case_id,
        output_format=output_format,
        engine=engine,
        path=table_name,
        runtime=runtime,
    )
