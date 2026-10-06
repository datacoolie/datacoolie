"""Small native Iceberg helpers for focused REST-catalog validators.

The helper keeps catalog construction on the simulator's official runner
factory and creates a fresh Polars engine for every Driver session.  The
latter matters because ``DataCoolieDriver`` attaches its platform to an
injected engine during a run.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pyarrow as pa

from _stateful_support import PolarsEngine
from runner._runner_utils import MINIO_STORAGE_OPTIONS, build_iceberg_rest_catalog


TECHNICAL_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("__file_name", pa.string()),
    ("__file_path", pa.string()),
    ("__file_modification_time", pa.timestamp("us", tz="UTC")),
    ("__created_at", pa.timestamp("us", tz="UTC")),
    ("__updated_at", pa.timestamp("us", tz="UTC")),
    ("__updated_by", pa.string()),
    ("__dataflow_run_id", pa.string()),
)


def new_catalog() -> Any:
    """Build the shared REST catalog through the runner-owned factory."""

    return build_iceberg_rest_catalog(storage_opts=MINIO_STORAGE_OPTIONS)


def new_engine(catalog: Any) -> PolarsEngine:
    """Build an unbound engine for one normal Driver invocation."""

    return PolarsEngine(
        storage_options=MINIO_STORAGE_OPTIONS,
        iceberg_catalog=catalog,
    )


def table_id(namespace: str, table: str) -> str:
    return f"{namespace}.{table}"


def table_rows(catalog: Any, namespace: str, table: str) -> list[dict[str, Any]]:
    return catalog.load_table(table_id(namespace, table)).scan().to_arrow().to_pylist()


def schema_fingerprint(catalog: Any, namespace: str, table: str) -> list[dict[str, Any]]:
    loaded = catalog.load_table(table_id(namespace, table))
    return [
        {
            "field_id": field.field_id,
            "name": field.name,
            "type": str(field.field_type),
            "required": field.required,
        }
        for field in loaded.schema().fields
    ]


def spec_fingerprint(catalog: Any, namespace: str, table: str) -> list[dict[str, Any]]:
    loaded = catalog.load_table(table_id(namespace, table))
    return [
        {
            "field_id": field.field_id,
            "source_id": field.source_id,
            "name": field.name,
            "transform": str(field.transform),
        }
        for field in loaded.spec().fields
    ]


def snapshot_ids(catalog: Any, namespace: str, table: str) -> list[int]:
    loaded = catalog.load_table(table_id(namespace, table))
    return [snapshot.snapshot_id for snapshot in (loaded.snapshots() or [])]


def seed_schema(
    business_fields: Sequence[tuple[str, pa.DataType]],
    *,
    include_technical: bool = True,
) -> pa.Schema:
    fields = [pa.field(name, dtype) for name, dtype in business_fields]
    if include_technical:
        fields.extend(pa.field(name, dtype) for name, dtype in TECHNICAL_FIELDS)
    return pa.schema(fields)


def seed_table(
    catalog: Any,
    namespace: str,
    table: str,
    rows: Iterable[Mapping[str, Any]],
    schema: pa.Schema,
) -> Any:
    """Create and append a case-owned native seed table without dropping data."""

    catalog.create_namespace_if_not_exists(namespace)
    loaded = catalog.create_table(table_id(namespace, table), schema=schema)
    loaded.append(pa.Table.from_pylist([dict(row) for row in rows], schema=schema))
    return loaded


def native_rows(
    rows: Iterable[Mapping[str, Any]],
    fields: Sequence[str],
) -> list[dict[str, Any]]:
    return [{field: row.get(field) for field in fields} for row in rows]


def rewrite_iceberg_namespace(case: Any, namespace: str) -> dict[str, Any]:
    """Replace the authored namespace token after fixture materialization."""

    document = json.loads(case.metadata.read_text(encoding="utf-8"))
    for connection in document.get("connections", []):
        if connection.get("format") == "iceberg":
            connection["database"] = namespace
    case.metadata.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


__all__ = [
    "MINIO_STORAGE_OPTIONS",
    "TECHNICAL_FIELDS",
    "native_rows",
    "new_catalog",
    "new_engine",
    "rewrite_iceberg_namespace",
    "schema_fingerprint",
    "seed_schema",
    "seed_table",
    "snapshot_ids",
    "spec_fingerprint",
    "table_id",
    "table_rows",
]
