"""Read an existing case-owned Iceberg table through the normal Driver path."""

from __future__ import annotations

import argparse
import importlib.metadata
import sys
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping

import polars as pl
import pyarrow as pa


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _fixture_support import compile_existing  # noqa: E402
from _stateful_support import (  # noqa: E402
    assert_exact_rows,
    assert_schema,
    assert_successful_flow,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_receipt,
)
from merge.iceberg_native_support import (  # noqa: E402
    new_catalog,
    new_engine,
    rewrite_iceberg_namespace,
    schema_fingerprint,
    seed_schema,
    seed_table,
    snapshot_ids,
    table_rows,
)


AUTHORED_FLOW = "read__iceberg"
SOURCE_TABLE = "orders_from_json"
DESTINATION_TABLE = "orders_read_iceberg"
BUSINESS_FIELDS = ("id", "value")
SEED_FIELDS = (("id", pa.int64()), ("value", pa.string()))
EXPECTED_ROWS = [
    {"id": 1, "value": "alpha"},
    {"id": 2, "value": "beta"},
    {"id": 3, "value": "unicode Việt"},
]
EXPECTED_SOURCE_SCHEMA = [
    {"field_id": 1, "name": "id", "type": "long", "required": False},
    {"field_id": 2, "name": "value", "type": "string", "required": False},
]
EXPECTED_DESTINATION_SCHEMA = {
    "id": "Int64",
    "value": "String",
    "__created_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_by": "String",
    "__dataflow_run_id": "String",
}


def assert_o2_rows(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]] = EXPECTED_ROWS,
) -> None:
    projected = [{field: row.get(field) for field in BUSINESS_FIELDS} for row in actual]
    assert_exact_rows(projected, expected, key="id", label="Iceberg readback")


def assert_o2_schema(actual: Mapping[str, str]) -> None:
    assert_schema(actual, EXPECTED_DESTINATION_SCHEMA, label="Iceberg readback schema")


def assert_o2_generated_columns(rows: Iterable[Mapping[str, Any]]) -> None:
    for index, row in enumerate(rows):
        if row.get("__created_at") is None or row.get("__updated_at") is None:
            raise AssertionError(f"Iceberg readback row {index} lacks generated timestamps: {row!r}")
        if row.get("__updated_by") != "DataCoolie":
            raise AssertionError(f"Iceberg readback row {index} has wrong updater: {row!r}")
        if not row.get("__dataflow_run_id"):
            raise AssertionError(f"Iceberg readback row {index} lacks a run id: {row!r}")


def _seed_rows() -> list[dict[str, Any]]:
    return [dict(row) for row in EXPECTED_ROWS]


def _read_parquet_target(target: Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    files = sorted(target.rglob("*.parquet"))
    if not files:
        raise AssertionError(f"Iceberg readback produced no parquet files below {target}")
    frame = pl.concat([pl.read_parquet(path) for path in files], how="vertical")
    return frame.to_dicts(), {name: str(dtype) for name, dtype in frame.schema.items()}


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("read", root)
    document, identity = compile_existing(case, "read", AUTHORED_FLOW)
    namespace = f"a9_o2_{uuid.uuid4().hex[:12]}"
    rewrite_iceberg_namespace(case, namespace)
    catalog = new_catalog()
    seed_table(catalog, namespace, SOURCE_TABLE, _seed_rows(), seed_schema(SEED_FIELDS, include_technical=False))

    evidence = run_driver(
        case,
        stage=identity["runtime_stage"],
        iteration=1,
        engine=new_engine(catalog),
    )
    assert_successful_flow(evidence, identity["runtime_flow"])

    target = case.output / DESTINATION_TABLE
    actual_rows, actual_schema = _read_parquet_target(target)
    assert_o2_rows(actual_rows)
    assert_o2_schema(actual_schema)
    assert_o2_generated_columns(actual_rows)

    reloaded = new_catalog()
    source_rows = table_rows(reloaded, namespace, SOURCE_TABLE)
    source_schema = schema_fingerprint(reloaded, namespace, SOURCE_TABLE)
    if source_rows != _seed_rows():
        raise AssertionError(f"Iceberg source changed during readback: {source_rows!r}")
    if source_schema != EXPECTED_SOURCE_SCHEMA:
        raise AssertionError(
            f"Iceberg source schema changed during readback: actual={source_schema!r}, "
            f"expected={EXPECTED_SOURCE_SCHEMA!r}"
        )
    snapshots = snapshot_ids(reloaded, namespace, SOURCE_TABLE)
    if len(snapshots) != 1:
        raise AssertionError(f"readback should preserve one seeded source snapshot: {snapshots!r}")

    payload = receipt_payload(
        case,
        runs=[evidence],
        details={
            "identity_mapping": identity,
            "namespace": namespace,
            "source_table": SOURCE_TABLE,
            "destination_table": DESTINATION_TABLE,
            "source_schema": source_schema,
            "destination_schema": actual_schema,
            "rows": actual_rows,
            "source_snapshot_ids": snapshots,
            "package_versions": {
                name: importlib.metadata.version(name)
                for name in ("polars", "pyiceberg", "pyarrow")
            },
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies the Polars REST-catalog Iceberg source readback to local Parquet only; other engines and remote cloud profiles remain separate.",
        ),
        references=(
            "src/datacoolie/engines/_polars/iceberg/operations.py",
            "src/datacoolie/sources/iceberg_reader.py",
        ),
    )
    payload["profile"]["destination_format"] = "parquet"
    payload["profile"]["source_format"] = "iceberg"
    payload["profile"]["qualification_scope"] = "docker-polars-rest-native"
    write_receipt(case, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    args = parser.parse_args()
    payload = validate(args.root)
    print(
        {
            "status": payload["status"],
            "receipt": payload["receipt"],
            "root": payload["root"],
            "profile": payload["profile"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
