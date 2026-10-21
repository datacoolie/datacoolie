"""Qualify native REST-catalog Iceberg SCD2 history and mtime watermarking."""

from __future__ import annotations

import argparse
import importlib.metadata
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
import uuid


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    assert_exact_rows,
    assert_successful_flow,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)
from merge.iceberg_native_support import (  # noqa: E402
    new_catalog,
    new_engine,
    rewrite_iceberg_namespace,
    schema_fingerprint,
    snapshot_ids,
    spec_fingerprint,
    table_rows,
)


FLOW_NAME_AUTHORED = "load_iceberg__scd2"
TABLE_NAME = "orders_scd2"
HISTORY_FIELDS = (
    "order_id",
    "value",
    "modified_at",
    "__valid_from",
    "__valid_to",
    "__is_current",
)
INITIAL_ROWS = [
    {"order_id": 1, "value": "old-one", "modified_at": datetime(2024, 1, 1)},
    {"order_id": 2, "value": "stable-two", "modified_at": datetime(2024, 1, 1)},
]
CHANGED_ROWS = [
    {"order_id": 1, "value": "new-one", "modified_at": datetime(2024, 2, 1)},
    {"order_id": 3, "value": "new-three", "modified_at": datetime(2024, 2, 2)},
]
EXPECTED_AFTER_CHANGE = [
    {
        "order_id": 1,
        "value": "old-one",
        "modified_at": datetime(2024, 1, 1),
        "__valid_from": datetime(2024, 1, 1),
        "__valid_to": datetime(2024, 2, 1),
        "__is_current": False,
    },
    {
        "order_id": 1,
        "value": "new-one",
        "modified_at": datetime(2024, 2, 1),
        "__valid_from": datetime(2024, 2, 1),
        "__valid_to": None,
        "__is_current": True,
    },
    {
        "order_id": 2,
        "value": "stable-two",
        "modified_at": datetime(2024, 1, 1),
        "__valid_from": datetime(2024, 1, 1),
        "__valid_to": None,
        "__is_current": True,
    },
    {
        "order_id": 3,
        "value": "new-three",
        "modified_at": datetime(2024, 2, 2),
        "__valid_from": datetime(2024, 2, 2),
        "__valid_to": None,
        "__is_current": True,
    },
]
EXPECTED_AFTER_REPLAY = EXPECTED_AFTER_CHANGE + [
    {
        "order_id": 1,
        "value": "new-one",
        "modified_at": datetime(2024, 2, 1),
        "__valid_from": datetime(2024, 2, 1),
        "__valid_to": None,
        "__is_current": True,
    },
    {
        "order_id": 3,
        "value": "new-three",
        "modified_at": datetime(2024, 2, 2),
        "__valid_from": datetime(2024, 2, 2),
        "__valid_to": None,
        "__is_current": True,
    },
]
EXPECTED_SCHEMA = [
    {"field_id": 1, "name": "order_id", "type": "long", "required": False},
    {"field_id": 2, "name": "value", "type": "string", "required": False},
    {"field_id": 3, "name": "modified_at", "type": "timestamp", "required": False},
    {"field_id": 4, "name": "__valid_from", "type": "timestamp", "required": False},
    {"field_id": 5, "name": "__valid_to", "type": "timestamp", "required": False},
    {"field_id": 6, "name": "__is_current", "type": "boolean", "required": False},
    {"field_id": 7, "name": "__file_name", "type": "string", "required": False},
    {"field_id": 8, "name": "__file_path", "type": "string", "required": False},
    {"field_id": 9, "name": "__file_modification_time", "type": "timestamptz", "required": False},
    {"field_id": 10, "name": "__created_at", "type": "timestamptz", "required": False},
    {"field_id": 11, "name": "__updated_at", "type": "timestamptz", "required": False},
    {"field_id": 12, "name": "__updated_by", "type": "string", "required": False},
    {"field_id": 13, "name": "__dataflow_run_id", "type": "string", "required": False},
]
EXPECTED_SPEC: list[dict[str, Any]] = []


def _project_history(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [{field: row.get(field) for field in HISTORY_FIELDS} for row in rows]


def assert_i4_history_types(rows: Iterable[Mapping[str, Any]]) -> None:
    temporal_fields = ("modified_at", "__valid_from", "__valid_to")
    for index, row in enumerate(rows):
        if type(row.get("order_id")) is not int:
            raise AssertionError(f"Iceberg SCD2 row {index} has a non-int key: {row!r}")
        if not isinstance(row.get("value"), str):
            raise AssertionError(f"Iceberg SCD2 row {index} has a non-string value: {row!r}")
        for field in temporal_fields:
            value = row.get(field)
            if value is not None and not isinstance(value, datetime):
                raise AssertionError(f"Iceberg SCD2 row {index} has a non-datetime {field}: {row!r}")
        if type(row.get("__is_current")) is not bool:
            raise AssertionError(f"Iceberg SCD2 row {index} has a non-bool current flag: {row!r}")


def assert_i4_history(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]] = EXPECTED_AFTER_CHANGE,
) -> None:
    rows = list(actual)
    assert_i4_history_types(rows)
    projected = _project_history(rows)
    # Duplicate keys are meaningful here: an equal-effective replay is
    # admitted by the file-mtime watermark and native Iceberg SCD2 appends
    # the incoming version without closing an equal-effective current row.
    assert_exact_rows(projected, expected, label="Iceberg SCD2 history")
    current = {
        row["order_id"]
        for row in projected
        if row.get("__is_current") is True
    }
    if current != {1, 2, 3}:
        raise AssertionError(f"Iceberg SCD2 current keys mismatch: {current!r}")


def assert_i4_schema(actual: list[dict[str, Any]]) -> None:
    if actual != EXPECTED_SCHEMA:
        raise AssertionError(f"Iceberg SCD2 schema mismatch: actual={actual!r}, expected={EXPECTED_SCHEMA!r}")


def assert_i4_spec(actual: list[dict[str, Any]]) -> None:
    if actual != EXPECTED_SPEC:
        raise AssertionError(f"Iceberg SCD2 partition spec mismatch: actual={actual!r}, expected={EXPECTED_SPEC!r}")


def assert_i4_admitted_update(result: Mapping[str, Any]) -> None:
    expected = {"succeeded": 1, "failed": 0, "skipped": 0}
    actual = {key: result.get(key) for key in expected}
    if actual != expected:
        raise AssertionError(f"new Iceberg SCD2 watermark batch was not admitted: {actual!r}")


def assert_i4_watermark_rerun(result: Mapping[str, Any]) -> None:
    expected = {"succeeded": 1, "failed": 0, "skipped": 0}
    actual = {key: result.get(key) for key in expected}
    if actual != expected:
        raise AssertionError(f"Iceberg SCD2 admitted watermark rerun mismatch: {actual!r}")


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("scd2", root)
    namespace = f"a9_i4_{uuid.uuid4().hex[:12]}"
    identity: dict[str, Any]
    try:
        from _fixture_support import compile_existing
    except ImportError:  # pragma: no cover - compatibility for pre-T6 worktrees
        raise RuntimeError("T6 requires the existing-flow compiler for the Iceberg SCD2 baseline")
    document, identity = compile_existing(case, "scd2", FLOW_NAME_AUTHORED)
    rewrite_iceberg_namespace(case, namespace)
    catalog = new_catalog()
    source_engine = new_engine(catalog)
    first_mtime = datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp()
    second_mtime = datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp()
    replay_mtime = datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp()
    write_parquet(source_engine, case.input / "sample", INITIAL_ROWS, mtime=first_mtime)

    first = run_driver(
        case,
        stage=identity["runtime_stage"],
        iteration=1,
        engine=new_engine(catalog),
    )
    assert_successful_flow(first, identity["runtime_flow"])
    first_rows = table_rows(catalog, namespace, TABLE_NAME)
    if {row["order_id"] for row in _project_history(first_rows)} != {1, 2}:
        raise AssertionError(f"unexpected Iceberg SCD2 initial keys: {first_rows!r}")
    first_schema = schema_fingerprint(catalog, namespace, TABLE_NAME)
    first_spec = spec_fingerprint(catalog, namespace, TABLE_NAME)
    assert_i4_schema(first_schema)
    assert_i4_spec(first_spec)

    write_parquet(
        source_engine,
        case.input / "sample",
        CHANGED_ROWS,
        mode="append",
        mtime=second_mtime,
    )
    second = run_driver(
        case,
        stage=identity["runtime_stage"],
        iteration=2,
        engine=new_engine(catalog),
    )
    assert_successful_flow(second, identity["runtime_flow"])
    assert_i4_admitted_update(second.result)
    after_change = table_rows(catalog, namespace, TABLE_NAME)
    assert_i4_history(after_change)
    second_schema = schema_fingerprint(catalog, namespace, TABLE_NAME)
    second_spec = spec_fingerprint(catalog, namespace, TABLE_NAME)
    assert_i4_schema(second_schema)
    assert_i4_spec(second_spec)
    if second_schema != first_schema or second_spec != first_spec:
        raise AssertionError("Iceberg SCD2 schema/spec changed during changed batch")

    # A new file repeats the last incoming versions with a newer source mtime.
    # The same normal Driver flow therefore admits the bounded replay even
    # though its effective timestamps are equal to the current versions.
    write_parquet(
        source_engine,
        case.input / "sample",
        CHANGED_ROWS,
        mode="append",
        mtime=replay_mtime,
    )
    third = run_driver(
        case,
        stage=identity["runtime_stage"],
        iteration=3,
        engine=new_engine(catalog),
    )
    assert_successful_flow(third, identity["runtime_flow"])
    assert_i4_watermark_rerun(third.result)
    after_replay = table_rows(catalog, namespace, TABLE_NAME)
    assert_i4_history(after_replay, EXPECTED_AFTER_REPLAY)
    reloaded = new_catalog()
    final_rows = table_rows(reloaded, namespace, TABLE_NAME)
    assert_i4_history(final_rows, EXPECTED_AFTER_REPLAY)
    final_schema = schema_fingerprint(reloaded, namespace, TABLE_NAME)
    final_spec = spec_fingerprint(reloaded, namespace, TABLE_NAME)
    assert_i4_schema(final_schema)
    assert_i4_spec(final_spec)
    if final_schema != second_schema or final_spec != second_spec:
        raise AssertionError("Iceberg SCD2 schema/spec changed during admitted replay")
    snapshots = snapshot_ids(reloaded, namespace, TABLE_NAME)
    if len(snapshots) < 4:
        raise AssertionError(f"expected initial/changed/replay Iceberg SCD2 snapshots, got {snapshots!r}")

    payload = receipt_payload(
        case,
        runs=[first, second, third],
        details={
            "identity_mapping": identity,
            "namespace": namespace,
            "table": TABLE_NAME,
            "history": _project_history(final_rows),
            "schema": final_schema,
            "partition_spec": final_spec,
            "source_file_mtimes": {
                "initial": datetime.fromtimestamp(first_mtime, tz=timezone.utc),
                "changed": datetime.fromtimestamp(second_mtime, tz=timezone.utc),
                "replay": datetime.fromtimestamp(replay_mtime, tz=timezone.utc),
            },
            "watermark_contract": {
                "changed_batch": "succeeded=1, failed=0, skipped=0",
                "admitted_equal_effective_replay": "succeeded=1, failed=0, skipped=0",
                "native_equal_effective_replay": "appends incoming current rows",
            },
            "snapshot_ids": snapshots,
            "package_versions": {
                name: importlib.metadata.version(name)
                for name in ("polars", "pyiceberg", "pyarrow")
            },
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies the Polars REST-catalog Iceberg SCD2 profile only; Delta, Spark, and remote cloud profiles remain separate.",
            "The focused profile admits an equal-effective replay through a newer source mtime and records native append behavior; older-effective inputs remain separately documented.",
        ),
        references=(
            "src/datacoolie/engines/_polars/iceberg/operations.py",
            "src/datacoolie/destinations/strategies/load.py",
            "src/datacoolie/sources/file_reader.py",
        ),
    )
    payload["profile"]["destination_format"] = "iceberg"
    payload["profile"]["qualification_scope"] = "docker-polars-rest-native"
    write_receipt(case, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    args = parser.parse_args()
    payload = validate(args.root)
    print({"status": payload["status"], "receipt": payload["receipt"], "root": payload["root"], "profile": payload["profile"]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
