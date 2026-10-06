"""Qualify Iceberg transactional merge for an added nullable business field."""

from __future__ import annotations

import argparse
import importlib.metadata
import sys
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow as pa


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    assert_exact_rows,
    assert_successful_flow,
    business_rows,
    materialize_fixture,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)
from runner.runtime_logs import records  # noqa: E402
from runner.runtime_paths import LOG_DIR  # noqa: E402
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


FLOW_NAME = "a9-iceberg-schema-merge"
STAGE_NAME = "a9_iceberg_schema_merge"
TABLE_NAME = "events"
NEW_FIELD = "new_field"
BUSINESS_FIELDS = ("id", "value", NEW_FIELD)
SEED_FIELDS = (("id", pa.int64()), ("value", pa.string()))
EXPECTED_ROWS = [
    {"id": 1, "value": "changed-one", NEW_FIELD: 10},
    {"id": 2, "value": "old-two", NEW_FIELD: None},
    {"id": 3, "value": "new-three", NEW_FIELD: 30},
]
EXPECTED_SCHEMA = [
    {"field_id": 1, "name": "id", "type": "long", "required": False},
    {"field_id": 2, "name": "value", "type": "string", "required": False},
    {"field_id": 10, "name": NEW_FIELD, "type": "long", "required": False},
    {"field_id": 3, "name": "__file_name", "type": "string", "required": False},
    {"field_id": 4, "name": "__file_path", "type": "string", "required": False},
    {"field_id": 5, "name": "__file_modification_time", "type": "timestamptz", "required": False},
    {"field_id": 6, "name": "__created_at", "type": "timestamptz", "required": False},
    {"field_id": 7, "name": "__updated_at", "type": "timestamptz", "required": False},
    {"field_id": 8, "name": "__updated_by", "type": "string", "required": False},
    {"field_id": 9, "name": "__dataflow_run_id", "type": "string", "required": False},
]


def assert_i2_output(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]] = EXPECTED_ROWS,
) -> None:
    projected = [
        {field: row.get(field) for field in BUSINESS_FIELDS}
        for row in actual
    ]
    assert_exact_rows(projected, expected, key="id", label="Iceberg added-column merge")


def assert_i2_schema(actual: list[dict[str, Any]]) -> None:
    if actual != EXPECTED_SCHEMA:
        raise AssertionError(
            f"Iceberg added-column schema mismatch: actual={actual!r}, expected={EXPECTED_SCHEMA!r}"
        )


def assert_i2_transactional_evidence(
    before_schema: list[dict[str, Any]],
    after_schema: list[dict[str, Any]],
    before_snapshots: list[int],
    after_snapshots: list[int],
    before_schema_id: int,
    after_schema_id: int,
) -> None:
    if any(field["name"] == NEW_FIELD for field in before_schema):
        raise AssertionError("new field was pre-materialized in the native seed schema")
    if not any(field["name"] == NEW_FIELD for field in after_schema):
        raise AssertionError("transactional merge did not persist the new field")
    if after_schema_id == before_schema_id:
        raise AssertionError(
            f"schema id did not evolve transactionally: before={before_schema_id}, after={after_schema_id}"
        )
    if len(after_snapshots) <= len(before_snapshots):
        raise AssertionError(
            f"transactional merge did not create a new snapshot: before={before_snapshots!r}, after={after_snapshots!r}"
        )


def _assert_transactional_strategy(
    evidence: Any,
    flow_name: str,
    expected_schema_id: int,
) -> str:
    messages = [
        value.get("msg", "")
        for _, value in records(LOG_DIR, evidence.job_id, "system_logs", evidence.session_id)
        if value.get("logger") == "datacoolie.engines._polars.iceberg.operations"
        and "Iceberg merge strategy=" in value.get("msg", "")
    ]
    if len(messages) != 1:
        raise AssertionError(f"{flow_name} strategy log count mismatch: {messages!r}")
    message = messages[0]
    if (
        "Iceberg merge strategy=transactional_key_overwrite" not in message
        or f"current_schema_id={expected_schema_id}" not in message
        or f"snapshot_schema_id={expected_schema_id}" not in message
    ):
        raise AssertionError(f"{flow_name} did not take the transactional branch: {message!r}")
    if "added_columns=['new_field']" not in message or "reorder_required=True" not in message:
        raise AssertionError(f"{flow_name} route omitted schema evolution evidence: {message!r}")
    return message


def _seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": 1,
            "value": "old-one",
            "__file_name": None,
            "__file_path": None,
            "__file_modification_time": None,
            "__created_at": None,
            "__updated_at": None,
            "__updated_by": None,
            "__dataflow_run_id": None,
        },
        {
            "id": 2,
            "value": "old-two",
            "__file_name": None,
            "__file_path": None,
            "__file_modification_time": None,
            "__created_at": None,
            "__updated_at": None,
            "__updated_by": None,
            "__dataflow_run_id": None,
        },
    ]


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("merge", root)
    materialize_fixture(case, "a9_iceberg_schema_merge.json")
    namespace = f"a9_i2_{uuid.uuid4().hex[:12]}"
    rewrite_iceberg_namespace(case, namespace)
    catalog = new_catalog()
    seed_schema_value = seed_schema(SEED_FIELDS)
    seed_table(catalog, namespace, TABLE_NAME, _seed_rows(), seed_schema_value)
    before_table = catalog.load_table(f"{namespace}.{TABLE_NAME}")
    before_schema = schema_fingerprint(catalog, namespace, TABLE_NAME)
    before_snapshots = snapshot_ids(catalog, namespace, TABLE_NAME)
    before_schema_id = before_table.metadata.current_schema_id
    if any(field["name"] == NEW_FIELD for field in before_schema):
        raise AssertionError("seed unexpectedly contains the added business field")

    source_engine = new_engine(catalog)
    write_parquet(
        source_engine,
        case.input / "records",
        [
            {"id": 1, "value": "changed-one", NEW_FIELD: 10},
            {"id": 3, "value": "new-three", NEW_FIELD: 30},
        ],
    )
    evidence = run_driver(
        case,
        stage=STAGE_NAME,
        iteration=1,
        engine=new_engine(catalog),
    )
    assert_successful_flow(evidence, FLOW_NAME)
    strategy_log = _assert_transactional_strategy(evidence, FLOW_NAME, before_schema_id)

    reloaded = new_catalog()
    rows = table_rows(reloaded, namespace, TABLE_NAME)
    schema = schema_fingerprint(reloaded, namespace, TABLE_NAME)
    after_table = reloaded.load_table(f"{namespace}.{TABLE_NAME}")
    after_snapshots = snapshot_ids(reloaded, namespace, TABLE_NAME)
    assert_i2_output(business_rows(rows))
    assert_i2_schema(schema)
    assert_i2_transactional_evidence(
        before_schema,
        schema,
        before_snapshots,
        after_snapshots,
        before_schema_id,
        after_table.metadata.current_schema_id,
    )

    payload = receipt_payload(
        case,
        runs=[evidence],
        details={
            "namespace": namespace,
            "table": TABLE_NAME,
            "seed_schema": before_schema,
            "seed_new_field_absent": NEW_FIELD not in {field["name"] for field in before_schema},
            "schema": schema,
            "rows": business_rows(rows),
            "snapshot_ids": after_snapshots,
            "transactional_branch": {
                "name": "transactional_key_overwrite",
                "seed_schema_id": before_schema_id,
                "final_schema_id": after_table.metadata.current_schema_id,
                "added_field_id": next(field["field_id"] for field in schema if field["name"] == NEW_FIELD),
                "routing_log": strategy_log,
            },
            "package_versions": {
                name: importlib.metadata.version(name)
                for name in ("polars", "pyiceberg", "pyarrow")
            },
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies the Polars REST-catalog Iceberg transactional schema-evolution profile only; Spark, Delta, and remote cloud profiles remain separate.",
        ),
        references=(
            "src/datacoolie/engines/_polars/iceberg/operations.py",
            "src/datacoolie/engines/_polars/iceberg/schema.py",
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
