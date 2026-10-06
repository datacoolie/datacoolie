"""Qualify an Iceberg identity partition-spec change through normal Driver."""

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
    spec_fingerprint,
    table_rows,
)


FLOW_NAME = "a9-iceberg-partition-change"
STAGE_NAME = "a9_iceberg_partition_change"
TABLE_NAME = "events"
BUSINESS_FIELDS = ("id", "value", "region")
SEED_FIELDS = (("id", pa.int64()), ("value", pa.string()), ("region", pa.string()))
EXPECTED_ROWS = [
    {"id": 1, "value": "updated-one", "region": "north"},
    {"id": 2, "value": "untouched-two", "region": "south"},
    {"id": 3, "value": "inserted-three", "region": "east"},
]
EXPECTED_SCHEMA = [
    {"field_id": 1, "name": "id", "type": "long", "required": False},
    {"field_id": 2, "name": "value", "type": "string", "required": False},
    {"field_id": 3, "name": "region", "type": "string", "required": False},
    {"field_id": 4, "name": "__file_name", "type": "string", "required": False},
    {"field_id": 5, "name": "__file_path", "type": "string", "required": False},
    {"field_id": 6, "name": "__file_modification_time", "type": "timestamptz", "required": False},
    {"field_id": 7, "name": "__created_at", "type": "timestamptz", "required": False},
    {"field_id": 8, "name": "__updated_at", "type": "timestamptz", "required": False},
    {"field_id": 9, "name": "__updated_by", "type": "string", "required": False},
    {"field_id": 10, "name": "__dataflow_run_id", "type": "string", "required": False},
]
EXPECTED_SPEC = [
    {"field_id": 1000, "source_id": 3, "name": "region", "transform": "identity"}
]


def assert_i3_output(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]] = EXPECTED_ROWS,
) -> None:
    projected = [
        {field: row.get(field) for field in BUSINESS_FIELDS}
        for row in actual
    ]
    assert_exact_rows(projected, expected, key="id", label="Iceberg partition merge")


def assert_i3_schema(actual: list[dict[str, Any]]) -> None:
    if actual != EXPECTED_SCHEMA:
        raise AssertionError(
            f"Iceberg partition schema changed unexpectedly: actual={actual!r}, expected={EXPECTED_SCHEMA!r}"
        )


def assert_i3_spec(actual: list[dict[str, Any]]) -> None:
    if actual != EXPECTED_SPEC:
        raise AssertionError(
            f"Iceberg partition spec mismatch: actual={actual!r}, expected={EXPECTED_SPEC!r}"
        )


def assert_i3_branch_evidence(
    before_schema: list[dict[str, Any]],
    after_schema: list[dict[str, Any]],
    before_spec: list[dict[str, Any]],
    after_spec: list[dict[str, Any]],
) -> None:
    before_business = {field["name"] for field in before_schema if not field["name"].startswith("__")}
    after_business = {field["name"] for field in after_schema if not field["name"].startswith("__")}
    if before_business != after_business:
        raise AssertionError(
            f"partition branch added or removed a business column: before={before_business!r}, after={after_business!r}"
        )
    if before_spec:
        raise AssertionError(f"native seed unexpectedly had a partition spec: {before_spec!r}")
    assert_i3_spec(after_spec)


def _assert_strategy(
    evidence: Any,
    flow_name: str,
    expected: str,
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
        f"Iceberg merge strategy={expected}" not in message
        or f"current_schema_id={expected_schema_id}" not in message
        or f"snapshot_schema_id={expected_schema_id}" not in message
    ):
        raise AssertionError(f"{flow_name} strategy mismatch: {message!r}")
    return message


def _seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": 1,
            "value": "old-one",
            "region": "north",
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
            "value": "untouched-two",
            "region": "south",
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
    materialize_fixture(case, "a9_iceberg_partition_change.json")
    namespace = f"a9_i3_{uuid.uuid4().hex[:12]}"
    rewrite_iceberg_namespace(case, namespace)
    catalog = new_catalog()
    seed_table(catalog, namespace, TABLE_NAME, _seed_rows(), seed_schema(SEED_FIELDS))
    before_schema = schema_fingerprint(catalog, namespace, TABLE_NAME)
    before_spec = spec_fingerprint(catalog, namespace, TABLE_NAME)
    before_schema_id = catalog.load_table(f"{namespace}.{TABLE_NAME}").metadata.current_schema_id
    before_snapshots = snapshot_ids(catalog, namespace, TABLE_NAME)
    source_engine = new_engine(catalog)
    write_parquet(
        source_engine,
        case.input / "records",
        [
            {"id": 1, "value": "updated-one", "region": "north"},
            {"id": 3, "value": "inserted-three", "region": "east"},
        ],
    )

    first = run_driver(
        case,
        stage=STAGE_NAME,
        iteration=1,
        engine=new_engine(catalog),
    )
    assert_successful_flow(first, FLOW_NAME)
    first_strategy_log = _assert_strategy(
        first, FLOW_NAME, "transactional_key_overwrite", before_schema_id
    )
    after_first_rows = table_rows(catalog, namespace, TABLE_NAME)
    after_first_schema = schema_fingerprint(catalog, namespace, TABLE_NAME)
    after_first_spec = spec_fingerprint(catalog, namespace, TABLE_NAME)
    assert_i3_output(business_rows(after_first_rows))
    assert_i3_schema(after_first_schema)
    assert_i3_branch_evidence(before_schema, after_first_schema, before_spec, after_first_spec)
    after_first_schema_id = catalog.load_table(f"{namespace}.{TABLE_NAME}").metadata.current_schema_id
    if after_first_schema_id != before_schema_id:
        raise AssertionError(
            f"partition-only merge changed schema id: before={before_schema_id}, after={after_first_schema_id}"
        )

    second = run_driver(
        case,
        stage=STAGE_NAME,
        iteration=2,
        engine=new_engine(catalog),
    )
    assert_successful_flow(second, FLOW_NAME)
    second_strategy_log = _assert_strategy(
        second, FLOW_NAME, "native_upsert", after_first_schema_id
    )
    reloaded = new_catalog()
    final_rows = table_rows(reloaded, namespace, TABLE_NAME)
    final_schema = schema_fingerprint(reloaded, namespace, TABLE_NAME)
    final_spec = spec_fingerprint(reloaded, namespace, TABLE_NAME)
    assert_i3_output(business_rows(final_rows))
    if final_schema != after_first_schema or final_spec != after_first_spec:
        raise AssertionError(
            "partition rerun changed persisted schema/spec: "
            f"first=({after_first_schema!r}, {after_first_spec!r}), "
            f"rerun=({final_schema!r}, {final_spec!r})"
        )
    snapshots = snapshot_ids(reloaded, namespace, TABLE_NAME)
    if len(snapshots) <= len(before_snapshots):
        raise AssertionError(f"partition branch did not create snapshots: {snapshots!r}")

    payload = receipt_payload(
        case,
        runs=[first, second],
        details={
            "namespace": namespace,
            "table": TABLE_NAME,
            "seed_schema": before_schema,
            "schema": final_schema,
            "rows": business_rows(final_rows),
            "partition_spec": final_spec,
            "schema_ids": {
                "seed": before_schema_id,
                "partition_change": after_first_schema_id,
                "stable_rerun": reloaded.load_table(f"{namespace}.{TABLE_NAME}").metadata.current_schema_id,
            },
            "snapshot_ids": snapshots,
            "transactional_branch": {
                "name": "transactional_key_overwrite",
                "missing_partition_before": True,
                "business_columns_added": [],
                "rerun_branch": "native_upsert",
                "routing_logs": {
                    "partition_change": first_strategy_log,
                    "stable_rerun": second_strategy_log,
                },
            },
            "package_versions": {
                name: importlib.metadata.version(name)
                for name in ("polars", "pyiceberg", "pyarrow")
            },
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies the Polars REST-catalog Iceberg partition-spec profile only; Spark, Delta, and remote cloud profiles remain separate.",
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
