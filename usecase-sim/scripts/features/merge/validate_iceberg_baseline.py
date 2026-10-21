"""Qualify the existing Iceberg merge baseline through REST and normal Driver."""

from __future__ import annotations

import argparse
import importlib.metadata
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    assert_exact_rows,
    assert_successful_flow,
    business_rows,
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
    snapshot_ids,
    spec_fingerprint,
    table_rows,
)


INITIAL_ROWS = [
    {
        "order_id": 1,
        "order_date": "2024-01-01",
        "amount": "10.50",
        "modified_at": "2024-01-01T10:00:00",
        "region": "North",
    },
    {
        "order_id": 2,
        "order_date": "2024-01-02",
        "amount": "11.25",
        "modified_at": "2024-01-02T10:00:00",
        "region": "South",
    },
]
INCOMING_ROWS = [
    {
        "order_id": 1,
        "order_date": "2024-01-01",
        "amount": "12.00",
        "modified_at": "2024-02-01T10:00:00",
        "region": "North",
    },
    {
        "order_id": 3,
        "order_date": "2024-01-03",
        "amount": "13.75",
        "modified_at": "2024-02-02T10:00:00",
        "region": "East",
    },
]
EXPECTED_FINAL = [
    {
        "order_id": 1,
        "order_date": datetime(2024, 1, 1).date(),
        "amount": "12.00",
        "modified_at": "2024-02-01T10:00:00",
        "region": "North",
        "order_year": 2024,
    },
    {
        "order_id": 2,
        "order_date": datetime(2024, 1, 2).date(),
        "amount": "11.25",
        "modified_at": "2024-01-02T10:00:00",
        "region": "South",
        "order_year": 2024,
    },
    {
        "order_id": 3,
        "order_date": datetime(2024, 1, 3).date(),
        "amount": "13.75",
        "modified_at": "2024-02-02T10:00:00",
        "region": "East",
        "order_year": 2024,
    },
]
BUSINESS_FIELDS = (
    "order_id",
    "order_date",
    "amount",
    "modified_at",
    "region",
    "order_year",
)


def assert_i1_output(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]] = EXPECTED_FINAL,
) -> None:
    """Require exact update, insert, and untouched-tail business state."""

    projected = [
        {name: row.get(name) for name in BUSINESS_FIELDS}
        for row in actual
    ]
    assert_exact_rows(projected, expected, key="order_id", label="Iceberg baseline merge")


def assert_i1_branch_precondition(
    before_schema: list[dict[str, Any]],
    after_schema: list[dict[str, Any]],
    before_spec: list[dict[str, Any]],
    after_spec: list[dict[str, Any]],
) -> None:
    """The rerun keeps schema/spec unchanged, activating native upsert."""

    if before_schema != after_schema:
        raise AssertionError(
            f"baseline rerun evolved schema unexpectedly: before={before_schema!r}, after={after_schema!r}"
        )
    if before_spec != after_spec:
        raise AssertionError(
            f"baseline rerun changed partition spec unexpectedly: before={before_spec!r}, after={after_spec!r}"
        )


def _compile_existing(case: Any) -> dict[str, Any]:
    """Compile the authored baseline flow without editing canonical JSON."""
    from _fixture_support import compile_existing

    document, identity = compile_existing(case, "merge", "load_iceberg__merge_upsert")
    return identity


def _assert_admitted(evidence: Any, flow_name: str) -> None:
    expected = {"succeeded": 1, "failed": 0, "skipped": 0}
    actual = {key: evidence.result.get(key) for key in expected}
    if actual != expected:
        raise AssertionError(f"{flow_name} admitted watermark rerun mismatch: {actual!r}")


def _assert_native_branch(evidence: Any, flow_name: str, expected_schema_id: int) -> str:
    """Require the engine's persisted system-log routing decision."""

    messages = [
        value.get("msg", "")
        for _, value in records(LOG_DIR, evidence.job_id, "system_logs", evidence.session_id)
        if value.get("logger") == "datacoolie.engines._polars.iceberg.operations"
        and "Iceberg merge strategy=" in value.get("msg", "")
    ]
    if len(messages) != 1:
        raise AssertionError(f"{flow_name} native branch log count mismatch: {messages!r}")
    message = messages[0]
    required = (
        "Iceberg merge strategy=native_upsert",
        f"current_schema_id={expected_schema_id}",
        f"snapshot_schema_id={expected_schema_id}",
        "added_columns=[]",
        "reorder_required=False",
        "missing_partitions=[]",
    )
    missing = [part for part in required if part not in message]
    if missing:
        raise AssertionError(f"{flow_name} did not take the native branch: {message!r}")
    return message


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("merge", root)
    identity = _compile_existing(case)
    namespace = f"a9_i1_{uuid.uuid4().hex[:12]}"
    rewrite_iceberg_namespace(case, namespace)
    catalog = new_catalog()
    seed_engine = new_engine(catalog)
    first_mtime = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
    second_mtime = datetime(2026, 2, 1, tzinfo=timezone.utc).timestamp()
    replay_mtime = datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp()

    write_parquet(seed_engine, case.input / "sample", INITIAL_ROWS, mtime=first_mtime)
    first = run_driver(
        case,
        stage=identity["runtime_stage"],
        iteration=1,
        engine=new_engine(catalog),
    )
    assert_successful_flow(first, identity["runtime_flow"])
    table_name = "orders_merge_upsert"
    first_schema = schema_fingerprint(catalog, namespace, table_name)
    first_spec = spec_fingerprint(catalog, namespace, table_name)
    first_schema_id = catalog.load_table(f"{namespace}.{table_name}").metadata.current_schema_id
    first_rows = table_rows(catalog, namespace, table_name)

    write_parquet(
        seed_engine,
        case.input / "sample",
        INCOMING_ROWS,
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
    second_native_log = _assert_native_branch(second, identity["runtime_flow"], first_schema_id)
    after_update = table_rows(catalog, namespace, table_name)
    assert_i1_output(business_rows(after_update))
    second_schema = schema_fingerprint(catalog, namespace, table_name)
    second_spec = spec_fingerprint(catalog, namespace, table_name)
    assert_i1_branch_precondition(first_schema, second_schema, first_spec, second_spec)
    second_schema_id = catalog.load_table(f"{namespace}.{table_name}").metadata.current_schema_id
    if second_schema_id != first_schema_id:
        raise AssertionError(
            f"native baseline merge changed schema id: before={first_schema_id}, after={second_schema_id}"
        )

    # Bump only the incoming file mtime by creating a new, identical source
    # file.  This exercises an admitted watermark replay while native upsert
    # must preserve the exact business state and schema/spec.
    write_parquet(
        seed_engine,
        case.input / "sample",
        INCOMING_ROWS,
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
    _assert_admitted(third, identity["runtime_flow"])
    third_native_log = _assert_native_branch(third, identity["runtime_flow"], second_schema_id)
    reloaded = new_catalog()
    reloaded_rows = table_rows(reloaded, namespace, table_name)
    assert_i1_output(business_rows(reloaded_rows))
    third_schema = schema_fingerprint(reloaded, namespace, table_name)
    third_spec = spec_fingerprint(reloaded, namespace, table_name)
    assert_i1_branch_precondition(second_schema, third_schema, second_spec, third_spec)
    third_schema_id = reloaded.load_table(f"{namespace}.{table_name}").metadata.current_schema_id
    if third_schema_id != second_schema_id:
        raise AssertionError(
            f"native replay changed schema id: before={second_schema_id}, after={third_schema_id}"
        )
    snapshots = snapshot_ids(reloaded, namespace, table_name)
    if len(snapshots) < 3:
        raise AssertionError(f"expected seed/update/admitted-replay Iceberg snapshots, got {snapshots!r}")

    payload = receipt_payload(
        case,
        runs=[first, second, third],
        details={
            "identity_mapping": identity,
            "namespace": namespace,
            "table": table_name,
            "first_rows": business_rows(first_rows),
            "rows": business_rows(reloaded_rows),
            "schema": third_schema,
            "schema_ids": {
                "seed": first_schema_id,
                "changed_batch": second_schema_id,
                "admitted_replay": third_schema_id,
            },
            "partition_spec": third_spec,
            "snapshot_ids": snapshots,
            "native_branch": "native_upsert",
            "native_branch_logs": {
                "changed_batch": second_native_log,
                "admitted_replay": third_native_log,
            },
            "branch_precondition": {
                "schema_unchanged": first_schema == second_schema,
                "spec_unchanged": first_spec == second_spec,
                "admitted_replay_schema_unchanged": second_schema == third_schema,
                "admitted_replay_spec_unchanged": second_spec == third_spec,
                "replay_mtime": datetime.fromtimestamp(replay_mtime, tz=timezone.utc),
            },
            "package_versions": {
                name: importlib.metadata.version(name)
                for name in ("polars", "pyiceberg", "pyarrow")
            },
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies the Polars REST-catalog Iceberg profile only; Spark, Delta, and remote cloud profiles remain separate.",
        ),
        references=(
            "src/datacoolie/engines/_polars/iceberg/operations.py",
            "src/datacoolie/engines/_polars/iceberg/schema.py",
            "src/datacoolie/destinations/strategies/load.py",
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
