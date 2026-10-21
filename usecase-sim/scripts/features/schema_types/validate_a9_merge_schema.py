"""Qualify explicit Delta ``mergeSchema`` append after a native old-table seed."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    PolarsEngine,
    assert_exact_rows,
    assert_schema,
    assert_successful_flow,
    business_rows,
    materialize_fixture,
    read_delta_rows,
    read_delta_schema,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)


FLOW_NAME = "a9-schema-merge-schema-append"
STAGE_NAME = "a9_schema_merge_schema_append"
NEW_COLUMN = "new_field"
OLD_ROWS = [
    {"id": 1, "value": "old-one"},
    {"id": 2, "value": "old-two"},
]
NEW_ROWS = [
    {"id": 3, "value": "new-three", NEW_COLUMN: 30},
]
EXPECTED_ROWS = [
    {"id": 1, "value": "old-one", NEW_COLUMN: None},
    {"id": 2, "value": "old-two", NEW_COLUMN: None},
    {"id": 3, "value": "new-three", NEW_COLUMN: 30},
]
SEED_SCHEMA = {"id": "Int64", "value": "String"}
EXPECTED_SCHEMA = {
    "id": "Int64",
    "value": "String",
    NEW_COLUMN: "Int64",
    "__file_path": "String",
    "__file_name": "String",
    "__file_modification_time": "Datetime(time_unit='us', time_zone='UTC')",
    "__created_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_by": "String",
    "__dataflow_run_id": "String",
}
PHASE_CONTRACT = {
    "seed": {
        "execution": "native_setup",
        "target": "events",
        "new_field_present": False,
    },
    "feature_write": {
        "execution": "normal_driver",
        "stage": STAGE_NAME,
        "flow": FLOW_NAME,
        "expected_result": {"succeeded": 1, "failed": 0, "skipped": 0},
    },
}


def assert_seed_schema(
    schema: Mapping[str, str],
    *,
    label: str = "a9-schema-merge-schema-seed",
) -> None:
    if NEW_COLUMN in schema:
        raise AssertionError(f"{label}: {NEW_COLUMN!r} was pre-materialized")
    assert_schema(schema, SEED_SCHEMA, label=label)


def assert_merge_schema_output(
    rows: Iterable[Mapping[str, Any]],
    schema: Mapping[str, str],
    *,
    label: str = "a9-schema-merge-schema",
) -> None:
    assert_exact_rows(
        business_rows(rows),
        EXPECTED_ROWS,
        key="id",
        label=f"{label}-rows",
    )
    assert_schema(schema, EXPECTED_SCHEMA, label=f"{label}-schema")


def assert_a9_merge_schema_metadata(metadata: Mapping[str, Any]) -> None:
    flows = metadata["dataflows"]
    assert len(flows) == 1
    flow = flows[0]
    assert flow["name"] == FLOW_NAME
    assert flow["stage"] == STAGE_NAME
    assert flow["source"]["table"] == "new_schema"
    assert flow["destination"]["load_type"] == "append"
    assert flow["destination"]["configure"]["write_options"] == {
        "mergeSchema": True
    }
    assert flow.get("transform", {}) in ({}, {"schema_hints": []})


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("schema_types", root)
    materialize_fixture(case, "a9_merge_schema.json")
    metadata = case.document
    assert_a9_merge_schema_metadata(metadata)

    engine = PolarsEngine()
    target = case.output / "events"
    # Native seed is setup only.  The feature write below must cross the
    # normal Driver path so the explicit option is exercised end to end.
    engine.write_to_path(
        engine.create_dataframe(OLD_ROWS),
        str(target),
        mode="overwrite",
        fmt="delta",
    )
    seed_schema = read_delta_schema(engine, target)
    assert_seed_schema(seed_schema)

    write_parquet(engine, case.input / "new_schema", NEW_ROWS)
    evidence = run_driver(case, stage=STAGE_NAME, iteration=1)
    assert_successful_flow(evidence, FLOW_NAME)

    rows = read_delta_rows(engine, target)
    schema = read_delta_schema(engine, target)
    assert_merge_schema_output(rows, schema)

    payload = receipt_payload(
        case,
        runs=[evidence],
        details={
            "phase_contract": PHASE_CONTRACT,
            "seed_schema": seed_schema,
            "seed_new_column_absent": NEW_COLUMN not in seed_schema,
            "rows": business_rows(rows),
            "schema": schema,
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs schema merge only; Spark, Iceberg, and remote schema evolution profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/engines/_polars/delta.py",
            "src/datacoolie/destinations/strategies/load.py",
        ),
    )
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
