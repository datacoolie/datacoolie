"""Qualify explicit timestamp-NTZ conversion to a UTC instant."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl


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


FLOW_NAME = "ntz-timezone-conversion"
STAGE_NAME = "ntz_timezone"
SOURCE_TABLE = "rows"
TARGET_TABLE = "timestamps"
SOURCE_TIMEZONE = "Asia/Ho_Chi_Minh"

NAIVE_WALL_CLOCK = datetime(2024, 1, 15, 10, 30, 45, 123456)
EXPECTED_UTC_INSTANT = datetime(
    2024, 1, 15, 3, 30, 45, 123456, tzinfo=timezone.utc
)
CONTROL_INSTANTS = (
    datetime(2024, 1, 15, 1, 2, 3, 654321, tzinfo=timezone.utc),
    datetime(2024, 1, 15, 5, 6, 7, 765432, tzinfo=timezone.utc),
)

INPUT_ROWS = [
    {
        "id": 1,
        "value": "wall-clock-value",
        "wall_clock": NAIVE_WALL_CLOCK,
        "aware_control": CONTROL_INSTANTS[0],
    },
    {
        "id": 2,
        "value": "null-wall-clock",
        "wall_clock": None,
        "aware_control": CONTROL_INSTANTS[1],
    },
]

EXPECTED_ROWS = [
    {
        "id": 1,
        "value": "wall-clock-value",
        "wall_clock": EXPECTED_UTC_INSTANT,
        "aware_control": CONTROL_INSTANTS[0],
    },
    {
        "id": 2,
        "value": "null-wall-clock",
        "wall_clock": None,
        "aware_control": CONTROL_INSTANTS[1],
    },
]

INPUT_SCHEMA = {
    "id": "Int64",
    "value": "String",
    "wall_clock": "Datetime(time_unit='us', time_zone=None)",
    "aware_control": "Datetime(time_unit='us', time_zone='UTC')",
}

PERSISTED_SCHEMA = {
    "id": "Int64",
    "value": "String",
    "wall_clock": "Datetime(time_unit='us', time_zone='UTC')",
    "aware_control": "Datetime(time_unit='us', time_zone='UTC')",
    "__file_name": "String",
    "__file_path": "String",
    "__file_modification_time": "Datetime(time_unit='us', time_zone='UTC')",
    "__created_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_by": "String",
    "__dataflow_run_id": "String",
}


def _schema(frame: Any) -> dict[str, str]:
    return {name: str(dtype) for name, dtype in frame.collect_schema().items()}


def assert_input_ntz_schema(actual_schema: dict[str, str]) -> None:
    """Require the fixture to contain distinct naive and UTC-aware columns."""

    assert_schema(actual_schema, INPUT_SCHEMA, label="ntz input schema")


def assert_ntz_output(
    actual_rows: list[dict[str, Any]],
    actual_schema: dict[str, str],
    *,
    label: str = "ntz output",
) -> None:
    """Require exact converted instants, null/control values, and persisted types."""

    assert_exact_rows(actual_rows, EXPECTED_ROWS, key="id", label=f"{label} rows")
    assert_schema(actual_schema, PERSISTED_SCHEMA, label=f"{label} schema")


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("schema_types", root)
    materialize_fixture(case, "ntz_timezone.json")
    engine = PolarsEngine()
    source = case.input / SOURCE_TABLE
    target = case.output / TARGET_TABLE

    write_parquet(engine, source, INPUT_ROWS)
    # Inspect the physical Parquet schema directly.  The engine reader adds
    # ``__file_path`` for Driver metadata, which is deliberately outside this
    # typed-input proof.
    assert_input_ntz_schema(_schema(pl.scan_parquet(str(source))))

    runs = []
    first = run_driver(case, stage=STAGE_NAME, iteration=1)
    assert_successful_flow(first, FLOW_NAME)
    runs.append(first)
    first_rows = business_rows(read_delta_rows(engine, target))
    first_schema = read_delta_schema(engine, target)
    assert_ntz_output(first_rows, first_schema, label="ntz overwrite")

    # Overwrite replay must execute the same flow and preserve the exact
    # business result, including the microsecond instant and null row.
    second = run_driver(case, stage=STAGE_NAME, iteration=2)
    assert_successful_flow(second, FLOW_NAME)
    runs.append(second)
    second_rows = business_rows(read_delta_rows(engine, target))
    second_schema = read_delta_schema(engine, target)
    assert_ntz_output(second_rows, second_schema, label="ntz overwrite rerun")
    assert_exact_rows(second_rows, first_rows, key="id", label="ntz stable rerun")
    assert_schema(second_schema, first_schema, label="ntz stable rerun schema")

    payload = receipt_payload(
        case,
        runs=runs,
        details={
            "flow": FLOW_NAME,
            "stage": STAGE_NAME,
            "source_timezone": SOURCE_TIMEZONE,
            "input_schema": INPUT_SCHEMA,
            "persisted_schema": second_schema,
            "rows": second_rows,
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs only; Spark, Iceberg, and remote lakehouse timezone profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/core/models/transform.py",
            "src/datacoolie/transformers/schema_converter.py",
            "src/datacoolie/engines/_polars/transforms.py",
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
