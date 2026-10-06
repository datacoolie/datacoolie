"""Qualify the four focused A9 local transformer branches through Driver."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
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


UPPERCASE_FLOW = "a9-transform-uppercase"
MAP_NULL_FLOW = "a9-transform-map-null"
PARTIAL_KEEP_START_FLOW = "a9-transform-partial-keep-start"
TIMESTAMP_TRUNCATE_FLOW = "a9-transform-timestamp-truncate-hour"

UPPERCASE_STAGE = "a9_transform_uppercase"
MAP_NULL_STAGE = "a9_transform_map_null"
PARTIAL_KEEP_START_STAGE = "a9_transform_partial_keep_start"
TIMESTAMP_TRUNCATE_STAGE = "a9_transform_timestamp_truncate_hour"

SOURCE_ROWS = [
    {
        "id": 1,
        "mixed_text": "hello Đặng",
        "status": "A",
        "phone": "123456789",
        "event_time": datetime(2024, 1, 1, 10, 34, 56, 123456),
    },
    {
        "id": 2,
        "mixed_text": "ß café",
        "status": "unknown",
        "phone": "xy",
        "event_time": datetime(2024, 1, 1, 23, 59, 59, 999999),
    },
    {
        "id": 3,
        "mixed_text": None,
        "status": None,
        "phone": None,
        "event_time": None,
    },
    {
        "id": 4,
        "mixed_text": "ASCII Mix",
        "status": "I",
        "phone": "",
        "event_time": datetime(2024, 1, 2, 9, 1, 0, 1),
    },
]

EXPECTED_UPPERCASE_ROWS = [
    {**row, "mixed_text": expected}
    for row, expected in zip(
        SOURCE_ROWS,
        ["HELLO ĐẶNG", "SS CAFÉ", None, "ASCII MIX"],
        strict=True,
    )
]
EXPECTED_MAP_NULL_ROWS = [
    {**row, "status": expected}
    for row, expected in zip(
        SOURCE_ROWS,
        ["active", None, None, "inactive"],
        strict=True,
    )
]
EXPECTED_PARTIAL_KEEP_START_ROWS = [
    {**row, "phone": expected}
    for row, expected in zip(
        SOURCE_ROWS,
        ["12#89", "#", None, ""],
        strict=True,
    )
]
EXPECTED_TIMESTAMP_TRUNCATE_ROWS = [
    {**row, "event_time": expected}
    for row, expected in zip(
        SOURCE_ROWS,
        [
            datetime(2024, 1, 1, 10, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 23, tzinfo=timezone.utc),
            None,
            datetime(2024, 1, 2, 9, tzinfo=timezone.utc),
        ],
        strict=True,
    )
]

EXPECTED_SCHEMA = {
    "id": "Int64",
    "mixed_text": "String",
    "status": "String",
    "phone": "String",
    "event_time": "Datetime(time_unit='us', time_zone='UTC')",
    "__file_path": "String",
    "__file_name": "String",
    "__file_modification_time": "Datetime(time_unit='us', time_zone='UTC')",
    "__created_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_by": "String",
    "__dataflow_run_id": "String",
}
EXPECTED_TIMESTAMP_SCHEMA = {
    **EXPECTED_SCHEMA,
    "event_time": "Datetime(time_unit='us', time_zone='UTC')",
}
EXPECTED_SCHEMA["event_time"] = "Datetime(time_unit='us', time_zone=None)"

EXPECTED_OUTPUTS = {
    UPPERCASE_FLOW: ("uppercase", EXPECTED_UPPERCASE_ROWS, EXPECTED_SCHEMA),
    MAP_NULL_FLOW: ("map_null", EXPECTED_MAP_NULL_ROWS, EXPECTED_SCHEMA),
    PARTIAL_KEEP_START_FLOW: (
        "partial_keep_start",
        EXPECTED_PARTIAL_KEEP_START_ROWS,
        EXPECTED_SCHEMA,
    ),
    TIMESTAMP_TRUNCATE_FLOW: (
        "timestamp_truncate_hour",
        EXPECTED_TIMESTAMP_TRUNCATE_ROWS,
        EXPECTED_TIMESTAMP_SCHEMA,
    ),
}


def _assert_output(
    rows: Iterable[Mapping[str, Any]],
    schema: Mapping[str, str],
    expected: Iterable[Mapping[str, Any]],
    *,
    expected_schema: Mapping[str, str] = EXPECTED_SCHEMA,
    label: str,
) -> None:
    assert_exact_rows(
        business_rows(rows),
        expected,
        key="id",
        label=f"{label}-rows",
    )
    assert_schema(schema, expected_schema, label=f"{label}-schema")


def assert_uppercase_output(
    rows: Iterable[Mapping[str, Any]],
    schema: Mapping[str, str],
    *,
    label: str = "a9-uppercase",
) -> None:
    _assert_output(rows, schema, EXPECTED_UPPERCASE_ROWS, label=label)


def assert_map_null_output(
    rows: Iterable[Mapping[str, Any]],
    schema: Mapping[str, str],
    *,
    label: str = "a9-map-null",
) -> None:
    _assert_output(rows, schema, EXPECTED_MAP_NULL_ROWS, label=label)


def assert_partial_keep_start_output(
    rows: Iterable[Mapping[str, Any]],
    schema: Mapping[str, str],
    *,
    label: str = "a9-partial-keep-start",
) -> None:
    _assert_output(rows, schema, EXPECTED_PARTIAL_KEEP_START_ROWS, label=label)


def assert_timestamp_truncate_output(
    rows: Iterable[Mapping[str, Any]],
    schema: Mapping[str, str],
    *,
    label: str = "a9-timestamp-truncate-hour",
) -> None:
    _assert_output(
        rows,
        schema,
        EXPECTED_TIMESTAMP_TRUNCATE_ROWS,
        expected_schema=EXPECTED_TIMESTAMP_SCHEMA,
        label=label,
    )


def assert_a9_transform_metadata(metadata: Mapping[str, Any]) -> None:
    """Verify the authored options activate each requested branch."""

    flows = {item["name"]: item for item in metadata["dataflows"]}
    assert set(flows) == {
        UPPERCASE_FLOW,
        MAP_NULL_FLOW,
        PARTIAL_KEEP_START_FLOW,
        TIMESTAMP_TRUNCATE_FLOW,
    }
    assert flows[UPPERCASE_FLOW]["transform"]["value_rules"] == [
        {"operation": "case", "columns": ["mixed_text"], "mode": "upper"}
    ]
    assert flows[MAP_NULL_FLOW]["transform"]["value_rules"][0]["on_unmapped"] == "null"
    assert flows[PARTIAL_KEEP_START_FLOW]["transform"]["masking_rules"] == [
        {
            "method": "partial",
            "columns": ["phone"],
            "keep_start": 2,
            "keep_end": 2,
            "mask_char": "#",
        }
    ]
    timestamp = flows[TIMESTAMP_TRUNCATE_FLOW]["transform"]
    assert timestamp["schema_hints"] == [
        {"column_name": "event_time", "data_type": "TIMESTAMP"}
    ]
    assert timestamp["masking_rules"] == [
        {"method": "date_truncate", "columns": ["event_time"], "unit": "hour"}
    ]


BEHAVIORS = {
    "uppercase": UPPERCASE_FLOW,
    "map_null": MAP_NULL_FLOW,
    "partial_keep_start": PARTIAL_KEEP_START_FLOW,
    "timestamp_hour": TIMESTAMP_TRUNCATE_FLOW,
}


def validate(root: Path | None = None, behavior: str = "all") -> dict[str, Any]:
    if behavior != "all" and behavior not in BEHAVIORS:
        raise ValueError(f"Unknown transform behavior: {behavior}")
    case = resolve_case_root("transform", root)
    materialize_fixture(case, "a9_local.json")
    engine = PolarsEngine()
    write_parquet(engine, case.input / "rows", SOURCE_ROWS)

    runs = []
    for flow_name, stage in (
        (UPPERCASE_FLOW, UPPERCASE_STAGE),
        (MAP_NULL_FLOW, MAP_NULL_STAGE),
        (PARTIAL_KEEP_START_FLOW, PARTIAL_KEEP_START_STAGE),
        (TIMESTAMP_TRUNCATE_FLOW, TIMESTAMP_TRUNCATE_STAGE),
    ):
        if behavior != "all" and flow_name != BEHAVIORS[behavior]:
            continue
        evidence = run_driver(case, stage=stage, iteration=1)
        assert_successful_flow(evidence, flow_name)
        runs.append(evidence)

    output_details: dict[str, Any] = {}
    for flow_name, (table, expected, expected_schema) in EXPECTED_OUTPUTS.items():
        if behavior != "all" and flow_name != BEHAVIORS[behavior]:
            continue
        rows = read_delta_rows(engine, case.output / table)
        schema = read_delta_schema(engine, case.output / table)
        _assert_output(
            rows,
            schema,
            expected,
            expected_schema=expected_schema,
            label=flow_name,
        )
        output_details[flow_name] = {"rows": business_rows(rows), "schema": schema}

    payload = receipt_payload(
        case,
        runs=runs,
        details={
            "behavior": behavior,
            "source_rows": SOURCE_ROWS,
            "outputs": output_details,
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs only; Spark and remote lakehouse profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/core/models/transform.py",
            "src/datacoolie/engines/_polars/transforms.py",
        ),
    )
    write_receipt(case, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    parser.add_argument("--behavior", choices=["all", *BEHAVIORS], default="all")
    args = parser.parse_args()
    payload = validate(args.root, args.behavior)
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
