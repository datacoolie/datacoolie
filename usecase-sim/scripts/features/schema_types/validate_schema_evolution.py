"""Qualify native Delta schema evolution with new and fewer source columns."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    PolarsEngine,
    assert_exact_rows,
    assert_schema,
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


def validate(root: Path | None = None) -> dict:
    case = resolve_case_root("schema_types", root)
    materialize_fixture(case, "schema_evolution.json")
    engine = PolarsEngine()
    write_parquet(
        engine,
        case.input / "old_schema",
        [
            {"id": 1, "event_date": date(2024, 1, 1), "value": "old-one"},
            {"id": 2, "event_date": date(2024, 1, 2), "value": "old-two"},
        ],
    )
    write_parquet(
        engine,
        case.input / "new_schema",
        [
            {
                "id": 3,
                "event_date": date(2024, 1, 3),
                "value": "new-three",
                "extra": 30,
            }
        ],
    )
    write_parquet(
        engine,
        case.input / "fewer_schema",
        [{"id": 4, "event_date": date(2024, 1, 4), "value": "fewer-four"}],
    )

    runs = [
        run_driver(case, stage="schema_seed", iteration=1),
        run_driver(case, stage="schema_new", iteration=1),
        run_driver(case, stage="schema_fewer", iteration=1),
    ]
    for evidence in runs:
        assert evidence.result["succeeded"] == 1, evidence.result

    target = case.output / "events"
    assert_exact_rows(
        business_rows(read_delta_rows(engine, target)),
        [
            {"id": 1, "event_date": date(2024, 1, 1), "value": "old-one", "extra": None},
            {"id": 2, "event_date": date(2024, 1, 2), "value": "old-two", "extra": None},
            {"id": 3, "event_date": date(2024, 1, 3), "value": "new-three", "extra": 30},
            {"id": 4, "event_date": date(2024, 1, 4), "value": "fewer-four", "extra": None},
        ],
        key="id",
        label="schema evolution rows",
    )
    schema = read_delta_schema(engine, target)
    assert_schema(
        schema,
        {
            "id": "Int64",
            "event_date": "Date",
            "value": "String",
            "__file_name": "String",
            "__file_path": "String",
            "__file_modification_time": "Datetime(time_unit='us', time_zone='UTC')",
            "__created_at": "Datetime(time_unit='us', time_zone='UTC')",
            "__updated_at": "Datetime(time_unit='us', time_zone='UTC')",
            "__updated_by": "String",
            "__dataflow_run_id": "String",
            "extra": "Int64",
        },
        label="schema evolution types",
    )

    payload = receipt_payload(
        case,
        runs=runs,
        details={"rows": business_rows(read_delta_rows(engine, target)), "schema": schema},
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs append schema merge; Spark, Iceberg, and remote schema evolution profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/transformers/schema_converter.py",
            "src/datacoolie/engines/_polars/delta.py",
        ),
    )
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
