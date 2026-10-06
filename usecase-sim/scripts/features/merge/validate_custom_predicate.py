"""Qualify a native Delta merge with an authored scoped match predicate."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


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


KEY_FIELDS = ("id", "scope")
MERGE_OPTIONS = {
    "source_alias": "s",
    "target_alias": "t",
    "predicate": "t.id = s.id AND t.scope = s.scope",
}
CUSTOM_PREDICATE_SCHEMA = {
    "id": "Int64",
    "scope": "String",
    "value": "String",
    "segment": "String",
    "__file_name": "String",
    "__file_path": "String",
    "__file_modification_time": "Datetime(time_unit='us', time_zone='UTC')",
    "__created_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_at": "Datetime(time_unit='us', time_zone='UTC')",
    "__updated_by": "String",
    "__dataflow_run_id": "String",
}

SEED_ROWS = [
    {"id": 1, "scope": "A", "value": "old-1A", "segment": "updated"},
    {"id": 1, "scope": "B", "value": "keep-1B", "segment": "scope-b"},
    {"id": 9, "scope": "tail", "value": "keep-tail", "segment": "tail"},
]
FINAL_ROWS = [
    {"id": 1, "scope": "A", "value": "new-1A", "segment": "updated"},
    {"id": 1, "scope": "B", "value": "keep-1B", "segment": "scope-b"},
    {"id": 2, "scope": "A", "value": "new-2A", "segment": "inserted"},
    {"id": 9, "scope": "tail", "value": "keep-tail", "segment": "tail"},
]
INCOMING_ROWS = [
    {"id": 1, "scope": "A", "value": "new-1A", "segment": "updated"},
    {"id": 2, "scope": "A", "value": "new-2A", "segment": "inserted"},
]


def assert_unique_key_tuples(
    rows: Iterable[Mapping[str, Any]],
    *,
    label: str = "custom predicate merge keys",
) -> None:
    """Require one persisted row for every ``(id, scope)`` tuple."""

    values = [tuple(row.get(field) for field in KEY_FIELDS) for row in rows]
    if len(values) != len(set(values)):
        raise AssertionError(f"{label} contains duplicate key tuples: {values!r}")


def assert_custom_predicate_output(
    actual_rows: Iterable[Mapping[str, Any]],
    actual_schema: Mapping[str, str],
    expected_rows: Iterable[Mapping[str, Any]],
    *,
    label: str = "custom predicate merge output",
) -> None:
    """Require exact scoped values, schema and unique predicate tuples."""

    rows = list(actual_rows)
    assert_exact_rows(rows, expected_rows, label=label)
    assert_unique_key_tuples(rows, label=label)
    assert_schema(actual_schema, CUSTOM_PREDICATE_SCHEMA, label=f"{label} schema")


def validate(root: Path | None = None) -> dict:
    case = resolve_case_root("merge", root)
    materialize_fixture(case, "custom_predicate.json")
    engine = PolarsEngine()
    source = case.input / "rows"
    target = case.output / "orders"

    write_parquet(engine, source, SEED_ROWS)
    if target.exists():
        raise AssertionError(f"custom predicate target must be absent before bootstrap: {target}")

    runs = []
    evidence = run_driver(case, stage="merge_custom_predicate", iteration=1)
    assert_successful_flow(evidence, "merge-custom-predicate")
    runs.append(evidence)
    assert_custom_predicate_output(
        business_rows(read_delta_rows(engine, target)),
        read_delta_schema(engine, target),
        SEED_ROWS,
        label="custom predicate bootstrap",
    )
    seed_schema = read_delta_schema(engine, target)

    write_parquet(engine, source, INCOMING_ROWS, mode="overwrite")
    evidence = run_driver(case, stage="merge_custom_predicate", iteration=2)
    assert_successful_flow(evidence, "merge-custom-predicate")
    runs.append(evidence)
    assert_custom_predicate_output(
        business_rows(read_delta_rows(engine, target)),
        read_delta_schema(engine, target),
        FINAL_ROWS,
        label="custom predicate update/insert/tail",
    )

    # The same incoming files are replayed.  Predicate matching must be
    # evidenced by a successful third invocation as well as stable rows.
    evidence = run_driver(case, stage="merge_custom_predicate", iteration=3)
    assert_successful_flow(evidence, "merge-custom-predicate")
    runs.append(evidence)
    assert_custom_predicate_output(
        business_rows(read_delta_rows(engine, target)),
        read_delta_schema(engine, target),
        FINAL_ROWS,
        label="custom predicate rerun",
    )

    payload = receipt_payload(
        case,
        runs=runs,
        details={
            "key_fields": list(KEY_FIELDS),
            "merge_options": MERGE_OPTIONS,
            "seed_rows": SEED_ROWS,
            "seed_schema": seed_schema,
            "rows": business_rows(read_delta_rows(engine, target)),
            "schema": read_delta_schema(engine, target),
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs only; Spark, Iceberg, and remote lakehouse merge profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/destinations/strategies/load.py",
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
