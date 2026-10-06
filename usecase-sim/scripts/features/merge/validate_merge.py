"""Qualify native Delta merge updates, inserts, untouched rows, and reruns."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    PolarsEngine,
    assert_exact_rows,
    business_rows,
    materialize_fixture,
    read_delta_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)


def validate(root: Path | None = None) -> dict:
    case = resolve_case_root("merge", root)
    materialize_fixture(case, "merge.json")
    engine = PolarsEngine()
    write_parquet(
        engine,
        case.input / "seed",
        [
            {"id": 1, "value": "old-one", "segment": "keep"},
            {"id": 2, "value": "tail", "segment": "untouched"},
        ],
    )
    write_parquet(
        engine,
        case.input / "incoming",
        [
            {"id": 1, "value": "new-one", "segment": "updated"},
            {"id": 3, "value": "new-three", "segment": "inserted"},
        ],
    )

    runs = [
        run_driver(case, stage="merge_seed", iteration=1),
        run_driver(case, stage="merge_upsert", iteration=1),
    ]
    for evidence in runs:
        assert evidence.result["succeeded"] == 1, evidence.result
    expected = [
        {"id": 1, "value": "new-one", "segment": "updated"},
        {"id": 2, "value": "tail", "segment": "untouched"},
        {"id": 3, "value": "new-three", "segment": "inserted"},
    ]
    target = case.output / "orders"
    assert_exact_rows(
        business_rows(read_delta_rows(engine, target)),
        expected,
        key="id",
        label="merge update/insert/tail",
    )

    # The same incoming files are intentionally replayed. Native merge key
    # matching must keep one row per ID and retain the untouched tail.
    runs.append(run_driver(case, stage="merge_upsert", iteration=2))
    assert runs[-1].result["succeeded"] == 1, runs[-1].result
    assert_exact_rows(
        business_rows(read_delta_rows(engine, target)),
        expected,
        key="id",
        label="merge rerun",
    )

    payload = receipt_payload(
        case,
        runs=runs,
        details={"rows": business_rows(read_delta_rows(engine, target))},
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs only; Spark, Iceberg, and remote lakehouse merge profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/destinations/delta_writer.py",
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
