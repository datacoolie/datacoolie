"""Qualify native Delta SCD2 initial, changed, unchanged, and rerun state."""

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
    canonical_rows,
    materialize_fixture,
    read_delta_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)


def assert_scd2_history(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]],
    *,
    label: str = "SCD2 history",
) -> None:
    """Require exact version rows, close/open boundaries, and current flags."""

    fields = (
        "id",
        "value",
        "modified_at",
        "__valid_from",
        "__valid_to",
        "__is_current",
    )
    actual_projected = [{field: row.get(field) for field in fields} for row in actual]
    expected_projected = [{field: row.get(field) for field in fields} for row in expected]
    if canonical_rows(actual_projected) != canonical_rows(expected_projected):
        raise AssertionError(
            f"{label} mismatch: actual={canonical_rows(actual_projected)!r}, "
            f"expected={canonical_rows(expected_projected)!r}"
        )


def validate(root: Path | None = None) -> dict:
    case = resolve_case_root("scd2", root)
    materialize_fixture(case, "scd2.json")
    engine = PolarsEngine()
    first_mtime = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc).timestamp()
    second_mtime = datetime(2024, 2, 1, 0, 0, tzinfo=timezone.utc).timestamp()
    write_parquet(
        engine,
        case.input / "initial",
        [
            {"id": 1, "value": "old-one", "modified_at": datetime(2024, 1, 1)},
            {"id": 2, "value": "stable-two", "modified_at": datetime(2024, 1, 1)},
        ],
        mtime=first_mtime,
    )
    write_parquet(
        engine,
        case.input / "changed",
        [{"id": 1, "value": "new-one", "modified_at": datetime(2024, 2, 1)}],
        mtime=second_mtime,
    )

    runs = [
        run_driver(case, stage="scd2_seed", iteration=1),
    ]
    assert runs[-1].result["succeeded"] == 1, runs[-1].result
    target = case.output / "customers"
    assert_scd2_history(
        read_delta_rows(engine, target),
        [
            {
                "id": 1,
                "value": "old-one",
                "modified_at": datetime(2024, 1, 1),
                "__valid_from": datetime(2024, 1, 1),
                "__valid_to": None,
                "__is_current": True,
            },
            {
                "id": 2,
                "value": "stable-two",
                "modified_at": datetime(2024, 1, 1),
                "__valid_from": datetime(2024, 1, 1),
                "__valid_to": None,
                "__is_current": True,
            },
        ],
        label="SCD2 initial history",
    )

    runs.append(run_driver(case, stage="scd2_change", iteration=1))
    assert runs[-1].result["succeeded"] == 1, runs[-1].result
    assert_scd2_history(
        read_delta_rows(engine, target),
        [
            {
                "id": 1,
                "value": "old-one",
                "modified_at": datetime(2024, 1, 1),
                "__valid_from": datetime(2024, 1, 1),
                "__valid_to": datetime(2024, 2, 1),
                "__is_current": False,
            },
            {
                "id": 1,
                "value": "new-one",
                "modified_at": datetime(2024, 2, 1),
                "__valid_from": datetime(2024, 2, 1),
                "__valid_to": None,
                "__is_current": True,
            },
            {
                "id": 2,
                "value": "stable-two",
                "modified_at": datetime(2024, 1, 1),
                "__valid_from": datetime(2024, 1, 1),
                "__valid_to": None,
                "__is_current": True,
            },
        ],
        label="SCD2 changed and untouched history",
    )

    # The changed input file is now behind the persisted mtime watermark. A
    # normal rerun therefore skips without appending duplicate versions.
    runs.append(run_driver(case, stage="scd2_change", iteration=2))
    assert runs[-1].result["skipped"] == 1, runs[-1].result
    assert_scd2_history(
        read_delta_rows(engine, target),
        [
            {
                "id": 1,
                "value": "old-one",
                "modified_at": datetime(2024, 1, 1),
                "__valid_from": datetime(2024, 1, 1),
                "__valid_to": datetime(2024, 2, 1),
                "__is_current": False,
            },
            {
                "id": 1,
                "value": "new-one",
                "modified_at": datetime(2024, 2, 1),
                "__valid_from": datetime(2024, 2, 1),
                "__valid_to": None,
                "__is_current": True,
            },
            {
                "id": 2,
                "value": "stable-two",
                "modified_at": datetime(2024, 1, 1),
                "__valid_from": datetime(2024, 1, 1),
                "__valid_to": None,
                "__is_current": True,
            },
        ],
        label="SCD2 watermark-stable rerun",
    )

    # The native contract appends each incoming version.  Keep a concrete
    # isolated probe in the receipt so equal-timestamp input is recorded as a
    # permitted duplicate current version rather than silently treated as a
    # suppression guarantee.  The supported Driver flow above uses a source
    # mtime watermark, so its actual rerun remains a no-op.
    probe_target = case.output / "incoming_unchanged_probe"
    import polars as pl

    probe_input = pl.DataFrame(
        {
            "id": [7],
            "value": ["same"],
            "__valid_from": [datetime(2024, 1, 1)],
            "__valid_to": pl.Series("__valid_to", [None], dtype=pl.Datetime("us")),
            "__is_current": [True],
        }
    ).lazy()
    engine.write_to_path(probe_input, str(probe_target), mode="overwrite", fmt="delta")
    unchanged = probe_input
    engine.scd2_to_path(unchanged, str(probe_target), merge_keys=["id"], fmt="delta")
    probe_after_first = read_delta_rows(engine, probe_target)
    probe_expected_row = {
        "id": 7,
        "value": "same",
        "modified_at": None,
        "__valid_from": datetime(2024, 1, 1),
        "__valid_to": None,
        "__is_current": True,
    }
    assert_scd2_history(
        probe_after_first,
        [probe_expected_row, probe_expected_row],
        label="SCD2 equal-timestamp permitted append",
    )
    engine.scd2_to_path(unchanged, str(probe_target), merge_keys=["id"], fmt="delta")
    probe_after_second = read_delta_rows(engine, probe_target)
    assert_scd2_history(
        probe_after_second,
        [probe_expected_row, probe_expected_row, probe_expected_row],
        label="SCD2 equal-timestamp repeated append",
    )
    if len(probe_after_first) != 2 or len(probe_after_second) != 3:
        raise AssertionError(
            "native equal-timestamp SCD2 append contract changed unexpectedly: "
            f"first={probe_after_first!r}, second={probe_after_second!r}"
        )

    payload = receipt_payload(
        case,
        runs=runs,
        details={
            "history": read_delta_rows(engine, target),
            "current_ids": [
                row["id"]
                for row in read_delta_rows(engine, target)
                if row.get("__is_current")
            ],
            "incoming_unchanged_probe": {
                "after_first": probe_after_first,
                "after_second": probe_after_second,
            },
        },
        script=SCRIPT,
        qualification_gaps=(
            "Native Polars SCD2 appends every incoming row, including equal or older effective timestamps; upstream filtering/deduplication owns unchanged-input suppression.",
            "The receipt includes an isolated equal-timestamp native append probe and qualifies a file-mtime-watermarked Driver rerun; it does not claim unbounded replay idempotency.",
            "Only local Polars plus native Delta-rs is qualified; Spark, Iceberg, and remote lakehouse SCD2 profiles remain unqualified.",
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
    print({"status": payload["status"], "receipt": payload["receipt"], "root": payload["root"], "profile": payload["profile"]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
