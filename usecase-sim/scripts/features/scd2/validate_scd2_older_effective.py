"""Qualify an older-effective SCD2 input admitted by a newer source mtime."""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SCRIPT = Path(__file__).resolve()
FEATURES_DIR = SCRIPT.parents[1]
if str(FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(FEATURES_DIR))

from _stateful_support import (  # noqa: E402
    PolarsEngine,
    materialize_fixture,
    read_delta_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)
from scd2.validate_scd2 import assert_scd2_history  # noqa: E402


def assert_older_effective_history(
    actual: Iterable[Mapping[str, Any]],
    expected: Iterable[Mapping[str, Any]],
    *,
    label: str = "older-effective SCD2 history",
) -> None:
    """Require exact rows and the append-all current-row cardinality."""

    actual_rows = list(actual)
    expected_rows = list(expected)
    required = {"id", "value", "modified_at", "__valid_from", "__valid_to", "__is_current"}
    for index, row in enumerate(actual_rows):
        missing = required.difference(row)
        if missing:
            raise AssertionError(f"{label} row {index} missing history columns: {sorted(missing)!r}")
    if len(actual_rows) != len(expected_rows):
        raise AssertionError(
            f"{label} count mismatch: actual={len(actual_rows)!r}, "
            f"expected={len(expected_rows)!r}"
        )

    assert_scd2_history(actual_rows, expected_rows, label=label)

    actual_counts = Counter(row.get("id") for row in actual_rows)
    expected_counts = Counter(row.get("id") for row in expected_rows)
    if actual_counts != expected_counts:
        raise AssertionError(
            f"{label} key counts mismatch: actual={actual_counts!r}, "
            f"expected={expected_counts!r}"
        )

    actual_current = Counter(
        row.get("id") for row in actual_rows if row.get("__is_current") is True
    )
    expected_current = Counter(
        row.get("id") for row in expected_rows if row.get("__is_current") is True
    )
    if actual_current != expected_current:
        raise AssertionError(
            f"{label} current-key counts mismatch: actual={actual_current!r}, "
            f"expected={expected_current!r}"
        )


def _assert_admitted(result: Mapping[str, Any], *, label: str) -> None:
    if result.get("succeeded") != 1 or result.get("skipped") != 0:
        raise AssertionError(f"{label} was not admitted: {dict(result)!r}")


def _history_row(
    *,
    identifier: int,
    value: str,
    modified_at: datetime,
    current: bool = True,
    valid_to: datetime | None = None,
) -> dict[str, Any]:
    return {
        "id": identifier,
        "value": value,
        "modified_at": modified_at,
        "__valid_from": modified_at,
        "__valid_to": valid_to,
        "__is_current": current,
    }


def validate(root: Path | None = None) -> dict[str, Any]:
    case = resolve_case_root("scd2", root)
    materialize_fixture(case, "older_effective.json")
    engine = PolarsEngine()

    effective_old = datetime(2024, 1, 1)
    effective_control = datetime(2024, 1, 15)
    effective_new = datetime(2024, 2, 1)
    seed_mtime = datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()
    late_mtime = datetime(2024, 2, 1, tzinfo=timezone.utc).timestamp()

    write_parquet(
        engine,
        case.input / "records",
        [
            {"id": 1, "value": "new-version", "modified_at": effective_new},
            {"id": 2, "value": "untouched", "modified_at": effective_control},
        ],
        mtime=seed_mtime,
    )

    seed_history = [
        _history_row(identifier=1, value="new-version", modified_at=effective_new),
        _history_row(identifier=2, value="untouched", modified_at=effective_control),
    ]
    late_history = [
        _history_row(identifier=1, value="new-version", modified_at=effective_new),
        _history_row(
            identifier=1, value="late-version", modified_at=effective_old
        ),
        _history_row(identifier=2, value="untouched", modified_at=effective_control),
    ]

    runs = [run_driver(case, stage="scd2_older_effective", iteration=1)]
    _assert_admitted(runs[-1].result, label="seed flow")
    target = case.output / "customers"
    actual_after_seed = read_delta_rows(engine, target)
    assert_older_effective_history(
        actual_after_seed,
        seed_history,
        label="older-effective seed history",
    )

    # Append the late file only after the seed has been acknowledged.  The
    # second invocation reuses this flow's persisted mtime watermark, while
    # its effective time intentionally moves backwards.
    write_parquet(
        engine,
        case.input / "records",
        [{"id": 1, "value": "late-version", "modified_at": effective_old}],
        mode="append",
        mtime=late_mtime,
    )
    runs.append(run_driver(case, stage="scd2_older_effective", iteration=2))
    _assert_admitted(runs[-1].result, label="late flow")
    actual_after_late = read_delta_rows(engine, target)
    assert_older_effective_history(
        actual_after_late,
        late_history,
        label="older-effective late history",
    )

    current_row_counts = dict(
        sorted(
            Counter(
                row["id"]
                for row in actual_after_late
                if row.get("__is_current") is True
            ).items()
        )
    )
    if current_row_counts != {1: 2, 2: 1}:
        raise AssertionError(f"unexpected current-row counts: {current_row_counts!r}")

    payload = receipt_payload(
        case,
        runs=runs,
        details={
            "primary_flow": "scd2-older-effective-late",
            "flow_roles": {
                "scd2-older-effective-late": "primary",
                "seed_invocation": "support",
            },
            "effective_times": {
                "old": effective_old,
                "control": effective_control,
                "new": effective_new,
            },
            "source_file_mtimes": {
                "seed": datetime.fromtimestamp(seed_mtime, tz=timezone.utc),
                "late": datetime.fromtimestamp(late_mtime, tz=timezone.utc),
            },
            "expected_history_after_seed": seed_history,
            "expected_history_after_late": late_history,
            "history_after_seed": actual_after_seed,
            "history_after_late": actual_after_late,
            "current_row_counts": current_row_counts,
        },
        script=SCRIPT,
        qualification_gaps=(
            "Only host-local Polars plus native Delta-rs is qualified; Spark, Iceberg, and remote lakehouse SCD2 profiles remain unqualified.",
            "The late input is admitted by a file mtime newer than the persisted seed watermark even though its effective timestamp is older; native SCD2 therefore appends it and permits two current rows for id=1.",
        ),
        references=(
            "src/datacoolie/sources/file_reader.py",
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
