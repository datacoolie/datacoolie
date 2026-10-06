"""Qualify native Delta maintenance evidence and missing-target skips."""

from __future__ import annotations

import argparse
import os
import sys
import time
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
    business_rows,
    materialize_fixture,
    read_delta_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_receipt,
)


def _active_delta_files(path: Path) -> set[str]:
    """Return active native Delta file names from the table transaction log."""

    from deltalake import DeltaTable

    return {
        str(uri).replace("\\", "/").rsplit("/", 1)[-1]
        for uri in DeltaTable(str(path)).file_uris()
    }


def _physical_delta_files(path: Path) -> set[str]:
    return {item.name for item in path.rglob("*.parquet")}


def assert_maintenance_operations(
    details: Iterable[Mapping[str, Any]],
    expected: Mapping[str, str],
    *,
    label: str = "maintenance operations",
) -> None:
    """Require each requested operation's persisted status exactly."""

    actual = {
        str(item.get("operation")): str(item.get("status"))
        for item in details
        if item.get("operation") in expected
    }
    actual = {name: actual[name] for name in expected if name in actual}
    if actual != dict(expected):
        raise AssertionError(f"{label} mismatch: actual={actual!r}, expected={dict(expected)!r}")


def _operation_details(evidence) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for record in evidence.records:
        candidate = record.get("operation_details")
        if isinstance(candidate, list):
            details.extend(item for item in candidate if isinstance(item, dict))
    if not details:
        raise AssertionError(
            f"no persisted maintenance operation_details for {evidence.job_id}/{evidence.session_id}"
        )
    return details


def _block_native_cleanup(
    case,
    runs,
    details: Mapping[str, Any],
    reason: str,
) -> None:
    """Persist an explicit blocked receipt before surfacing a native gap."""

    payload = receipt_payload(
        case,
        runs=runs,
        details=details,
        script=SCRIPT,
        status="blocked",
        actual_exit_code=1,
        qualification_gaps=(
            reason,
            "Local Polars compaction and missing-target gates have native evidence; maintenance cleanup deletion remains blocked until the runtime forwards dry_run=False.",
            "Spark, Iceberg, and remote lakehouse maintenance profiles remain unqualified.",
        ),
        references=(
            "src/datacoolie/engines/_polars/delta.py",
            "src/datacoolie/destinations/base.py",
        ),
    )
    write_receipt(case, payload)
    print(
        {
            "status": payload["status"],
            "receipt": payload["receipt"],
            "root": payload["root"],
            "profile": payload["profile"],
        }
    )
    raise RuntimeError(reason)


def validate(root: Path | None = None) -> dict:
    case = resolve_case_root("maintenance", root)
    materialize_fixture(case, "maintenance.json")
    engine = PolarsEngine()

    eligible = case.output / "eligible"
    protected = case.output / "protected"
    engine.write_to_path(
        engine.create_dataframe([{"id": 1, "value": "eligible-one"}]),
        str(eligible),
        mode="overwrite",
        fmt="delta",
    )
    engine.write_to_path(
        engine.create_dataframe([{"id": 2, "value": "eligible-two"}]),
        str(eligible),
        mode="append",
        fmt="delta",
    )
    eligible_active_before = _active_delta_files(eligible)
    # Make the two eligible source files older than the zero-hour retention
    # cutoff.  This keeps the native vacuum decision deterministic even when
    # compaction and cleanup share one fast Driver session.
    old_file_time = datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp()
    for file_path in eligible.rglob("*.parquet"):
        os.utime(file_path, (old_file_time, old_file_time))
    engine.write_to_path(
        engine.create_dataframe([{"id": 8, "value": "protected-old"}]),
        str(protected),
        mode="overwrite",
        fmt="delta",
    )
    protected_initial_active = _active_delta_files(protected)
    # A second overwrite creates a recent tombstone.  Cleanup with the native
    # 168-hour retention must preserve that obsolete file.
    engine.write_to_path(
        engine.create_dataframe([{"id": 9, "value": "protected"}]),
        str(protected),
        mode="overwrite",
        fmt="delta",
    )
    protected_files_after_overwrite = _active_delta_files(protected)
    protected_obsolete = protected_initial_active - protected_files_after_overwrite
    if not protected_obsolete:
        raise AssertionError(
            "protected setup did not create a physical recent tombstone for retention evidence"
        )

    eligible_compact_run = run_driver(
        case,
        stage="maintenance_eligible",
        iteration=1,
        maintenance=True,
        do_compact=True,
        do_cleanup=False,
        retention_hours=168,
    )
    # delta-rs compares tombstone commit timestamps with the current cutoff;
    # keep cleanup as a separate native Driver run after a clock boundary.
    time.sleep(1.2)
    eligible_cleanup_run = run_driver(
        case,
        stage="maintenance_eligible",
        iteration=2,
        maintenance=True,
        do_compact=False,
        do_cleanup=True,
        retention_hours=0,
    )
    runs = [
        eligible_compact_run,
        eligible_cleanup_run,
        run_driver(
            case,
            stage="maintenance_protected",
            iteration=1,
            maintenance=True,
            do_compact=False,
            do_cleanup=True,
            retention_hours=168,
        ),
        run_driver(
            case,
            stage="maintenance_missing",
            iteration=1,
            maintenance=True,
            do_compact=True,
            do_cleanup=True,
            retention_hours=168,
        ),
    ]
    assert runs[0].result["succeeded"] == 1, runs[0].result
    assert runs[1].result["succeeded"] == 1, runs[1].result
    assert runs[2].result["succeeded"] == 1, runs[2].result
    assert runs[3].result["skipped"] == 1, runs[3].result

    eligible_ops = _operation_details(runs[0]) + _operation_details(runs[1])
    protected_ops = _operation_details(runs[2])
    missing_ops = _operation_details(runs[3])
    assert_maintenance_operations(
        eligible_ops,
        {"compact": "succeeded", "cleanup": "succeeded"},
        label="eligible native Delta operations",
    )
    assert_maintenance_operations(
        protected_ops,
        {"cleanup": "succeeded"},
        label="protected cleanup operation",
    )
    assert_maintenance_operations(
        missing_ops,
        {"compact": "skipped", "cleanup": "skipped"},
        label="missing target owner reuse",
    )

    assert_exact_rows(
        business_rows(read_delta_rows(engine, eligible)),
        [{"id": 1, "value": "eligible-one"}, {"id": 2, "value": "eligible-two"}],
        key="id",
        label="eligible rows after maintenance",
    )
    assert_exact_rows(
        business_rows(read_delta_rows(engine, protected)),
        [{"id": 9, "value": "protected"}],
        key="id",
        label="protected rows after cleanup",
    )
    eligible_active_after = _active_delta_files(eligible)
    eligible_obsolete = eligible_active_before - eligible_active_after
    if not eligible_obsolete:
        _block_native_cleanup(
            case,
            runs,
            {
                "eligible_operations": eligible_ops,
                "protected_operations": protected_ops,
                "missing_operations": missing_ops,
                "eligible_active_before": sorted(eligible_active_before),
                "eligible_active_after": sorted(eligible_active_after),
            },
            f"eligible compaction did not tombstone active files: before={eligible_active_before!r}, after={eligible_active_after!r}",
        )
    eligible_physical_after = _physical_delta_files(eligible)
    if eligible_obsolete & eligible_physical_after:
        _block_native_cleanup(
            case,
            runs,
            {
                "eligible_operations": eligible_ops,
                "protected_operations": protected_ops,
                "missing_operations": missing_ops,
                "eligible_active_before": sorted(eligible_active_before),
                "eligible_active_after": sorted(eligible_active_after),
                "eligible_obsolete": sorted(eligible_obsolete),
                "eligible_physical_after": sorted(eligible_physical_after),
            },
            "native eligible cleanup left tombstoned files physically present; Polars delta_ops.cleanup calls DeltaTable.vacuum without dry_run=False, so delta-rs performs its default dry run",
        )
    protected_files_after_cleanup = _physical_delta_files(protected)
    if not protected_obsolete.issubset(protected_files_after_cleanup):
        _block_native_cleanup(
            case,
            runs,
            {
                "eligible_operations": eligible_ops,
                "protected_operations": protected_ops,
                "missing_operations": missing_ops,
                "protected_obsolete": sorted(protected_obsolete),
                "protected_files_after_cleanup": sorted(protected_files_after_cleanup),
            },
            f"native protected cleanup removed a recent tombstone before 168-hour retention: obsolete={protected_obsolete!r}, after={protected_files_after_cleanup!r}",
        )
    history = engine.get_history(path=str(eligible), limit=20, fmt="delta")
    history_operations = [str(item.get("operation", "")) for item in history]
    if not any("optimize" in operation.lower() or "compact" in operation.lower() for operation in history_operations):
        _block_native_cleanup(
            case,
            runs,
            {
                "eligible_operations": eligible_ops,
                "protected_operations": protected_ops,
                "missing_operations": missing_ops,
                "eligible_history": history,
            },
            f"eligible compaction lacks native Delta history evidence: {history_operations!r}",
        )

    payload = receipt_payload(
        case,
        runs=runs,
        details={
            "eligible_operations": eligible_ops,
            "protected_operations": protected_ops,
            "missing_operations": missing_ops,
            "eligible_history": history,
            "eligible_active_before": sorted(eligible_active_before),
            "eligible_active_after": sorted(eligible_active_after),
            "eligible_obsolete": sorted(eligible_obsolete),
            "eligible_physical_after": sorted(eligible_physical_after),
            "protected_initial_active": sorted(protected_initial_active),
            "protected_active_after_overwrite": sorted(protected_files_after_overwrite),
            "protected_obsolete": sorted(protected_obsolete),
            "protected_files_after_cleanup": sorted(protected_files_after_cleanup),
        },
        script=SCRIPT,
        qualification_gaps=(
            "This receipt qualifies local Polars plus native Delta-rs maintenance only; Spark, Iceberg, and remote lakehouse maintenance profiles remain unqualified.",
            "Delta-rs cleanup is exercised with a zero-hour retention for the eligible target and a default retention for recent protected files; retention policy semantics outside this profile remain unqualified.",
        ),
        references=(
            "src/datacoolie/destinations/base.py",
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
