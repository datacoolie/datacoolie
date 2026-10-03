"""Validate the v4 execution/system log contract for one simulator job."""

from __future__ import annotations

import os

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
sys.path.insert(0, str(SIM_ROOT / 'scripts'))

from typing import Any
from runtime_logs import records


USECASE_SIM_DIR = SIM_ROOT
DEFAULT_LOG_ROOT = USECASE_SIM_DIR / ".runtime" / "logs"


def validate(
    job_id: str,
    *,
    dataflow_name: str,
    source_query: str,
    mode: str,
    log_root: Path = DEFAULT_LOG_ROOT,
    run_attributes: dict[str, Any] | None = None,
    log_session_id: str | None = None,
) -> None:
    execution = list(records(log_root, job_id, "execution_logs", log_session_id))
    dataflows = [
        (path, record)
        for path, record in execution
        if record.get("_type") == "dataflow_run_log"
    ]
    jobs = [
        (path, record)
        for path, record in execution
        if record.get("_type") == "job_run_log"
    ]
    if len(dataflows) != 1:
        raise AssertionError(f"Expected one terminal dataflow record, got {len(dataflows)}")
    if len(jobs) != 1:
        raise AssertionError(f"Expected one replace-one job snapshot, got {len(jobs)}")

    dataflow_path, dataflow = dataflows[0]
    job_path, job = jobs[0]
    for label, record in (("dataflow", dataflow), ("job", job)):
        if record.get("log_schema_version") != 4:
            raise AssertionError(f"{label} record is not schema v4: {record}")
    if dataflow.get("dataflow_name") != dataflow_name:
        raise AssertionError(f"Unexpected dataflow name: {dataflow.get('dataflow_name')}")
    if dataflow.get("source_query") != source_query:
        raise AssertionError(
            f"Original source_query was not preserved: {dataflow.get('source_query')!r}"
        )
    action = dataflow.get("source_action")
    if not isinstance(action, str):
        raise AssertionError("source_action must be a serialized JSON object")
    action_value = json.loads(action)
    executed_query = action_value.get("query") if isinstance(action_value, dict) else None
    if not isinstance(executed_query, str) or not executed_query.strip().lower().startswith("select"):
        raise AssertionError(f"Executed SQL was not recorded in source_action: {action}")
    if dataflow.get("status") != "succeeded" or job.get("status") != "succeeded":
        raise AssertionError("Expected the contract workload to succeed")
    if job.get("job_id") != job_id:
        raise AssertionError("Job snapshot identity does not match the requested job_id")
    if run_attributes is not None:
        stored = job.get("run_attributes")
        if not isinstance(stored, str) or json.loads(stored) != run_attributes:
            raise AssertionError(f"run_attributes were not persisted as expected: {stored!r}")

    if mode == "snapshot":
        if "_part_" in dataflow_path.name:
            raise AssertionError(f"Snapshot dataflow log unexpectedly uses a batch part: {dataflow_path}")
    elif mode == "batch":
        if "_part_" not in dataflow_path.name:
            raise AssertionError(f"Batch dataflow log is missing a part suffix: {dataflow_path}")
        if "_part_" in job_path.name:
            raise AssertionError("Job runtime must remain a replace-one snapshot")
    else:
        raise AssertionError(f"Unsupported log mode: {mode}")

    system = list(records(log_root, job_id, "system_logs", log_session_id))
    if not system:
        raise AssertionError("No system log records were persisted for the job")
    if not all(record.get("log_schema_version") == 4 for _, record in system):
        raise AssertionError("System log records are not schema v4")


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--log-session-id")
    parser.add_argument("--dataflow-name", required=True)
    parser.add_argument("--source-query", required=True)
    parser.add_argument("--mode", choices=("snapshot", "batch"), default="snapshot")
    parser.add_argument("--run-attributes", default=None)
    parser.add_argument("--log-root", default=str(DEFAULT_LOG_ROOT))
    args = parser.parse_args()
    attributes = json.loads(args.run_attributes) if args.run_attributes else None
    validate(
        args.job_id,
        dataflow_name=args.dataflow_name,
        source_query=args.source_query,
        mode=args.mode,
        log_root=Path(args.log_root).expanduser().resolve(),
        run_attributes=attributes,
        log_session_id=args.log_session_id,
    )
    print(f"validated {args.mode} execution logs for job_id={args.job_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
