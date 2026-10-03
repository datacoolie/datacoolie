"""Check that provider startup failure is visible in system and job logs."""

from __future__ import annotations

import os

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
sys.path.insert(0, str(SIM_ROOT / 'scripts'))
from runtime_logs import records


USECASE_SIM_DIR = SIM_ROOT
DEFAULT_LOG_ROOT = USECASE_SIM_DIR / ".runtime" / "logs"


def validate(job_id: str, *, log_root: Path = DEFAULT_LOG_ROOT, log_session_id: str | None = None) -> None:
    values = [value for category in ("execution_logs", "system_logs")
              for _, value in records(log_root, job_id, category, log_session_id)]
    events = {record.get("event_name") for record in values}
    if "session.starting" not in events or "session.startup_failed" not in events:
        raise AssertionError(f"Startup lifecycle events are incomplete: {events}")
    jobs = [record for record in values if record.get("_type") == "job_run_log"]
    if len(jobs) != 1 or jobs[0].get("status") != "failed":
        raise AssertionError(f"Expected one failed startup JobRuntime snapshot: {jobs}")
    dataflows = [record for record in values if record.get("_type") == "dataflow_run_log"]
    if dataflows:
        raise AssertionError("Startup failure must occur before a dataflow runtime record")


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--log-session-id")
    parser.add_argument("--log-root", default=str(DEFAULT_LOG_ROOT))
    args = parser.parse_args()
    validate(args.job_id, log_root=Path(args.log_root).expanduser().resolve(), log_session_id=args.log_session_id)
    print(f"validated startup failure logs for job_id={args.job_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
