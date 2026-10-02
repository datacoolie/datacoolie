"""Check that provider startup failure is visible in system and job logs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


USECASE_SIM_DIR = Path(__file__).resolve().parent.parent
DEFAULT_LOG_ROOT = USECASE_SIM_DIR / ".runtime" / "logs"


def _load_records(root: Path, job_id: str) -> list[dict]:
    records: list[dict] = []
    for path in root.rglob("*.json") if root.exists() else []:
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                value = json.loads(line)
                if isinstance(value, dict) and value.get("job_id") == job_id:
                    records.append(value)
        except (OSError, json.JSONDecodeError):
            continue
    return records


def validate(job_id: str, *, log_root: Path = DEFAULT_LOG_ROOT) -> None:
    records = _load_records(log_root, job_id)
    events = {record.get("event_name") for record in records}
    if "session.starting" not in events or "session.startup_failed" not in events:
        raise AssertionError(f"Startup lifecycle events are incomplete: {events}")
    jobs = [record for record in records if record.get("_type") == "job_run_log"]
    if len(jobs) != 1 or jobs[0].get("status") != "failed":
        raise AssertionError(f"Expected one failed startup JobRuntime snapshot: {jobs}")
    dataflows = [record for record in records if record.get("_type") == "dataflow_run_log"]
    if dataflows:
        raise AssertionError("Startup failure must occur before a dataflow runtime record")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--log-root", default=str(DEFAULT_LOG_ROOT))
    args = parser.parse_args()
    validate(args.job_id, log_root=Path(args.log_root).expanduser().resolve())
    print(f"validated startup failure logs for job_id={args.job_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
