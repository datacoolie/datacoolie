"""Validate independent simulator roots for logs, watermarks, and output."""

from __future__ import annotations

import os

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runner.runtime_paths import DATACOOLIE_ROOT, LOG_DIR, USECASE_SIM_DIR as SIM_ROOT
from runner.runtime_logs import require_job_logs
sys.path.insert(0, str(SIM_ROOT / 'scripts'))


import pyarrow.parquet as pq
from datacoolie.watermark.base import WatermarkSerializer
from datacoolie.utils.identity import name_to_uuid
from runner.runtime_logs import records

EXPECTED_ROWS = [(101, 10.0, "alpha"), (102, 20.0, "beta"), (103, 30.0, "gamma")]


def assert_state_values(rows, checkpoint) -> None:
    if sorted(rows) != EXPECTED_ROWS:
        raise AssertionError(f"Exact state rows mismatch: {rows!r}")
    if checkpoint != {"order_id": 103}:
        raise AssertionError(f"Exact state checkpoint mismatch: {checkpoint!r}")


USECASE_SIM_DIR = SIM_ROOT
DEFAULT_STATE_ROOT = USECASE_SIM_DIR / ".runtime" / "watermarks" / "state_contract"
DEFAULT_LOG_ROOT = LOG_DIR


def validate(
    state_root: Path = DEFAULT_STATE_ROOT,
    *,
    output: Path,
    log_root: Path = DEFAULT_LOG_ROOT,
    job_id: str = "usecase-state-base-path",
    log_session_id: str | None = None,
) -> None:
    state_root = state_root.expanduser().resolve()
    output = output.expanduser().resolve()
    log_root = log_root.expanduser().resolve()
    watermark_root = state_root / "watermarks"
    if not log_session_id:
        raise AssertionError("A current log session ID is required")
    if any(a == b or a.is_relative_to(b) or b.is_relative_to(a)
           for a, b in [(state_root, log_root), (output, log_root), (output, state_root)]):
        raise AssertionError("Output, state and log roots must be independent")
    require_job_logs(log_root, job_id, log_session_id)
    flows = [value for _, value in records(log_root, job_id, "execution_logs", log_session_id)
             if value.get("_type") == "dataflow_run_log"]
    if len(flows) != 1 or flows[0].get("dataflow_id") != name_to_uuid("state_base_path") or flows[0].get("status") != "succeeded":
        raise AssertionError(f"No unique successful state flow in current session: {flows!r}")
    if Path(flows[0].get("destination_path") or "").resolve() != output:
        raise AssertionError("Current-session destination does not match the output root")
    watermark_files = list(watermark_root.rglob("watermark_value.json")) if watermark_root.exists() else []
    if len(watermark_files) != 1:
        raise AssertionError(f"Expected one checkpoint below {watermark_root}")
    if not watermark_files[0].parent.name.endswith(name_to_uuid("state_base_path")):
        raise AssertionError("Checkpoint does not belong to the expected state flow")
    if (state_root / "logs").exists():
        raise AssertionError(f"Logs leaked into the watermark area: {state_root}")

    parquet_files = sorted(output.rglob("*.parquet")) if output.exists() else []
    if not parquet_files:
        raise AssertionError(f"No Parquet output found below {output}")
    table = pq.read_table([str(path) for path in parquet_files], columns=["order_id", "amount", "category"])
    rows = [(row["order_id"], row["amount"], row["category"]) for row in table.to_pylist()]
    assert_state_values(rows, WatermarkSerializer.deserialize(watermark_files[0].read_text()))


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", default=str(DEFAULT_STATE_ROOT))
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-root", default=str(DEFAULT_LOG_ROOT))
    parser.add_argument("--job-id", default="usecase-state-base-path")
    parser.add_argument("--log-session-id")
    args = parser.parse_args()
    validate(
        Path(args.state_root),
        output=Path(args.output),
        log_root=Path(args.log_root),
        job_id=args.job_id,
        log_session_id=args.log_session_id,
    )
    print(f"validated state paths below {Path(args.state_root).expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
