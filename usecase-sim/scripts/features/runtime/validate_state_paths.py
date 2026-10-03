"""Validate independent simulator roots for logs, watermarks, and output."""

from __future__ import annotations

import os

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runtime_paths import DATACOOLIE_ROOT, LOG_DIR, USECASE_SIM_DIR as SIM_ROOT
from runtime_logs import require_job_logs
sys.path.insert(0, str(SIM_ROOT / 'scripts'))


import pyarrow.parquet as pq


USECASE_SIM_DIR = SIM_ROOT
DEFAULT_STATE_ROOT = USECASE_SIM_DIR / ".runtime" / "watermarks" / "state_contract"
DEFAULT_LOG_ROOT = LOG_DIR


def validate(
    state_root: Path = DEFAULT_STATE_ROOT,
    *,
    output: Path,
    log_root: Path = DEFAULT_LOG_ROOT,
    minimum_rows: int = 1,
    job_id: str = "usecase-state-base-path",
    log_session_id: str | None = None,
) -> None:
    state_root = state_root.expanduser().resolve()
    output = output.expanduser().resolve()
    log_root = log_root.expanduser().resolve()
    watermark_root = state_root / "watermarks"
    require_job_logs(log_root, job_id, log_session_id)
    watermark_files = list(watermark_root.rglob("*.json")) if watermark_root.exists() else []
    if not watermark_files:
        raise AssertionError(f"No watermark was written below {watermark_root}")
    if (state_root / "logs").exists():
        raise AssertionError(f"Logs leaked into the watermark area: {state_root}")

    parquet_files = sorted(output.rglob("*.parquet")) if output.exists() else []
    if not parquet_files:
        raise AssertionError(f"No Parquet output found below {output}")
    rows = pq.read_table([str(path) for path in parquet_files]).num_rows
    if rows < minimum_rows:
        raise AssertionError(f"Expected at least {minimum_rows} rows, got {rows}")


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", default=str(DEFAULT_STATE_ROOT))
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-root", default=str(DEFAULT_LOG_ROOT))
    parser.add_argument("--minimum-rows", type=int, default=1)
    parser.add_argument("--job-id", default="usecase-state-base-path")
    parser.add_argument("--log-session-id")
    args = parser.parse_args()
    validate(
        Path(args.state_root),
        output=Path(args.output),
        log_root=Path(args.log_root),
        minimum_rows=args.minimum_rows,
        job_id=args.job_id,
        log_session_id=args.log_session_id,
    )
    print(f"validated state paths below {Path(args.state_root).expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
