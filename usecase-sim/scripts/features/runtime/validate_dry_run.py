"""Validate that a dry-run performs preparation but no business/state I/O."""

from __future__ import annotations

import os

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
from runtime_logs import require_job_logs
sys.path.insert(0, str(SIM_ROOT / 'scripts'))


USECASE_SIM_DIR = SIM_ROOT


def validate(*, output: Path, state_root: Path, log_root: Path,
             job_id: str = "usecase-dry-run-query-file", log_session_id: str | None = None) -> None:
    output = output.expanduser().resolve()
    state_root = state_root.expanduser().resolve()
    if output.exists():
        raise AssertionError(f"Dry-run unexpectedly created business output: {output}")
    watermark_root = state_root / "watermarks"
    if watermark_root.exists() and any(watermark_root.rglob("*.json")):
        raise AssertionError(
            f"Dry-run unexpectedly wrote watermark state below {watermark_root}"
        )
    require_job_logs(log_root.expanduser().resolve(), job_id, log_session_id)


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--job-id", default="usecase-dry-run-query-file")
    parser.add_argument("--log-session-id")
    parser.add_argument(
        "--log-root",
        default=str(USECASE_SIM_DIR / ".runtime" / "logs"),
        help="Explicit logger root; state_base_path remains the watermark root",
    )
    args = parser.parse_args()
    validate(
        output=Path(args.output),
        state_root=Path(args.state_root),
        log_root=Path(args.log_root),
        job_id=args.job_id,
        log_session_id=args.log_session_id,
    )
    print("validated dry-run side-effect boundary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
