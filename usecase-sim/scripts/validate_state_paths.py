"""Validate state-root derivation for logs, watermarks, and output."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq


USECASE_SIM_DIR = Path(__file__).resolve().parent.parent
DEFAULT_STATE_ROOT = USECASE_SIM_DIR / ".runtime" / "state_contract"


def validate(
    state_root: Path = DEFAULT_STATE_ROOT,
    *,
    output: Path,
    minimum_rows: int = 1,
) -> None:
    state_root = state_root.expanduser().resolve()
    output = output.expanduser().resolve()
    log_root = state_root / "logs"
    watermark_root = state_root / "watermarks"
    if not (log_root / "system_logs").exists():
        raise AssertionError(f"Derived system log root is missing: {log_root / 'system_logs'}")
    if not (log_root / "execution_logs").exists():
        raise AssertionError(
            f"Derived execution log root is missing: {log_root / 'execution_logs'}"
        )
    watermark_files = list(watermark_root.rglob("*.json")) if watermark_root.exists() else []
    if not watermark_files:
        raise AssertionError(f"No watermark was written below derived root: {watermark_root}")

    parquet_files = sorted(output.rglob("*.parquet")) if output.exists() else []
    if not parquet_files:
        raise AssertionError(f"No Parquet output found below {output}")
    rows = pq.read_table([str(path) for path in parquet_files]).num_rows
    if rows < minimum_rows:
        raise AssertionError(f"Expected at least {minimum_rows} rows, got {rows}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", default=str(DEFAULT_STATE_ROOT))
    parser.add_argument("--output", required=True)
    parser.add_argument("--minimum-rows", type=int, default=1)
    args = parser.parse_args()
    validate(
        Path(args.state_root),
        output=Path(args.output),
        minimum_rows=args.minimum_rows,
    )
    print(f"validated state paths below {Path(args.state_root).expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
