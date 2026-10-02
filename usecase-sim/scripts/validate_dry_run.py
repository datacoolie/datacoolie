"""Validate that a dry-run performs preparation but no business/state I/O."""

from __future__ import annotations

import argparse
from pathlib import Path


USECASE_SIM_DIR = Path(__file__).resolve().parent.parent


def validate(*, output: Path, state_root: Path, log_root: Path) -> None:
    output = output.expanduser().resolve()
    state_root = state_root.expanduser().resolve()
    if output.exists():
        raise AssertionError(f"Dry-run unexpectedly created business output: {output}")
    watermark_root = state_root / "watermarks"
    if watermark_root.exists() and any(watermark_root.rglob("*.json")):
        raise AssertionError(
            f"Dry-run unexpectedly wrote watermark state below {watermark_root}"
        )
    execution_root = log_root.expanduser().resolve() / "execution_logs"
    if not execution_root.exists():
        raise AssertionError(f"Expected dry-run execution logs below {execution_root}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--state-root", required=True)
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
    )
    print("validated dry-run side-effect boundary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
