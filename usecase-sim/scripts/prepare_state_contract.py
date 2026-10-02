"""Prepare the state-root scenario's Delta fixture and reset its watermark."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
USECASE_SIM_DIR = SCRIPT_DIR.parent
STATE_ROOT = (USECASE_SIM_DIR / ".runtime" / "state_contract").resolve()
RUNTIME_ROOT = (USECASE_SIM_DIR / ".runtime").resolve()

sys.path.insert(0, str(SCRIPT_DIR))
from prepare_polars_qualified_sql import prepare_delta_suite  # noqa: E402


def main() -> int:
    state_root = STATE_ROOT
    if state_root == RUNTIME_ROOT or not state_root.is_relative_to(RUNTIME_ROOT):
        raise RuntimeError(f"State fixture root must stay below {RUNTIME_ROOT}")
    watermark_root = state_root / "watermarks"
    if watermark_root.exists():
        shutil.rmtree(watermark_root)
    prepare_delta_suite("delta-positive")
    print(f"Prepared state contract below {state_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
