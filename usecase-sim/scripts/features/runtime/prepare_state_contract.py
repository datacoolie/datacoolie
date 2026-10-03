"""Prepare the state-root scenario's Delta fixture and reset its watermark."""

from __future__ import annotations

import os

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
sys.path.insert(0, str(SIM_ROOT / 'scripts'))


SCRIPT_DIR = Path(__file__).resolve().parent
USECASE_SIM_DIR = SIM_ROOT
STATE_ROOT = (USECASE_SIM_DIR / ".runtime" / "watermarks" / "state_contract").resolve()
RUNTIME_ROOT = (USECASE_SIM_DIR / ".runtime" / "watermarks").resolve()

from features.sql.prepare_polars_qualified_sql import prepare_delta_suite  # noqa: E402


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
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
