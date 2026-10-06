"""Prepare the state-root scenario's Delta fixture and reset its watermark."""

from __future__ import annotations

import os

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runner.runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
sys.path.insert(0, str(SIM_ROOT / 'scripts'))


SCRIPT_DIR = Path(__file__).resolve().parent
USECASE_SIM_DIR = SIM_ROOT
STATE_ROOT = (USECASE_SIM_DIR / ".runtime" / "watermarks" / "state_contract").resolve()
RUNTIME_ROOT = (USECASE_SIM_DIR / ".runtime" / "watermarks").resolve()

from features.read.prepare_polars_qualified_sql import SAMPLE_ROWS  # noqa: E402
import polars as pl

FIXTURE_ROOT = USECASE_SIM_DIR / ".runtime" / "data" / "output" / "state_contract" / "fixtures"


def register_tables(*, engine, args) -> None:
    engine.register_delta_tables(
        str(FIXTURE_ROOT), logical_prefix=("catalog_A",), recursive=True
    )


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    state_root = STATE_ROOT
    if state_root == RUNTIME_ROOT or not state_root.is_relative_to(RUNTIME_ROOT):
        raise RuntimeError(f"State fixture root must stay below {RUNTIME_ROOT}")
    watermark_root = state_root / "watermarks"
    if watermark_root.exists():
        shutil.rmtree(watermark_root)
    # Seed only this case's input; never reset the shared qualified-SQL suite.
    pl.DataFrame(SAMPLE_ROWS).write_delta(
        str(FIXTURE_ROOT / "database_B" / "schema_C" / "orders_l1"), mode="overwrite"
    )
    print(f"Prepared state contract below {state_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
