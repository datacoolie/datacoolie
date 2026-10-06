"""Validate the local date-folder + file-mtime replay qualification output."""

from __future__ import annotations

import os

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from runner.runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
sys.path.insert(0, str(SIM_ROOT / 'scripts'))
sys.path.insert(0, str(DATACOOLIE_ROOT))


from deltalake import DeltaTable
from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY, FileInfoColumn
from datacoolie.utils.identity import name_to_uuid
from datacoolie.watermark.base import WatermarkSerializer


ROOT = SIM_ROOT
RUNTIME_DATA = ROOT / ".runtime" / "data"
_DATAFLOW_NAME = "date_mtime_replay_validation"
_DATAFLOW_STAGE = "date_mtime_replay_validation"
_WATERMARK_FILE_NAME = "watermark_value.json"
_EXPECTED_WATERMARK = {
    DATE_FOLDER_PARTITION_KEY: "2024-01-16T00:00:00+00:00",
    FileInfoColumn.FILE_MODIFICATION_TIME.value: "2024-01-16T12:00:00+00:00",
}


def _case_watermark_path(state_root: Path) -> Path:
    """Return the persisted watermark path owned by this replay case."""

    dataflow_id = name_to_uuid(_DATAFLOW_NAME)
    owner = f"{_DATAFLOW_STAGE}_{_DATAFLOW_NAME}_{dataflow_id}"
    return state_root / "watermarks" / owner / _WATERMARK_FILE_NAME


def _normalize_value(value: Any) -> Any:
    """Normalize decoded temporal values to canonical UTC ISO strings."""

    if isinstance(value, datetime):
        normalized = (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
        return normalized.isoformat()
    if isinstance(value, dict):
        return {key: _normalize_value(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [_normalize_value(nested) for nested in value]
    return value


def _assert_case_watermark(state_root: Path) -> dict[str, Any]:
    """Assert the exact checkpoint for the case-owned dataflow state."""

    path = _case_watermark_path(state_root)
    if not path.is_file():
        raise AssertionError(f"No case-owned saved watermark found at {path}")
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise AssertionError(f"Case-owned watermark is empty: {path}")
    try:
        decoded = WatermarkSerializer.deserialize(raw)
    except Exception as exc:
        raise AssertionError(f"Invalid case-owned watermark at {path}") from exc
    actual = _normalize_value(decoded)
    if actual != _EXPECTED_WATERMARK:
        raise AssertionError(
            "Unexpected case-owned date/mtime checkpoint: "
            f"expected={_EXPECTED_WATERMARK}, actual={actual}, path={path}"
        )
    return actual


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    parser.add_argument("--state-root", required=True)
    args = parser.parse_args()

    output = RUNTIME_DATA / "output" / "delta" / f"date_mtime_replay_validation_{args.engine}"
    table = DeltaTable(str(output)).to_pyarrow_table()
    ids = sorted(table.column("order_id").to_pylist())
    if ids != [1, 2]:
        raise AssertionError(
            "Expected the lower-bound file and the interior file only; "
            f"got ids={ids}"
        )
    if table.num_rows != 2:
        raise AssertionError(f"Expected exactly 2 replay rows, got {table.num_rows}")

    state_root = (ROOT.parent / args.state_root).resolve()
    checkpoint = _assert_case_watermark(state_root)
    print(
        "validated local date-folder/mtime replay: "
        f"engine={args.engine} rows={table.num_rows} ids={ids} "
        "boundary=[2024-01-15T00:00:00+00:00,2024-01-17T00:00:00+00:00) "
        f"checkpoint={checkpoint}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
