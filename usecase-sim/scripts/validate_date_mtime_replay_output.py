"""Validate the local date-folder + file-mtime replay qualification output."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from deltalake import DeltaTable


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DATA = ROOT / ".runtime" / "data"


def main() -> int:
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

    state_root = (ROOT.parent / args.state_root).resolve()
    values = []
    for path in state_root.rglob("watermark_value.json"):
        values.append(json.loads(path.read_text(encoding="utf-8")))
    if not values:
        raise AssertionError(f"No saved watermark found below {state_root}")
    if not any("__file_modification_time" in value for value in values):
        raise AssertionError("Saved state does not contain the file mtime watermark")
    print(
        "validated local date-folder/mtime replay: "
        f"engine={args.engine} rows={table.num_rows} ids={ids} "
        "boundary=[2024-01-15T00:00:00+00:00,2024-01-17T00:00:00+00:00)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
