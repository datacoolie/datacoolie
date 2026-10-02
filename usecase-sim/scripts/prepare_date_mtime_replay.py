"""Prepare a deterministic local date-folder + file-mtime replay fixture."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DATA = ROOT / ".runtime" / "data"
INPUT_ROOT = RUNTIME_DATA / "date_mtime_replay" / "input" / "sample"
METADATA_ROOT = RUNTIME_DATA


def _write_file(path: Path, *, order_id: int, order_date: str, modified_at: str, mtime: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "order_id": pa.array([order_id], type=pa.int64()),
            "order_date": pa.array([date.fromisoformat(order_date)], type=pa.date32()),
            "modified_at": pa.array([modified_at], type=pa.string()),
        }
    )
    pq.write_table(table, path)
    timestamp = mtime.timestamp()
    os.utime(path, (timestamp, timestamp))


def _metadata(engine: str) -> dict:
    return {
        "connections": [
            {
                "name": "replay_date_mtime_source",
                "connection_type": "file",
                "format": "parquet",
                "configure": {
                    "base_path": "./usecase-sim/.runtime/data/date_mtime_replay/input",
                    "date_folder_partitions": "{year}/{month}/{day}",
                },
            },
            {
                "name": "replay_date_mtime_dest",
                "connection_type": "lakehouse",
                "format": "delta",
                "configure": {
                    "base_path": "./usecase-sim/.runtime/data/output/delta",
                },
            },
        ],
        "dataflows": [
            {
                "name": "date_mtime_replay_validation",
                "stage": "date_mtime_replay_validation",
                "processing_mode": "batch",
                "source": {
                    "connection_name": "replay_date_mtime_source",
                    "table": "sample",
                    "watermark_columns": ["__file_modification_time"],
                },
                "destination": {
                    "connection_name": "replay_date_mtime_dest",
                    "table": f"date_mtime_replay_validation_{engine}",
                    "load_type": "append",
                },
                "transform": {},
            }
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    args = parser.parse_args()

    if INPUT_ROOT.exists():
        shutil.rmtree(INPUT_ROOT)
    INPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # The first two files are inside the replay range. The first sits exactly
    # on the inclusive lower boundary, while the third sits exactly on the
    # exclusive upper boundary and must be omitted. An older folder/file
    # proves that folder discovery does not replace the source-owned mtime
    # decision.
    _write_file(
        INPUT_ROOT / "2024" / "01" / "15" / "part-1.parquet",
        order_id=1,
        order_date="2024-01-15",
        modified_at="2024-01-15T10:00:00",
        mtime=datetime(2024, 1, 15, tzinfo=timezone.utc),
    )
    _write_file(
        INPUT_ROOT / "2024" / "01" / "16" / "part-2.parquet",
        order_id=2,
        order_date="2024-01-16",
        modified_at="2024-01-16T10:00:00",
        mtime=datetime(2024, 1, 16, 12, tzinfo=timezone.utc),
    )
    _write_file(
        INPUT_ROOT / "2024" / "01" / "17" / "part-3.parquet",
        order_id=3,
        order_date="2024-01-17",
        modified_at="2024-01-17T10:00:00",
        mtime=datetime(2024, 1, 17, tzinfo=timezone.utc),
    )
    _write_file(
        INPUT_ROOT / "2024" / "01" / "14" / "part-old.parquet",
        order_id=99,
        order_date="2024-01-14",
        modified_at="2024-01-14T10:00:00",
        mtime=datetime(2024, 1, 14, tzinfo=timezone.utc),
    )

    metadata_path = METADATA_ROOT / f"replay_date_mtime_validation_{args.engine}.json"
    metadata_path.write_text(
        json.dumps(_metadata(args.engine), indent=2),
        encoding="utf-8",
    )
    print(
        "prepared local date-folder/mtime replay: "
        f"engine={args.engine} metadata={metadata_path} input={INPUT_ROOT}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
