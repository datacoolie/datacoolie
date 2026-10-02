"""Qualify empty replay and internal-folder replacement safety on native Delta.

Run after scenario cleanup. The supplied root must be a new simulator-owned
directory; fixture state is seeded there before Driver execution.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.watermark.base import WatermarkSerializer


ROOT = Path(__file__).resolve().parents[1]


def metadata(path: Path, *, folder: bool, tracked: bool) -> Path:
    source_config = {"base_path": str(path / "input")}
    if folder:
        source_config["date_folder_partitions"] = "{year}/{month}/{day}"
    document = {
        "connections": [
            {"name": "source", "connection_type": "file" if folder else "lakehouse",
             "format": "parquet" if folder else "delta", "configure": source_config},
            {"name": "target", "connection_type": "lakehouse", "format": "delta",
             "configure": {"base_path": str(path / "output")}},
        ],
        "dataflows": [{
            "name": "window-safety", "stage": "window-safety",
            "source": {"connection_name": "source", "table": "events",
                       "watermark_columns": ["seq"] if tracked else [],
                       "configure": {"backward_days": 1}},
            "destination": {"connection_name": "target", "table": "events",
                            "load_type": "merge_overwrite",
                            "configure": {"replace_by_watermark": True}},
        }],
    }
    path.mkdir(parents=True)
    config_path = path / "metadata.json"
    config_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    return config_path


def read_rows(engine, path: Path, kind: str) -> list[dict]:
    frame = engine.read_delta(str(path))
    records = ([row.asDict() for row in frame.collect()] if kind == "spark"
               else frame.collect().to_dicts())
    return sorted(({key: row[key] for key in ("id", "seq", "value")}
                   for row in records), key=lambda row: row["id"])


def run_case(engine, platform, root: Path, kind: str, case: str) -> dict:
    folder = case != "typed-empty"
    tracked = case != "folder-only"
    config_path = metadata(root, folder=folder, tracked=tracked)
    source_rows = [{"id": 200, "seq": 0, "value": "before"},
                   {"id": 201, "seq": 10, "value": "after"}] if not folder else [
                       {"id": 200, "seq": 2, "value": "new"}]
    source_path = root / "input" / "events"
    if folder:
        source_path = source_path / "2026" / "01" / "02"
    engine.write_to_path(engine.create_dataframe(source_rows), str(source_path),
                         mode="overwrite", fmt="parquet" if folder else "delta")
    target_path = root / "output" / "events"
    points = [0, 2, 8, 10] if not folder else [1, 2, 3]
    initial = [{"id": 100 + i, "seq": point, "value": f"old-{point}"}
               for i, point in enumerate(points)]
    engine.write_to_path(engine.create_dataframe(initial), str(target_path),
                         mode="overwrite", fmt="delta")
    state_root = root / "state"
    provider = FileProvider(config_path=str(config_path), platform=platform,
                            watermark_base_path=str(state_root))
    dataflow = provider.get_dataflows()[0]
    state = ({"seq": 100, "auxiliary": "keep"} if not folder else {
        DATE_FOLDER_PARTITION_KEY: "2026-01-01T00:00:00+00:00",
        **({"seq": 1} if tracked else {}),
    })
    provider.update_watermark(dataflow.dataflow_id, WatermarkSerializer.serialize(state))
    before_state = provider.get_watermark(dataflow.dataflow_id)
    before_files = {path.relative_to(state_root).as_posix(): (path.read_bytes(), path.stat().st_mtime_ns)
                    for path in state_root.rglob("watermark_value.json")}
    assert len(before_files) == 1, before_files
    before_rows = read_rows(engine, target_path, kind)
    with DataCoolieDriver(
        engine=engine, platform=platform, metadata_provider=provider,
        state_base_path=str(state_root), log_base_path=str(root / "logs"),
        config=DataCoolieRunConfig(job_id=f"window-{kind}-{case}", max_workers=1,
                                 retry_count=0, retry_delay=0),
    ) as driver:
        if folder:
            result = driver.run(stage="window-safety")
        else:
            result = driver.run_replay(
                dataflows=[dataflow],
                replay=ReplayConfig(start=1, end=10, chunk_column="seq", save_watermark=True),
            )
        after_state = provider.get_watermark(dataflow.dataflow_id)
    actual = read_rows(engine, target_path, kind)
    after_files = {path.relative_to(state_root).as_posix(): (path.read_bytes(), path.stat().st_mtime_ns)
                   for path in state_root.rglob("watermark_value.json")}
    assert result.total == 1, result
    if case == "folder-only":
        assert (result.succeeded, result.failed) == (0, 1), result.errors
        assert "requires merge_keys when no usable replacement window" in str(result.errors), result.errors
        assert actual == before_rows
        assert after_state == before_state
        assert after_files == before_files, "Folder-only failure rewrote persisted state"
    else:
        assert (result.succeeded, result.failed) == (1, 0), result.errors
        expected = [initial[0], initial[-1]]
        if folder:
            expected += source_rows
        assert actual == sorted(expected, key=lambda row: row["id"]), actual
        if folder:
            saved = WatermarkSerializer.deserialize(after_state)
            assert saved["seq"] == 2, saved
            assert saved[DATE_FOLDER_PARTITION_KEY] == "2026-01-02T00:00:00+00:00", saved
        else:
            assert after_state == before_state
            assert after_files == before_files, "Empty replay rewrote persisted state"
    return {"case": case, "engine": kind, "ids": [row["id"] for row in actual],
            "before_state": json.loads(before_state), "after_state": json.loads(after_state),
            "succeeded": result.succeeded, "failed": result.failed,
            "errors": result.errors}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    allowed = (ROOT / ".runtime" / "data").resolve()
    if root == allowed or not root.is_relative_to(allowed) or root.exists():
        parser.error("--root must be a new directory strictly below usecase-sim/.runtime/data")
    platform = LocalPlatform()
    spark = None
    if args.engine == "spark":
        sys.path.insert(0, str(ROOT / "runner"))
        from _runner_utils import build_spark_session
        from datacoolie.engines.spark_engine import SparkEngine

        spark = build_spark_session(app_name="replay-window-safety", extra_config={
            "spark.master": "local[2]", "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "2", "spark.sql.session.timeZone": "UTC",
        }, verify_local_file_checksums=False)
        engine = SparkEngine(spark_session=spark, platform=platform)
    else:
        from datacoolie.engines.polars_engine import PolarsEngine

        engine = PolarsEngine(platform=platform)
    try:
        receipts = [run_case(engine, platform, root / case, args.engine, case)
                    for case in ("typed-empty", "folder-only", "mixed-folder-row")]
        print(json.dumps({"window_safety": receipts}, sort_keys=True))
        (root / "receipt.json").write_text(json.dumps(receipts, indent=2), encoding="utf-8")
    finally:
        if spark is not None:
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
