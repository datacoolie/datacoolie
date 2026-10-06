"""Qualify source-empty no-op, folder safety and whole-attempt native retry.

Run after scenario cleanup. The supplied root must be a new simulator-owned
directory; fixture state is seeded there before Driver execution.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve()


USECASE_SIM_DIR = SCRIPT.parents[3]
if str(USECASE_SIM_DIR) not in sys.path:
    sys.path.insert(0, str(USECASE_SIM_DIR))
from runner.runtime_paths import DATA_DIR, LOG_DIR, WATERMARKS_DIR, DATACOOLIE_ROOT  # noqa: E402
from runner.runtime_logs import job_sessions, records  # noqa: E402

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY  # noqa: E402
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig  # noqa: E402
from datacoolie.metadata.file_provider import FileProvider  # noqa: E402
from datacoolie.orchestration.driver import DataCoolieDriver  # noqa: E402
from datacoolie.platforms.local_platform import LocalPlatform  # noqa: E402
from datacoolie.watermark.base import WatermarkSerializer  # noqa: E402


ROOT = USECASE_SIM_DIR

_V6_METRIC_FIELDS = frozenset(
    {
        "rows_read",
        "rows_written",
        "rows_inserted",
        "rows_updated",
        "rows_deleted",
        "files_added",
        "files_removed",
        "bytes_added",
        "bytes_removed",
        "bytes_saved",
    }
)
_V6_SELECTION_FIELDS = frozenset(
    {
        "mode",
        "lower",
        "upper",
        "lower_operator",
        "upper_operator",
        "combine_operator",
        "upper_origin",
    }
)
_V6_CANDIDATE_KINDS = frozenset(
    {"observed_max", "request_end", "opaque", "metadata"}
)
_V6_REMOVED_ROOT_FIELDS = frozenset(
    {
        "source_start_time",
        "source_end_time",
        "source_duration_seconds",
        "source_status",
        "source_message",
        "source_rows_read",
        "source_action",
        "source_watermark_effective",
        "transform_start_time",
        "transform_end_time",
        "transform_duration_seconds",
        "transform_status",
        "transform_message",
        "destination_start_time",
        "destination_end_time",
        "destination_duration_seconds",
        "destination_status",
        "destination_message",
        "destination_rows_written",
        "watermark_checkpoint_status",
        "watermark_checkpoint_saved",
    }
)


class RetryAfterWriteDriver(DataCoolieDriver):
    """Lose one write acknowledgement without mocking native destination I/O."""

    write_calls = 0

    def _create_destination_writer(self, fmt):
        writer = super()._create_destination_writer(fmt)
        native_write = writer.write

        def write(*args, **kwargs):
            result = native_write(*args, **kwargs)
            self.write_calls += 1
            if self.write_calls == 1:
                raise OSError("simulator intentional post-write acknowledgement loss")
            return result

        writer.write = write
        return writer


def validate_history(job_id: str, session_id: str, case: str) -> dict:
    """Assert persisted execution grain, not just the in-memory return value."""
    runs = [
        value
        for _, value in records(
            LOG_DIR, job_id, "execution_logs/dataflow_run_log", session_id
        )
    ]
    jobs = [
        value
        for _, value in records(
            LOG_DIR, job_id, "execution_logs/job_run_log", session_id
        )
    ]
    assert len(runs) == len(jobs) == 1, (runs, jobs)
    run, job = runs[0], jobs[0]
    system_logs = list(records(LOG_DIR, job_id, "system_logs", session_id))
    assert run.get("log_schema_version") == 6, run
    assert job.get("log_schema_version") == 6, job
    assert system_logs and all(value.get("log_schema_version") == 6 for _, value in system_logs)
    assert not _V6_REMOVED_ROOT_FIELDS.intersection(run), run

    attempts = run.get("attempts")
    assert isinstance(attempts, list), run
    metrics = run.get("metrics")
    assert isinstance(metrics, dict), run
    assert set(metrics) == _V6_METRIC_FIELDS, metrics
    expected = (
        ["failed", "succeeded"]
        if case == "retry-after-write"
        else ["skipped"]
        if case == "typed-empty"
        else ["failed"]
        if case == "folder-only"
        else ["succeeded"]
    )
    assert len(attempts) == len(expected), attempts
    assert [attempt["status"] for attempt in attempts] == expected, attempts
    assert [attempt["attempt_number"] for attempt in attempts] == list(
        range(1, len(expected) + 1)
    ), attempts
    assert run["retry_attempts"] == len(expected) - 1, run
    assert run["status"] == expected[-1], run
    for attempt, status in zip(attempts, expected):
        _assert_v6_attempt(attempt, expected_status=status)

    expected_rows_read = 0 if case == "typed-empty" else 1
    assert metrics["rows_read"] == expected_rows_read, metrics
    if case == "typed-empty":
        assert metrics["rows_written"] == 0, metrics
    else:
        assert metrics["rows_written"] in {None, 1}, metrics
    assert job["total_dataflows"] == 1, job
    assert job["total_rows_read"] == metrics["rows_read"], (job, run)
    assert job["total_rows_written"] == metrics["rows_written"], (job, run)
    assert (job["total_succeeded"], job["total_failed"], job["total_skipped"]) == (
        int(expected[-1] == "succeeded"),
        int(expected[-1] == "failed"),
        int(expected[-1] == "skipped"),
    ), job
    if case == "typed-empty":
        assert attempts[0]["transform"] is attempts[0]["destination"] is None
        assert attempts[0]["watermark"]["save_status"] == "not_attempted"
    if case == "retry-after-write":
        first, last = attempts
        assert first["watermark"]["load_status"] == "loaded"
        assert last["watermark"]["load_status"] == "reused"
        assert first["watermark"]["saved_watermark"] is None
        assert last["watermark"]["save_status"] == "saved"
        assert (
            first["source"]["selection"]
            == last["source"]["selection"]
        )
        assert first["watermark"]["candidate"] == last["watermark"]["candidate"]
        assert last["watermark"]["saved_watermark"] == last["watermark"]["watermark_to_save"], last
        assert last["watermark"]["saved_watermark"]["seq"] == 2, last
        retry_logs = [value for _, value in system_logs if value.get("event_name") == "retry.scheduled"]
        assert len(retry_logs) == 1, retry_logs
        assert retry_logs[0]["attempt_number"] == 1
        assert retry_logs[0]["dataflow_run_id"] == run["dataflow_run_id"]
    return {"dataflow_run_id": run["dataflow_run_id"], "attempt_statuses": expected}


def _assert_v6_attempt(attempt: object, *, expected_status: str) -> None:
    """Check the simulator's persisted attempt shape at the v6 boundary."""
    assert isinstance(attempt, dict), attempt
    assert attempt.get("status") == expected_status, attempt
    assert isinstance(attempt.get("attempt_number"), int), attempt
    for phase_name in ("source", "transform", "destination", "watermark"):
        phase = attempt.get(phase_name)
        assert phase is None or isinstance(phase, dict), (phase_name, attempt)

    source = attempt.get("source")
    if source is not None:
        assert isinstance(source.get("source_action"), dict), source
        selection = source.get("selection")
        assert isinstance(selection, dict), source
        assert set(selection) == _V6_SELECTION_FIELDS, selection
        assert selection["upper_origin"] in {"none", "explicit", "generated"}, selection
        assert "watermark_effective" not in source, source

    watermark = attempt.get("watermark")
    if watermark is not None:
        candidate = watermark.get("candidate")
        if candidate is not None:
            assert isinstance(candidate, dict), watermark
            assert set(candidate) == {"values", "kind"}, candidate
            assert isinstance(candidate["values"], dict), candidate
            assert candidate["kind"] in _V6_CANDIDATE_KINDS, candidate


def _mapped_runtime_root(data_root: Path, runtime_root: Path) -> Path:
    """Map a data fixture root to its isolated watermark/log runtime area."""

    data_root = data_root.resolve()
    data_base = DATA_DIR.resolve()
    if data_root == data_base or not data_root.is_relative_to(data_base):
        raise ValueError(
            f"fixture root must stay strictly below {data_base}: {data_root}"
        )
    relative = data_root.relative_to(data_base)
    mapped = (runtime_root / relative).resolve()
    runtime_base = runtime_root.resolve()
    if mapped == runtime_base or not mapped.is_relative_to(runtime_base):
        raise ValueError(f"mapped runtime root escaped {runtime_base}: {mapped}")
    return mapped


def metadata(path: Path, *, folder: bool, tracked: bool) -> Path:
    source_config = {"base_path": str(path / "input")}
    if folder:
        source_config["date_folder_partitions"] = "{year}/{month}/{day}"
    document = {
        "connections": [
            {
                "name": "source",
                "connection_type": "file" if folder else "lakehouse",
                "format": "parquet" if folder else "delta",
                "configure": source_config,
            },
            {
                "name": "target",
                "connection_type": "lakehouse",
                "format": "delta",
                "configure": {"base_path": str(path / "output")},
            },
        ],
        "dataflows": [
            {
                "name": "window-safety",
                "stage": "window-safety",
                "source": {
                    "connection_name": "source",
                    "table": "events",
                    "watermark_columns": ["seq"] if tracked else [],
                    "configure": {"backward_days": 1},
                },
                "destination": {
                    "connection_name": "target",
                    "table": "events",
                    "load_type": "merge_overwrite",
                    "configure": {"replace_by_watermark": True},
                },
            }
        ],
    }
    path.mkdir(parents=True)
    config_path = path / "metadata.json"
    config_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
    return config_path


def read_rows(engine, path: Path, kind: str) -> list[dict]:
    frame = engine.read_delta(str(path))
    records = (
        [row.asDict() for row in frame.collect()]
        if kind == "spark"
        else frame.collect().to_dicts()
    )
    return sorted(
        ({key: row[key] for key in ("id", "seq", "value")} for row in records),
        key=lambda row: row["id"],
    )


def run_case(engine, platform, root: Path, kind: str, case: str) -> dict:
    receipt_root = _mapped_runtime_root(root, LOG_DIR)
    if receipt_root.exists():
        raise ValueError(f"native receipt root must be new and unused: {receipt_root}")

    folder = case != "typed-empty"
    tracked = case != "folder-only"
    config_path = metadata(root, folder=folder, tracked=tracked)
    source_rows = (
        [
            {"id": 200, "seq": 0, "value": "before"},
            {"id": 201, "seq": 10, "value": "after"},
        ]
        if not folder
        else [{"id": 200, "seq": 2, "value": "new"}]
    )
    source_path = root / "input" / "events"
    if folder:
        source_path = source_path / "2026" / "01" / "02"
    engine.write_to_path(
        engine.create_dataframe(source_rows),
        str(source_path),
        mode="overwrite",
        fmt="parquet" if folder else "delta",
    )
    target_path = root / "output" / "events"
    points = [0, 2, 8, 10] if not folder else [1, 2, 3]
    initial = [
        {"id": 100 + i, "seq": point, "value": f"old-{point}"}
        for i, point in enumerate(points)
    ]
    engine.write_to_path(
        engine.create_dataframe(initial),
        str(target_path),
        mode="overwrite",
        fmt="delta",
    )
    # Keep case-specific watermarks independent of the shared framework logs.
    watermark_root = _mapped_runtime_root(root, WATERMARKS_DIR)
    if watermark_root.exists():
        raise ValueError(f"native watermark root must be new and unused: {watermark_root}")
    watermark_root.mkdir(parents=True, exist_ok=False)
    state_root = watermark_root / "state"
    state_root.mkdir()
    provider = FileProvider(
        config_path=str(config_path),
        platform=platform,
        watermark_base_path=str(state_root),
    )
    dataflow = provider.get_dataflows()[0]
    state = (
        {"seq": 100, "auxiliary": "keep"}
        if not folder
        else {
            DATE_FOLDER_PARTITION_KEY: "2026-01-01T00:00:00+00:00",
            **({"seq": 1} if tracked else {}),
        }
    )
    provider.update_watermark(
        dataflow.dataflow_id, WatermarkSerializer.serialize(state)
    )
    before_state = provider.get_watermark(dataflow.dataflow_id)
    before_files = {
        path.relative_to(state_root).as_posix(): (
            path.read_bytes(),
            path.stat().st_mtime_ns,
        )
        for path in state_root.rglob("watermark_value.json")
    }
    assert len(before_files) == 1, before_files
    before_rows = read_rows(engine, target_path, kind)
    before_commits = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (target_path / "_delta_log").glob("*.json")
    }
    job_id = f"window-{kind}-{case}"
    previous_sessions = job_sessions(LOG_DIR, job_id)
    driver_type = (
        RetryAfterWriteDriver if case == "retry-after-write" else DataCoolieDriver
    )
    with driver_type(
        engine=engine,
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(state_root),
        log_base_path=str(LOG_DIR),
        config=DataCoolieRunConfig(
            job_id=job_id,
            max_workers=1,
            retry_count=int(case == "retry-after-write"),
            retry_delay=0,
        ),
    ) as driver:
        if folder:
            result = driver.run(stage="window-safety")
        else:
            result = driver.run_replay(
                dataflows=[dataflow],
                replay=ReplayConfig(
                    start=1, end=10, chunk_column="seq", save_watermark=True
                ),
            )
        after_state = provider.get_watermark(dataflow.dataflow_id)
        if case == "retry-after-write":
            assert driver.write_calls == 2, driver.write_calls
    actual = read_rows(engine, target_path, kind)
    after_files = {
        path.relative_to(state_root).as_posix(): (
            path.read_bytes(),
            path.stat().st_mtime_ns,
        )
        for path in state_root.rglob("watermark_value.json")
    }
    assert result.total == 1, result
    if case == "typed-empty":
        assert (result.succeeded, result.failed, result.skipped) == (0, 0, 1), result
        assert actual == before_rows, actual
        assert after_state == before_state
        assert after_files == before_files, (
            "Source-empty replay rewrote persisted state"
        )
        after_commits = {
            path.name: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in (target_path / "_delta_log").glob("*.json")
        }
        assert after_commits == before_commits, "Source-empty replay committed to Delta"
    elif case == "folder-only":
        assert (result.succeeded, result.failed) == (0, 1), result.errors
        assert "requires merge_keys when no usable replacement window" in str(
            result.errors
        ), result.errors
        assert actual == before_rows
        assert after_state == before_state
        assert after_files == before_files, (
            "Folder-only failure rewrote persisted state"
        )
    else:
        assert (result.succeeded, result.failed) == (1, 0), result.errors
        expected = [initial[0], initial[-1]]
        if folder:
            expected += source_rows
        assert actual == sorted(expected, key=lambda row: row["id"]), actual
        if folder:
            saved = WatermarkSerializer.deserialize(after_state)
            assert saved["seq"] == 2, saved
            assert saved[DATE_FOLDER_PARTITION_KEY] == "2026-01-02T00:00:00+00:00", (
                saved
            )
    current_sessions = job_sessions(LOG_DIR, job_id) - previous_sessions
    assert len(current_sessions) == 1, current_sessions
    history = validate_history(job_id, current_sessions.pop(), case)
    if case == "retry-after-write":
        frame = engine.read_delta(str(target_path))
        output = (
            [row.asDict() for row in frame.collect()]
            if kind == "spark"
            else frame.collect().to_dicts()
        )
        assert [row["__dataflow_run_id"] for row in output if row["id"] == 200] == [
            history["dataflow_run_id"]
        ], output
    return {
        "case": case,
        "engine": kind,
        "ids": [row["id"] for row in actual],
        "before_state": json.loads(before_state),
        "after_state": json.loads(after_state),
        "succeeded": result.succeeded,
        "failed": result.failed,
        "skipped": result.skipped,
        "errors": result.errors,
        **history,
    }


def main() -> int:
    # Resolve relative fixture roots from the product root, matching the
    # executable runners and Docker working directory convention.
    import os

    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("polars", "spark"), required=True)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root = (DATACOOLIE_ROOT / args.root).resolve()
    allowed = DATA_DIR.resolve()
    if root == allowed or not root.is_relative_to(allowed) or root.exists():
        parser.error(
            "--root must be a new directory strictly below usecase-sim/.runtime/data"
        )
    # Validate derived runtime locations before creating any fixture bytes.
    _mapped_runtime_root(root, WATERMARKS_DIR)
    receipt_root = _mapped_runtime_root(root, LOG_DIR)
    if receipt_root.exists():
        parser.error(f"native receipt root must be new and unused: {receipt_root}")
    receipt_root.mkdir(parents=True, exist_ok=False)
    platform = LocalPlatform()
    spark = None
    if args.engine == "spark":
        sys.path.insert(0, str(ROOT / "runner"))
        from _runner_utils import build_spark_session
        from datacoolie.engines.spark_engine import SparkEngine

        spark = build_spark_session(
            app_name="replay-window-safety",
            extra_config={
                "spark.master": "local[2]",
                "spark.driver.memory": "2g",
                "spark.sql.shuffle.partitions": "2",
                "spark.sql.session.timeZone": "UTC",
            },
            verify_local_file_checksums=False,
        )
        engine = SparkEngine(spark_session=spark, platform=platform)
    else:
        from datacoolie.engines.polars_engine import PolarsEngine

        engine = PolarsEngine(platform=platform)
    try:
        receipts = [
            run_case(engine, platform, root / case, args.engine, case)
            for case in (
                "typed-empty",
                "folder-only",
                "mixed-folder-row",
                "retry-after-write",
            )
        ]
        print(json.dumps({"window_safety": receipts}, sort_keys=True))
        receipt_path = receipt_root / "receipt.json"
        with receipt_path.open("x", encoding="utf-8") as handle:
            json.dump(receipts, handle, indent=2)
            handle.write("\n")
    finally:
        if spark is not None:
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
