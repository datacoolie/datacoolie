"""Contract tests for the framework logging persistence v4 output."""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datacoolie import __version__
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging import ExecutionLogger, LogConfig, SystemLogger
from datacoolie.logging.persistence.writer import JsonLogWriter, SharedByteBudget, encode_json_line
from datacoolie.platforms.local_platform import LocalPlatform

from tests.unit.logging.support import make_dataflow, make_runtime


def _json_files(root):
    return sorted(path for path in root.rglob("*.json") if path.is_file())


def test_run_attributes_are_strict_and_detached():
    values = {"factory_run_id": "abc", "nested": {"attempt": 2}}
    config = DataCoolieRunConfig(run_attributes=values)
    values["nested"]["attempt"] = 99
    assert config.run_attributes == {"factory_run_id": "abc", "nested": {"attempt": 2}}

    with pytest.raises(ConfigurationError):
        DataCoolieRunConfig(run_attributes="{\"id\": 1}")
    with pytest.raises(ConfigurationError):
        DataCoolieRunConfig(run_attributes={"id": float("nan")})
    with pytest.raises(ConfigurationError):
        DataCoolieRunConfig(run_attributes={1: "bad"})


def test_log_config_uses_snapshot_defaults_and_rejects_invalid_capacity():
    config = LogConfig()
    assert config.persistence_mode == "snapshot"
    assert config.flush_interval_seconds == 300.0
    assert config.flush_batch_bytes == 4 * 1024 * 1024
    assert config.buffer_memory_bytes == 64 * 1024 * 1024
    with pytest.raises(ValueError):
        LogConfig(persistence_mode="append")
    with pytest.raises(ValueError):
        LogConfig(buffer_memory_bytes=10, spool_max_bytes=9)


def test_execution_snapshot_writes_job_and_dataflow_json(tmp_path):
    platform = LocalPlatform(base_path=str(tmp_path))
    logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
    logger.set_run_config(
        DataCoolieRunConfig(
            job_id="factory/job-1",
            run_attributes={"pipeline_run_id": "p-1"},
        )
    )
    logger.activate()
    dataflow = make_dataflow("df-1")
    dataflow.source.query = "sql/orders/incremental.sql"
    runtime = make_runtime("df-1")
    runtime.source.source_action = {"query": "SELECT * FROM orders WHERE id > 10"}
    logger.log(dataflow, runtime)
    logger.finish_job("succeeded")
    logger.close()

    paths = _json_files(tmp_path)
    assert len(paths) == 2
    assert not list(tmp_path.rglob("*.jsonl"))
    assert not list(tmp_path.rglob("*.parquet"))
    dataflow_file = next(path for path in paths if "dataflow_run_log" in path.parts)
    job_file = next(path for path in paths if "job_run_log" in path.parts)
    dataflow_payload = dataflow_file.read_text(encoding="utf-8")
    job_payload = job_file.read_text(encoding="utf-8")
    dataflow_pairs = json.loads(dataflow_payload, object_pairs_hook=list)
    job_pairs = json.loads(job_payload, object_pairs_hook=list)
    assert [key for key, _ in dataflow_pairs[:2]] == [
        "log_schema_version",
        "_type",
    ]
    assert [key for key, _ in job_pairs[:2]] == [
        "log_schema_version",
        "_type",
    ]
    dataflow_row = dict(dataflow_pairs)
    job_row = dict(job_pairs)
    assert dataflow_row["datacoolie_version"] == job_row["datacoolie_version"] == __version__
    assert dataflow_row["log_schema_version"] == 4
    assert dataflow_row["log_session_id"]
    assert dataflow_row["log_session_id"] == job_row["log_session_id"]
    assert dataflow_row["source_query"] == "sql/orders/incremental.sql"
    assert dataflow_row["message"] is None
    assert "error_message" not in dataflow_row
    assert "skip_reason" not in dataflow_row
    assert json.loads(dataflow_row["source_action"])["query"].startswith("SELECT")
    assert job_row["run_attributes"] == '{"pipeline_run_id":"p-1"}'
    assert job_row["status"] == "succeeded"
    assert job_row["log_records_dropped"] == 0
    assert job_row["log_bytes_dropped"] == 0
    assert job_row["message"] is None
    assert job_row["message_truncated"] is False
    assert "error_message" not in job_row
    assert "error_message_truncated" not in job_row
    assert "dropped_records" not in job_row
    assert "dropped_bytes" not in job_row


def test_activation_writes_running_job_snapshot_before_work(tmp_path):
    platform = LocalPlatform(base_path=str(tmp_path))
    logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
    logger.set_run_config(DataCoolieRunConfig(job_id="empty-job"))
    logger.activate()
    job_file = next(tmp_path.rglob("job_*.json"))
    row = json.loads(job_file.read_text(encoding="utf-8"))
    assert row["status"] == "running"
    assert row["end_time"] is None
    logger.close()


def test_initial_job_checkpoint_failure_does_not_reject_activation(tmp_path):
    class FlakyPlatform(LocalPlatform):
        def __init__(self, base_path):
            super().__init__(base_path=base_path)
            self.fail = True

        def upload_file(self, local_path, dest, *, overwrite=False):
            if self.fail:
                self.fail = False
                raise RuntimeError("checkpoint unavailable")
            return super().upload_file(local_path, dest, overwrite=overwrite)

    logger = ExecutionLogger(
        LogConfig(output_path="logs", flush_interval_seconds=0),
        FlakyPlatform(str(tmp_path)),
    )
    logger.set_run_config(DataCoolieRunConfig(job_id="flaky-job"))
    logger.activate()
    assert logger.is_active is True
    assert isinstance(logger.last_flush_error, RuntimeError)
    logger.close()
    assert list(tmp_path.rglob("job_*.json"))


def test_execution_rejects_nonterminal_runtime():
    logger = ExecutionLogger(LogConfig(output_path=None), platform=None)
    with pytest.raises(ConfigurationError):
        logger.log(make_dataflow("df-1"), make_runtime("df-1", status="running"))
    logger.close()


def test_execution_batch_uses_part_files_and_upload_only(tmp_path):
    platform = LocalPlatform(base_path=str(tmp_path))
    logger = ExecutionLogger(
        LogConfig(
            output_path="logs",
            persistence_mode="batch",
            flush_batch_bytes=1,
            flush_interval_seconds=0,
        ),
        platform,
    )
    logger.set_run_config(DataCoolieRunConfig(job_id="batch-job"))
    logger.activate()
    logger.log(make_dataflow("df-1"), make_runtime("df-1"))
    logger._on_periodic_flush()
    logger.finish_job("succeeded")
    logger.close()

    parts = list(tmp_path.rglob("dataflow_*_part_*.json"))
    assert len(parts) == 1
    assert len(parts[0].read_text(encoding="utf-8").splitlines()) == 1


def test_system_logger_uses_shared_json_writer(tmp_path):
    platform = LocalPlatform(base_path=str(tmp_path))
    logger = SystemLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
    logger.set_run_config(DataCoolieRunConfig(job_id="system-job"))
    logger.activate()
    logging.getLogger("datacoolie.logging.v3.test").info("hello")
    logger.close()

    files = list(tmp_path.rglob("system_*.json"))
    assert len(files) == 1
    record = json.loads(files[0].read_text(encoding="utf-8"))
    assert record["log_schema_version"] == 4
    assert record["datacoolie_version"] == __version__
    assert record["_type"] == "system_log"
    assert record["log_session_id"]
    assert record["msg"] == "hello"


def test_snapshot_clean_tick_does_not_reupload_history(tmp_path):
    platform = MagicMock()
    logger = SystemLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
    logger.activate()
    logging.getLogger("datacoolie.logging.v2.clean").info("one")
    logger._on_periodic_flush()
    assert platform.upload_file.call_count == 1
    logger._on_periodic_flush()
    assert platform.upload_file.call_count == 1
    logger.close()


def test_batch_retry_reuses_the_same_destination(tmp_path):
    class FailingPlatform:
        def __init__(self):
            self.destinations = []
            self.fail = True

        def upload_file(self, local_path, dest, *, overwrite=False):
            self.destinations.append(dest)
            if self.fail:
                self.fail = False
                raise RuntimeError("temporary storage failure")
            Path(local_path).read_bytes()

    platform = FailingPlatform()
    writer = JsonLogWriter(
        platform,
        "unused.json",
        LogConfig(
            persistence_mode="batch",
            flush_interval_seconds=0,
            flush_batch_bytes=1,
            spool_directory=str(tmp_path),
        ),
        batch_path_factory=lambda sequence: f"parts/part-{sequence}.json",
    )
    writer.append({"id": 1})
    with pytest.raises(RuntimeError):
        writer.flush(force=True)
    writer.flush(force=True)
    assert platform.destinations == ["parts/part-1.json", "parts/part-1.json"]


def test_batch_retry_keeps_payload_immutable_when_new_records_arrive(tmp_path):
    class FlakyPlatform:
        def __init__(self):
            self.writer = None
            self.payloads = []
            self.fail = True

        def upload_file(self, local_path, dest, *, overwrite=False):
            self.payloads.append((dest, Path(local_path).read_bytes()))
            if self.fail:
                self.fail = False
                self.writer.append({"id": 2})
                raise RuntimeError("temporary storage failure")

    platform = FlakyPlatform()
    writer = JsonLogWriter(
        platform,
        "unused.json",
        LogConfig(
            persistence_mode="batch",
            flush_interval_seconds=0,
            flush_batch_bytes=1,
            spool_directory=str(tmp_path),
        ),
        batch_path_factory=lambda sequence: f"parts/part-{sequence}.json",
    )
    platform.writer = writer
    writer.append({"id": 1})
    with pytest.raises(RuntimeError):
        writer.flush(force=True)
    writer.flush(force=True)
    assert writer.stats.pending_records == 1
    writer.flush(force=True)

    assert [json.loads(line)["id"] for line in platform.payloads[0][1].splitlines()] == [1]
    assert [json.loads(line)["id"] for line in platform.payloads[1][1].splitlines()] == [1]
    assert [json.loads(line)["id"] for line in platform.payloads[2][1].splitlines()] == [2]
    assert platform.payloads[0][0] == platform.payloads[1][0]
    assert platform.payloads[2][0] != platform.payloads[1][0]
    assert writer.stats.pending_records == 0


def test_writer_drops_new_records_at_capacity_without_raising(tmp_path):
    writer = JsonLogWriter(
        LocalPlatform(base_path=str(tmp_path)),
        "logs/out.json",
        LogConfig(
            flush_interval_seconds=0,
            buffer_memory_bytes=64,
            spool_max_bytes=128,
            spool_directory=str(tmp_path / "spool"),
        ),
    )
    assert writer.append({"payload": "x" * 20}) is True
    assert writer.append({"payload": "y" * 20}) is False
    assert writer.stats.dropped_records == 1


def test_shared_budget_limits_two_streams_as_one_capacity_pool():
    budget = SharedByteBudget(40)
    platform = LocalPlatform()
    first = JsonLogWriter(
        platform,
        "first.json",
        LogConfig(buffer_memory_bytes=32, spool_max_bytes=64, flush_interval_seconds=0),
        capacity_budget=budget,
    )
    second = JsonLogWriter(
        platform,
        "second.json",
        LogConfig(buffer_memory_bytes=32, spool_max_bytes=64, flush_interval_seconds=0),
        capacity_budget=budget,
    )
    payload = {"value": "x"}
    assert first.append(payload) is True
    assert budget.used_bytes == 2 * len(encode_json_line(payload))
    assert second.append({"value": "y" * 100}) is False
    assert second.stats.dropped_records == 1
    first.close()
    second.close()
    assert budget.used_bytes == 0


def test_shared_budget_keeps_protected_headroom_for_job_stream():
    budget = SharedByteBudget(20, protected_limit=8)
    assert budget.try_reserve(12) is True
    assert budget.try_reserve(1) is False
    assert budget.try_reserve(8, protected=True) is True
    budget.release(8, protected=True)
    budget.release(12)
    assert budget.used_bytes == 0


def test_late_failed_worker_does_not_recreate_spool_after_close(tmp_path):
    started = threading.Event()
    release = threading.Event()

    class BlockingPlatform:
        def upload_file(self, local_path, dest, *, overwrite=False):
            started.set()
            release.wait(timeout=2)
            raise RuntimeError("late failure")

    writer = JsonLogWriter(
        BlockingPlatform(),
        "unused.json",
        LogConfig(
            persistence_mode="batch",
            flush_batch_bytes=1,
            spool_directory=str(tmp_path),
        ),
    )
    writer.append({"id": 1})
    worker = threading.Thread(target=lambda: _flush_ignoring_error(writer))
    worker.start()
    assert started.wait(timeout=2)
    writer.close()
    release.set()
    worker.join(timeout=2)

    assert not list(tmp_path.glob("*.spool"))


def test_replacement_failure_releases_superseded_reservation(tmp_path):
    class FlakyPlatform:
        def __init__(self):
            self.writer = None
            self.failed = False

        def upload_file(self, local_path, dest, *, overwrite=False):
            if not self.failed:
                self.failed = True
                self.writer.replace({"id": "queued"})
                raise RuntimeError("temporary storage failure")

    budget = SharedByteBudget(1_000)
    platform = FlakyPlatform()
    writer = JsonLogWriter(
        platform,
        "job.json",
        LogConfig(
            spool_directory=str(tmp_path),
            spool_max_bytes=1_000,
            buffer_memory_bytes=1_000,
            flush_interval_seconds=0,
        ),
        capacity_budget=budget,
    )
    platform.writer = writer
    writer.replace({"id": "old"})
    with pytest.raises(RuntimeError):
        writer.flush(force=True)

    writer.replace({"id": "latest"})
    latest_reservation = 2 * len(encode_json_line({"id": "latest"}))
    assert budget.used_bytes == latest_reservation + 2 * len(encode_json_line({"id": "old"}))
    writer.flush(force=True)
    assert budget.used_bytes == latest_reservation
    writer.flush(force=True)
    assert budget.used_bytes == latest_reservation
    writer.close()
    assert budget.used_bytes == 0


def test_materialized_upload_copy_is_within_reserved_spool_budget(tmp_path):
    observed_sizes = []

    class MeasuringPlatform:
        def upload_file(self, local_path, dest, *, overwrite=False):
            observed_sizes.append(
                sum(path.stat().st_size for path in tmp_path.iterdir() if path.is_file())
            )

    config = LogConfig(
        spool_directory=str(tmp_path),
        buffer_memory_bytes=1,
        spool_max_bytes=256,
        flush_interval_seconds=0,
    )
    payload = {"value": "x" * 40}
    writer = JsonLogWriter(MeasuringPlatform(), "job.json", config)
    assert writer.append(payload) is True
    encoded_size = len(encode_json_line(payload))
    assert writer.stats.pending_bytes == encoded_size
    assert writer.flush(force=True)
    assert observed_sizes
    assert observed_sizes[0] <= config.spool_max_bytes
    writer.close()


def _flush_ignoring_error(writer):
    try:
        writer.flush(force=True)
    except RuntimeError:
        pass
