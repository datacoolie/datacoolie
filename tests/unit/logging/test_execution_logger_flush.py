"""Snapshot/batch lifecycle and failure-isolation tests for ExecutionLogger."""

from __future__ import annotations

import json
import threading
import time
from unittest.mock import MagicMock
from pathlib import Path

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.runtime.manager import LogManager
from datacoolie.logging.execution_logger import ExecutionLogger
from datacoolie.platforms.local_platform import LocalPlatform

from tests.unit.logging.support import make_dataflow, make_runtime


class TestExecutionLoggerFlush:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_snapshot_writes_one_job_and_one_dataflow_json_file(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        logger.log(make_dataflow("df-1"), make_runtime("df-1"))
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

        files = sorted(tmp_path.rglob("*.json"))
        assert len(files) == 2
        assert not list(tmp_path.rglob("*.jsonl"))
        assert not list(tmp_path.rglob("*.parquet"))
        dataflow_file = next(path for path in files if "dataflow_run_log" in path.parts)
        job_file = next(path for path in files if "job_run_log" in path.parts)
        dataflow_rows = [json.loads(line) for line in dataflow_file.read_text().splitlines()]
        job_rows = [json.loads(line) for line in job_file.read_text().splitlines()]
        assert len(dataflow_rows) == 1
        assert len(job_rows) == 1
        assert dataflow_rows[0]["log_schema_version"] == 4
        assert dataflow_rows[0]["job_id"] == "job-1"
        assert dataflow_rows[0]["log_session_id"]
        assert job_rows[0]["status"] == DataFlowStatus.SUCCEEDED.value

    def test_snapshot_refreshes_the_same_dataflow_file_without_duplicate_rows(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger._on_periodic_flush()
        logger.log(make_dataflow("b"), make_runtime("b"))
        logger._on_periodic_flush()
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

        dataflow_file = next(tmp_path.rglob("dataflow_*.json"))
        rows = [json.loads(line) for line in dataflow_file.read_text().splitlines()]
        assert [row["dataflow_id"] for row in rows] == ["a", "b"]

    def test_batch_writes_immutable_part_files(self, tmp_path):
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
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger.log(make_dataflow("b"), make_runtime("b"))
        logger.close()

        parts = sorted(tmp_path.rglob("dataflow_*_part_*.json"))
        assert len(parts) == 1
        assert [json.loads(line)["dataflow_id"] for line in parts[0].read_text().splitlines()] == [
            "a",
            "b",
        ]

    def test_initial_job_checkpoint_is_running_and_close_finalizes_it(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        job_file = next(tmp_path.rglob("job_*.json"))
        initial = json.loads(job_file.read_text())
        assert initial["status"] == DataFlowStatus.RUNNING.value
        assert initial["end_time"] is None

        logger.finish_job(DataFlowStatus.SKIPPED.value)
        logger.close()
        final = json.loads(job_file.read_text())
        assert final["status"] == DataFlowStatus.SKIPPED.value
        assert final["message"] == "No dataflows were executed"
        assert final["end_time"] is not None
        assert logger.job_runtime.status == DataFlowStatus.SKIPPED.value

    def test_initial_checkpoint_failure_does_not_reject_activation(self, tmp_path):
        class FlakyPlatform(LocalPlatform):
            def __init__(self, base_path):
                super().__init__(base_path=base_path)
                self.failed = False

            def upload_file(self, local_path, dest, *, overwrite=False):
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("checkpoint unavailable")
                return super().upload_file(local_path, dest, overwrite=overwrite)

        logger = ExecutionLogger(
            LogConfig(output_path="logs", flush_interval_seconds=0),
            FlakyPlatform(str(tmp_path)),
        )
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        assert logger.is_active
        assert isinstance(logger.last_flush_error, RuntimeError)
        logger.close()
        assert list(tmp_path.rglob("job_*.json"))

    def test_initial_checkpoint_wait_is_bounded(self, tmp_path):
        class BlockingPlatform:
            def __init__(self):
                self.started = threading.Event()
                self.release = threading.Event()

            def upload_file(self, local_path, dest, *, overwrite=False):
                self.started.set()
                self.release.wait(timeout=2)

        platform = BlockingPlatform()
        logger = ExecutionLogger(
            LogConfig(
                output_path="logs",
                spool_directory=str(tmp_path),
                flush_interval_seconds=0,
                close_timeout_seconds=0.05,
            ),
            platform,
        )
        activation = threading.Thread(target=logger.activate)
        started = time.monotonic()
        activation.start()
        assert platform.started.wait(timeout=1)
        activation.join(timeout=1)

        assert not activation.is_alive()
        assert time.monotonic() - started < 0.5
        assert logger.is_active
        logger.close()
        assert logger.terminal_outcomes[0].name == "execution_job_log"
        assert logger.terminal_outcomes[0].status == "timed_out"
        platform.release.set()

    def test_batch_time_tick_flushes_below_size_threshold(self):
        platform = MagicMock()
        logger = ExecutionLogger(
            LogConfig(
                output_path="logs",
                persistence_mode="batch",
                flush_batch_bytes=1024 * 1024,
                flush_interval_seconds=60,
            ),
            platform,
        )
        logger.activate()
        logger.log(make_dataflow(), make_runtime())
        logger._on_periodic_flush(time_due=True)

        assert any(
            "dataflow_run_log" in call.args[1]
            for call in platform.upload_file.call_args_list
        )
        logger.close()

    def test_close_drains_retry_and_new_batch_parts(self):
        class RetryThenSuccessPlatform:
            def __init__(self):
                self.dataflow_payloads = []
                self.fail_dataflow_once = True

            def upload_file(self, local_path, destination, *, overwrite=False):
                payload = [
                    json.loads(line)
                    for line in Path(local_path).read_text(encoding="utf-8").splitlines()
                ]
                if "dataflow_run_log" in destination:
                    self.dataflow_payloads.append((destination, payload))
                    if self.fail_dataflow_once:
                        self.fail_dataflow_once = False
                        raise RuntimeError("temporary dataflow failure")

        platform = RetryThenSuccessPlatform()
        logger = ExecutionLogger(
            LogConfig(
                output_path="logs",
                persistence_mode="batch",
                flush_batch_bytes=1,
                flush_interval_seconds=0,
            ),
            platform,
        )
        logger.set_run_config(DataCoolieRunConfig(job_id="job-1"))
        logger.activate()
        logger.log(make_dataflow("a"), make_runtime("a"))
        logger._on_periodic_flush(time_due=True)
        logger.log(make_dataflow("b"), make_runtime("b"))

        logger.close()

        assert [row["dataflow_id"] for row in platform.dataflow_payloads[0][1]] == ["a"]
        assert [row["dataflow_id"] for row in platform.dataflow_payloads[1][1]] == ["a"]
        assert [row["dataflow_id"] for row in platform.dataflow_payloads[2][1]] == ["b"]
        assert platform.dataflow_payloads[1][0] != platform.dataflow_payloads[2][0]
        assert any(
            outcome.name == "execution_dataflow_log"
            and outcome.status == "succeeded"
            for outcome in logger.terminal_outcomes
        )

    def test_close_reports_the_blocked_job_stream(self):
        class BlockingJobPlatform:
            def __init__(self):
                self.job_calls = 0
                self.started = threading.Event()
                self.release = threading.Event()

            def upload_file(self, local_path, dest, *, overwrite=False):
                if "job_run_log" not in dest:
                    return
                self.job_calls += 1
                if self.job_calls >= 2:
                    self.started.set()
                    self.release.wait(timeout=2)

        platform = BlockingJobPlatform()
        logger = ExecutionLogger(
            LogConfig(
                output_path="logs",
                flush_interval_seconds=0.01,
                close_timeout_seconds=0.05,
            ),
            platform,
        )
        logger.activate()
        logger.log(make_dataflow(), make_runtime())
        assert platform.started.wait(timeout=1)

        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

        assert any(
            outcome.name == "execution_job_log" and outcome.status == "timed_out"
            for outcome in logger.terminal_outcomes
        )
        platform.release.set()

    def test_independent_dataflow_failure_does_not_block_job_snapshot(self):
        platform = MagicMock()

        def upload(local_path, destination, *, overwrite=False):
            if "dataflow_run_log" in destination:
                raise RuntimeError("dataflow sink unavailable")

        platform.upload_file.side_effect = upload
        logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.activate()
        logger.log(make_dataflow(), make_runtime())
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

        assert len(platform.upload_file.call_args_list) >= 2
        assert any("job_run_log" in call.args[1] for call in platform.upload_file.call_args_list)
        assert logger.last_flush_error is not None

    def test_writer_calls_typed_upload_not_append(self):
        platform = MagicMock()
        logger = ExecutionLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.activate()
        logger.log(make_dataflow(), make_runtime())
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
        logger.close()

        assert platform.upload_file.called
        assert not platform.append_file.called
