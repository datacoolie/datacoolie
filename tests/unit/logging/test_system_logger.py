"""Contract tests for system-log capture and JSON Lines persistence."""

from __future__ import annotations

import json
import threading
from unittest.mock import MagicMock

import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.runtime.manager import LogManager, get_logger
from datacoolie.logging.runtime.context import dataflow_context
from datacoolie.logging.configuration.constants import LogEvent
from datacoolie.logging.system_logger import SystemLogger, create_system_logger
from datacoolie.platforms.local_platform import LocalPlatform


class TestSystemLogger:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_construction_is_inert_until_activation(self):
        SystemLogger(LogConfig())
        manager = LogManager.get_instance()
        assert manager.capture_handler is None
        assert manager._configured is False

    def test_activation_starts_a_fresh_capture_session(self):
        get_logger("datacoolie.system.before").info("before")
        logger = SystemLogger(LogConfig())
        logger.activate()
        get_logger("datacoolie.system.after").info("after")

        handler = LogManager.get_instance().capture_handler
        assert handler is not None
        assert [record.message for record in handler.get_records()] == ["after"]
        logger.close()

    def test_capture_ownership_is_exclusive(self):
        first = SystemLogger(LogConfig())
        second = SystemLogger(LogConfig())
        first.activate()

        with pytest.raises(RuntimeError, match="capture session is already active"):
            second.activate()

        assert first.is_active
        second.close()
        first.close()

    def test_snapshot_uses_upload_file_and_json_extension(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = SystemLogger(
            LogConfig(output_path="logs", flush_interval_seconds=0),
            platform,
        )
        logger.set_run_config(DataCoolieRunConfig(job_id="factory/job-1"))
        logger.activate()
        get_logger("datacoolie.system.snapshot").info("hello")
        logger.close()

        files = sorted(tmp_path.rglob("system_*.json"))
        assert len(files) == 1
        assert not list(tmp_path.rglob("*.jsonl"))
        assert not list(tmp_path.rglob("*.parquet"))
        payload = files[0].read_text(encoding="utf-8")
        ordered_pairs = json.loads(payload, object_pairs_hook=list)
        assert [key for key, _ in ordered_pairs[:2]] == [
            "log_schema_version",
            "_type",
        ]
        row = dict(ordered_pairs)
        assert row["log_schema_version"] == 4
        assert row["_type"] == "system_log"
        assert row["log_session_id"]
        assert row["job_id"] == "factory/job-1"
        assert row["msg"] == "hello"

    def test_capture_is_consumed_by_writer_without_a_second_queue(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = SystemLogger(LogConfig(output_path="logs"), platform)
        logger.activate()
        get_logger("datacoolie.system.direct").info("direct")

        handler = LogManager.get_instance().capture_handler
        assert handler is not None
        assert handler.get_records() == []
        logger.close()

        rows = [
            json.loads(line)
            for path in tmp_path.rglob("system_*.json")
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert [row["msg"] for row in rows] == ["direct"]

    def test_capture_persists_event_and_execution_correlation(self):
        logger = SystemLogger(LogConfig(), MagicMock())
        logger.activate()
        with dataflow_context("df-1", "run-1"):
            get_logger("datacoolie.system.context").info(
                "started",
                extra={"event_name": LogEvent.DATAFLOW_STARTED.value},
            )

        handler = LogManager.get_instance().capture_handler
        assert handler is not None
        record = handler.get_records()[0]
        assert record.dataflow_id == "df-1"
        assert record.dataflow_run_id == "run-1"
        assert record.event_name == LogEvent.DATAFLOW_STARTED.value
        logger.close()

    @pytest.mark.parametrize("storage_mode", ["memory", "file"])
    def test_capture_handoff_restores_only_unhandled_suffix(self, storage_mode):
        """A pre-callback batch keeps B/C when handoff fails before admitting B."""
        logger = SystemLogger(
            LogConfig(output_path="logs", storage_mode=storage_mode),
            MagicMock(),
        )
        logger.activate()
        handler = LogManager.get_instance().capture_handler
        assert handler is not None

        # Exercise the fallback queue used while the callback is detached.
        handler.set_record_callback(None)
        fallback = get_logger("datacoolie.system.handoff")
        fallback.info("A")
        fallback.info("B")
        fallback.info("C")

        calls = 0

        def append(record):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("handoff failure")
            return True

        logger._append_capture_record = append
        assert logger._drain_capture() == 1
        assert [record.message for record in handler.get_records()] == ["B", "C"]
        logger.close()

    def test_capture_handoff_does_not_requeue_intentional_drop(self):
        logger = SystemLogger(LogConfig(output_path="logs"), MagicMock())
        logger.activate()
        handler = LogManager.get_instance().capture_handler
        assert handler is not None
        handler.set_record_callback(None)
        fallback = get_logger("datacoolie.system.drop")
        fallback.info("A")
        fallback.info("B")

        logger._append_capture_record = MagicMock(side_effect=[True, False])
        assert logger._drain_capture() == 2
        assert handler.get_records() == []
        logger.close()

    def test_batch_writes_part_files(self, tmp_path):
        platform = LocalPlatform(base_path=str(tmp_path))
        logger = SystemLogger(
            LogConfig(
                output_path="logs",
                persistence_mode="batch",
                flush_batch_bytes=1,
                flush_interval_seconds=0,
            ),
            platform,
        )
        logger.activate()
        get_logger("datacoolie.system.batch").info("batch")
        logger.close()

        parts = sorted(tmp_path.rglob("system_*_part_*.json"))
        assert len(parts) == 1
        assert json.loads(parts[0].read_text(encoding="utf-8"))["msg"] == "batch"

    def test_batch_size_wakeup_does_not_wait_for_long_timer(self):
        uploaded = threading.Event()
        platform = MagicMock()
        platform.upload_file.side_effect = lambda *args, **kwargs: uploaded.set()
        logger = SystemLogger(
            LogConfig(
                output_path="logs",
                persistence_mode="batch",
                flush_batch_bytes=1,
                flush_interval_seconds=60,
            ),
            platform,
        )
        logger.activate()
        get_logger("datacoolie.system.wakeup").info("wake")

        assert uploaded.wait(timeout=1)
        logger.close()

    def test_batch_time_tick_flushes_a_batch_below_size_threshold(self, tmp_path):
        platform = MagicMock()
        logger = SystemLogger(
            LogConfig(
                output_path="logs",
                persistence_mode="batch",
                flush_batch_bytes=1024 * 1024,
                flush_interval_seconds=60,
            ),
            platform,
        )
        logger.activate()
        get_logger("datacoolie.system.time-trigger").info("small batch")

        logger._on_periodic_flush(time_due=True)

        assert platform.upload_file.call_count == 1
        logger.close()

    def test_failed_periodic_upload_is_retryable_with_same_snapshot(self):
        platform = MagicMock()
        platform.upload_file.side_effect = [RuntimeError("unavailable"), None]
        logger = SystemLogger(
            LogConfig(output_path="logs", flush_interval_seconds=0),
            platform,
        )
        logger.activate()
        get_logger("datacoolie.system.retry").info("retry-me")

        logger._on_periodic_flush()
        assert isinstance(logger.last_flush_error, RuntimeError)
        logger._on_periodic_flush()
        assert logger.last_flush_error is None
        assert platform.upload_file.call_count == 2
        assert (
            platform.upload_file.call_args_list[0].args[1]
            == platform.upload_file.call_args_list[1].args[1]
        )
        logger.close()

    def test_failed_terminal_upload_is_reported_without_business_exception(self):
        platform = MagicMock()
        platform.upload_file.side_effect = RuntimeError("storage down")
        logger = SystemLogger(LogConfig(output_path="logs", flush_interval_seconds=0), platform)
        logger.activate()
        get_logger("datacoolie.system.failure").info("message")
        logger.close()

        assert logger.terminal_outcomes
        assert logger.terminal_outcomes[0].status == "failed"
        assert logger.last_flush_error is not None

    def test_no_output_or_platform_is_a_valid_noop(self):
        logger = SystemLogger(LogConfig())
        logger.activate()
        get_logger("datacoolie.system.noop").info("noop")
        logger.close()
        assert logger.terminal_outcomes == ()

    def test_file_level_captures_debug(self):
        logger = SystemLogger(LogConfig(log_level="INFO", file_level="DEBUG"))
        logger.activate()
        get_logger("datacoolie.system.level").debug("debug")
        get_logger("datacoolie.system.level").info("info")

        handler = LogManager.get_instance().capture_handler
        assert handler is not None
        assert {record.message for record in handler.get_records()} == {"debug", "info"}
        logger.close()

    def test_partition_can_be_disabled(self):
        platform = MagicMock()
        logger = SystemLogger(
            LogConfig(output_path="logs", partition_by_date=False, flush_interval_seconds=0),
            platform,
        )
        logger.activate()
        get_logger("datacoolie.system.partition").info("hello")
        logger.close()

        destination = platform.upload_file.call_args.args[1]
        assert destination.startswith("logs/system_")
        assert destination.endswith(".json")
        assert "run_date=" not in destination


class TestCreateSystemLogger:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_factory_defaults_and_config_override(self):
        logger = create_system_logger()
        assert logger.config.persistence_mode == "snapshot"
        logger.close()

        configured = LogConfig(output_path="configured", persistence_mode="batch")
        logger = create_system_logger(config=configured)
        assert logger.config.output_path == "configured"
        assert logger.config.persistence_mode == "batch"
        logger.close()

    def test_factory_validates_override_without_mutating_input(self):
        configured = LogConfig()

        with pytest.raises(ValueError, match="output_path"):
            create_system_logger(output_path=" ", config=configured)

        assert configured.output_path is None
