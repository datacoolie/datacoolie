"""Tests for datacoolie.logging.system_logger — SystemLogger + factory."""

from __future__ import annotations

import logging
import threading
import time
from unittest.mock import MagicMock

import pytest

from datacoolie.logging.base import LogConfig, LogManager, StorageMode
from datacoolie.logging.etl_logger import ETLLogger
from datacoolie.logging.system_logger import SystemLogger, create_system_logger
from datacoolie.platforms.local_platform import LocalPlatform
from tests.unit.logging.support import make_dataflow, make_runtime


class TestSystemLogger:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_init_configures_log_manager(self):
        cfg = LogConfig(log_level="DEBUG", storage_mode=StorageMode.MEMORY.value)
        lgr = SystemLogger(cfg)
        mgr = LogManager.get_instance()
        assert mgr._configured is True
        assert mgr.capture_handler is not None
        lgr.close()

    def test_close_no_output_path(self):
        """close does nothing when output_path is not set."""
        lgr = SystemLogger(LogConfig())
        lgr.close()
        assert lgr.terminal_outcomes == ()

    def test_close_no_platform(self):
        """close does nothing when platform is not set."""
        cfg = LogConfig(output_path="/logs")
        lgr = SystemLogger(cfg, platform=None)
        lgr.close()
        assert lgr.terminal_outcomes == ()

    def test_close_appends_and_reports_exact_path_to_console(self, capsys):
        """close calls append_file with a .jsonl remote path."""
        platform = MagicMock()
        cfg = LogConfig(
            output_path="/logs",
            storage_mode=StorageMode.MEMORY.value,
            flush_interval_seconds=0,  # disable timer
        )
        lgr = SystemLogger(cfg, platform=platform)
        from datacoolie.core.models import DataCoolieRunConfig
        lgr.set_run_config(DataCoolieRunConfig(job_id="job-1"))

        from datacoolie.logging.base import get_logger
        child = get_logger("test.flush")
        child.info("some captured message")

        lgr.close()

        platform.append_file.assert_called_once()
        remote_path = platform.append_file.call_args[0][0]
        uploaded_content = platform.append_file.call_args[0][1]
        assert "system_log" in remote_path
        assert "run_date=" in remote_path
        assert remote_path.endswith(".jsonl")
        # filename: system_log_YYYYMMDD_HHMMSS_{job_id}.jsonl
        import re
        assert re.search(r"system_log_\d{8}_\d{6}_\S+\.jsonl$", remote_path)
        assert f"System log pushed: {remote_path}" in capsys.readouterr().err
        assert "System log pushed:" not in uploaded_content

    def test_failed_terminal_push_does_not_report_success(self, capsys):
        platform = MagicMock()
        platform.append_file.side_effect = RuntimeError("write failed")
        lgr = SystemLogger(
            LogConfig(output_path="/logs", flush_interval_seconds=0),
            platform,
        )
        logging.getLogger("DataCoolie.test.final_failure").info("message")

        lgr.close()

        assert lgr.terminal_outcomes[0].status == "failed"
        assert "System log pushed:" not in capsys.readouterr().err

    def test_system_final_payload_contains_etl_push_info(self):
        platform = MagicMock()
        system_logger = SystemLogger(
            LogConfig(output_path="/logs/system", flush_interval_seconds=0),
            platform,
        )
        etl_logger = ETLLogger(
            LogConfig(output_path="/logs/etl", flush_interval_seconds=0),
            platform,
        )
        etl_logger.log(make_dataflow("a"), make_runtime("a"))

        etl_logger.close()
        successful_etl_paths = [
            outcome.path
            for outcome in etl_logger.terminal_outcomes
            if outcome.status == "succeeded" and outcome.path
        ]
        system_logger.close()

        system_payload = next(
            call.args[1]
            for call in platform.append_file.call_args_list
            if "system_log_" in call.args[0]
        )
        assert successful_etl_paths
        for path in successful_etl_paths:
            assert f"ETL log pushed: {path}" in system_payload

    def test_flush_content_plain_text(self, tmp_path):
        """Appended file contains JSONL records (one JSON object per line)."""
        platform = LocalPlatform(base_path=str(tmp_path))
        cfg = LogConfig(
            output_path="logs",
            storage_mode=StorageMode.MEMORY.value,
            flush_interval_seconds=0,  # disable timer
        )
        lgr = SystemLogger(cfg, platform=platform)
        from datacoolie.core.models import DataCoolieRunConfig
        lgr.set_run_config(DataCoolieRunConfig(job_id="job-1"))

        from datacoolie.logging.base import get_logger
        child = get_logger("test.flush.content")
        child.info("hello content test")

        lgr.close()

        log_files = list(tmp_path.rglob("*.jsonl"))
        # Filter to system_log files only (exclude ETL logs if any)
        log_files = [f for f in log_files if "system_log" in f.name]
        assert len(log_files) == 1
        import json
        lines = [
            line
            for line in log_files[0].read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert len(lines) >= 1
        record = json.loads(lines[0])
        assert "hello content test" in record["msg"]
        assert "ts" in record and "level" in record and "logger" in record

    @pytest.mark.parametrize(
        "storage_mode",
        [StorageMode.MEMORY.value, StorageMode.FILE.value],
    )
    def test_periodic_error_retains_batch_for_retry(self, storage_mode):
        """A definite periodic failure retains records for a later interval."""
        platform = MagicMock()
        platform.append_file.side_effect = [RuntimeError("write fail"), None]

        cfg = LogConfig(
            output_path="/logs",
            storage_mode=storage_mode,
            flush_interval_seconds=0,
        )
        lgr = SystemLogger(cfg, platform=platform)
        from datacoolie.core.models import DataCoolieRunConfig
        lgr.set_run_config(DataCoolieRunConfig(job_id="j"))

        from datacoolie.logging.base import get_logger
        child = get_logger("test.flush.err")
        child.info("msg")

        lgr._on_periodic_flush()
        assert isinstance(lgr.last_flush_error, RuntimeError)
        pending = lgr._log_manager.capture_handler.get_records()
        assert [record.message for record in pending] == ["msg"]

        lgr._on_periodic_flush()
        assert lgr.last_flush_error is None
        assert platform.append_file.call_count == 2
        retry_payload = platform.append_file.call_args_list[1].args[1]
        assert retry_payload.count('"msg": "msg"') == 1
        lgr.close()

    def test_periodic_flush_does_not_capture_its_own_success(self):
        platform = MagicMock()
        lgr = SystemLogger(
            LogConfig(output_path="/logs", flush_interval_seconds=0),
            platform,
        )
        from datacoolie.logging.base import get_logger

        get_logger("test.periodic.feedback").info("one event")
        lgr._on_periodic_flush()

        assert platform.append_file.call_count == 1
        assert lgr._log_manager.get_captured_logs() == ""
        lgr.close()

    def test_cleanup_clears_captured(self):
        cfg = LogConfig(storage_mode=StorageMode.MEMORY.value, flush_interval_seconds=0)
        lgr = SystemLogger(cfg)
        mgr = LogManager.get_instance()

        from datacoolie.logging.base import get_logger
        child = get_logger("test.cleanup")
        child.info("msg")
        assert "msg" in mgr.get_captured_logs()
        lgr._cleanup()
        assert mgr.get_captured_logs() == ""

    def test_close_detaches_owned_capture_handler(self):
        lgr = SystemLogger(
            LogConfig(storage_mode=StorageMode.MEMORY.value),
        )
        mgr = LogManager.get_instance()
        handler = mgr.capture_handler
        assert handler is not None
        assert handler in logging.getLogger("DataCoolie").handlers

        lgr.close()
        from datacoolie.logging.base import get_logger

        get_logger("test.after_close").info("must not be captured")

        assert handler not in logging.getLogger("DataCoolie").handlers
        assert handler.get_records() == []
        assert mgr.capture_handler is None

    def test_context_manager(self):
        platform = MagicMock()
        cfg = LogConfig(output_path="/logs", flush_interval_seconds=0)
        with SystemLogger(cfg, platform=platform) as lgr:
            from datacoolie.logging.base import get_logger
            child = get_logger("test.ctx")
            child.info("context msg")
        assert lgr.is_closed

    def test_file_level_captures_debug_when_console_is_info(self):
        """With file_level=DEBUG and log_level=INFO, the capture handler
        receives DEBUG records even though the console does not."""
        mgr = LogManager.get_instance()
        cfg = LogConfig(
            log_level="INFO",
            file_level="DEBUG",
            flush_interval_seconds=0,
        )
        lgr = SystemLogger(cfg)

        from datacoolie.logging.base import get_logger
        child = get_logger("test.file_level")
        child.debug("debug only msg")
        child.info("info msg")

        # Console handler at INFO — debug not shown.
        assert mgr._console_handler is not None
        assert mgr._console_handler.level == logging.INFO
        # Capture handler at DEBUG — debug record captured.
        assert mgr._capture_handler is not None
        assert mgr._capture_handler.level == logging.DEBUG
        captured = mgr.get_captured_logs()
        assert "debug only msg" in captured
        lgr.close()

    def test_periodic_flush_via_timer(self, tmp_path):
        """Timer fires and appends content without waiting for close."""
        platform = LocalPlatform(base_path=str(tmp_path))
        cfg = LogConfig(
            output_path="logs",
            storage_mode=StorageMode.MEMORY.value,
            flush_interval_seconds=1,
        )
        lgr = SystemLogger(cfg, platform=platform)
        from datacoolie.core.models import DataCoolieRunConfig
        lgr.set_run_config(DataCoolieRunConfig(job_id="timer-job"))
        lgr.activate()

        from datacoolie.logging.base import get_logger
        child = get_logger("test.timer")
        child.info("timer triggered msg")

        # Wait for timer to fire.
        time.sleep(1.5)

        log_files = list(tmp_path.rglob("*.jsonl"))
        log_files = [f for f in log_files if "system_log" in f.name]
        # File should exist from the periodic flush.
        assert len(log_files) >= 1
        import json
        lines = [
            line
            for line in log_files[0].read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert any(
            "timer triggered msg" in json.loads(line)["msg"]
            for line in lines
        )

        lgr.close()

    def test_blocked_periodic_flush_cannot_hang_close(self):
        platform = MagicMock()
        append_started = threading.Event()
        release_append = threading.Event()

        def append_file(_path, _content):
            append_started.set()
            release_append.wait(timeout=2)

        platform.append_file.side_effect = append_file
        lgr = SystemLogger(
            LogConfig(
                output_path="/logs",
                flush_interval_seconds=0.01,
                close_timeout_seconds=0.05,
            ),
            platform,
        )
        lgr.activate()
        logging.getLogger("DataCoolie.test.blocked").info("blocked")
        assert append_started.wait(timeout=2)

        started = time.monotonic()
        lgr.close()
        elapsed = time.monotonic() - started
        release_append.set()

        assert elapsed < 0.3
        assert lgr.terminal_outcomes[0].name == "system_jsonl"
        assert lgr.terminal_outcomes[0].status == "timed_out"
        assert LogManager.get_instance().capture_handler is None


# ============================================================================
# create_system_logger factory
# ============================================================================


class TestCreateSystemLogger:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_defaults(self):
        lgr = create_system_logger()
        assert isinstance(lgr, SystemLogger)
        assert lgr.config.log_level == "INFO"
        assert lgr.config.file_level == "DEBUG"
        lgr.close()

    def test_custom_params(self):
        platform = MagicMock()
        lgr = create_system_logger(
            output_path="/logs/system",
            log_level="WARNING",
            file_level="INFO",
            platform=platform,
            storage_mode=StorageMode.FILE.value,
        )
        assert lgr.config.output_path == "/logs/system"
        assert lgr.config.log_level == "WARNING"
        assert lgr.config.file_level == "INFO"
        lgr.close()


# ============================================================================
# Additional edge cases
# ============================================================================


class TestSystemLoggerEdgeCases:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_flush_without_partition_by_date(self):
        platform = MagicMock()
        cfg = LogConfig(
            output_path="/logs",
            partition_by_date=False,
            flush_interval_seconds=0,
        )
        logger = SystemLogger(cfg, platform=platform)

        child = logging.getLogger("DataCoolie.test.system.no_partition")
        child.info("hello")

        logger.close()
        assert platform.append_file.called
        remote = platform.append_file.call_args.args[0]
        assert "run_date=" not in remote
        assert remote.endswith(".jsonl")

