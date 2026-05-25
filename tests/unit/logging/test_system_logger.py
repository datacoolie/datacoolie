"""Tests for datacoolie.logging.system_logger — SystemLogger + factory."""

from __future__ import annotations

import logging
import time
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.logging.base import LogConfig, LogLevel, LogManager, StorageMode
from datacoolie.logging.system_logger import SystemLogger, create_system_logger
from datacoolie.platforms.local_platform import LocalPlatform


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

    def test_flush_no_output_path(self):
        """flush does nothing when output_path is not set."""
        lgr = SystemLogger(LogConfig())
        lgr.flush()  # should not raise
        lgr.close()

    def test_flush_no_platform(self):
        """flush does nothing when platform is not set."""
        cfg = LogConfig(output_path="/logs")
        lgr = SystemLogger(cfg, platform=None)
        lgr.flush()  # should not raise
        lgr.close()

    def test_flush_appends(self):
        """flush calls append_file with a .log remote path."""
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

        lgr.flush()

        platform.append_file.assert_called_once()
        remote_path = platform.append_file.call_args[0][0]
        assert "system_log" in remote_path
        assert "run_date=" in remote_path
        assert remote_path.endswith(".log")
        # filename: system_log_YYYYMMDD_HHMMSS_{job_id}.log
        import re
        assert re.search(r"system_log_\d{8}_\d{6}_\S+\.log$", remote_path)

    def test_flush_content_plain_text(self, tmp_path):
        """Appended file contains plain-text log lines (not JSON)."""
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

        log_files = list(tmp_path.rglob("*.log"))
        assert len(log_files) == 1
        text = log_files[0].read_text(encoding="utf-8")
        assert "hello content test" in text
        # Plain text — not JSON.
        assert not text.strip().startswith("{")

    def test_flush_error_handled(self):
        """If flush fails, no exception propagates."""
        platform = MagicMock()
        platform.append_file.side_effect = RuntimeError("write fail")

        cfg = LogConfig(output_path="/logs", flush_interval_seconds=0)
        lgr = SystemLogger(cfg, platform=platform)
        from datacoolie.core.models import DataCoolieRunConfig
        lgr.set_run_config(DataCoolieRunConfig(job_id="j"))

        from datacoolie.logging.base import get_logger
        child = get_logger("test.flush.err")
        child.info("msg")

        # Should not raise
        lgr.flush()
        lgr.close()

    def test_cleanup_clears_captured(self):
        cfg = LogConfig(storage_mode=StorageMode.MEMORY.value, flush_interval_seconds=0)
        lgr = SystemLogger(cfg)
        mgr = LogManager.get_instance()

        from datacoolie.logging.base import get_logger
        child = get_logger("test.cleanup")
        child.info("msg")
        # get_and_clear is used on flush; call _cleanup directly to test clear.
        lgr._log_manager.clear_captured_logs()
        assert mgr.get_captured_logs() == ""
        lgr._cleanup()

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

        from datacoolie.logging.base import get_logger
        child = get_logger("test.timer")
        child.info("timer triggered msg")

        # Wait for timer to fire.
        time.sleep(1.5)

        log_files = list(tmp_path.rglob("*.log"))
        # File should exist from the periodic flush.
        assert len(log_files) >= 1
        text = log_files[0].read_text(encoding="utf-8")
        assert "timer triggered msg" in text

        lgr.close()


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

        logger.flush()
        assert platform.append_file.called
        remote = platform.append_file.call_args.args[0]
        assert "run_date=" not in remote
        assert remote.endswith(".log")
        logger.close()

