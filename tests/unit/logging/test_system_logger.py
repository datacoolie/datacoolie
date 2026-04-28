"""Tests for datacoolie.logging.system_logger — SystemLogger + factory."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.logging.base import LogConfig, LogManager, StorageMode
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

    def test_flush_uploads(self):
        platform = MagicMock()
        cfg = LogConfig(
            output_path="/logs",
            storage_mode=StorageMode.MEMORY.value,
        )
        lgr = SystemLogger(cfg, platform=platform)
        from datacoolie.core.models import DataCoolieRunConfig
        lgr.set_run_config(DataCoolieRunConfig(job_id="job-1", job_num=2, job_index=1))

        # Generate some captured logs
        from datacoolie.logging.base import get_logger
        child = get_logger("test.flush")
        child.info("some captured message")

        lgr.flush()

        platform.upload_file.assert_called_once()
        call_args = platform.upload_file.call_args
        remote_path = call_args[0][1]
        assert "system_log" in remote_path
        assert "job-1" in remote_path
        assert "2_1" in remote_path
        assert "run_date=" in remote_path
        assert remote_path.endswith(".jsonl")

    def test_flush_content(self, tmp_path):
        """Uploaded file contains valid JSONL with the captured message."""
        platform = LocalPlatform(base_path=str(tmp_path))
        cfg = LogConfig(
            output_path="logs",
            storage_mode=StorageMode.MEMORY.value,
        )
        lgr = SystemLogger(cfg, platform=platform)
        from datacoolie.core.models import DataCoolieRunConfig
        lgr.set_run_config(DataCoolieRunConfig(job_id="job-1"))

        from datacoolie.logging.base import get_logger
        child = get_logger("test.flush.content")
        child.info("hello content test")

        lgr.close()

        jsonl_files = list(tmp_path.rglob("*.jsonl"))
        assert len(jsonl_files) == 1
        lines = [l for l in jsonl_files[0].read_text(encoding="utf-8").strip().split("\n") if l.strip()]
        assert len(lines) >= 1
        parsed = json.loads(lines[-1])
        assert "hello content test" in parsed["msg"]

    def test_flush_error_handled(self):
        """If flush fails, no exception propagates."""
        platform = MagicMock()
        platform.upload_file.side_effect = RuntimeError("write fail")

        cfg = LogConfig(output_path="/logs")
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
        cfg = LogConfig(storage_mode=StorageMode.MEMORY.value)
        lgr = SystemLogger(cfg)
        mgr = LogManager.get_instance()

        from datacoolie.logging.base import get_logger
        child = get_logger("test.cleanup")
        child.info("msg")
        assert mgr.get_captured_logs() != ""

        lgr._cleanup()
        assert mgr.get_captured_logs() == ""

    def test_context_manager(self):
        platform = MagicMock()
        cfg = LogConfig(output_path="/logs")
        with SystemLogger(cfg, platform=platform) as lgr:
            from datacoolie.logging.base import get_logger
            child = get_logger("test.ctx")
            child.info("context msg")
        # After exit, flush should have been called
        assert lgr.is_closed


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
        lgr.close()

    def test_custom_params(self):
        platform = MagicMock()
        lgr = create_system_logger(
            output_path="/logs/system",
            log_level="DEBUG",
            platform=platform,
            storage_mode=StorageMode.FILE.value,
        )
        assert lgr.config.output_path == "/logs/system"
        assert lgr.config.log_level == "DEBUG"
        lgr.close()


# ============================================================================
# Additional edge cases (merged from test_logging_edge_cases.py)
# ============================================================================


class TestSystemLoggerEdgeCases:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_flush_without_partition_by_date(self):
        platform = MagicMock()
        cfg = LogConfig(output_path="/logs", partition_by_date=False)
        logger = SystemLogger(cfg, platform=platform)

        child = logging.getLogger("DataCoolie.test.system.no_partition")
        child.info("hello")

        logger.flush()
        assert platform.upload_file.called
        remote = platform.upload_file.call_args.args[1]
        assert "run_date=" not in remote
        logger.close()
