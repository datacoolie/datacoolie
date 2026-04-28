"""Tests for datacoolie.logging.base — LogManager, CaptureHandler, BaseLogger, LogConfig."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from datacoolie.logging.base import (
    BaseLogger,
    CaptureHandler,
    DataflowContextFilter,
    LogConfig,
    LogLevel,
    LogManager,
    LogRecord,
    StorageMode,
    format_partition_path,
    get_logger,
)


# ============================================================================
# LogLevel / StorageMode enums
# ============================================================================


class TestEnums:
    def test_log_levels(self):
        assert LogLevel.DEBUG.value == "DEBUG"
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.CRITICAL.value == "CRITICAL"

    def test_storage_modes(self):
        assert StorageMode.MEMORY.value == "memory"
        assert StorageMode.FILE.value == "file"


# ============================================================================
# LogRecord
# ============================================================================


class TestLogRecord:
    def test_basic_format(self):
        ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        rec = LogRecord(
            timestamp=ts,
            level="INFO",
            logger_name="test",
            message="hello",
        )
        formatted = rec.format()
        assert "INFO" in formatted
        assert "test" in formatted
        assert "hello" in formatted

    def test_format_with_location(self):
        rec = LogRecord(
            timestamp=datetime.now(timezone.utc),
            level="DEBUG",
            logger_name="mod",
            message="msg",
            module="mymod",
            func_name="myfn",
            line_no=42,
        )
        formatted = rec.format(include_location=True)
        assert "[mymod.myfn:42]" in formatted

    def test_format_with_location_partial(self):
        rec = LogRecord(
            timestamp=datetime.now(timezone.utc),
            level="DEBUG",
            logger_name="mod",
            message="msg",
            module="mymod",
        )
        formatted = rec.format(include_location=True)
        assert "[mymod]" in formatted

    def test_format_with_exc_info(self):
        rec = LogRecord(
            timestamp=datetime.now(timezone.utc),
            level="ERROR",
            logger_name="test",
            message="fail",
            exc_info="Traceback: some error",
        )
        formatted = rec.format()
        assert "Traceback: some error" in formatted

    def test_to_dict(self):
        ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        rec = LogRecord(
            timestamp=ts,
            level="WARNING",
            logger_name="test.mod",
            message="hello world",
            module="mymod",
            func_name="myfn",
            line_no=42,
        )
        d = rec.to_dict()
        assert d["ts"] == ts.isoformat()
        assert d["level"] == "WARNING"
        assert d["logger"] == "test.mod"
        assert d["msg"] == "hello world"
        assert d["module"] == "mymod"
        assert d["func"] == "myfn"
        assert d["line"] == 42

    def test_to_dict_minimal(self):
        ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        rec = LogRecord(timestamp=ts, level="INFO", logger_name="t", message="m")
        d = rec.to_dict()
        assert "module" not in d
        assert "func" not in d
        assert "line" not in d
        assert "exc_info" not in d

    def test_from_dict_round_trip(self):
        ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        original = LogRecord(
            timestamp=ts,
            level="ERROR",
            logger_name="test",
            message="fail",
            module="mod",
            func_name="fn",
            line_no=10,
            exc_info="Traceback: err",
        )
        restored = LogRecord.from_dict(original.to_dict())
        assert restored.timestamp == original.timestamp
        assert restored.level == original.level
        assert restored.logger_name == original.logger_name
        assert restored.message == original.message
        assert restored.module == original.module
        assert restored.func_name == original.func_name
        assert restored.line_no == original.line_no
        assert restored.exc_info == original.exc_info


# ============================================================================
# CaptureHandler
# ============================================================================


class TestCaptureHandler:
    def test_memory_mode(self):
        handler = CaptureHandler(storage_mode=StorageMode.MEMORY.value)
        lgr = logging.getLogger("test.capture.mem")
        lgr.addHandler(handler)
        lgr.setLevel(logging.DEBUG)

        lgr.info("test message")

        records = handler.get_records()
        assert len(records) == 1
        assert records[0].message == "test message"
        assert records[0].level == "INFO"

        lgr.removeHandler(handler)

    def test_formatted_logs(self):
        handler = CaptureHandler(storage_mode=StorageMode.MEMORY.value)
        lgr = logging.getLogger("test.capture.fmt")
        lgr.addHandler(handler)
        lgr.setLevel(logging.DEBUG)

        lgr.info("line1")
        lgr.warning("line2")

        text = handler.get_formatted_logs()
        assert "line1" in text
        assert "line2" in text

        lgr.removeHandler(handler)

    def test_clear(self):
        handler = CaptureHandler(storage_mode=StorageMode.MEMORY.value)
        lgr = logging.getLogger("test.capture.clear")
        lgr.addHandler(handler)
        lgr.setLevel(logging.DEBUG)

        lgr.info("msg")
        assert len(handler.get_records()) == 1

        handler.clear()
        assert len(handler.get_records()) == 0

        lgr.removeHandler(handler)

    def test_cleanup(self):
        handler = CaptureHandler(storage_mode=StorageMode.MEMORY.value)
        lgr = logging.getLogger("test.capture.cleanup")
        lgr.addHandler(handler)
        lgr.setLevel(logging.DEBUG)

        lgr.info("msg")
        handler.cleanup()
        assert len(handler.get_records()) == 0

        lgr.removeHandler(handler)

    def test_file_mode(self):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        lgr = logging.getLogger("test.capture.file")
        lgr.addHandler(handler)
        lgr.setLevel(logging.DEBUG)

        lgr.info("file message")

        assert handler._temp_file is not None
        records = handler.get_records()
        # File mode stores to disk as JSONL — records faithfully round-tripped
        assert len(records) >= 1
        assert records[0].message == "file message"
        assert records[0].level == "INFO"

        text = handler.get_formatted_logs()
        assert "file message" in text

        handler.cleanup()
        lgr.removeHandler(handler)

    def test_jsonl_logs_memory(self):
        handler = CaptureHandler(storage_mode=StorageMode.MEMORY.value)
        lgr = logging.getLogger("test.capture.jsonl.mem")
        lgr.addHandler(handler)
        lgr.setLevel(logging.DEBUG)

        lgr.info("msg1")
        lgr.warning("msg2")

        jsonl = handler.get_jsonl_logs()
        lines = jsonl.strip().split("\n")
        assert len(lines) == 2
        row1 = json.loads(lines[0])
        assert row1["msg"] == "msg1"
        assert row1["level"] == "INFO"
        row2 = json.loads(lines[1])
        assert row2["msg"] == "msg2"
        assert row2["level"] == "WARNING"

        lgr.removeHandler(handler)

    def test_jsonl_logs_file(self):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        lgr = logging.getLogger("test.capture.jsonl.file")
        lgr.addHandler(handler)
        lgr.setLevel(logging.DEBUG)

        lgr.error("err msg")

        jsonl = handler.get_jsonl_logs()
        lines = jsonl.strip().split("\n")
        assert len(lines) == 1
        row = json.loads(lines[0])
        assert row["msg"] == "err msg"
        assert row["level"] == "ERROR"

        handler.cleanup()
        lgr.removeHandler(handler)

    def test_file_mode_cleanup_removes_file(self):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        lgr = logging.getLogger("test.capture.file.clean")
        lgr.addHandler(handler)
        lgr.setLevel(logging.DEBUG)

        lgr.info("msg")
        temp_path = handler._temp_file
        assert temp_path and os.path.exists(temp_path)

        handler.cleanup()
        assert not os.path.exists(temp_path)

        lgr.removeHandler(handler)

    def test_formatted_logs_file_empty(self):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        # No messages → empty string or empty file
        text = handler.get_formatted_logs()
        assert text == "" or isinstance(text, str)
        handler.cleanup()


# ============================================================================
# LogManager (singleton)
# ============================================================================


class TestLogManager:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_singleton(self):
        a = LogManager.get_instance()
        b = LogManager.get_instance()
        assert a is b

    def test_reset(self):
        a = LogManager.get_instance()
        LogManager.reset()
        b = LogManager.get_instance()
        assert a is not b

    def test_configure(self):
        mgr = LogManager.get_instance()
        mgr.configure(level="DEBUG", capture_logs=True, console_output=False)
        assert mgr._configured is True
        assert mgr.capture_handler is not None

    def test_configure_no_capture(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=False)
        assert mgr.capture_handler is None

    def test_get_logger(self):
        mgr = LogManager.get_instance()
        lgr = mgr.get_logger("test.child")
        assert isinstance(lgr, logging.Logger)
        assert "DataCoolie" in lgr.name or "test.child" in lgr.name

    def test_get_logger_auto_configures(self):
        mgr = LogManager.get_instance()
        assert mgr._configured is False
        mgr.get_logger("auto_config")
        assert mgr._configured is True

    def test_get_captured_logs(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False)
        lgr = mgr.get_logger("test.cap")
        lgr.info("captured msg")
        logs = mgr.get_captured_logs()
        assert "captured msg" in logs

    def test_clear_captured_logs(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False)
        lgr = mgr.get_logger("test.clr")
        lgr.info("msg")
        mgr.clear_captured_logs()
        assert mgr.get_captured_logs() == ""

    def test_get_captured_logs_no_handler(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=False)
        assert mgr.get_captured_logs() == ""

    def test_get_captured_jsonl_logs(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False)
        lgr = mgr.get_logger("test.jsonl")
        lgr.info("jsonl msg")
        jsonl = mgr.get_captured_jsonl_logs()
        lines = [l for l in jsonl.strip().split("\n") if l.strip()]
        assert len(lines) >= 1
        row = json.loads(lines[-1])
        assert row["msg"] == "jsonl msg"
        assert row["level"] == "INFO"

    def test_get_captured_jsonl_logs_no_handler(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=False)
        assert mgr.get_captured_jsonl_logs() == ""


# ============================================================================
# get_logger (module-level convenience)
# ============================================================================


class TestGetLogger:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_returns_logger(self):
        lgr = get_logger("mymod")
        assert isinstance(lgr, logging.Logger)


# ============================================================================
# LogConfig
# ============================================================================


class TestLogConfig:
    def test_defaults(self):
        cfg = LogConfig()
        assert cfg.log_level == "INFO"
        assert cfg.storage_mode == StorageMode.MEMORY.value
        assert cfg.output_path is None
        assert cfg.partition_by_date is True
        assert cfg.flush_interval_seconds == 60

    def test_level_uppercased(self):
        cfg = LogConfig(log_level="debug")
        assert cfg.log_level == "DEBUG"

    def test_custom(self):
        cfg = LogConfig(
            log_level="WARNING",
            storage_mode="file",
            output_path="/logs",
            partition_by_date=False,
        )
        assert cfg.log_level == "WARNING"
        assert cfg.output_path == "/logs"
        assert cfg.partition_by_date is False


# ============================================================================
# BaseLogger (via concrete stub)
# ============================================================================


class _StubLogger(BaseLogger):
    """Minimal concrete implementation for testing the ABC."""

    def __init__(self, config, platform=None):
        super().__init__(config, platform)
        self.flushed = False

    def flush(self):
        self.flushed = True


class TestBaseLogger:
    def test_properties(self):
        cfg = LogConfig()
        lgr = _StubLogger(cfg)
        assert lgr.config is cfg
        assert lgr.is_closed is False
        assert lgr.run_config is None

    def test_set_run_config(self):
        cfg = LogConfig()
        lgr = _StubLogger(cfg)
        rc = object()
        lgr.set_run_config(rc)
        assert lgr.run_config is rc

    def test_close_flushes(self):
        lgr = _StubLogger(LogConfig())
        lgr.close()
        assert lgr.flushed is True
        assert lgr.is_closed is True

    def test_close_idempotent(self):
        lgr = _StubLogger(LogConfig())
        lgr.close()
        lgr.flushed = False  # reset
        lgr.close()
        assert lgr.flushed is False  # not flushed again

    def test_context_manager(self):
        cfg = LogConfig()
        with _StubLogger(cfg) as lgr:
            pass
        assert lgr.flushed is True
        assert lgr.is_closed is True

    def test_partition_path(self):
        dt = datetime(2024, 1, 15, tzinfo=timezone.utc)
        path = format_partition_path("/base", run_date=dt)
        assert path == "/base/run_date=2024-01-15"

    def test_partition_path_defaults_to_now(self):
        path = format_partition_path("/base")
        assert path.startswith("/base/run_date=")

    def test_partition_path_strips_trailing_slash(self):
        dt = datetime(2024, 3, 5, tzinfo=timezone.utc)
        path = format_partition_path("/base/", run_date=dt)
        assert path == "/base/run_date=2024-03-05"

    def test_partition_path_custom_pattern_with_hour(self):
        dt = datetime(2024, 6, 7, 9, 30, tzinfo=timezone.utc)
        path = format_partition_path("/base", run_date=dt, pattern="year={year}/month={month}/day={day}/hour={hour}")
        assert path == "/base/year=2024/month=06/day=07/hour=09"

    def test_partition_path_custom_pattern_year_month(self):
        dt = datetime(2024, 1, 5, tzinfo=timezone.utc)
        path = format_partition_path("/base", run_date=dt, pattern="dt={year}-{month}")
        assert path == "/base/dt=2024-01"

    def test_partition_path_hour_zero_padded(self):
        dt = datetime(2024, 12, 31, 3, tzinfo=timezone.utc)
        path = format_partition_path("/base", run_date=dt, pattern="run_date={year}-{month}-{day}/hour={hour}")
        assert path == "/base/run_date=2024-12-31/hour=03"


class TestLogConfigPartitionPattern:
    def test_default_partition_pattern(self):
        cfg = LogConfig()
        assert cfg.partition_pattern == "run_date={year}-{month}-{day}"

    def test_custom_partition_pattern(self):
        cfg = LogConfig(partition_pattern="year={year}/month={month}/day={day}/hour={hour}")
        assert cfg.partition_pattern == "year={year}/month={month}/day={day}/hour={hour}"


# ============================================================================
# Additional edge cases (merged from test_logging_edge_cases.py)
# ============================================================================


class TestCaptureHandlerEdgeCases:
    def test_emit_records_exc_info_text(self):
        handler = CaptureHandler(storage_mode=StorageMode.MEMORY.value)
        logger = logging.getLogger("test.capture.exc")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            raise ValueError("boom")
        except ValueError:
            logger.exception("failed")

        records = handler.get_records()
        assert len(records) == 1
        assert records[0].exc_info is not None
        assert "Traceback" in records[0].exc_info

        logger.removeHandler(handler)

    def test_emit_handles_internal_error(self):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        handler._write_to_file = MagicMock(side_effect=RuntimeError("write failed"))  # type: ignore[method-assign]
        handler.handleError = MagicMock()  # type: ignore[method-assign]

        record = logging.LogRecord(
            name="t",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="msg",
            args=(),
            exc_info=None,
        )
        handler.emit(record)
        handler.handleError.assert_called_once_with(record)

    def test_write_to_file_no_temp_file_is_noop(self):
        handler = CaptureHandler(storage_mode=StorageMode.MEMORY.value)
        handler._temp_file = None
        rec = LogRecord(
            timestamp=datetime.now(timezone.utc),
            level="INFO",
            logger_name="x",
            message="m",
        )
        handler._write_to_file(rec)
        assert handler.get_records() == []

    def test_write_to_file_fallbacks_to_memory_on_open_error(self, monkeypatch):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        handler._temp_file = "D:/definitely_missing_dir/forbidden.tmp"

        def _raise(*args, **kwargs):
            raise OSError("cannot open")

        monkeypatch.setattr("builtins.open", _raise)
        rec = LogRecord(
            timestamp=datetime.now(timezone.utc),
            level="INFO",
            logger_name="x",
            message="m",
        )
        handler._write_to_file(rec)
        assert len(handler._records) == 1

    def test_load_from_file_handles_bad_json_and_blank_lines(self, tmp_path):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        handler._temp_file = str(tmp_path / "bad.jsonl")
        (tmp_path / "bad.jsonl").write_text("\n{", encoding="utf-8")

        records = handler._load_from_file()
        assert len(records) == 1
        assert records[0].logger_name == "file"
        assert records[0].message == "{"

    def test_load_from_file_swallow_read_error(self, monkeypatch, tmp_path):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        fp = tmp_path / "x.jsonl"
        fp.write_text('{"ts":"2024-01-01T00:00:00+00:00","level":"INFO","logger":"a","msg":"b"}\n', encoding="utf-8")
        handler._temp_file = str(fp)

        def _raise(*args, **kwargs):
            raise OSError("cannot read")

        monkeypatch.setattr("builtins.open", _raise)
        assert handler._load_from_file() == []

    def test_clear_swallow_remove_error(self, monkeypatch, tmp_path):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        fp = tmp_path / "capture.tmp"
        fp.write_text("x", encoding="utf-8")
        handler._temp_file = str(fp)

        monkeypatch.setattr("os.remove", lambda *_: (_ for _ in ()).throw(OSError("deny")))
        handler.clear()

    def test_cleanup_swallow_remove_error(self, monkeypatch, tmp_path):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        fp = tmp_path / "capture.tmp"
        fp.write_text("x", encoding="utf-8")
        handler._temp_file = str(fp)

        monkeypatch.setattr("os.remove", lambda *_: (_ for _ in ()).throw(OSError("deny")))
        handler.cleanup()
        assert handler._temp_file is None

    def test_clear_recreates_temp_file_after_remove_succeeds(self, tmp_path):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        fp = tmp_path / "capture.tmp"
        fp.write_text("x", encoding="utf-8")
        handler._temp_file = str(fp)

        handler.clear()
        assert handler._temp_file is not None
        assert handler._temp_file != str(fp)


class TestLogManagerEdgeCases:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_get_instance_double_checked_lock_inner_false_branch(self, monkeypatch):
        class FakeLock:
            def __enter__(self_inner):
                LogManager._instance = LogManager()
                return self_inner

            def __exit__(self_inner, exc_type, exc, tb):
                return False

        LogManager._instance = None
        monkeypatch.setattr(LogManager, "_lock", FakeLock())
        inst = LogManager.get_instance()
        assert isinstance(inst, LogManager)

    def test_configure_noop_when_already_configured_and_not_forced(self):
        mgr = LogManager.get_instance()
        mgr.configure(level="INFO", capture_logs=True, force=True)
        first_handler = mgr.capture_handler
        mgr.configure(level="DEBUG", capture_logs=True, force=False)
        assert mgr.capture_handler is first_handler

    def test_configure_updates_existing_logger_levels(self):
        mgr = LogManager.get_instance()
        child = mgr.get_logger("edge.level")
        mgr.configure(level="ERROR", force=True)
        assert child.level == logging.ERROR

    def test_get_logger_with_prefixed_name_reused(self):
        mgr = LogManager.get_instance()
        l1 = mgr.get_logger("DataCoolie.prefixed")
        l2 = mgr.get_logger("DataCoolie.prefixed")
        assert l1 is l2

    def test_clear_captured_logs_no_handler(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=False, force=True)
        mgr.clear_captured_logs()


# ============================================================================
# DataflowContextFilter
# ============================================================================


class TestDataflowContextFilter:
    def test_sets_dataflow_id_from_contextvar(self):
        from datacoolie.logging.context import clear_dataflow_id, set_dataflow_id

        filt = DataflowContextFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello", args=None, exc_info=None,
        )
        token = set_dataflow_id("df-42")
        try:
            filt.filter(record)
            assert record.dataflow_id == "df-42"  # type: ignore[attr-defined]
        finally:
            clear_dataflow_id(token)

    def test_default_dataflow_id_is_empty(self):
        filt = DataflowContextFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello", args=None, exc_info=None,
        )
        filt.filter(record)
        assert record.dataflow_id == ""  # type: ignore[attr-defined]

    def test_filter_always_returns_true(self):
        filt = DataflowContextFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello", args=None, exc_info=None,
        )
        assert filt.filter(record) is True


class TestLogManagerContextFilter:
    """Verify LogManager.configure attaches DataflowContextFilter to handlers."""

    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_context_filter_attached_to_handlers(self):
        mgr = LogManager.get_instance()
        mgr.configure(force=True)
        root = logging.getLogger(mgr._root_logger_name)
        for h in root.handlers:
            filter_types = [type(f) for f in h.filters]
            assert DataflowContextFilter in filter_types

    def test_dataflow_id_appears_in_formatted_output(self):
        from datacoolie.logging.context import clear_dataflow_id, set_dataflow_id

        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False, force=True)
        lgr = mgr.get_logger("test.ctx")

        # Capture what the Python formatter actually produces
        formatted_lines: list[str] = []
        original_emit = mgr.capture_handler.emit

        def spy_emit(record: logging.LogRecord) -> None:
            formatted_lines.append(mgr.capture_handler.format(record))
            original_emit(record)

        mgr.capture_handler.emit = spy_emit  # type: ignore[assignment]

        token = set_dataflow_id("df-fmt-test")
        try:
            lgr.info("check format")
        finally:
            clear_dataflow_id(token)

        assert any("[df-fmt-test]" in line for line in formatted_lines)

