"""Tests for captured records and local capture storage."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from datacoolie.logging.runtime.capture import CaptureHandler, LogRecord
from datacoolie.logging.configuration.constants import StorageMode


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
            dataflow_id="df-123",
            dataflow_run_id="run-456",
            event_name="dataflow.started",
        )
        formatted = rec.format(include_location=True)
        assert "mod:myfn:42" in formatted
        assert "[df-123] [run-456:dataflow.started]" in formatted

    def test_format_with_location_partial(self):
        rec = LogRecord(
            timestamp=datetime.now(timezone.utc),
            level="DEBUG",
            logger_name="mod",
            message="msg",
            module="mymod",
        )
        formatted = rec.format(include_location=True)
        assert "msg" in formatted  # no func_name so no location bracket
        assert "[" not in formatted.split(" - ")[-1]  # no location bracket appended

    def test_format_without_context_has_no_empty_segments(self):
        rec = LogRecord(
            timestamp=datetime.now(timezone.utc),
            level="INFO",
            logger_name="test",
            message="plain",
        )

        formatted = rec.format()

        assert formatted.endswith(" - plain")
        assert "[]" not in formatted
        assert " -  - " not in formatted

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
            dataflow_id="df-123",
            dataflow_run_id="run-456",
            event_name="dataflow.started",
        )
        d = rec.to_dict()
        assert d["ts"] == ts.isoformat()
        assert d["level"] == "WARNING"
        assert d["logger"] == "test.mod"
        assert d["msg"] == "hello world"
        assert d["module"] == "mymod"
        assert d["func"] == "myfn"
        assert d["line"] == 42
        assert d["dataflow_id"] == "df-123"
        assert d["dataflow_run_id"] == "run-456"
        assert d["event_name"] == "dataflow.started"

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
            dataflow_id="df-1",
            dataflow_run_id="run-1",
            event_name="dataflow.finished",
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
        assert restored.dataflow_id == original.dataflow_id
        assert restored.dataflow_run_id == original.dataflow_run_id
        assert restored.event_name == original.event_name

    def test_from_dict_omits_malformed_optional_context_fields(self):
        record = LogRecord.from_dict(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": "INFO",
                "logger": "test",
                "msg": "message",
                "dataflow_id": 123,
                "dataflow_run_id": {"id": "run"},
                "event_name": ["dataflow.started"],
            }
        )

        assert record.dataflow_id is None
        assert record.dataflow_run_id is None
        assert record.event_name is None


# ============================================================================
# CaptureHandler
# ============================================================================


class TestCaptureHandler:
    def test_file_mode_handlers_use_distinct_spool_paths(self):
        first = CaptureHandler(storage_mode=StorageMode.FILE.value)
        second = CaptureHandler(storage_mode=StorageMode.FILE.value)

        try:
            assert first._temp_file != second._temp_file
        finally:
            first.cleanup()
            second.cleanup()

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

    @pytest.mark.parametrize(
        ("source_mode", "target_mode"),
        [
            (StorageMode.MEMORY.value, StorageMode.FILE.value),
            (StorageMode.FILE.value, StorageMode.MEMORY.value),
        ],
    )
    def test_reconfigure_migrates_records_without_reordering(
        self,
        source_mode,
        target_mode,
    ):
        handler = CaptureHandler(level=logging.DEBUG, storage_mode=source_mode)
        old_temp_file = handler._temp_file
        for message in ["first", "second"]:
            handler.handle(
                logging.LogRecord(
                    "datacoolie.test",
                    logging.INFO,
                    __file__,
                    1,
                    message,
                    (),
                    None,
                )
            )

        formatter = logging.Formatter("%(message)s")
        handler.reconfigure(
            level=logging.WARNING,
            storage_mode=target_mode,
            formatter=formatter,
        )

        assert handler.level == logging.WARNING
        assert handler.formatter is formatter
        assert [record.message for record in handler.get_records()] == ["first", "second"]
        if old_temp_file:
            assert not os.path.exists(old_temp_file)
        handler.cleanup()

    def test_reconfigure_failure_preserves_existing_state(self, tmp_path):
        handler = CaptureHandler(level=logging.DEBUG, storage_mode=StorageMode.MEMORY.value)
        handler.handle(
            logging.LogRecord(
                "datacoolie.test",
                logging.INFO,
                __file__,
                1,
                "preserved",
                (),
                None,
            )
        )
        original_formatter = handler.formatter
        handler._new_temp_file_path = lambda: str(tmp_path / "missing" / "capture.tmp")

        with pytest.raises(OSError):
            handler.reconfigure(
                level=logging.ERROR,
                storage_mode=StorageMode.FILE.value,
                formatter=logging.Formatter("%(message)s"),
            )

        assert handler._storage_mode == StorageMode.MEMORY.value
        assert handler.level == logging.DEBUG
        assert handler.formatter is original_formatter
        assert [record.message for record in handler.get_records()] == ["preserved"]
        handler.cleanup()

    def test_reconfigure_lower_capacity_drops_newest_records(self):
        handler = CaptureHandler(
            storage_mode=StorageMode.MEMORY.value,
            max_buffer_bytes=10_000,
        )
        for message in ("first", "second", "third"):
            handler.handle(
                logging.LogRecord(
                    "datacoolie.test",
                    logging.INFO,
                    __file__,
                    1,
                    message,
                    (),
                    None,
                )
            )
        first_size = handler._record_size(handler.get_records()[0])
        handler.reconfigure(
            level=logging.INFO,
            storage_mode=StorageMode.MEMORY.value,
            formatter=logging.Formatter("%(message)s"),
            max_buffer_bytes=first_size,
        )

        assert [record.message for record in handler.get_records()] == ["first"]
        assert handler.dropped_records == 2
        assert handler._buffered_bytes <= first_size
        handler.cleanup()


# ============================================================================
# CaptureHandler edge cases


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
        # The structured message is stored separately; exception text is the
        # traceback body only, so presentation does not repeat ``failed``.
        assert not records[0].exc_info.startswith("failed")

        logger.removeHandler(handler)

    def test_emit_handles_internal_error(self):
        handler = CaptureHandler(storage_mode=StorageMode.FILE.value)
        handler._retain_fallback = MagicMock(side_effect=RuntimeError("write failed"))  # type: ignore[method-assign]
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

    def test_write_to_file_without_temp_file_uses_memory_fallback(self):
        handler = CaptureHandler(storage_mode=StorageMode.MEMORY.value)
        handler._temp_file = None
        rec = LogRecord(
            timestamp=datetime.now(timezone.utc),
            level="INFO",
            logger_name="x",
            message="m",
        )
        handler._write_to_file(rec)
        assert [item.message for item in handler.get_records()] == ["m"]

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


class TestLogRecordDataflowId:
    """Cover line 144: dataflow_id inclusion in LogRecord.to_dict()."""

    def test_to_dict_includes_dataflow_id_when_set(self) -> None:
        """Line 144: dataflow_id present when non-empty."""
        from datacoolie.logging.runtime.capture import LogRecord
        import datetime
        rec = LogRecord(
            level='INFO',
            message='test msg',
            dataflow_id='df-123',
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            logger_name='test',
        )
        d = rec.to_dict()
        assert d.get('dataflow_id') == 'df-123'
