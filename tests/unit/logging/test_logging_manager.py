"""Tests for explicit Python logging configuration and context capture."""

from __future__ import annotations

import logging
import threading

import pytest

from datacoolie.logging.configuration.constants import StorageMode
from datacoolie.logging.runtime.context import DataflowContextFilter
from datacoolie.logging.runtime.manager import LogManager, _diagnostic_logger, get_logger


class TestDataflowContextFilter:
    def test_sets_dataflow_id_from_contextvar(self):
        from datacoolie.logging.runtime.context import clear_dataflow_id, set_dataflow_id

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

    def test_context_filter_attached_only_to_manager_owned_handlers(self):
        mgr = LogManager.get_instance()
        mgr.configure(force=True)
        owned_handlers = [mgr._capture_handler, mgr._console_handler]
        assert any(handler is not None for handler in owned_handlers)
        for handler in owned_handlers:
            if handler is not None:
                filter_types = [type(f) for f in handler.filters]
                assert DataflowContextFilter in filter_types

    def test_execution_context_appears_in_formatted_output(self):
        from datacoolie.logging.runtime.context import (
            clear_dataflow_context,
            set_dataflow_context,
        )

        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False, force=True)
        lgr = mgr.get_logger("datacoolie.test.ctx")

        # Capture what the Python formatter actually produces
        formatted_lines: list[str] = []
        original_emit = mgr.capture_handler.emit

        def spy_emit(record: logging.LogRecord) -> None:
            formatted_lines.append(mgr.capture_handler.format(record))
            original_emit(record)

        mgr.capture_handler.emit = spy_emit  # type: ignore[assignment]

        token = set_dataflow_context("df-fmt-test", "run-fmt-test")
        try:
            lgr.info("check format", extra={"event_name": "dataflow.started"})
        finally:
            clear_dataflow_context(token)

        assert any(
            "[df-fmt-test] [run-fmt-test:dataflow.started]" in line
            for line in formatted_lines
        )
        assert any(record.message == "check format" for record in mgr.capture_handler.get_records())



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

    def test_configure_keeps_owner_error_before_buffer_validation(self):
        mgr = LogManager.get_instance()
        owner = object()
        mgr.claim_capture(owner)

        with pytest.raises(RuntimeError, match="owns LogManager state"):
            mgr.configure(max_buffer_bytes=0)

        mgr.release_capture(owner)

    def test_configure_file_level_splits_handlers(self):
        """Capture handler uses file_level; console handler uses level."""
        mgr = LogManager.get_instance()
        mgr.configure(level="WARNING", file_level="DEBUG", capture_logs=True, console_output=True, force=True)
        assert mgr._capture_handler is not None
        assert mgr._console_handler is not None
        # Capture handler captures DEBUG+, console handler shows WARNING+
        assert mgr._capture_handler.level == logging.DEBUG
        assert mgr._console_handler.level == logging.WARNING

    def test_configure_file_level_defaults_to_level(self):
        """When file_level is omitted it mirrors the console level."""
        mgr = LogManager.get_instance()
        mgr.configure(level="ERROR", capture_logs=True, console_output=False, force=True)
        assert mgr._capture_handler is not None
        assert mgr._capture_handler.level == logging.ERROR

    def test_configure_no_capture(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=False)
        assert mgr.capture_handler is None

    @pytest.mark.parametrize(
        "storage_mode",
        [StorageMode.MEMORY.value, StorageMode.FILE.value],
    )
    def test_force_configure_preserves_pending_records(self, storage_mode):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False)
        lgr = mgr.get_logger("datacoolie.metadata.base")
        old_handler = mgr.capture_handler
        assert old_handler is not None
        lgr.debug("debug before driver")
        lgr.info("prefetch before driver")

        mgr.configure(
            capture_logs=True,
            file_level="INFO",
            storage_mode=storage_mode,
            console_output=False,
            force=True,
        )

        assert mgr.capture_handler is old_handler
        assert [r.message for r in mgr.capture_handler.get_records()] == [
            "prefetch before driver",
        ]

    def test_force_configure_preserves_concurrent_record_exactly_once(self):
        mgr = LogManager.get_instance()
        mgr.configure(
            capture_logs=True,
            file_level="DEBUG",
            storage_mode=StorageMode.MEMORY.value,
            console_output=False,
            force=True,
        )
        handler = mgr.capture_handler
        assert handler is not None
        lgr = mgr.get_logger("datacoolie.concurrent")
        entered = threading.Event()
        release = threading.Event()
        original_reconfigure = handler.reconfigure

        def paused_reconfigure(**kwargs):
            with handler.lock:
                entered.set()
                assert release.wait(timeout=5)
                return original_reconfigure(**kwargs)

        handler.reconfigure = paused_reconfigure
        configure_thread = threading.Thread(
            target=lambda: mgr.configure(
                capture_logs=True,
                file_level="DEBUG",
                storage_mode=StorageMode.FILE.value,
                console_output=False,
                force=True,
            )
        )
        configure_thread.start()
        assert entered.wait(timeout=5)

        emit_thread = threading.Thread(target=lambda: lgr.info("during reconfigure"))
        emit_thread.start()
        release.set()
        configure_thread.join(timeout=5)
        emit_thread.join(timeout=5)

        assert not configure_thread.is_alive()
        assert not emit_thread.is_alive()
        assert mgr.capture_handler is handler
        messages = [record.message for record in handler.get_records()]
        assert messages.count("during reconfigure") == 1

    def test_diagnostic_logger_uses_console_only(self, capsys):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=True, force=True)
        console_handler = mgr._console_handler
        capture_handler = mgr.capture_handler
        assert console_handler is not None
        assert capture_handler is not None
        assert console_handler in _diagnostic_logger.handlers
        assert capture_handler not in _diagnostic_logger.handlers

        _diagnostic_logger.warning("diagnostic only")

        assert "datacoolie.logging.internal" in capsys.readouterr().err
        assert not any(
            record.message == "diagnostic only"
            for record in capture_handler.get_records()
        )

        mgr.cleanup()
        assert console_handler not in _diagnostic_logger.handlers

    def test_get_logger(self):
        mgr = LogManager.get_instance()
        lgr = mgr.get_logger("datacoolie.test.child")
        assert isinstance(lgr, logging.Logger)
        assert lgr.name == "datacoolie.test.child"

    def test_external_logger_names_are_canonicalized(self):
        mgr = LogManager.get_instance()

        extension = mgr.get_logger("project.reader")
        canonical = mgr.get_logger("datacoolie.project.reader")

        assert extension is canonical
        assert extension.name == "datacoolie.project.reader"

    @pytest.mark.parametrize("value", [None, "", "   ", 123])
    def test_logger_name_must_be_non_empty_string(self, value):
        with pytest.raises(ValueError, match="logger name"):
            LogManager.get_instance().get_logger(value)

    def test_get_logger_does_not_auto_configure(self):
        mgr = LogManager.get_instance()
        assert mgr._configured is False
        mgr.get_logger("datacoolie.test.auto_config")
        assert mgr._configured is False

    def test_get_captured_logs(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False)
        lgr = mgr.get_logger("datacoolie.test.cap")
        lgr.info("captured msg")
        logs = mgr.get_captured_logs()
        assert "captured msg" in logs

    def test_clear_captured_logs(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False)
        lgr = mgr.get_logger("datacoolie.test.clr")
        lgr.info("msg")
        mgr.clear_captured_logs()
        assert mgr.get_captured_logs() == ""

    def test_get_captured_logs_no_handler(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=False)
        assert mgr.get_captured_logs() == ""

    def test_cleanup_allows_reconfiguration_without_force(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False)
        first_handler = mgr.capture_handler
        assert first_handler is not None

        mgr.cleanup()
        assert mgr._configured is False
        assert mgr.capture_handler is None

        mgr.configure(capture_logs=True, console_output=False)
        assert mgr._configured is True
        assert mgr.capture_handler is not None
        assert mgr.capture_handler is not first_handler

    def test_active_capture_rejects_unauthorized_mutation(self):
        mgr = LogManager.get_instance()
        owner = object()
        other = object()
        mgr.claim_capture(owner, console_color="never")
        try:
            logger = mgr.get_logger("datacoolie.owner_contract")
            logger.info("owned")
            with pytest.raises(RuntimeError, match="owns LogManager"):
                mgr.configure(force=True)
            with pytest.raises(RuntimeError, match="owns LogManager"):
                mgr.clear_captured_logs()
            with pytest.raises(RuntimeError, match="owns LogManager"):
                mgr.drain_captured_records(owner=other)
            with pytest.raises(RuntimeError, match="owns LogManager"):
                mgr.restore_captured_records([], owner=other)
            with pytest.raises(RuntimeError, match="owns LogManager"):
                mgr.cleanup()
            with pytest.raises(RuntimeError, match="owns LogManager"):
                LogManager.reset()

            # The owner can transfer records and release the session.
            assert [record.message for record in mgr.drain_captured_records(owner=owner)] == [
                "owned"
            ]
        finally:
            mgr.release_capture(owner)

    def test_cleanup_restores_host_logger_state(self):
        mgr = LogManager.get_instance()
        root = logging.getLogger("datacoolie")
        child = mgr.get_logger("datacoolie.host.restore")
        original_root = (root.level, root.propagate)
        original_child = child.level
        try:
            root.setLevel(logging.ERROR)
            root.propagate = True
            child.setLevel(logging.CRITICAL)
            mgr.configure(level="DEBUG", file_level="DEBUG", console_output=False)
            assert root.level == logging.DEBUG
            assert child.level == logging.DEBUG
            mgr.cleanup()
            assert (root.level, root.propagate) == (logging.ERROR, True)
            assert child.level == logging.CRITICAL
        finally:
            root.setLevel(original_root[0])
            root.propagate = original_root[1]
            child.setLevel(original_child)

    def test_cleanup_preserves_external_host_mutation(self):
        mgr = LogManager.get_instance()
        root = logging.getLogger("datacoolie")
        child = mgr.get_logger("datacoolie.host.external")
        original_root = (root.level, root.propagate)
        original_child = child.level
        try:
            mgr.configure(level="DEBUG", file_level="DEBUG", console_output=False)
            root.setLevel(logging.WARNING)
            root.propagate = True
            child.setLevel(logging.ERROR)
            mgr.cleanup()
            assert (root.level, root.propagate) == (logging.WARNING, True)
            assert child.level == logging.ERROR
        finally:
            root.setLevel(original_root[0])
            root.propagate = original_root[1]
            child.setLevel(original_child)

# ============================================================================
# get_logger (module-level convenience)
# ============================================================================


class TestGetLogger:
    def setup_method(self):
        LogManager.reset()

    def teardown_method(self):
        LogManager.reset()

    def test_returns_logger(self):
        lgr = get_logger("datacoolie.mymod")
        assert isinstance(lgr, logging.Logger)
        assert lgr.name == "datacoolie.mymod"


# ============================================================================
# Manager configuration edge cases


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
        child = mgr.get_logger("datacoolie.edge.level")
        mgr.configure(level="ERROR", force=True)
        assert child.level == logging.ERROR

    def test_get_logger_with_module_name_reused(self):
        mgr = LogManager.get_instance()
        l1 = mgr.get_logger("datacoolie.prefixed")
        l2 = mgr.get_logger("datacoolie.prefixed")
        assert l1 is l2

    def test_clear_captured_logs_no_handler(self):
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=False, force=True)
        mgr.clear_captured_logs()


# ============================================================================
# DataflowContextFilter
