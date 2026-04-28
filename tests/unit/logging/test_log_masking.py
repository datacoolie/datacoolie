"""Tests for global log masking via SensitiveValueFilter."""

from __future__ import annotations

import logging
import threading

import pytest

from datacoolie.core.secret_provider import (
    _resolved_values,
    _resolved_values_lock,
    register_secret_values,
)
from datacoolie.logging.base import LogManager, SensitiveValueFilter


@pytest.fixture(autouse=True)
def _clear_global_values():
    """Clear the global resolved-values set before and after each test."""
    with _resolved_values_lock:
        _resolved_values.clear()
    yield
    with _resolved_values_lock:
        _resolved_values.clear()


@pytest.fixture()
def _reset_log_manager():
    """Reset LogManager singleton before and after each test."""
    LogManager.reset()
    yield
    LogManager.reset()


class TestSensitiveValueFilter:
    def test_no_secrets_passes_through(self):
        f = SensitiveValueFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello world", args=None, exc_info=None,
        )
        assert f.filter(record) is True
        assert record.getMessage() == "hello world"

    def test_masks_registered_secret(self):
        register_secret_values("s3cret!")
        f = SensitiveValueFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="password is s3cret! in log", args=None, exc_info=None,
        )
        f.filter(record)
        assert "s3cret!" not in record.getMessage()
        assert "***" in record.getMessage()

    def test_masks_multiple_secrets(self):
        register_secret_values("alpha", "beta")
        f = SensitiveValueFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="alpha and beta are secrets", args=None, exc_info=None,
        )
        f.filter(record)
        text = record.getMessage()
        assert "alpha" not in text
        assert "beta" not in text
        assert text.count("***") == 2

    def test_no_false_positive(self):
        register_secret_values("s3cret!")
        f = SensitiveValueFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="no secrets here", args=None, exc_info=None,
        )
        f.filter(record)
        assert record.getMessage() == "no secrets here"

    def test_masks_with_format_args(self):
        register_secret_values("mypassword")
        f = SensitiveValueFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Connecting with password=%s", args=("mypassword",), exc_info=None,
        )
        f.filter(record)
        text = record.getMessage()
        assert "mypassword" not in text
        assert "***" in text


class TestLogManagerIntegration:
    def test_filter_attached_on_configure(self, _reset_log_manager):
        mgr = LogManager.get_instance()
        mgr.configure(force=True)
        assert mgr._sensitive_filter is not None

    def test_secrets_masked_in_captured_logs(self, _reset_log_manager):
        register_secret_values("TOPSECRET")
        mgr = LogManager.get_instance()
        mgr.configure(capture_logs=True, console_output=False, force=True)
        lgr = mgr.get_logger("test.masking")
        lgr.info("The password is TOPSECRET")
        captured = mgr.get_captured_logs()
        assert "TOPSECRET" not in captured
        assert "***" in captured
