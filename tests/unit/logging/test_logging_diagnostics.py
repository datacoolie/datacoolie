"""Tests for failure-boundary logging protection."""

from __future__ import annotations

import logging

import pytest

from datacoolie.logging.runtime.diagnostics import emit_safely


class _RaisingLogger:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def log(self, *_args, **_kwargs):
        raise self.error


def test_emit_safely_returns_handler_error_without_replacing_business_flow():
    error = RuntimeError("handler failed")

    assert (
        emit_safely(_RaisingLogger(error), logging.ERROR, "diagnostic")
        is error
    )


def test_emit_safely_can_capture_interrupt_only_at_explicit_boundary():
    error = KeyboardInterrupt("handler interrupted")

    with pytest.raises(KeyboardInterrupt, match="handler interrupted"):
        emit_safely(_RaisingLogger(error), logging.ERROR, "diagnostic")

    assert (
        emit_safely(
            _RaisingLogger(error),
            logging.ERROR,
            "diagnostic",
            catch_base=True,
        )
        is error
    )


def test_emit_safely_preserves_original_caller_location(caplog):
    target = logging.getLogger("datacoolie.tests.diagnostics")

    with caplog.at_level(logging.INFO, logger=target.name):
        def emit_from_test():
            emit_safely(target, logging.INFO, "diagnostic")

        emit_from_test()

    record = caplog.records[-1]
    assert record.funcName == "emit_from_test"

