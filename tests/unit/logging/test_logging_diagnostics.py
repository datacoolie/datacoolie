"""Focused checks for typed serializers and bounded logging diagnostics."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from datacoolie.core.secrets.provider import SecretStr
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.persistence.diagnostics import DiagnosticBudget, OMITTED
from datacoolie.logging.persistence.serialization import serialize_source_runtime
from datacoolie.logging.runtime.capture import CaptureHandler, LogRecord
from datacoolie.logging.runtime.manager import LogManager


def _source(**overrides):
    values = {
        "start_time": datetime(2026, 10, 4, tzinfo=timezone.utc),
        "end_time": None,
        "duration_seconds": None,
        "status": "succeeded",
        "message": None,
        "rows_read": None,
        "selection": None,
        "source_action": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_typed_serializer_preserves_nullable_members_and_rejects_missing_members():
    encoded = serialize_source_runtime(_source())
    assert encoded["message"] is None
    assert encoded["rows_read"] is None
    assert encoded["source_action"] is None

    missing = _source()
    del missing.source_action
    with pytest.raises(AttributeError):
        serialize_source_runtime(missing)


def test_shared_budget_normalizes_supported_scalars_and_bounds_utf8():
    budget = DiagnosticBudget(max_bytes=64)
    value = {
        "date": date(2026, 10, 4),
        "datetime": datetime(2026, 10, 4, tzinfo=timezone.utc),
        "time": time(12, 34),
        "decimal": Decimal("3.14"),
        "bytes": b"abc",
        "secret": SecretStr("hidden"),
    }
    encoded = budget.normalize(value, path="/attempts/0/source_action")
    assert encoded is OMITTED
    assert budget.issues == [
        {"field": "/attempts/0/source_action", "reason": "omitted"}
    ]

    budget = DiagnosticBudget(max_bytes=12)
    encoded = budget.normalize("é" * 100, path="/message")
    assert encoded != OMITTED
    assert len(encoded.encode("utf-8")) <= 10
    assert budget.issues == [{"field": "/message", "reason": "truncated"}]


def test_custom_objects_are_omitted_without_stringification():
    class Unsafe:
        def __str__(self):
            raise AssertionError("custom stringification must not run")

    budget = DiagnosticBudget()
    assert budget.normalize(Unsafe(), path="/source_action") is OMITTED
    assert budget.issues == [{"field": "/source_action", "reason": "invalid"}]


def test_capture_issue_markers_round_trip_and_reconfigure():
    record = LogRecord(
        timestamp=datetime(2026, 10, 4, tzinfo=timezone.utc),
        level="INFO",
        logger_name="test",
        message=None,
        diagnostic_issues=[{"field": "/msg", "reason": "omitted"}],
    )
    restored = LogRecord.from_dict(record.to_dict())
    assert restored.to_dict()["diagnostic_issues"] == record.to_dict()["diagnostic_issues"]

    handler = CaptureHandler(diagnostic_max_bytes=16)
    assert handler._retain_fallback(record)
    handler.reconfigure(
        level=handler.level,
        storage_mode="memory",
        formatter=handler.formatter,
        diagnostic_max_bytes=8,
    )
    assert handler.get_records()[0].diagnostic_issues
    handler.cleanup()


def test_log_config_and_manager_propagate_diagnostic_limit():
    assert LogConfig().diagnostic_max_bytes == 65_536
    assert LogConfig().max_record_bytes == 1_048_576
    manager = LogManager.get_instance()
    try:
        manager.configure(
            capture_logs=True,
            console_output=False,
            diagnostic_max_bytes=32,
            force=True,
        )
        assert manager.capture_handler is not None
        assert manager.capture_handler._diagnostic_max_bytes == 32
    finally:
        LogManager.reset()
