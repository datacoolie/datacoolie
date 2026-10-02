"""Direct contract tests for complete logging record projections."""

from __future__ import annotations

from datetime import datetime, timezone

from datacoolie import __version__
from datacoolie.core.models.runtime import JobRuntimeInfo
from datacoolie.logging.runtime.capture import LogRecord
from datacoolie.logging.persistence.projection import (
    build_dataflow_entry,
    build_system_entry,
    flatten_job_runtime,
)

from tests.unit.logging.support import make_dataflow, make_runtime


def _assert_header(entry: dict, record_type: str) -> None:
    assert list(entry)[:2] == ["log_schema_version", "_type"]
    assert entry["log_schema_version"] == 4
    assert entry["datacoolie_version"] == __version__
    assert entry["_type"] == record_type


def test_job_projection_owns_complete_header_and_optional_observation_time():
    observed_at = datetime(2026, 9, 14, 10, 30, tzinfo=timezone.utc)
    entry = flatten_job_runtime(
        JobRuntimeInfo(job_id="job-1"),
        observed_at=observed_at,
        log_session_id="session-1",
    )

    _assert_header(entry, "job_run_log")
    assert entry["job_id"] == "job-1"
    assert entry["log_session_id"] == "session-1"
    assert entry["observed_at"] == observed_at
    assert entry["message"] is None
    assert entry["message_truncated"] is False


def test_dataflow_projection_owns_complete_header():
    entry = build_dataflow_entry(
        make_dataflow("df-1"),
        make_runtime("df-1"),
        job_info=JobRuntimeInfo(job_id="job-1"),
        log_session_id="session-1",
    )

    _assert_header(entry, "dataflow_run_log")
    assert entry["job_id"] == "job-1"
    assert entry["log_session_id"] == "session-1"
    assert entry["dataflow_id"] == "df-1"
    assert entry["message"] is None


def test_dataflow_projection_uses_one_terminal_message():
    runtime = make_runtime("df-1")
    runtime.status = "skipped"
    runtime.message = "source connection 'source' is inactive"
    runtime.source.message = "source phase detail"
    runtime.transform.message = "transform phase detail"
    runtime.destination.message = "destination phase detail"
    entry = build_dataflow_entry(
        make_dataflow("df-1"), runtime, job_info=JobRuntimeInfo(job_id="job-1")
    )
    assert entry["status"] == "skipped"
    assert entry["message"] == runtime.message
    assert entry["source_message"] == runtime.source.message
    assert entry["transform_message"] == runtime.transform.message
    assert entry["destination_message"] == runtime.destination.message
    assert "skip_reason" not in entry
    assert "error_message" not in entry


def test_system_projection_owns_complete_header_and_captured_context():
    record = LogRecord(
        timestamp=datetime(2026, 9, 14, 10, 30, tzinfo=timezone.utc),
        level="INFO",
        logger_name="datacoolie.test",
        message="ready",
        exc_info="Traceback: failed",
        dataflow_id="df-1",
        dataflow_run_id="run-1",
        event_name="dataflow.finished",
    )
    entry = build_system_entry(
        record,
        job_id="job-1",
        job_num=2,
        job_index=1,
        log_session_id="session-1",
    )

    _assert_header(entry, "system_log")
    assert entry["job_id"] == "job-1"
    assert entry["log_session_id"] == "session-1"
    assert entry["dataflow_id"] == "df-1"
    assert entry["dataflow_run_id"] == "run-1"
    assert entry["event_name"] == "dataflow.finished"
    assert entry["exc_info"] == "Traceback: failed"
