"""Direct contract tests for complete logging record projections."""

from __future__ import annotations

from datetime import date, datetime, timezone

from datacoolie import __version__
from datacoolie.core.constants import DataFlowStatus
from datacoolie.logging.persistence.projection import (
    build_dataflow_entry,
    build_system_entry,
    flatten_job_runtime,
)
from datacoolie.logging.runtime.capture import LogRecord
from datacoolie.orchestration.runtime import JobRuntimeInfo
from datacoolie.sources.contracts import (
    SourceReadMode,
    SourceReadRequest,
    SourceReadSelection,
    SourceReadUpperOrigin,
)
from datacoolie.watermark.contracts import (
    WatermarkCandidate,
    WatermarkCandidateKind,
    WatermarkLoadStatus,
    WatermarkSaveStatus,
)
from datacoolie.watermark.runtime import WatermarkRuntimeInfo

from tests.unit.logging.support import make_dataflow, make_runtime


def _assert_header(entry: dict, record_type: str) -> None:
    assert list(entry)[:2] == ["log_schema_version", "_type"]
    assert entry["log_schema_version"] == 6
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
    assert entry["observed_at"] == observed_at.isoformat()
    assert entry["message"] is None
    assert entry["message_truncated"] is False


def test_dataflow_projection_owns_complete_header_and_nested_runtime():
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
    assert isinstance(entry["attempts"], list)
    assert isinstance(entry["metrics"], dict)
    assert entry["attempts"][0]["source"]["rows_read"] == 100
    assert entry["attempts"][0]["destination"]["rows_written"] == 100


def test_dataflow_projection_includes_native_read_and_watermark_evidence():
    runtime = make_runtime("df-1")
    runtime.read_request = SourceReadRequest(
        reference_time=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        watermark_start={"event_date": date(2026, 1, 1)},
    )
    runtime.attempts[0].source.selection = SourceReadSelection(
        mode=SourceReadMode.INCREMENTAL,
        lower={"event_date": date(2026, 1, 1)},
        upper={"event_date": date(2026, 2, 1)},
        upper_origin=SourceReadUpperOrigin.GENERATED,
    )
    runtime.attempts[0].watermark = WatermarkRuntimeInfo(
        load_status=WatermarkLoadStatus.LOADED,
        loaded_watermark={"event_date": date(2025, 12, 31)},
        candidate=WatermarkCandidate(
            values={"event_date": date(2026, 1, 31)},
            kind=WatermarkCandidateKind.OBSERVED_MAX,
        ),
        watermark_to_save={"event_date": date(2026, 1, 31)},
        save_status=WatermarkSaveStatus.SAVED,
        saved_watermark={"event_date": date(2026, 1, 31)},
    )

    entry = build_dataflow_entry(
        make_dataflow("df-1"),
        runtime,
        job_info=JobRuntimeInfo(job_id="job-1"),
    )

    assert entry["read_request"]["reference_time"] == "2026-01-02T03:04:05+00:00"
    assert entry["read_request"]["watermark_end"] is None
    assert entry["read_request"]["watermark_start"] == {
        "event_date": {"__date__": "2026-01-01"}
    }
    selection = entry["attempts"][0]["source"]["selection"]
    assert selection["upper_origin"] == "generated"
    assert selection["upper"] == {"event_date": {"__date__": "2026-02-01"}}
    watermark = entry["attempts"][0]["watermark"]
    assert watermark["candidate"]["kind"] == "observed_max"
    assert watermark["saved_watermark"] == {
        "event_date": {"__date__": "2026-01-31"}
    }


def test_dataflow_projection_uses_one_terminal_message_and_no_flat_phase_fields():
    runtime = make_runtime("df-1")
    runtime.status = DataFlowStatus.SKIPPED.value
    runtime.message = "source connection 'source' is inactive"
    runtime.attempts[0].source.message = "source phase detail"
    entry = build_dataflow_entry(
        make_dataflow("df-1"), runtime, job_info=JobRuntimeInfo(job_id="job-1")
    )
    assert entry["status"] == "skipped"
    assert entry["message"] == runtime.message
    assert entry["attempts"][0]["source"]["message"] == "source phase detail"
    assert "source_start_time" not in entry
    assert "source_message" not in entry
    assert "destination_message" not in entry
    assert "watermark_checkpoint_status" not in entry


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
        attempt_number=2,
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
    assert entry["attempt_number"] == 2
    assert entry["exc_info"] == "Traceback: failed"


def test_system_projection_omits_attempt_correlation_outside_attempts():
    record = LogRecord(
        timestamp=datetime(2026, 9, 14, 10, 30, tzinfo=timezone.utc),
        level="INFO",
        logger_name="datacoolie.test",
        message="ready",
    )

    entry = build_system_entry(
        record,
        job_id=None,
        job_num=None,
        job_index=None,
    )

    assert "attempt_number" not in entry


def test_system_projection_omits_invalid_attempt_correlation():
    record = LogRecord(
        timestamp=datetime(2026, 9, 14, 10, 30, tzinfo=timezone.utc),
        level="INFO",
        logger_name="datacoolie.test",
        message="ready",
        attempt_number=True,
    )

    entry = build_system_entry(
        record,
        job_id=None,
        job_num=None,
        job_index=None,
    )

    assert "attempt_number" not in entry
