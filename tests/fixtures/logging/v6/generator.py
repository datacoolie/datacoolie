"""Generate deterministic v6 logging fixtures from the production projectors.

The checked-in JSON Lines are intentionally produced through the same
projection and writer encoding used by the logging implementation.  Tests can
regenerate them in a temporary directory and compare both payloads and the
manifest without relying on hand-authored JSON that could drift from the
producer.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from datacoolie.core.constants import DataFlowStatus, ExecutionType
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie import __version__
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.logging.persistence.projection import (
    build_dataflow_entry,
    build_system_entry,
    flatten_job_runtime,
)
from datacoolie.logging.persistence.writer import encode_json_line
from datacoolie.logging.runtime.capture import LogRecord
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.execution_logger import ExecutionLogger
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.orchestration.execution.runtime import (
    AttemptRuntimeInfo,
    DataFlowRuntimeInfo,
)
from datacoolie.orchestration.runtime import JobRuntimeInfo
from datacoolie.sources.contracts import (
    SourceReadMode,
    SourceReadRange,
    SourceReadRequest,
    SourceReadSelection,
    SourceReadUpperOrigin,
)
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.transformers.runtime import TransformRuntimeInfo
from datacoolie.watermark.contracts import (
    WatermarkCandidate,
    WatermarkCandidateKind,
    WatermarkLoadStatus,
    WatermarkSaveStatus,
)
from datacoolie.watermark.runtime import WatermarkRuntimeInfo

# Direct script execution puts the fixture directory, rather than the test
# package root, on ``sys.path``.  Keep the generator usable both as a module
# and as the documented standalone producer command.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from tests.unit.logging.support import make_dataflow


FIXTURE_VERSION = 6
GENERATOR_VERSION = "t6-2026-10-04"
FIXTURE_PROFILE = "canonical-execution-v6-t6-terminal-cases"
SESSION_ID = "fixture-v6-session"
JOB_ID = "fixture-v6-job"
_UTC = timezone.utc
_BASE = datetime(2026, 10, 4, 12, 0, tzinfo=_UTC)


def _typed_values(day: date) -> dict[str, Any]:
    """Return every scalar supported by the canonical watermark codec."""

    return {
        "event_date": day,
        "event_time": time(12, 34, 56, 789000),
        "event_ts": datetime(day.year, day.month, day.day, 12, 34, 56, tzinfo=_UTC),
        "amount": Decimal("12.50"),
        "payload": b"v6\x00\xff",
        "nested": {"observed": date(day.year, day.month, day.day)},
        "items": [day, Decimal("0.25")],
    }


def _source(
    *,
    status: str,
    rows_read: int | None,
    selection: SourceReadSelection | None = None,
    action: dict[str, Any] | None = None,
    start: datetime = _BASE,
) -> SourceRuntimeInfo:
    return SourceRuntimeInfo(
        start_time=start,
        end_time=start + timedelta(seconds=4),
        status=status,
        rows_read=rows_read,
        source_action=action or {},
        selection=selection,
    )


def _destination(
    *,
    status: str,
    start: datetime = _BASE + timedelta(seconds=8),
    rows_written: int | None = None,
    files_added: int | None = None,
    files_removed: int | None = None,
    bytes_added: int | None = None,
    bytes_removed: int | None = None,
    operation_type: str | None = None,
) -> DestinationRuntimeInfo:
    return DestinationRuntimeInfo(
        start_time=start,
        end_time=start + timedelta(seconds=5),
        status=status,
        operation_type=operation_type,
        rows_written=rows_written,
        rows_inserted=rows_written,
        rows_updated=0 if rows_written is not None else None,
        rows_deleted=0 if rows_written is not None else None,
        files_added=files_added,
        files_removed=files_removed,
        bytes_added=bytes_added,
        bytes_removed=bytes_removed,
        operation_details=[{"operation": "write", "mode": "fixture"}]
        if operation_type is None
        else [{"operation": "compact", "mode": "fixture"}],
    )


def _watermark(values: dict[str, Any]) -> WatermarkRuntimeInfo:
    return WatermarkRuntimeInfo(
        load_status=WatermarkLoadStatus.LOADED,
        loaded_watermark=values,
        merge_base={"event_date": values["event_date"]},
        candidate=WatermarkCandidate(
            values=values,
            kind=WatermarkCandidateKind.OBSERVED_MAX,
        ),
        watermark_to_save=values,
        save_status=WatermarkSaveStatus.SAVED,
        saved_watermark=values,
    )


def _success_runtime() -> DataFlowRuntimeInfo:
    range_start = datetime(2026, 10, 1, tzinfo=_UTC)
    range_end = datetime(2026, 10, 4, tzinfo=_UTC)
    selection = SourceReadSelection(
        mode=SourceReadMode.RANGE,
        lower={"event_ts": range_start},
        upper={"event_ts": range_end},
        lower_operator=">=",
        upper_operator="<",
        upper_origin=SourceReadUpperOrigin.EXPLICIT,
    )
    typed = _typed_values(date(2026, 10, 4))
    request = SourceReadRequest(
        reference_time=_BASE,
        watermark_start=None,
        watermark_end=None,
        watermark_start_operator=None,
        watermark_end_operator=None,
        read_range=SourceReadRange(
            column="event_ts",
            start=range_start,
            end=range_end,
        ),
    )
    source = _source(
        status=DataFlowStatus.SUCCEEDED.value,
        rows_read=42,
        selection=selection,
        action={"query": "SELECT * FROM fixture", "observed": Decimal("3.14")},
    )
    transform = TransformRuntimeInfo(
        start_time=_BASE + timedelta(seconds=4),
        end_time=_BASE + timedelta(seconds=8),
        status=DataFlowStatus.SUCCEEDED.value,
        transformers_applied=["normalize", "mask"],
    )
    destination = _destination(
        status=DataFlowStatus.SUCCEEDED.value,
        rows_written=40,
        files_added=2,
        files_removed=1,
        bytes_added=2048,
        bytes_removed=512,
    )
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=_BASE,
        end_time=_BASE + timedelta(seconds=13),
        status=DataFlowStatus.SUCCEEDED.value,
        source=source,
        transform=transform,
        destination=destination,
        watermark=_watermark(typed),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-success-run",
        dataflow_id="fixture-v6-success",
        operation_type=ExecutionType.ETL.value,
        start_time=_BASE,
        end_time=_BASE + timedelta(seconds=13),
        status=DataFlowStatus.SUCCEEDED.value,
        read_request=request,
        attempts=[attempt],
    )


def _skipped_runtime() -> DataFlowRuntimeInfo:
    request = SourceReadRequest(
        reference_time=_BASE + timedelta(days=1),
        watermark_start=_typed_values(date(2026, 10, 5)),
        watermark_end=_typed_values(date(2026, 10, 6)),
        watermark_start_operator=">=",
        watermark_end_operator="<",
    )
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=_BASE + timedelta(days=1),
        end_time=_BASE + timedelta(days=1, seconds=1),
        status=DataFlowStatus.SKIPPED.value,
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=0,
            start=_BASE + timedelta(days=1),
        ),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-skipped-run",
        dataflow_id="fixture-v6-skipped",
        operation_type=ExecutionType.ETL.value,
        start_time=_BASE + timedelta(days=1),
        end_time=_BASE + timedelta(days=1, seconds=1),
        status=DataFlowStatus.SKIPPED.value,
        message="source inactive",
        read_request=request,
        attempts=[attempt],
    )


def _failed_runtime() -> DataFlowRuntimeInfo:
    first = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=_BASE + timedelta(days=2),
        end_time=_BASE + timedelta(days=2, seconds=2),
        status=DataFlowStatus.FAILED.value,
        message="transient destination failure",
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=9,
            start=_BASE + timedelta(days=2),
        ),
    )
    final = AttemptRuntimeInfo(
        attempt_number=2,
        start_time=_BASE + timedelta(days=2, seconds=3),
        end_time=_BASE + timedelta(days=2, seconds=4),
        status=DataFlowStatus.FAILED.value,
        message="terminal destination failure",
        source=_source(
            status=DataFlowStatus.FAILED.value,
            rows_read=None,
            start=_BASE + timedelta(days=2, seconds=3),
        ),
        destination=_destination(
            status=DataFlowStatus.FAILED.value,
            start=_BASE + timedelta(days=2, seconds=3),
            rows_written=None,
        ),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-failed-run",
        dataflow_id="fixture-v6-failed",
        operation_type=ExecutionType.ETL.value,
        start_time=_BASE + timedelta(days=2),
        end_time=_BASE + timedelta(days=2, seconds=4),
        status=DataFlowStatus.FAILED.value,
        message="terminal destination failure",
        attempts=[first, final],
    )


def _empty_runtime() -> DataFlowRuntimeInfo:
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-empty-run",
        dataflow_id="fixture-v6-empty",
        operation_type=ExecutionType.ETL.value,
        start_time=_BASE + timedelta(days=3),
        end_time=_BASE + timedelta(days=3, seconds=1),
        status=DataFlowStatus.SKIPPED.value,
        message="no attempt after preflight",
        attempts=[],
    )


def _generated_selection_runtime() -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=4)
    lower = {"event_date": date(2026, 10, 8)}
    upper = {"event_date": date(2026, 10, 9)}
    request = SourceReadRequest(
        reference_time=start,
        watermark_start=lower,
        watermark_end=upper,
        watermark_start_operator=">=",
        watermark_end_operator="<",
    )
    selection = SourceReadSelection(
        mode=SourceReadMode.INCREMENTAL,
        lower=lower,
        upper=upper,
        lower_operator=">=",
        upper_operator="<",
        upper_origin=SourceReadUpperOrigin.GENERATED,
    )
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=6),
        status=DataFlowStatus.SUCCEEDED.value,
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=7,
            selection=selection,
            start=start,
        ),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-generated-selection-run",
        dataflow_id="fixture-v6-generated-selection",
        operation_type=ExecutionType.ETL.value,
        start_time=start,
        end_time=start + timedelta(seconds=6),
        status=DataFlowStatus.SUCCEEDED.value,
        read_request=request,
        attempts=[attempt],
    )


def _retry_success_runtime() -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=5)
    first = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=2),
        status=DataFlowStatus.FAILED.value,
        message="transient destination failure",
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=20,
            start=start,
        ),
        destination=_destination(
            status=DataFlowStatus.FAILED.value,
            start=start,
            rows_written=None,
        ),
    )
    final = AttemptRuntimeInfo(
        attempt_number=2,
        start_time=start + timedelta(seconds=3),
        end_time=start + timedelta(seconds=10),
        status=DataFlowStatus.SUCCEEDED.value,
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=20,
            start=start + timedelta(seconds=3),
        ),
        transform=TransformRuntimeInfo(
            start_time=start + timedelta(seconds=5),
            end_time=start + timedelta(seconds=7),
            status=DataFlowStatus.SUCCEEDED.value,
            transformers_applied=["normalize"],
        ),
        destination=_destination(
            status=DataFlowStatus.SUCCEEDED.value,
            start=start + timedelta(seconds=7),
            rows_written=18,
            files_added=1,
            files_removed=0,
            bytes_added=900,
            bytes_removed=0,
        ),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-retry-success-run",
        dataflow_id="fixture-v6-retry-success",
        operation_type=ExecutionType.ETL.value,
        start_time=start,
        end_time=start + timedelta(seconds=10),
        status=DataFlowStatus.SUCCEEDED.value,
        attempts=[first, final],
    )


def _transformed_empty_runtime() -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=6)
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=9),
        status=DataFlowStatus.SUCCEEDED.value,
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=10,
            start=start,
        ),
        transform=TransformRuntimeInfo(
            start_time=start + timedelta(seconds=4),
            end_time=start + timedelta(seconds=6),
            status=DataFlowStatus.SUCCEEDED.value,
            transformers_applied=["filter_all"],
        ),
        destination=_destination(
            status=DataFlowStatus.SUCCEEDED.value,
            start=start + timedelta(seconds=6),
            rows_written=0,
            files_added=0,
            files_removed=0,
            bytes_added=0,
            bytes_removed=0,
        ),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-transformed-empty-run",
        dataflow_id="fixture-v6-transformed-empty",
        operation_type=ExecutionType.ETL.value,
        start_time=start,
        end_time=start + timedelta(seconds=9),
        status=DataFlowStatus.SUCCEEDED.value,
        attempts=[attempt],
    )


def _maintenance_runtime() -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=7)
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=3),
        status=DataFlowStatus.SUCCEEDED.value,
        destination=DestinationRuntimeInfo(
            start_time=start,
            end_time=start + timedelta(seconds=3),
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type=ExecutionType.MAINTENANCE.value,
            files_added=1,
            files_removed=3,
            bytes_added=100,
            bytes_removed=500,
            operation_details=[{"operation": "compact", "mode": "fixture"}],
        ),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-maintenance-run",
        dataflow_id="fixture-v6-maintenance",
        operation_type=ExecutionType.MAINTENANCE.value,
        start_time=start,
        end_time=start + timedelta(seconds=3),
        status=DataFlowStatus.SUCCEEDED.value,
        attempts=[attempt],
    )


def _replay_no_attempt_runtime(
    *,
    dataflow_id: str,
    run_id: str,
    status: str,
    message: str,
) -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=8)
    return DataFlowRuntimeInfo(
        dataflow_run_id=run_id,
        dataflow_id=dataflow_id,
        operation_type=ExecutionType.REPLAY.value,
        start_time=start,
        end_time=start + timedelta(seconds=1),
        status=status,
        message=message,
        attempts=[],
    )


def _replay_chunk_runtime(
    *,
    dataflow_id: str,
    run_id: str,
    status: str,
    message: str | None = None,
    rows_read: int | None = 0,
    rows_written: int | None = 0,
    start: datetime = _BASE + timedelta(days=9),
) -> DataFlowRuntimeInfo:
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=2),
        status=status,
        message=message,
        source=_source(
            status=status,
            rows_read=rows_read,
            start=start,
        ),
        destination=(
            _destination(
                status=status,
                start=start,
                rows_written=rows_written,
                files_added=1 if rows_written else 0,
                files_removed=0,
                bytes_added=100 if rows_written else 0,
                bytes_removed=0,
            )
            if rows_written is not None and rows_written > 0
            else None
        ),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id=run_id,
        dataflow_id=dataflow_id,
        operation_type=ExecutionType.REPLAY.value,
        start_time=start,
        end_time=start + timedelta(seconds=2),
        status=status,
        message=message,
        attempts=[attempt],
    )


def _sparse_metrics_runtime() -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=10)
    destination = DestinationRuntimeInfo(
        start_time=start + timedelta(seconds=4),
        end_time=start + timedelta(seconds=6),
        status=DataFlowStatus.SUCCEEDED.value,
        rows_written=None,
        rows_inserted=4,
        rows_updated=None,
        rows_deleted=0,
        files_added=None,
        files_removed=1,
        bytes_added=None,
        bytes_removed=16,
    )
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=6),
        status=DataFlowStatus.SUCCEEDED.value,
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=12,
            start=start,
        ),
        destination=destination,
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-sparse-metrics-run",
        dataflow_id="fixture-v6-sparse-metrics",
        operation_type=ExecutionType.ETL.value,
        start_time=start,
        end_time=start + timedelta(seconds=6),
        status=DataFlowStatus.SUCCEEDED.value,
        attempts=[attempt],
    )


def _operation_known_zero_runtime() -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=11)
    destination = _destination(
        status=DataFlowStatus.SUCCEEDED.value,
        start=start + timedelta(seconds=4),
        rows_written=3,
        files_added=1,
        files_removed=0,
        bytes_added=100,
        bytes_removed=0,
        operation_type="append",
    )
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=9),
        status=DataFlowStatus.SUCCEEDED.value,
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=3,
            start=start,
        ),
        destination=destination,
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-operation-known-zero-run",
        dataflow_id="fixture-v6-operation-known-zero",
        operation_type=ExecutionType.ETL.value,
        start_time=start,
        end_time=start + timedelta(seconds=9),
        status=DataFlowStatus.SUCCEEDED.value,
        attempts=[attempt],
    )


def _capture_failure_runtime() -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=12)
    destination = _destination(
        status=DataFlowStatus.SUCCEEDED.value,
        start=start + timedelta(seconds=4),
        rows_written=11,
        files_added=1,
        files_removed=0,
        bytes_added=256,
        bytes_removed=0,
    )
    destination.operation_details = [
        {"operation": "write", "acknowledged": True},
    ]
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=9),
        status=DataFlowStatus.SUCCEEDED.value,
        message="capture failed after successful return",
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=11,
            start=start,
        ),
        destination=destination,
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-capture-failure-run",
        dataflow_id="fixture-v6-capture-failure",
        operation_type=ExecutionType.ETL.value,
        start_time=start,
        end_time=start + timedelta(seconds=9),
        status=DataFlowStatus.SUCCEEDED.value,
        message="capture failed after successful return",
        attempts=[attempt],
    )


def _adapter_raise_runtime() -> DataFlowRuntimeInfo:
    start = _BASE + timedelta(days=13)
    destination = _destination(
        status=DataFlowStatus.FAILED.value,
        start=start + timedelta(seconds=4),
        rows_written=11,
        files_added=1,
        files_removed=0,
        bytes_added=256,
        bytes_removed=0,
    )
    destination.operation_details = [
        {"operation": "write", "physical_write": "acknowledged", "adapter": "raised"},
    ]
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=9),
        status=DataFlowStatus.FAILED.value,
        message="adapter raised after physical write",
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=11,
            start=start,
        ),
        destination=destination,
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id="fixture-v6-adapter-raise-run",
        dataflow_id="fixture-v6-adapter-raise",
        operation_type=ExecutionType.ETL.value,
        start_time=start,
        end_time=start + timedelta(seconds=9),
        status=DataFlowStatus.FAILED.value,
        message="adapter raised after physical write",
        attempts=[attempt],
    )


def _diagnostic_runtime(kind: str) -> DataFlowRuntimeInfo:
    """Return producer inputs that exercise the v6 diagnostic markers."""

    start = _BASE + timedelta(days=15)
    if kind == "oversized":
        message = "root-" + ("診断 detail " * 10_000)
        attempt = AttemptRuntimeInfo(
            attempt_number=1,
            start_time=start,
            end_time=start + timedelta(seconds=3),
            status=DataFlowStatus.FAILED.value,
            message="attempt-" + ("retry detail " * 10_000),
            source=_source(
                status=DataFlowStatus.FAILED.value,
                rows_read=None,
                action={"query": "SELECT 1", "detail": "source-" + ("x" * 70_000)},
                start=start,
            ),
            transform=TransformRuntimeInfo(
                start_time=start,
                end_time=start + timedelta(seconds=1),
                status=DataFlowStatus.FAILED.value,
                message="phase-" + ("transform detail " * 10_000),
            ),
            destination=_destination(
                status=DataFlowStatus.FAILED.value,
                start=start + timedelta(seconds=1),
            ),
        )
        return DataFlowRuntimeInfo(
            dataflow_run_id="fixture-v6-diagnostic-oversized-run",
            dataflow_id="fixture-v6-diagnostic-oversized",
            operation_type=ExecutionType.ETL.value,
            start_time=start,
            end_time=start + timedelta(seconds=3),
            status=DataFlowStatus.FAILED.value,
            message=message,
            attempts=[attempt],
        )

    action: dict[str, Any]
    if kind == "invalid":
        action = {"unsupported": object()}
        run_id = "fixture-v6-diagnostic-invalid-run"
        dataflow_id = "fixture-v6-diagnostic-invalid"
    elif kind == "deep":
        action = {}
        current = action
        for index in range(20):
            current["level"] = {}
            current = current["level"]
        current["leaf"] = "too deep"
        run_id = "fixture-v6-diagnostic-deep-run"
        dataflow_id = "fixture-v6-diagnostic-deep"
    elif kind == "cycle":
        action = {}
        action["self"] = action
        run_id = "fixture-v6-diagnostic-cycle-run"
        dataflow_id = "fixture-v6-diagnostic-cycle"
    else:
        raise ValueError(f"Unsupported diagnostic fixture kind: {kind}")
    attempt = AttemptRuntimeInfo(
        attempt_number=1,
        start_time=start,
        end_time=start + timedelta(seconds=1),
        status=DataFlowStatus.SUCCEEDED.value,
        source=_source(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=1,
            action=action,
            start=start,
        ),
    )
    return DataFlowRuntimeInfo(
        dataflow_run_id=run_id,
        dataflow_id=dataflow_id,
        operation_type=ExecutionType.ETL.value,
        start_time=start,
        end_time=start + timedelta(seconds=1),
        status=DataFlowStatus.SUCCEEDED.value,
        attempts=[attempt],
    )


def _job() -> JobRuntimeInfo:
    return JobRuntimeInfo(
        job_id=JOB_ID,
        job_num=7,
        job_index=2,
        workspace_id="fixture-workspace",
        stages=["bronze", "silver"],
        engine_name="fixture-engine",
        platform_name="fixture-platform",
        metadata_provider_name="fixture-metadata",
        watermark_manager_name="fixture-watermark",
        start_time=_BASE - timedelta(minutes=1),
        end_time=_BASE + timedelta(days=2, seconds=5),
        status=DataFlowStatus.SUCCEEDED.value,
        total_dataflows=4,
        total_succeeded=2,
        total_failed=1,
        total_skipped=1,
        total_running=None,
        total_pending=None,
        total_rows_read=51,
        total_rows_written=40,
        total_rows_inserted=40,
        total_rows_updated=0,
        total_rows_deleted=0,
        total_files_added=2,
        total_files_removed=1,
        total_bytes_added=2048,
        total_bytes_removed=512,
        operation_types=["etl", "dry_run"],
    )


def _records() -> dict[str, list[dict[str, Any]]]:
    job = _job()
    dataflow_success = make_dataflow("fixture-v6-success")
    dataflow_skipped = make_dataflow("fixture-v6-skipped")
    dataflow_failed = make_dataflow("fixture-v6-failed")
    dataflow_empty = make_dataflow("fixture-v6-empty")
    streams = {
        "job_snapshot.json": [
            flatten_job_runtime(
                job,
                observed_at=_BASE + timedelta(days=2, seconds=6),
                log_session_id=SESSION_ID,
            )
        ],
        "dataflow_snapshot.json": [
            build_dataflow_entry(
                dataflow_success,
                _success_runtime(),
                job_info=job,
                log_session_id=SESSION_ID,
            ),
            build_dataflow_entry(
                dataflow_skipped,
                _skipped_runtime(),
                job_info=job,
                log_session_id=SESSION_ID,
            ),
            build_dataflow_entry(
                dataflow_empty,
                _empty_runtime(),
                job_info=job,
                log_session_id=SESSION_ID,
            ),
        ],
        "dataflow_part_00000001.json": [
            build_dataflow_entry(
                dataflow_failed,
                _failed_runtime(),
                job_info=job,
                log_session_id=SESSION_ID,
            )
        ],
        "system_snapshot.json": [
            build_system_entry(
                LogRecord(
                    timestamp=_BASE,
                    level="INFO",
                    logger_name="fixture.system",
                    message="fixture ready",
                    dataflow_id="fixture-v6-success",
                    dataflow_run_id="fixture-v6-success-run",
                    event_name="dataflow.finished",
                ),
                job_id=JOB_ID,
                job_num=7,
                job_index=2,
                log_session_id=SESSION_ID,
            )
        ],
        "system_part_00000001.json": [
            build_system_entry(
                LogRecord(
                    timestamp=_BASE + timedelta(seconds=1),
                    level="WARNING",
                    logger_name="fixture.system",
                    message="retry scheduled",
                    dataflow_id="fixture-v6-failed",
                    dataflow_run_id="fixture-v6-failed-run",
                    event_name="retry.scheduled",
                    attempt_number=2,
                ),
                job_id=JOB_ID,
                job_num=7,
                job_index=2,
                log_session_id=SESSION_ID,
            )
        ],
    }
    terminal_cases = [
        _generated_selection_runtime(), _retry_success_runtime(),
        _transformed_empty_runtime(), _maintenance_runtime(),
        _sparse_metrics_runtime(), _operation_known_zero_runtime(),
        _capture_failure_runtime(), _adapter_raise_runtime(),
        _replay_no_attempt_runtime(dataflow_id="replay-inactive", run_id="inactive-run",
            status="skipped", message="Inactive dataflow"),
        _replay_no_attempt_runtime(dataflow_id="replay-preparation", run_id="preparation-run",
            status="failed", message="Invalid range"),
        _replay_chunk_runtime(dataflow_id="replay-chunks", run_id="chunk-success",
            status="succeeded", rows_read=2, rows_written=2),
        _replay_chunk_runtime(dataflow_id="replay-chunks", run_id="chunk-skipped", status="skipped"),
        _replay_chunk_runtime(dataflow_id="replay-chunks", run_id="chunk-failed",
            status="failed", message="Source failed", rows_read=None, rows_written=None),
    ]
    streams["dataflow_part_00000002.json"] = [
        build_dataflow_entry(make_dataflow(runtime.dataflow_id), runtime,
            job_info=job, log_session_id=SESSION_ID)
        for runtime in terminal_cases
    ]
    diagnostic_cases = [
        _diagnostic_runtime("oversized"),
        _diagnostic_runtime("invalid"),
        _diagnostic_runtime("deep"),
        _diagnostic_runtime("cycle"),
    ]
    streams["dataflow_diagnostics_snapshot.json"] = [
        build_dataflow_entry(
            make_dataflow(runtime.dataflow_id),
            runtime,
            job_info=job,
            log_session_id=SESSION_ID,
        )
        for runtime in diagnostic_cases
    ]
    streams["job_bounded_snapshot.json"] = [_bounded_job_record()]
    return streams


def _bounded_job_record() -> dict[str, Any]:
    """Exercise the actual bounded writer; retain only its protected summary."""
    with TemporaryDirectory(prefix="datacoolie-v6-fixture-") as directory, patch(
        "datacoolie.logging.execution_logger.utc_now", return_value=_BASE
    ):
        logger = ExecutionLogger(LogConfig(output_path="logs", persistence_mode="batch",
            buffer_memory_bytes=64, spool_max_bytes=4096, flush_batch_bytes=1,
            flush_interval_seconds=0, spool_directory=str(Path(directory) / "spool")),
            LocalPlatform(base_path=directory))
        logger.set_log_session_id(SESSION_ID)
        logger.set_run_config(DataCoolieRunConfig(job_id=JOB_ID))
        runtime = DataFlowRuntimeInfo(dataflow_id="bounded-drop", dataflow_run_id="bounded-drop-run",
            status="failed", start_time=_BASE, end_time=_BASE,
            attempts=[AttemptRuntimeInfo(attempt_number=number, start_time=_BASE,
                end_time=_BASE, status="failed", message="x" * 600) for number in range(1, 6)])
        logger.activate(started_at=_BASE)
        try:
            logger.log(make_dataflow("bounded-drop"), runtime)
            assert logger.persistence_stats["dataflow"].dropped_records == 1
            logger.finish_job("failed", end_time=_BASE)
        finally:
            logger.close()
        paths = list(Path(directory).rglob("job_*.json"))
        assert len(paths) == 1
        return json.loads(paths[0].read_text(encoding="utf-8").strip())


def _manifest(streams: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    entries = {}
    for filename, records in streams.items():
        payload = b"".join(encode_json_line(record) for record in records)
        kind = (
            "job"
            if filename.startswith("job_")
            else "dataflow"
            if filename.startswith("dataflow_")
            else "system"
        )
        entries[filename] = {
            "kind": kind,
            "mode": "snapshot" if "snapshot" in filename else "batch",
            "rows": len(records),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    return {
        "schema_version": FIXTURE_VERSION,
        "producer_version": __version__,
        "generator_version": GENERATOR_VERSION,
        "fixture_profile": FIXTURE_PROFILE,
        "schema_file": "schema.json",
        "session_id": SESSION_ID,
        "job_id": JOB_ID,
        "streams": entries,
    }


def generate(output_dir: str | Path) -> Path:
    """Write all v6 fixture streams and return the output directory."""

    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    streams = _records()
    for filename, records in streams.items():
        payload = b"".join(encode_json_line(record) for record in records)
        (target / filename).write_bytes(payload)
    (target / "manifest.json").write_bytes(
        (json.dumps(_manifest(streams), indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
    )
    return target


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python generator.py OUTPUT_DIR")
    generate(sys.argv[1])
