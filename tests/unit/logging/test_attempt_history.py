"""Execution-attempt history and bounded persistence contract tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import PipelineError
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.orchestration.execution.runtime import (
    AttemptRuntimeInfo,
    DataFlowRuntimeInfo,
)
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.transformers.runtime import TransformRuntimeInfo
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.execution_logger import ExecutionLogger
from datacoolie.orchestration.execution.lifecycle import run_prepared_execution
from datacoolie.orchestration.preparation import PreparedDataFlow
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.utils.retry import RetryHandler

from tests.unit.logging.support import make_dataflow, make_runtime


_PERSISTENCE_MODES = ("snapshot", "batch")
_OBSERVED_AT = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def _prepared(dataflow_id: str) -> PreparedDataFlow:
    metadata = make_dataflow(dataflow_id)
    return PreparedDataFlow(
        metadata=metadata.model_copy(deep=True),
        execution=metadata.model_copy(deep=True),
    )


def _logger(tmp_path: Path, persistence_mode: str, *, small_budget: bool = False) -> ExecutionLogger:
    options = {
        "output_path": "logs",
        "persistence_mode": persistence_mode,
        "flush_batch_bytes": 1,
        "flush_interval_seconds": 0,
        "spool_directory": str(tmp_path / "spool"),
    }
    if small_budget:
        options.update(buffer_memory_bytes=64, spool_max_bytes=4096)
    logger = ExecutionLogger(
        LogConfig(**options),
        LocalPlatform(base_path=str(tmp_path)),
    )
    logger.set_run_config(DataCoolieRunConfig(job_id="attempt-history-job"))
    return logger


def _rows(tmp_path: Path, stream: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in sorted(tmp_path.rglob("*.json")):
        if stream not in path.parts:
            continue
        rows.extend(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return rows


def _nested_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {
            nested
            for child in value.values()
            for nested in _nested_keys(child)
        }
    if isinstance(value, list):
        return {nested for child in value for nested in _nested_keys(child)}
    return set()


@pytest.mark.parametrize("persistence_mode", _PERSISTENCE_MODES)
def test_retry_lifecycle_persists_one_row_with_final_attempt_projection(
    tmp_path: Path,
    persistence_mode: str,
) -> None:
    logger = _logger(tmp_path, persistence_mode)
    calls = 0
    logger.activate()

    def attempt(_dataflow, **_kwargs):
        nonlocal calls
        calls += 1
        result = _kwargs["attempt"]
        if calls == 1:
            result.source = SourceRuntimeInfo(rows_read=101, message="first-only")
            result.destination = DestinationRuntimeInfo(rows_written=202)
            raise PipelineError(
                "first failure",
                attempt=result,
            )
        result.status = DataFlowStatus.SUCCEEDED.value
        result.source = SourceRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=7,
            source_action={"decimal": Decimal("12.50"), "observed_at": _OBSERVED_AT},
        )
        result.transform = TransformRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            transformers_applied=["final-attempt"],
        )
        result.destination = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_written=9,
        )
        return result

    try:
        runtime = run_prepared_execution(
            _prepared("retry-success"),
            operation_type="etl",
            retry_handler=RetryHandler(retry_count=1, retry_delay=0),
            preflight=lambda _dataflow: None,
            attempt_runner=attempt,
            log_result=logger.log,
        )
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
    finally:
        logger.close()

    assert calls == 2
    assert runtime.status == DataFlowStatus.SUCCEEDED.value
    assert [attempt.attempt_number for attempt in runtime.attempts] == [1, 2]

    dataflow_rows = _rows(tmp_path, "dataflow_run_log")
    job_rows = _rows(tmp_path, "job_run_log")
    assert len(dataflow_rows) == 1
    assert len(job_rows) == 1

    row = dataflow_rows[0]
    assert row["retry_attempts"] == 1
    assert row["metrics"]["rows_read"] == 7
    assert row["metrics"]["rows_written"] == 9
    assert row["attempts"][1]["source"]["message"] is None
    assert row["attempts"][1]["destination"]["message"] is None

    history = row["attempts"]
    assert [attempt["attempt_number"] for attempt in history] == [1, 2]
    assert history[0]["source"]["rows_read"] == 101
    assert history[1]["source"]["rows_read"] == 7
    assert history[1]["source"]["source_action"]["decimal"] == "12.50"
    assert history[1]["source"]["source_action"]["observed_at"] == _OBSERVED_AT.isoformat()
    assert not {"metadata", "frame", "frames"} & _nested_keys(history)

    job = job_rows[0]
    assert job["total_dataflows"] == 1
    assert job["total_succeeded"] == 1
    assert job["total_failed"] == 0


@pytest.mark.parametrize("persistence_mode", _PERSISTENCE_MODES)
def test_exhausted_lifecycle_keeps_terminal_history_without_prior_phase_leakage(
    tmp_path: Path,
    persistence_mode: str,
) -> None:
    logger = _logger(tmp_path, persistence_mode)
    calls = 0
    logger.activate()

    def attempt(_dataflow, **_kwargs):
        nonlocal calls
        calls += 1
        partial = _kwargs["attempt"]
        partial.status = DataFlowStatus.FAILED.value
        if calls == 1:
            partial.source = SourceRuntimeInfo(rows_read=303, message="first-only")
            partial.destination = DestinationRuntimeInfo(rows_written=404)
        raise PipelineError("terminal failure", attempt=partial)

    try:
        runtime = run_prepared_execution(
            _prepared("retry-exhausted"),
            operation_type="etl",
            retry_handler=RetryHandler(retry_count=1, retry_delay=0),
            preflight=lambda _dataflow: None,
            attempt_runner=attempt,
            log_result=logger.log,
        )
        logger.finish_job(DataFlowStatus.FAILED.value)
    finally:
        logger.close()

    assert calls == 2
    assert runtime.status == DataFlowStatus.FAILED.value
    assert [attempt.attempt_number for attempt in runtime.attempts] == [1, 2]

    dataflow_rows = _rows(tmp_path, "dataflow_run_log")
    job_rows = _rows(tmp_path, "job_run_log")
    assert len(dataflow_rows) == 1
    assert len(job_rows) == 1

    row = dataflow_rows[0]
    assert row["retry_attempts"] == 1
    assert row["metrics"]["rows_read"] is None
    assert row["metrics"]["rows_written"] is None
    history = row["attempts"]
    assert [attempt["attempt_number"] for attempt in history] == [1, 2]
    assert history[0]["source"]["rows_read"] == 303
    assert history[1]["source"] is None
    assert history[1]["destination"] is None

    job = job_rows[0]
    assert job["total_dataflows"] == 1
    assert job["total_succeeded"] == 0
    assert job["total_failed"] == 1


@pytest.mark.parametrize("persistence_mode", _PERSISTENCE_MODES)
def test_oversized_nested_history_drops_detail_but_keeps_job_headroom(
    tmp_path: Path,
    persistence_mode: str,
) -> None:
    logger = _logger(tmp_path, persistence_mode, small_budget=True)
    runtime = DataFlowRuntimeInfo(
        dataflow_id="oversized-history",
        status=DataFlowStatus.FAILED.value,
        attempts=[
            AttemptRuntimeInfo(
                status=DataFlowStatus.FAILED.value,
                attempt_number=number,
                message="x" * 600,
            )
            for number in range(1, 6)
        ],
    )
    logger.activate()
    try:
        logger.log(make_dataflow("oversized-history"), runtime)
        dataflow_stats = logger.persistence_stats["dataflow"]
        logger.finish_job(DataFlowStatus.FAILED.value)
    finally:
        logger.close()

    assert dataflow_stats.dropped_records == 1
    assert dataflow_stats.dropped_bytes > 0
    assert _rows(tmp_path, "dataflow_run_log") == []

    job_rows = _rows(tmp_path, "job_run_log")
    assert len(job_rows) == 1
    assert job_rows[0]["total_dataflows"] == 1
    assert job_rows[0]["total_failed"] == 1
    assert job_rows[0]["log_records_dropped"] == 1
    assert job_rows[0]["log_bytes_dropped"] == dataflow_stats.dropped_bytes


@pytest.mark.parametrize("persistence_mode", _PERSISTENCE_MODES)
def test_runtime_without_attempts_persists_empty_history(
    tmp_path: Path,
    persistence_mode: str,
) -> None:
    logger = _logger(tmp_path, persistence_mode)
    logger.activate()
    try:
        logger.log(
            make_dataflow("empty-history"),
            DataFlowRuntimeInfo(
                dataflow_id="empty-history",
                status=DataFlowStatus.SUCCEEDED.value,
            ),
        )
        logger.finish_job(DataFlowStatus.SUCCEEDED.value)
    finally:
        logger.close()

    rows = _rows(tmp_path, "dataflow_run_log")
    assert len(rows) == 1
    assert rows[0]["attempts"] == []
