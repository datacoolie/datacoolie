"""High-value v5 execution-contract regressions."""

from __future__ import annotations

from copy import deepcopy

import pytest

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import PipelineError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.orchestration.execution.lifecycle import run_prepared_execution
from datacoolie.orchestration.execution.pipeline import execute_etl_pipeline
from datacoolie.orchestration.execution.read_baseline import ReadBaseline
from datacoolie.orchestration.execution.runtime import AttemptRuntimeInfo
from datacoolie.orchestration.preparation import PreparedDataFlow
from datacoolie.sources.contracts import (
    SourceReadRange,
    SourceReadRequest,
    SourceReadResult,
    SourceReadSelection,
    SourceReadUpperOrigin,
)
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.transformers.runtime import TransformRuntimeInfo
from datacoolie.utils.retry import RetryHandler
from datacoolie.watermark.contracts import WatermarkCandidate, WatermarkCandidateKind


def _flow() -> DataFlow:
    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    return DataFlow(
        dataflow_id="contract",
        source=Source(
            connection=connection,
            table="in",
            watermark_columns=["id"],
        ),
        destination=Destination(
            connection=connection,
            table="out",
            load_type="append",
        ),
    )


class Reader:
    def __init__(self, *, fail=False) -> None:
        self.fail = fail
        self.requests: list[SourceReadRequest] = []

    def read(self, _source, *, request: SourceReadRequest) -> SourceReadResult:
        self.requests.append(request)
        if self.fail:
            raise OSError("raw source failure")
        return SourceReadResult(
            frame={"id": [15]},
            rows_read=1,
            selection=SourceReadSelection(
                mode="incremental",
                lower=deepcopy(request.watermark_start),
                upper=None,
                lower_operator=request.watermark_start_operator or ">",
                upper_operator=request.watermark_end_operator or "<",
                upper_origin=SourceReadUpperOrigin.NONE,
            ),
            candidate=WatermarkCandidate(
                {"id": 15}, WatermarkCandidateKind.OBSERVED_MAX
            ),
            runtime=SourceRuntimeInfo(rows_read=1, status="succeeded"),
        )

    def validate_watermark(self, _value, *, boundary="watermark"):
        return None

    def merge_watermark(self, existing, candidate: WatermarkCandidate):
        assert isinstance(candidate, WatermarkCandidate)
        return {**(existing or {}), **candidate.values}


class Transform:
    def transform(self, frame, _dataflow):
        return frame

    def get_runtime_info(self):
        return TransformRuntimeInfo(status="succeeded")

    def get_column_mapping(self):
        return None

    def get_output_columns(self):
        return []


class Writer:
    def __init__(self, *, fail=False, capture_failure=False, observe=True) -> None:
        self.fail = fail
        self.capture_failure = capture_failure
        self.observe = observe
        self.writes = 0

    def write(self, _frame, _dataflow, *, watermark_window=None):
        self.writes += 1
        if self.fail and self.writes == 1:
            raise RuntimeError("adapter raised after physical write")

    def get_runtime_info(self):
        if not self.observe:
            raise AttributeError("diagnostic hook is not implemented")
        if self.capture_failure:
            raise ValueError("diagnostic capture failed after acknowledgement")
        return DestinationRuntimeInfo(status="succeeded", rows_written=1)


class WatermarkManager:
    def __init__(self, values=None):
        self.values = list(values or [None])
        self.loads = 0
        self.saves = []

    def get_watermark(self, **_kwargs):
        value = self.values[min(self.loads, len(self.values) - 1)]
        self.loads += 1
        return deepcopy(value)

    def serialize(self, value):
        return str(value)

    def save_watermark(self, **kwargs):
        self.saves.append(kwargs)


def _execute(
    flow: DataFlow,
    run_id: str,
    *,
    reader: Reader,
    writer: Writer,
    baseline: ReadBaseline,
    attempt: AttemptRuntimeInfo,
    manager=None,
    save_watermark=False,
):
    return execute_etl_pipeline(
        flow,
        run_id,
        baseline=baseline,
        attempt=attempt,
        column_name_mode="lower",
        watermark_start=None,
        watermark_end=None,
        save_watermark=save_watermark,
        watermark_start_operator=None,
        watermark_end_operator=None,
        watermark_manager=manager,
        job_id="job",
        create_source_reader=lambda _fmt: reader,
        create_transformer_pipeline=lambda **_kwargs: Transform(),
        create_destination_writer=lambda _fmt: writer,
        validate_watermark_storage=lambda *_args, **_kwargs: None,
    )


def test_whole_retry_shares_baseline_and_reference_but_uses_fresh_attempts() -> None:
    flow = _flow()
    reader = Reader()
    writer = Writer(fail=True)
    manager = WatermarkManager([{"id": 10}, {"id": 20}, {"id": 30}])
    baselines = []
    supplied_attempts = []

    def runner(
        execution: DataFlow,
        *,
        baseline: ReadBaseline,
        attempt: AttemptRuntimeInfo,
        dataflow_run_id: str,
        **_kwargs,
    ):
        baselines.append(baseline)
        supplied_attempts.append(attempt)
        return _execute(
            execution,
            dataflow_run_id,
            reader=reader,
            writer=writer,
            baseline=baseline,
            attempt=attempt,
            manager=manager,
            save_watermark=True,
        )

    runtime = run_prepared_execution(
        PreparedDataFlow(metadata=flow, execution=flow.model_copy(deep=True)),
        operation_type="etl",
        retry_handler=RetryHandler(retry_count=1, retry_delay=0),
        preflight=lambda _flow: None,
        attempt_runner=runner,
        log_result=lambda *_args: None,
    )

    assert runtime.status == DataFlowStatus.SUCCEEDED.value
    assert len(baselines) == 2
    assert baselines[0] is baselines[1]
    assert len(supplied_attempts) == 2
    assert supplied_attempts[0] is not supplied_attempts[1]
    assert supplied_attempts[0].attempt_number == 1
    assert supplied_attempts[1].attempt_number == 2
    assert reader.requests[0].reference_time is reader.requests[1].reference_time
    assert reader.requests[0].watermark_start == reader.requests[1].watermark_start == {"id": 10}
    assert manager.loads == 3  # one pinned load plus one fresh merge read per attempt
    assert [item.status for item in runtime.attempts] == ["failed", "succeeded"]
    assert runtime.final_attempt is not supplied_attempts[1]
    assert runtime.retry_attempts == 1


@pytest.mark.parametrize("phase", ["source", "destination"])
def test_raw_component_errors_keep_partial_attempt_evidence_and_unknown_metrics(phase):
    flow = _flow()
    reader = Reader(fail=phase == "source")
    writer = Writer(fail=phase == "destination", observe=phase != "destination")
    attempt = AttemptRuntimeInfo(attempt_number=1)

    with pytest.raises(PipelineError) as caught:
        _execute(
            flow,
            "run-raw-error",
            reader=reader,
            writer=writer,
            baseline=ReadBaseline(),
            attempt=attempt,
        )

    assert caught.value.attempt is attempt
    assert attempt.status == DataFlowStatus.FAILED.value
    if phase == "source":
        assert attempt.source is not None
        assert attempt.source.status == DataFlowStatus.FAILED.value
        assert attempt.source.rows_read is None
        assert attempt.transform is None
        assert attempt.destination is None
    else:
        assert attempt.source is not None
        assert attempt.source.status == DataFlowStatus.SUCCEEDED.value
        assert attempt.transform is not None
        assert attempt.destination is not None
        assert attempt.destination.status == DataFlowStatus.FAILED.value
        assert attempt.destination.rows_written is None


def test_acknowledged_write_capture_failure_does_not_retry_business_work():
    flow = _flow()
    writer = Writer(capture_failure=True)
    attempts = []

    def runner(
        execution: DataFlow,
        *,
        baseline: ReadBaseline,
        attempt: AttemptRuntimeInfo,
        dataflow_run_id: str,
        **_kwargs,
    ):
        attempts.append(attempt)
        return _execute(
            execution,
            dataflow_run_id,
            reader=Reader(),
            writer=writer,
            baseline=baseline,
            attempt=attempt,
        )

    runtime = run_prepared_execution(
        PreparedDataFlow(metadata=flow, execution=flow.model_copy(deep=True)),
        operation_type="etl",
        retry_handler=RetryHandler(retry_count=2, retry_delay=0),
        preflight=lambda _flow: None,
        attempt_runner=runner,
        log_result=lambda *_args: None,
    )

    assert runtime.status == DataFlowStatus.SUCCEEDED.value
    assert len(attempts) == 1
    assert writer.writes == 1
    assert runtime.retry_attempts == 0
    assert runtime.final_attempt.destination.status == DataFlowStatus.SUCCEEDED.value
    assert runtime.final_attempt.destination.rows_written is None


def test_adapter_raise_after_physical_write_is_retryable():
    flow = _flow()
    writer = Writer(fail=True)
    attempts = []

    def runner(
        execution: DataFlow,
        *,
        baseline: ReadBaseline,
        attempt: AttemptRuntimeInfo,
        dataflow_run_id: str,
        **_kwargs,
    ):
        attempts.append(attempt)
        return _execute(
            execution,
            dataflow_run_id,
            reader=Reader(),
            writer=writer,
            baseline=baseline,
            attempt=attempt,
        )

    runtime = run_prepared_execution(
        PreparedDataFlow(metadata=flow, execution=flow.model_copy(deep=True)),
        operation_type="etl",
        retry_handler=RetryHandler(retry_count=1, retry_delay=0),
        preflight=lambda _flow: None,
        attempt_runner=runner,
        log_result=lambda *_args: None,
    )

    assert runtime.status == DataFlowStatus.SUCCEEDED.value
    assert writer.writes == 2
    assert len(attempts) == 2
    assert runtime.retry_attempts == 1
    assert [item.status for item in runtime.attempts] == ["failed", "succeeded"]


def test_initial_request_validation_failure_is_a_final_no_attempt_failure():
    flow = _flow()
    prepared = PreparedDataFlow(metadata=flow, execution=flow.model_copy(deep=True))
    invoked = []
    logged = []

    runtime = run_prepared_execution(
        prepared,
        operation_type="replay",
        retry_handler=RetryHandler(retry_count=2, retry_delay=0),
        preflight=lambda _flow: None,
        attempt_runner=lambda *_args, **_kwargs: invoked.append(True),
        attempt_kwargs={
            "watermark_start": {"id": 1},
            "read_range": SourceReadRange("id", 1, 5),
        },
        log_result=lambda _metadata, result: logged.append(result),
    )

    assert runtime.status == DataFlowStatus.FAILED.value
    assert runtime.end_time is not None
    assert runtime.message and "cannot be combined" in runtime.message
    assert runtime.attempts == []
    assert invoked == []
    assert logged and logged[0].status == DataFlowStatus.FAILED.value


def test_replay_chunk_runtime_records_explicit_read_request():
    flow = _flow()
    prepared = PreparedDataFlow(metadata=flow, execution=flow.model_copy(deep=True))
    seen = []

    def runner(
        execution: DataFlow,
        *,
        baseline: ReadBaseline,
        attempt: AttemptRuntimeInfo,
        dataflow_run_id: str,
        **kwargs,
    ):
        seen.append((baseline, attempt, kwargs["read_range"]))
        return _finish(attempt)

    runtime = run_prepared_execution(
        prepared,
        operation_type="replay",
        retry_handler=RetryHandler(retry_count=0, retry_delay=0),
        preflight=lambda _flow: None,
        attempt_runner=runner,
        attempt_kwargs={
            "watermark_start": None,
            "watermark_end": None,
            "read_range": SourceReadRange("id", 1, 5),
            "save_watermark": False,
        },
        log_result=lambda *_args: None,
    )

    assert len(seen) == 1
    assert runtime.read_request is not None
    assert runtime.read_request.reference_time is seen[0][0].reference_time
    assert runtime.read_request.read_range == SourceReadRange("id", 1, 5)


def _finish(attempt: AttemptRuntimeInfo) -> AttemptRuntimeInfo:
    attempt.status = DataFlowStatus.SUCCEEDED.value
    return attempt


@pytest.mark.parametrize("timing", ["invalid", "naive_start", "naive_pair"])
def test_invalid_diagnostic_timestamp_keeps_owned_timing_and_success(timing):
    from datetime import datetime

    class BadTimingWriter(Writer):
        def get_runtime_info(self):
            return DestinationRuntimeInfo(
                start_time="invalid" if timing == "invalid" else datetime(2026, 10, 4),
                end_time=datetime(2026, 10, 4, 0, 0, 1) if timing == "naive_pair" else None,
                status="succeeded", rows_written=1)

    writer = BadTimingWriter()
    attempt = AttemptRuntimeInfo()
    _execute(_flow(), "run", reader=Reader(), writer=writer,
        baseline=ReadBaseline(), attempt=attempt)
    assert attempt.status == "succeeded"
    assert writer.writes == 1
    assert isinstance(attempt.destination.start_time, datetime)
    assert attempt.destination.duration_seconds >= 0


def test_invalid_candidate_fails_before_write_without_persistence():
    class InvalidCandidateReader(Reader):
        def read(self, source, *, request):
            result = super().read(source, request=request)
            return SourceReadResult(frame=result.frame, rows_read=1, selection=result.selection,
                candidate=WatermarkCandidate({"id": float("inf")}, "observed_max"))

    writer = Writer()
    attempt = AttemptRuntimeInfo()
    with pytest.raises(PipelineError, match="finite"):
        _execute(_flow(), "run", reader=InvalidCandidateReader(), writer=writer,
            baseline=ReadBaseline(), attempt=attempt, save_watermark=False)
    assert writer.writes == 0
    assert attempt.source.rows_read == 1
    assert attempt.source.status == "failed"
