"""Canonical v5 tests for Driver-independent single-attempt pipelines."""

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
from datacoolie.orchestration.execution.pipeline import (
    execute_etl_pipeline,
    execute_maintenance_pipeline,
)
from datacoolie.orchestration.execution.read_baseline import ReadBaseline
from datacoolie.orchestration.execution.runtime import AttemptRuntimeInfo
from datacoolie.sources.contracts import (
    SourceReadRange,
    SourceReadRequest,
    SourceReadResult,
    SourceReadSelection,
    SourceReadUpperOrigin,
)
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.transformers.runtime import TransformRuntimeInfo
from datacoolie.watermark.contracts import (
    WatermarkCandidate,
    WatermarkCandidateKind,
    WatermarkSaveStatus,
)


_DEFAULT_FRAME = object()


def _dataflow(*, replace: bool = False, with_watermark: bool = False) -> DataFlow:
    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    return DataFlow(
        dataflow_id="df-1",
        source=Source(
            connection=connection,
            table="orders",
            watermark_columns=["id"] if with_watermark else [],
            configure={"backward_days": 1} if replace else {},
        ),
        destination=Destination(
            connection=connection,
            table="orders_out",
            load_type="merge_overwrite" if replace else "append",
            merge_keys=[] if replace else None,
            configure={"replace_by_watermark": replace},
        ),
    )


def _selection(
    *,
    lower=None,
    upper=None,
    lower_operator=">",
    upper_operator="<",
    mode="incremental",
    upper_origin=SourceReadUpperOrigin.NONE,
) -> SourceReadSelection:
    return SourceReadSelection(
        mode=mode,
        lower=lower,
        upper=upper,
        lower_operator=lower_operator,
        upper_operator=upper_operator,
        combine_operator="OR",
        upper_origin=upper_origin,
    )


class Reader:
    """Minimal source double exposing only the required v5 reader contract."""

    def __init__(
        self,
        *,
        frame=_DEFAULT_FRAME,
        rows_read=1,
        selection=None,
        candidate=None,
        candidate_kind=WatermarkCandidateKind.OBSERVED_MAX,
        runtime=None,
    ) -> None:
        self.frame = {"id": [1]} if frame is _DEFAULT_FRAME else frame
        self.rows_read = rows_read
        self.selection = selection
        self.candidate = (
            None
            if candidate is None
            else candidate
            if isinstance(candidate, WatermarkCandidate)
            else WatermarkCandidate(
                values=deepcopy(candidate),
                kind=candidate_kind,
            )
        )
        self.runtime = runtime or SourceRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_read=rows_read,
        )
        self.requests: list[SourceReadRequest] = []
        self.merge_candidates: list[WatermarkCandidate] = []

    def read(self, _source, *, request: SourceReadRequest) -> SourceReadResult:
        self.requests.append(request)
        selection = self.selection
        if selection is None and request.read_range is not None:
            selection = _selection(
                lower={request.read_range.column: request.read_range.start},
                upper={request.read_range.column: request.read_range.end},
                lower_operator=request.read_range.lower_operator,
                upper_operator=request.read_range.upper_operator,
                mode="range",
                upper_origin=SourceReadUpperOrigin.EXPLICIT,
            )
        if selection is None:
            selection = _selection(
                lower=deepcopy(request.watermark_start),
                upper=deepcopy(request.watermark_end),
                lower_operator=request.watermark_start_operator or ">",
                upper_operator=request.watermark_end_operator or "<",
                mode=(
                    "incremental"
                    if request.watermark_start is not None
                    or request.watermark_end is not None
                    else "full"
                ),
                upper_origin=(
                    SourceReadUpperOrigin.EXPLICIT
                    if request.watermark_end is not None
                    else SourceReadUpperOrigin.NONE
                ),
            )
        runtime = deepcopy(self.runtime)
        runtime.selection = deepcopy(selection)
        return SourceReadResult(
            frame=self.frame,
            rows_read=self.rows_read,
            selection=selection,
            candidate=deepcopy(self.candidate),
            runtime=runtime,
        )

    def validate_watermark(self, watermark, *, boundary="watermark") -> None:
        def find_binary(value):
            if isinstance(value, (bytes, bytearray, memoryview)):
                return True
            if isinstance(value, dict):
                return any(find_binary(nested) for nested in value.values())
            if isinstance(value, (list, tuple)):
                return any(find_binary(nested) for nested in value)
            return False

        if find_binary(watermark):
            raise ValueError(
                "Binary watermark comparison requires a qualified backend "
                f"for {boundary}"
            )

    def merge_watermark(
        self,
        existing: dict | None,
        candidate: WatermarkCandidate | None,
    ) -> dict:
        assert candidate is None or isinstance(candidate, WatermarkCandidate)
        if candidate is None:
            return deepcopy(existing or {})
        self.merge_candidates.append(candidate)
        return {**(existing or {}), **candidate.values}


class Transform:
    def __init__(self, *, runtime=None, frame=None) -> None:
        self.runtime = runtime or TransformRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value
        )
        self.frame = frame

    def transform(self, frame, _dataflow):
        return self.frame if self.frame is not None else frame

    def get_runtime_info(self):
        return self.runtime

    def get_column_mapping(self):
        return None

    def get_output_columns(self):
        return []


class WindowTransform(Transform):
    def get_column_mapping(self):
        from datacoolie.transformers.base import ColumnMapping

        return ColumnMapping({"id": "id"})

    def get_output_columns(self):
        return ["id"]


class Writer:
    def __init__(self, *, runtime=None) -> None:
        self.runtime = runtime or DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            rows_written=1,
        )
        self.windows = []
        self.frames = []

    def write(self, frame, _dataflow, *, watermark_window=None):
        self.frames.append(frame)
        self.windows.append(watermark_window)

    def get_runtime_info(self):
        return self.runtime


def _run(
    dataflow: DataFlow,
    run_id: str,
    reader: Reader,
    pipeline: Transform,
    writer: Writer,
    *,
    manager=None,
    baseline: ReadBaseline | None = None,
    attempt: AttemptRuntimeInfo | None = None,
    validate_storage=lambda *_args, **_kwargs: None,
    **kwargs,
) -> AttemptRuntimeInfo:
    return execute_etl_pipeline(
        dataflow,
        run_id,
        baseline=baseline or ReadBaseline(),
        attempt=attempt or AttemptRuntimeInfo(),
        column_name_mode="lower",
        watermark_start=kwargs.pop("watermark_start", None),
        watermark_end=kwargs.pop("watermark_end", None),
        save_watermark=kwargs.pop("save_watermark", manager is not None),
        watermark_start_operator=kwargs.pop("watermark_start_operator", None),
        watermark_end_operator=kwargs.pop("watermark_end_operator", None),
        read_range=kwargs.pop("read_range", None),
        watermark_manager=manager,
        job_id="job-1",
        create_source_reader=lambda _fmt: reader,
        create_transformer_pipeline=lambda **_kwargs: pipeline,
        create_destination_writer=lambda _fmt: writer,
        validate_watermark_storage=validate_storage,
        **kwargs,
    )


def test_pipeline_mutates_and_returns_the_lifecycle_owned_attempt() -> None:
    attempt = AttemptRuntimeInfo(attempt_number=7)
    baseline = ReadBaseline()
    reader = Reader()
    returned = _run(
        _dataflow(),
        "run-1",
        reader,
        Transform(),
        Writer(),
        baseline=baseline,
        attempt=attempt,
        save_watermark=False,
    )

    assert returned is attempt
    assert returned.status == DataFlowStatus.SUCCEEDED.value
    assert reader.requests[0].reference_time is baseline.reference_time
    assert returned.source.rows_read == 1
    assert returned.destination.rows_written == 1


def test_source_result_is_authoritative_without_a_source_diagnostic_hook() -> None:
    reader = Reader(
        rows_read=3,
        runtime=SourceRuntimeInfo(rows_read=999, status="malformed-runtime-count"),
    )
    result = _run(
        _dataflow(),
        "run-source-result",
        reader,
        Transform(),
        Writer(),
        save_watermark=False,
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert result.source.rows_read == 3
    assert result.source.status == DataFlowStatus.SUCCEEDED.value


def test_empty_source_skips_transform_write_and_save_but_keeps_typed_candidate() -> None:
    events: list[str] = []

    class EmptyReader(Reader):
        def read(self, source, *, request):
            events.append("read")
            return super().read(source, request=request)

    class EventTransform(Transform):
        def transform(self, frame, dataflow):
            events.append("transform")
            return super().transform(frame, dataflow)

    class EventWriter(Writer):
        def write(self, frame, dataflow, *, watermark_window=None):
            events.append("write")
            return super().write(frame, dataflow, watermark_window=watermark_window)

    class Manager:
        def get_watermark(self, **_kwargs):
            events.append("get")
            return {"id": 1}

        def serialize(self, _value):
            events.append("serialize")

        def save_watermark(self, **_kwargs):
            raise AssertionError("empty source must not save")

    candidate = WatermarkCandidate({"id": 4}, WatermarkCandidateKind.OBSERVED_MAX)
    reader = EmptyReader(frame=None, rows_read=0, candidate=candidate)
    result = _run(
        _dataflow(with_watermark=True),
        "run-empty",
        reader,
        EventTransform(),
        EventWriter(),
        manager=Manager(),
        save_watermark=True,
        watermark_start={"id": 1},
        watermark_start_operator=">",
        watermark_end_operator="<",
    )

    assert result.status == DataFlowStatus.SKIPPED.value
    assert events == ["read"]
    assert result.watermark.candidate is not None
    assert result.watermark.candidate.values == {"id": 4}
    assert result.watermark.save_status is WatermarkSaveStatus.NOT_ATTEMPTED


def test_transformed_empty_frame_is_written_without_a_post_transform_count() -> None:
    class EmptyFrame:
        def count(self, *_args, **_kwargs):
            raise AssertionError("pipeline must not count transformed data")

        @property
        def is_empty(self):
            raise AssertionError("pipeline must not inspect transformed emptiness")

    writer = Writer()
    result = _run(
        _dataflow(),
        "run-transformed-empty",
        Reader(),
        Transform(frame=EmptyFrame()),
        writer,
        save_watermark=False,
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert len(writer.frames) == 1
    assert isinstance(writer.frames[0], EmptyFrame)


def test_observed_candidate_replacement_uses_selection_and_not_diagnostic_mutation() -> None:
    selection = _selection(
        lower={"id": 3},
        upper={"id": 100},
        lower_operator=">",
        upper_operator="<",
        upper_origin=SourceReadUpperOrigin.GENERATED,
    )
    candidate = WatermarkCandidate(
        {"id": 12}, WatermarkCandidateKind.OBSERVED_MAX
    )
    writer = Writer()
    result = _run(
        _dataflow(replace=True),
        "run-window",
        Reader(selection=selection, candidate=candidate),
        WindowTransform(),
        writer,
        save_watermark=False,
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert writer.windows[0].bounds == {"id": (3, 12)}
    assert writer.windows[0].upper_operator == "<="


def test_opaque_candidate_never_authorizes_an_ordered_replacement_window() -> None:
    selection = _selection(
        lower={"id": 3},
        upper={"id": 100},
        upper_origin=SourceReadUpperOrigin.GENERATED,
    )
    writer = Writer()
    result = _run(
        _dataflow(replace=True),
        "run-opaque-window",
        Reader(
            selection=selection,
            candidate=WatermarkCandidate(
                {"cursor": "next"}, WatermarkCandidateKind.OPAQUE
            ),
        ),
        WindowTransform(),
        writer,
        save_watermark=False,
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert writer.windows == [None]


def test_read_range_and_watermark_bounds_are_rejected_before_source_io() -> None:
    reader = Reader()
    with pytest.raises(PipelineError, match="read_range cannot be combined"):
        _run(
            _dataflow(),
            "run-conflicting-bounds",
            reader,
            Transform(),
            Writer(),
            save_watermark=False,
            watermark_start={"id": 1},
            read_range=SourceReadRange("id", 1, 5),
        )

    assert reader.requests == []


def test_typed_candidate_is_passed_to_reader_merge_and_save_follows_write() -> None:
    events: list[str] = []
    candidate = WatermarkCandidate(
        {"id": 2}, WatermarkCandidateKind.OBSERVED_MAX
    )

    class TrackingReader(Reader):
        def merge_watermark(self, existing, value):
            events.append("merge")
            assert isinstance(value, WatermarkCandidate)
            return super().merge_watermark(existing, value)

    class Manager:
        def get_watermark(self, **_kwargs):
            events.append("load")
            return {"id": 1}

        def serialize(self, value):
            events.append("serialize")
            assert value == {"id": 2}
            return str(value)

        def save_watermark(self, **_kwargs):
            events.append("save")

    class TrackingTransform(Transform):
        def transform(self, frame, dataflow):
            events.append("transform")
            return super().transform(frame, dataflow)

    class TrackingWriter(Writer):
        def write(self, frame, dataflow, *, watermark_window=None):
            events.append("write")
            return super().write(frame, dataflow, watermark_window=watermark_window)

    result = _run(
        _dataflow(with_watermark=True),
        "run-save-order",
        TrackingReader(candidate=candidate),
        TrackingTransform(),
        TrackingWriter(),
        manager=Manager(),
        save_watermark=True,
        watermark_start={"id": 1},
        watermark_start_operator=">",
        watermark_end_operator="<",
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert events == ["load", "merge", "serialize", "transform", "write", "save"]
    assert result.watermark.candidate is candidate or result.watermark.candidate.values == {"id": 2}
    assert result.watermark.watermark_to_save == {"id": 2}
    assert result.watermark.save_status is WatermarkSaveStatus.SAVED


def test_save_error_retains_partial_watermark_evidence_and_attempt() -> None:
    class Manager:
        def get_watermark(self, **_kwargs):
            return {"id": 1}

        def serialize(self, value):
            return str(value)

        def save_watermark(self, **_kwargs):
            raise OSError("save acknowledgement lost after commit")

    attempt = AttemptRuntimeInfo(attempt_number=2)
    with pytest.raises(PipelineError, match="save acknowledgement lost") as caught:
        _run(
            _dataflow(with_watermark=True),
            "run-save-error",
            Reader(candidate={"id": 2}),
            Transform(),
            Writer(),
            manager=Manager(),
            attempt=attempt,
            save_watermark=True,
            watermark_start={"id": 1},
        )

    assert caught.value.attempt is attempt
    assert attempt.watermark.save_status is WatermarkSaveStatus.SAVE_ERROR
    assert attempt.watermark.watermark_to_save == {"id": 2}
    assert attempt.watermark.saved_watermark is None


@pytest.mark.parametrize("missing", ["read", "validate_watermark", "merge_watermark"])
def test_required_reader_contract_fails_before_side_effects(missing) -> None:
    reader = Reader()
    setattr(reader, missing, None)
    events: list[str] = []

    with pytest.raises(PipelineError, match=missing):
        _run(
            _dataflow(),
            "run-missing-reader-contract",
            reader,
            Transform(),
            Writer(),
            save_watermark=False,
            validate_storage=lambda *_args, **_kwargs: events.append("validate"),
        )

    assert reader.requests == []
    assert events == []


def test_malformed_transform_and_destination_metrics_are_omitted_as_unknown() -> None:
    transform = Transform(
        runtime=TransformRuntimeInfo(status="succeeded")
    )
    writer = Writer(
        runtime=DestinationRuntimeInfo(
            status="succeeded",
            rows_written="not-an-int",
            bytes_added=-1,
            bytes_removed=4,
        )
    )
    result = _run(
        _dataflow(),
        "run-malformed-metrics",
        Reader(),
        transform,
        writer,
        save_watermark=False,
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert result.destination.rows_written is None
    assert result.destination.bytes_added is None
    assert result.destination.bytes_removed == 4
    assert result.destination.rows_written is None


def test_uncopyable_diagnostic_payload_preserves_known_metrics() -> None:
    class Uncopyable:
        def __deepcopy__(self, _memo):
            raise ValueError("diagnostics only")

    writer = Writer(
        runtime=DestinationRuntimeInfo(
            status="succeeded",
            rows_written=7,
            operation_details=[{"payload": Uncopyable()}],
        )
    )
    result = _run(
        _dataflow(),
        "run-uncopyable-diagnostics",
        Reader(),
        Transform(),
        writer,
        save_watermark=False,
    )

    assert result.status == DataFlowStatus.SUCCEEDED.value
    assert result.destination.rows_written == 7
    assert result.destination.operation_details == []


def test_invalid_watermark_is_rejected_before_destination_write() -> None:
    class TrackingWriter(Writer):
        def write(self, frame, dataflow, *, watermark_window=None):
            raise AssertionError("invalid watermark must fail before write")

    with pytest.raises(PipelineError, match="Binary watermark comparison"):
        _run(
            _dataflow(with_watermark=True),
            "run-invalid-watermark",
            Reader(candidate={"id": b"\\x00"}),
            Transform(),
            TrackingWriter(),
            manager=type(
                "Manager",
                (),
                {
                    "get_watermark": lambda self, **_kwargs: None,
                    "serialize": lambda self, value: str(value),
                    "save_watermark": lambda self, **_kwargs: None,
                },
            )(),
            save_watermark=True,
            watermark_start={"id": 1},
        )


def test_maintenance_attempt_has_destination_only_phase() -> None:
    class MaintenanceWriter:
        def run_maintenance(self, **_kwargs):
            return DestinationRuntimeInfo(
                status=DataFlowStatus.SKIPPED.value,
                message="No maintenance operations enabled",
            )

    attempt = AttemptRuntimeInfo(attempt_number=3)
    result = execute_maintenance_pipeline(
        _dataflow(),
        baseline=ReadBaseline(),
        attempt=attempt,
        create_destination_writer=lambda _fmt: MaintenanceWriter(),
        retention_hours=168,
        do_compact=False,
        do_cleanup=False,
    )

    assert result is attempt
    assert result.status == DataFlowStatus.SKIPPED.value
    assert result.destination is not None
    assert result.source is None
    assert result.transform is None
