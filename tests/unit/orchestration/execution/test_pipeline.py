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


def _isolated_transformer_registry(monkeypatch, *, missing=None, broken=None):
    import importlib.metadata
    from types import SimpleNamespace
    import datacoolie
    from datacoolie.core.registry import PluginRegistry
    from datacoolie.transformers.base import BaseTransformer

    entries = [entry for entry in importlib.metadata.entry_points(group="datacoolie.transformers")
               if entry.name not in {missing, broken}]
    cause = ImportError("synthetic optional plugin load failure")
    if broken:
        def fail_load():
            raise cause
        entries.append(SimpleNamespace(name=broken, load=fail_load))
    monkeypatch.setattr("datacoolie.core.registry.entry_points", lambda **_kwargs: entries)
    registry = PluginRegistry("datacoolie.transformers", BaseTransformer)
    monkeypatch.setattr(datacoolie, "transformer_registry", registry)
    return registry, cause


@pytest.mark.parametrize("failure", ["missing", "load"])
@pytest.mark.parametrize("configured", [False, True])
def test_required_default_masker_failure_precedes_write_and_save(monkeypatch, failure, configured):
    from unittest.mock import Mock
    from datacoolie.core.models.transform import Transform as TransformConfig
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.orchestration.execution.pipeline import build_transformer_pipeline
    import polars as pl

    _, cause = _isolated_transformer_registry(monkeypatch,
        missing="data_masker" if failure == "missing" else None,
        broken="data_masker" if failure == "load" else None)
    flow = _dataflow(with_watermark=True)
    if configured:
        flow.transform = TransformConfig(masking_rules=[{"method": "nullify", "columns": ["email"]}])
    engine = PolarsEngine()
    manager = Mock()
    manager.get_watermark.return_value = {"id": 1}
    writer = Mock()
    with pytest.raises(PipelineError, match="data_masker") as caught:
        execute_etl_pipeline(flow, "required-masker", baseline=ReadBaseline(),
            attempt=AttemptRuntimeInfo(), column_name_mode="snake", watermark_start={"id": 1},
            watermark_end=None, save_watermark=True, watermark_manager=manager, job_id="masker",
            create_source_reader=lambda _fmt: Reader(
                frame=pl.DataFrame({"id": [2], "email": ["synthetic@example.test"]}).lazy(),
                candidate={"id": 2}),
            create_transformer_pipeline=lambda **kwargs: build_transformer_pipeline(engine, **kwargs),
            create_destination_writer=writer, validate_watermark_storage=lambda *_args, **_kwargs: None)
    if failure == "load":
        assert caught.value.__cause__.__cause__ is cause
    writer.assert_not_called()
    manager.save_watermark.assert_not_called()


@pytest.mark.parametrize("configured", [False, True])
def test_complete_defaults_ignore_unrelated_failed_plugin_and_preserve_masking(monkeypatch, configured):
    import polars as pl
    from datacoolie.core.models.transform import Transform as TransformConfig
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.orchestration.execution.pipeline import DEFAULT_TRANSFORMERS, build_transformer_pipeline

    registry, _ = _isolated_transformer_registry(monkeypatch, broken="unused_external")
    flow = _dataflow()
    if configured:
        flow.transform = TransformConfig(masking_rules=[{"method": "nullify", "columns": ["email"]}])
    pipeline = build_transformer_pipeline(PolarsEngine(), column_name_mode="snake")
    assert len(pipeline.transformers) == len(DEFAULT_TRANSFORMERS)
    result = pipeline.transform(pl.DataFrame({"id": [2], "email": ["synthetic@example.test"]}).lazy(), flow)
    assert result.collect()["email"].to_list() == [None if configured else "synthetic@example.test"]
    assert not registry.is_available("unused_external")
    # Explicit custom pipelines do not implicitly select the broken default.
    registry.unregister("data_masker")
    from datacoolie.transformers.base import TransformerPipeline
    custom = TransformerPipeline(PolarsEngine())
    custom.transform(pl.DataFrame({"id": [2]}).lazy(), flow)
    assert custom.get_column_mapping().mapping == {"id": "id"}


@pytest.mark.parametrize("mapping, outputs", [
    (None, ["id"]),
    ({"id": None}, ["other"]),
    ({"id": "missing"}, ["other"]),
    ({"ID": "a", "Id": "b"}, ["a", "b"]),
    ({"id": "ID"}, ["Id", "iD"]),
])
def test_replacement_mapping_failure_cannot_create_writer_or_save(mapping, outputs):
    from datacoolie.transformers.base import ColumnMapping
    from unittest.mock import Mock

    class UnsafeTransform(Transform):
        def get_column_mapping(self):
            return None if mapping is None else ColumnMapping(mapping)

        def get_output_columns(self):
            return outputs

    manager = Mock()
    manager.get_watermark.return_value = {"id": 1}
    writer_factory = Mock(side_effect=AssertionError("guard must precede writer construction"))
    with pytest.raises(PipelineError):
        execute_etl_pipeline(
            _dataflow(replace=True, with_watermark=True), "unsafe-mapping",
            baseline=ReadBaseline(), attempt=AttemptRuntimeInfo(), column_name_mode="snake",
            watermark_start={"id": 1}, watermark_end=None, save_watermark=True,
            watermark_manager=manager, job_id="guard",
            create_source_reader=lambda _fmt: Reader(candidate={"id": 2}),
            create_transformer_pipeline=lambda **_kwargs: UnsafeTransform(),
            create_destination_writer=writer_factory,
            validate_watermark_storage=lambda *_args, **_kwargs: None,
        )
    writer_factory.assert_not_called()
    manager.save_watermark.assert_not_called()


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


@pytest.mark.parametrize("options", [{}, {"compact_options": {}, "cleanup_options": {}}])
def test_maintenance_attempt_has_destination_only_phase(options) -> None:
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
        **options,
    )

    assert result is attempt
    assert result.status == DataFlowStatus.SKIPPED.value
    assert result.destination is not None
    assert result.source is None
    assert result.transform is None

class BrokenMaintenanceDuration(DestinationRuntimeInfo):
    @property
    def duration_seconds(self):
        raise RuntimeError("diagnostic duration failed")


@pytest.mark.parametrize("status", ["succeeded", "skipped", "failed"])
@pytest.mark.parametrize("capture", ["duration", "copy", "observer"])
def test_maintenance_control_survives_failed_capture(monkeypatch, status, capture):
    from unittest.mock import Mock
    runtime_type = BrokenMaintenanceDuration if capture == "duration" else DestinationRuntimeInfo
    returned = runtime_type(status=status, message="operational evidence")
    if capture == "copy":
        class Uncopyable:
            def __deepcopy__(self, memo):
                raise RuntimeError("copy failed")
        returned.operation_details = Uncopyable()
    if capture == "observer":
        monkeypatch.setattr("datacoolie.orchestration.execution.pipeline._observe_runtime", lambda *a, **kw: None)
    writer = Mock()
    writer.run_maintenance.return_value = returned
    attempt = AttemptRuntimeInfo()
    kwargs = dict(baseline=ReadBaseline(), attempt=attempt,
                  create_destination_writer=lambda _: writer, retention_hours=168,
                  do_compact=True, do_cleanup=True)
    if status == "failed":
        with pytest.raises(PipelineError, match="operational evidence") as caught:
            execute_maintenance_pipeline(_dataflow(), **kwargs)
        assert str(caught.value.__cause__) == "operational evidence"
    else:
        assert execute_maintenance_pipeline(_dataflow(), **kwargs) is attempt
    assert attempt.status == attempt.destination.status == status
    assert attempt.message == attempt.destination.message == "operational evidence"
    assert attempt.destination.end_time is not None
    assert attempt.destination.rows_written is None
    writer.run_maintenance.assert_called_once()


@pytest.mark.parametrize("status", [None, "", "running", "pending", "invalid"])
def test_maintenance_typed_result_requires_terminal_status(status):
    from unittest.mock import Mock
    writer = Mock()
    writer.run_maintenance.return_value = DestinationRuntimeInfo(status=status)
    attempt = AttemptRuntimeInfo()
    with pytest.raises(PipelineError, match="terminal status"):
        execute_maintenance_pipeline(_dataflow(), baseline=ReadBaseline(), attempt=attempt,
            create_destination_writer=lambda _: writer, retention_hours=168,
            do_compact=True, do_cleanup=True)
    assert attempt.status == attempt.destination.status == "failed"
    assert attempt.destination.end_time is not None


def test_maintenance_untyped_return_acknowledges_success():
    from unittest.mock import Mock
    writer = Mock()
    writer.run_maintenance.return_value = None
    attempt = execute_maintenance_pipeline(_dataflow(), baseline=ReadBaseline(), attempt=AttemptRuntimeInfo(),
        create_destination_writer=lambda _: writer, retention_hours=168,
        do_compact=True, do_cleanup=True)
    assert attempt.status == attempt.destination.status == "succeeded"
    assert attempt.destination.end_time is not None

@pytest.mark.parametrize('status', ['succeeded', 'skipped', 'failed'])
@pytest.mark.parametrize('with_end', [False, True])
def test_maintenance_finalization_preserves_valid_fallback_timing(status, with_end):
    from datetime import timedelta
    from unittest.mock import Mock
    from datacoolie.utils.time import utc_now
    start = utc_now() + timedelta(days=1)
    writer = Mock()
    writer.run_maintenance.return_value = DestinationRuntimeInfo(status=status, message='work outcome',
        start_time=start, end_time=start + timedelta(seconds=5) if with_end else None)
    writer.get_runtime_info.return_value = writer.run_maintenance.return_value
    attempt = AttemptRuntimeInfo()
    kwargs = dict(baseline=ReadBaseline(), attempt=attempt,
        create_destination_writer=lambda _: writer, retention_hours=168,
        do_compact=True, do_cleanup=True)
    if status == 'failed':
        with pytest.raises(PipelineError, match='work outcome'):
            execute_maintenance_pipeline(_dataflow(), **kwargs)
    else:
        execute_maintenance_pipeline(_dataflow(), **kwargs)
    assert attempt.destination.end_time >= attempt.destination.start_time
    assert attempt.destination.duration_seconds >= 0
    assert attempt.status == attempt.destination.status == status
    assert attempt.destination.message == 'work outcome'

@pytest.mark.parametrize('status', ['succeeded', 'skipped', 'failed'])
@pytest.mark.parametrize('missing_start', [False, True])
def test_maintenance_enrichment_retains_pipeline_owned_phase(status, missing_start):
    from unittest.mock import Mock
    from tests.unit.orchestration.test_maintenance import ReadonlyMaintenanceRuntime
    returned = ReadonlyMaintenanceRuntime(status=status, message='work result', rows_written=5,
                                         operation_details=[{'action': 'compact'}],
                                         **({'start_time': None} if missing_start else {}))
    attempt = AttemptRuntimeInfo()
    owned = []
    writer = Mock()
    def work(**kwargs):
        owned.append(attempt.destination)
        return returned
    writer.run_maintenance.side_effect = work
    writer.get_runtime_info.return_value = returned
    kwargs = dict(baseline=ReadBaseline(), attempt=attempt, create_destination_writer=lambda _: writer,
                  retention_hours=168, do_compact=True, do_cleanup=True)
    if status == 'failed':
        with pytest.raises(PipelineError, match='work result') as caught:
            execute_maintenance_pipeline(_dataflow(), **kwargs)
        assert str(caught.value.__cause__) == 'work result'
    else:
        assert execute_maintenance_pipeline(_dataflow(), **kwargs) is attempt
    assert attempt.destination is owned[0]
    assert type(attempt.destination) is DestinationRuntimeInfo
    assert attempt.status == attempt.destination.status == status
    assert attempt.destination.message == 'work result'
    assert attempt.destination.rows_written == 5
    assert attempt.destination.operation_type == 'maintenance'
    assert attempt.destination.start_time is not None
    assert attempt.destination.duration_seconds >= 0
    assert attempt.destination.operation_details == [{'action': 'compact'}]
    assert attempt.destination.operation_details is not returned.operation_details
    attempt.destination.operation_details[0]['action'] = 'changed'
    assert returned.operation_details == [{'action': 'compact'}]


def test_maintenance_error_preserves_cause_with_readonly_observer():
    from unittest.mock import Mock
    from tests.unit.orchestration.test_maintenance import ReadonlyMaintenanceRuntime
    cause = RuntimeError('operational error')
    writer = Mock()
    writer.run_maintenance.side_effect = cause
    writer.get_runtime_info.return_value = ReadonlyMaintenanceRuntime(status='succeeded', message='diagnostic message')
    attempt = AttemptRuntimeInfo()
    with pytest.raises(PipelineError, match='operational error') as caught:
        execute_maintenance_pipeline(_dataflow(), baseline=ReadBaseline(), attempt=attempt,
            create_destination_writer=lambda _: writer, retention_hours=168,
            do_compact=True, do_cleanup=True)
    assert caught.value.__cause__ is cause
    assert attempt.status == attempt.destination.status == 'failed'
    assert attempt.destination.message == 'operational error'
    assert type(attempt.destination) is DestinationRuntimeInfo
