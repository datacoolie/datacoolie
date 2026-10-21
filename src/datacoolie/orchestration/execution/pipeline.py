"""Single-attempt sequencing; operational results drive execution, runtimes observe."""

from __future__ import annotations

import copy
import logging
from dataclasses import fields
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from datacoolie.core.constants import DataFlowStatus, ExecutionType, Format
from datacoolie.core.exceptions import PipelineError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.engines.base import BaseEngine
from datacoolie.engines.contracts.maintenance import default_options
from datacoolie.logging.runtime.diagnostics import emit_safely
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.orchestration.execution.read_baseline import ReadBaseline
from datacoolie.orchestration.execution.runtime import AttemptRuntimeInfo
from datacoolie.orchestration.execution.window import build_watermark_window, map_watermark_window
from datacoolie.sources.contracts import SourceReadRequest, SourceReadRange, SourceReadResult
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.transformers.base import TransformerPipeline
from datacoolie.transformers.runtime import TransformRuntimeInfo
from datacoolie.utils.time import utc_now
from datacoolie.watermark.base import BaseWatermarkManager, WatermarkSerializer
from datacoolie.watermark.contracts import WatermarkLoadStatus, WatermarkSaveStatus
from datacoolie.watermark.runtime import WatermarkRuntimeInfo

logger = get_logger(__name__)

DEFAULT_TRANSFORMERS = [
    "column_value_transformer", "schema_converter", "hash_column_adder",
    "deduplicator", "column_adder", "row_filter", "scd2_column_adder",
    "system_column_adder", "partition_handler", "data_masker", "column_projector",
    "column_name_sanitizer",
]


def _require_callable_contract(component, component_name, method_names):
    for name in method_names:
        if not callable(getattr(component, name, None)):
            raise PipelineError(f"{component_name} ({type(component).__name__}) must expose callable {name}()")


def _observe_runtime(component, expected_type, component_name, *, fallback=None, runtime_value=None):
    try:
        value = runtime_value if runtime_value is not None else component.get_runtime_info()
        if not isinstance(value, expected_type):
            raise TypeError(f"{component_name}.get_runtime_info() must return {expected_type.__name__}")
        try:
            snapshot = copy.deepcopy(value)
        except Exception:
            snapshot = expected_type()
            for item in fields(snapshot):
                try:
                    setattr(snapshot, item.name, copy.deepcopy(getattr(value, item.name)))
                except Exception:
                    continue
        for item in fields(snapshot):
            if item.name.startswith(("rows_", "files_", "bytes_")):
                measured = getattr(snapshot, item.name)
                if measured is not None and (type(measured) is not int or measured < 0):
                    setattr(snapshot, item.name, None)
                    emit_safely(logger, logging.WARNING, "%s invalid metric %s omitted",
                                component_name, item.name, catch_base=True)
        return _sanitize_timing(snapshot, fallback)
    except BaseException:
        emit_safely(logger, logging.WARNING, "%s runtime capture unavailable", component_name,
                    exc_info=True, catch_base=True)
        return None


def _sanitize_timing(snapshot, fallback):
    """Optional diagnostics cannot replace valid phase timing with bad values."""
    fallback = fallback if fallback is not None else type(snapshot)()
    for name in ("start_time", "end_time"):
        value = getattr(snapshot, name)
        if value is not None and (
            not isinstance(value, datetime)
            or value.tzinfo is None or value.utcoffset() is None
        ):
            setattr(snapshot, name, getattr(fallback, name))
    try:
        duration = snapshot.duration_seconds
        if duration is not None and duration < 0:
            raise ValueError("negative diagnostic duration")
    except (TypeError, ValueError, OverflowError):
        snapshot.start_time = fallback.start_time
        snapshot.end_time = fallback.end_time
    return snapshot


def _finish_phase(phase, *, status, error=None):
    phase.status = status
    try:
        phase.end_time = utc_now()
    except Exception:
        emit_safely(logger, logging.WARNING, "Phase end-time capture unavailable",
                    exc_info=True, catch_base=True)
    if error is not None:
        phase.message = str(error) or type(error).__name__


def _enrich_maintenance_phase(phase, writer, *, runtime_value=None):
    """Copy optional fields into the pipeline phase without adopting custom runtimes."""
    try:
        observed = _observe_runtime(writer, DestinationRuntimeInfo, "maintenance",
                                    fallback=phase, runtime_value=runtime_value)
        if observed is None:
            return
        # Control fields and completed end timing remain pipeline-owned. Stage
        # fields on a standard runtime: even a successful deepcopy can be readonly.
        staged = DestinationRuntimeInfo(start_time=phase.start_time, end_time=phase.end_time)
        diagnostic_fields = [item.name for item in fields(staged)
                             if item.name not in ("status", "message", "operation_type", "end_time")]
        for name in diagnostic_fields:
            try:
                value = copy.deepcopy(getattr(observed, name))
                if name == "start_time" and value is None:
                    continue
                setattr(staged, name, value)
            except Exception:
                continue
        _sanitize_timing(staged, phase)
        for name in diagnostic_fields:
            setattr(phase, name, getattr(staged, name))
    except BaseException:
        emit_safely(logger, logging.WARNING, "Maintenance runtime enrichment unavailable",
                    exc_info=True, catch_base=True)


def build_source_reader(engine: BaseEngine, fmt: str, *, allowed_prefixes=None):
    from datacoolie import source_registry
    kwargs = {"engine": engine}
    if fmt == Format.FUNCTION.value and allowed_prefixes:
        kwargs["allowed_prefixes"] = allowed_prefixes
    return source_registry.get(fmt, **kwargs)


def build_transformer_pipeline(engine: BaseEngine, *, dataflow_run_id=None, column_name_mode):
    from datacoolie import transformer_registry
    pipeline = TransformerPipeline(engine)
    kwargs = {"column_name_sanitizer": {"mode": column_name_mode},
              "system_column_adder": {"dataflow_run_id": dataflow_run_id}}
    for name in DEFAULT_TRANSFORMERS:
        pipeline.add_transformer(transformer_registry.get(name, engine=engine, **kwargs.get(name, {})))
    return pipeline


def build_destination_writer(engine: BaseEngine, fmt: str):
    from datacoolie import destination_registry
    return destination_registry.get(fmt, engine=engine)


def execute_etl_pipeline(
    dataflow: DataFlow, dataflow_run_id: str, *,
    baseline: ReadBaseline, attempt: AttemptRuntimeInfo,
    column_name_mode: Any, watermark_start: Optional[Dict[str, Any]],
    watermark_end: Optional[Dict[str, Any]], save_watermark: bool,
    watermark_start_operator=None, watermark_end_operator=None,
    read_range: Optional[SourceReadRange] = None,
    watermark_manager: BaseWatermarkManager | None, job_id: str,
    create_source_reader: Callable, create_transformer_pipeline: Callable,
    create_destination_writer: Callable, validate_watermark_storage: Callable,
) -> AttemptRuntimeInfo:
    """Populate the lifecycle-owned attempt and return that same observation."""
    state = WatermarkRuntimeInfo(
        save_status=WatermarkSaveStatus.NOT_ATTEMPTED if save_watermark and watermark_manager
        else WatermarkSaveStatus.NOT_REQUESTED,
    )
    attempt.watermark = state
    active = None
    component = None
    prepared = None
    try:
        reader = create_source_reader(dataflow.source.connection.format)
        _require_callable_contract(reader, "source reader", ("read", "validate_watermark", "merge_watermark"))
        if read_range is not None and any(value is not None for value in (
            watermark_start, watermark_end, watermark_start_operator, watermark_end_operator,
        )):
            raise PipelineError("read_range cannot be combined with explicit watermark bounds")
        if watermark_manager is not None:
            _require_callable_contract(watermark_manager, "watermark manager", ("get_watermark", "save_watermark", "serialize"))
        lower = watermark_start
        upper = watermark_end
        if lower is None and watermark_manager is not None and dataflow.source.has_watermark_state and read_range is None:
            reused = baseline.loaded
            try:
                lower = baseline.resolve(lambda: watermark_manager.get_watermark(dataflow_id=dataflow.dataflow_id))
            except Exception:
                state.load_status = WatermarkLoadStatus.LOAD_ERROR
                raise
            state.load_status = WatermarkLoadStatus.REUSED if reused else WatermarkLoadStatus.LOADED
            state.loaded_watermark = copy.deepcopy(lower)
        for value, boundary in ((lower, "lower bound"), (upper, "upper bound")):
            if value:
                reader.validate_watermark(value, boundary=boundary)
        request = SourceReadRequest(
            reference_time=baseline.reference_time, watermark_start=lower, watermark_end=upper,
            watermark_start_operator=watermark_start_operator,
            watermark_end_operator=watermark_end_operator, read_range=read_range,
        )
        active = "source"
        component = reader
        attempt.source = SourceRuntimeInfo(status=DataFlowStatus.RUNNING.value)
        result = reader.read(dataflow.source, request=request)
        if not isinstance(result, SourceReadResult):
            raise PipelineError("source reader.read() must return SourceReadResult")
        if isinstance(result.rows_read, bool) or not isinstance(result.rows_read, int) or result.rows_read < 0:
            raise PipelineError("successful source rows_read must be a nonnegative integer")
        if result.frame is None and result.rows_read != 0:
            raise PipelineError("source frame=None requires rows_read=0")
        # Operational result supplies authoritative counts even when its optional
        # diagnostic projection is malformed or cannot be detached.
        if isinstance(result.runtime, SourceRuntimeInfo):
            try:
                attempt.source = _sanitize_timing(copy.deepcopy(result.runtime), attempt.source)
            except Exception:
                pass
        attempt.source.rows_read = result.rows_read
        attempt.source.selection = copy.deepcopy(result.selection)
        if result.candidate is not None:
            # Candidate validity is a control contract, even when persistence
            # is disabled. A log-only codec failure must not follow a write.
            WatermarkSerializer.serialize(result.candidate.values)
        _finish_phase(attempt.source, status=DataFlowStatus.SUCCEEDED.value)
        active = None
        state.candidate = copy.deepcopy(result.candidate)
        if result.frame is None or result.rows_read == 0:
            attempt.status = DataFlowStatus.SKIPPED.value
            attempt.message = "Source returned no data to process"
            return attempt

        selection = result.selection
        if dataflow.destination.replace_by_watermark and selection.combine_operator != "OR":
            raise PipelineError("Watermark replacement supports only combine_operator='OR'")
        candidate = result.candidate
        values = candidate.values if candidate else None
        kind = candidate.kind if candidate else None
        # Explicit bounds authorize their exact interval. Generated ceilings
        # constrain candidates but cannot widen observed maxima replacement.
        explicit_upper = selection.upper if selection.upper_origin == "explicit" else None
        window = build_watermark_window(
            enabled=dataflow.destination.replace_by_watermark,
            watermark_effective=selection.lower, watermark_after=values,
            explicit_start=None, explicit_end=explicit_upper,
            start_operator=selection.lower_operator, end_operator=selection.upper_operator,
            watermark_kind=kind,
        )
        if save_watermark and watermark_manager:
            prepared = copy.deepcopy(values)
            if prepared and not any(value is not None for value in prepared.values()):
                prepared = None
            if prepared:
                existing = watermark_manager.get_watermark(dataflow_id=dataflow.dataflow_id)
                state.merge_base = copy.deepcopy(existing)
                prepared = reader.merge_watermark(existing, candidate)
                state.watermark_to_save = copy.deepcopy(prepared)
                reader.validate_watermark(prepared, boundary="new watermark")
                watermark_manager.serialize(prepared)
                validate_watermark_storage(dataflow, operation_type=ExecutionType.ETL.value,
                                           watermark_start=watermark_start,
                                           watermark_end=prepared, save_watermark=save_watermark)
            else:
                state.save_status = WatermarkSaveStatus.NO_CANDIDATE

        pipeline = create_transformer_pipeline(dataflow_run_id=dataflow_run_id, column_name_mode=column_name_mode)
        active, component = "transform", pipeline
        attempt.transform = TransformRuntimeInfo(status=DataFlowStatus.RUNNING.value)
        frame = pipeline.transform(result.frame, dataflow)
        enriched = _observe_runtime(pipeline, TransformRuntimeInfo, "transformer pipeline", fallback=attempt.transform)
        if enriched:
            attempt.transform = enriched
        _finish_phase(attempt.transform, status=DataFlowStatus.SUCCEEDED.value)
        active = None
        window = map_watermark_window(window, column_mapping=pipeline.get_column_mapping(),
                                      output_columns=pipeline.get_output_columns())
        writer = create_destination_writer(dataflow.destination.connection.format)
        _require_callable_contract(writer, "destination writer", ("write",))
        active, component = "destination", writer
        attempt.destination = DestinationRuntimeInfo(status=DataFlowStatus.RUNNING.value)
        writer.write(frame, dataflow, watermark_window=window)
        # Return acknowledges business work. Diagnostic enrichment is isolated.
        active = None
        _finish_phase(attempt.destination, status=DataFlowStatus.SUCCEEDED.value)
        enriched = _observe_runtime(writer, DestinationRuntimeInfo, "destination writer", fallback=attempt.destination)
        if enriched:
            attempt.destination = enriched
            _finish_phase(attempt.destination, status=DataFlowStatus.SUCCEEDED.value)
        if prepared and watermark_manager:
            try:
                watermark_manager.save_watermark(dataflow_id=dataflow.dataflow_id, watermark=prepared,
                                                 job_id=job_id, dataflow_run_id=dataflow_run_id)
            except Exception:
                state.save_status = WatermarkSaveStatus.SAVE_ERROR
                raise
            state.save_status = WatermarkSaveStatus.SAVED
            state.saved_watermark = copy.deepcopy(prepared)
        attempt.status = DataFlowStatus.SUCCEEDED.value
        return attempt
    except Exception as exc:
        if active is not None:
            phase = getattr(attempt, active)
            evidence = getattr(exc, "runtime_info", None)
            if isinstance(evidence, type(phase)):
                try:
                    phase = _sanitize_timing(copy.deepcopy(evidence), phase)
                except Exception:
                    pass
            elif active != "source":
                enriched = _observe_runtime(component, type(phase), active, fallback=phase)
                if enriched:
                    phase = enriched
            _finish_phase(phase, status=DataFlowStatus.FAILED.value, error=exc)
            setattr(attempt, active, phase)
        attempt.status = DataFlowStatus.FAILED.value
        attempt.message = str(exc) or type(exc).__name__
        raise PipelineError(attempt.message, attempt=attempt) from exc


def execute_maintenance_pipeline(
    dataflow: DataFlow, *, baseline: ReadBaseline, attempt: AttemptRuntimeInfo,
    create_destination_writer: Callable, retention_hours: int,
    do_compact: bool, do_cleanup: bool,
    prepared_maintenance=None, compact_options=None, cleanup_options=None,
) -> AttemptRuntimeInfo:
    writer = None
    try:
        writer = create_destination_writer(dataflow.destination.connection.format)
        _require_callable_contract(writer, "destination writer", ("run_maintenance",))
        attempt.destination = DestinationRuntimeInfo(
            status=DataFlowStatus.RUNNING.value, operation_type=ExecutionType.MAINTENANCE.value,
        )
        kwargs = dict(dataflow=dataflow, do_compact=do_compact,
                      do_cleanup=do_cleanup, retention_hours=retention_hours)
        compact_options, cleanup_options = default_options(compact_options), default_options(cleanup_options)
        if prepared_maintenance is not None or compact_options is not None or cleanup_options is not None:
            _require_callable_contract(writer, "destination writer", ("run_selected_maintenance",))
            returned = writer.run_selected_maintenance(**kwargs, prepared=prepared_maintenance,
                        compact_options=compact_options, cleanup_options=cleanup_options)
        else:
            returned = writer.run_maintenance(**kwargs)
        # Retain control evidence before optional copy/timing/metric capture.
        status = DataFlowStatus.SUCCEEDED.value
        message = None
        if isinstance(returned, DestinationRuntimeInfo):
            status, message = returned.status, returned.message
            if status not in (
                DataFlowStatus.SUCCEEDED.value,
                DataFlowStatus.SKIPPED.value,
                DataFlowStatus.FAILED.value,
            ):
                raise PipelineError("maintenance result must have a terminal status")
        _finish_phase(attempt.destination, status=status)
        attempt.destination.message = message
        if isinstance(returned, DestinationRuntimeInfo):
            _enrich_maintenance_phase(attempt.destination, writer, runtime_value=returned)
        if status == DataFlowStatus.FAILED.value:
            raise PipelineError(message or "Maintenance failed")
        attempt.status = status
        attempt.message = message
        if status == DataFlowStatus.SKIPPED.value and not message:
            attempt.message = "Maintenance performed no operations"
        return attempt
    except Exception as exc:
        if attempt.destination is not None:
            _finish_phase(attempt.destination, status=DataFlowStatus.FAILED.value, error=exc)
            if writer is not None:
                _enrich_maintenance_phase(attempt.destination, writer)
        attempt.status = DataFlowStatus.FAILED.value
        attempt.message = str(exc) or type(exc).__name__
        raise PipelineError(str(exc) or type(exc).__name__, attempt=attempt) from exc
