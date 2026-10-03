"""Single-attempt ETL and maintenance pipeline bodies.

The functions here consume already-prepared dataflow copies.  They own backend
I/O sequencing and partial runtime capture, but not preparation, retry policy,
outer scheduling or final execution logging.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, TypeVar

from datacoolie.core.constants import DataFlowStatus, ExecutionType, Format
from datacoolie.core.exceptions import PipelineError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DestinationRuntimeInfo, PipelineAttemptResult, SourceRuntimeInfo, TransformRuntimeInfo
from datacoolie.destinations import BaseDestinationWriter
from datacoolie.engines.base import BaseEngine
from datacoolie.watermark.base import BaseWatermarkManager
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.sources import BaseSourceReader, SourceReadRange
from datacoolie.transformers import TransformerPipeline
from datacoolie.orchestration.execution.window import (
    build_watermark_window,
    map_watermark_window,
)
from datacoolie.utils.time import utc_now

logger = get_logger(__name__)


_RuntimeT = TypeVar("_RuntimeT", SourceRuntimeInfo, TransformRuntimeInfo, DestinationRuntimeInfo)


DEFAULT_TRANSFORMERS: list[str] = [
    "column_value_transformer",
    "schema_converter",
    "hash_column_adder",
    "deduplicator",
    "column_adder",
    "row_filter",
    "scd2_column_adder",
    "system_column_adder",
    "partition_handler",
    "data_masker",
    "column_projector",
    "column_name_sanitizer",
]


def _validate_watermark_boundary(
    reader: Any,
    watermark: Optional[Dict[str, Any]],
    *,
    boundary: str,
) -> None:
    """Validate a lower/upper/candidate watermark before side effects."""

    if not watermark:
        return

    reader._validate_watermark_comparison(watermark, boundary=boundary)


def _require_callable_contract(
    component: Any,
    component_name: str,
    method_names: tuple[str, ...],
) -> None:
    """Reject incomplete structural test/plugin doubles before side effects.

    The lookup is intentionally confined to this injection boundary.  Once a
    component passes the contract check, the pipeline uses direct method calls
    so required framework hooks remain visible and fail loudly if a component
    violates the contract later.
    """

    type_name = type(component).__name__
    for method_name in method_names:
        try:
            method = getattr(component, method_name)
        except Exception as exc:  # noqa: BLE001
            raise PipelineError(
                f"{component_name} ({type_name}) must expose callable "
                f"{method_name}()"
            ) from exc
        if not callable(method):
            raise PipelineError(
                f"{component_name} ({type_name}) must expose callable "
                f"{method_name}()"
            )


def _runtime_info(
    component: Any,
    expected_type: type[_RuntimeT],
    component_name: str,
) -> _RuntimeT:
    """Read and validate one phase runtime object."""

    value = component.get_runtime_info()
    if not isinstance(value, expected_type):
        raise PipelineError(
            f"{component_name}.get_runtime_info() must return "
            f"{expected_type.__name__}"
        )
    return value


def _safe_runtime_capture(
    component: Any,
    expected_type: type[_RuntimeT],
    *,
    phase_started: bool,
    runtime_attempted: bool,
) -> Optional[_RuntimeT]:
    """Capture only a valid runtime without masking the primary exception."""

    if component is None or not phase_started or runtime_attempted:
        return None
    try:
        value = component.get_runtime_info()
    except Exception:  # noqa: BLE001
        return None
    return value if isinstance(value, expected_type) else None


def build_source_reader(
    engine: BaseEngine,
    fmt: str,
    *,
    allowed_prefixes: Optional[list[str]] = None,
) -> BaseSourceReader:
    """Build a registered reader without exposing registry mechanics to Driver."""
    from datacoolie import source_registry

    kwargs: Dict[str, Any] = {"engine": engine}
    if fmt == Format.FUNCTION.value and allowed_prefixes:
        kwargs["allowed_prefixes"] = allowed_prefixes
    return source_registry.get(fmt, **kwargs)


def build_transformer_pipeline(
    engine: BaseEngine,
    *,
    dataflow_run_id: Optional[str] = None,
    column_name_mode: Any,
) -> TransformerPipeline:
    """Build the default registered transformer sequence."""
    from datacoolie import transformer_registry

    pipeline = TransformerPipeline(engine)
    extra_kwargs: dict[str, dict[str, object]] = {
        "column_name_sanitizer": {"mode": column_name_mode},
        "system_column_adder": {"dataflow_run_id": dataflow_run_id},
    }
    for name in DEFAULT_TRANSFORMERS:
        if transformer_registry.is_available(name):
            pipeline.add_transformer(
                transformer_registry.get(
                    name,
                    engine=engine,
                    **extra_kwargs.get(name, {}),
                )
            )
    return pipeline


def build_destination_writer(
    engine: BaseEngine,
    fmt: str,
) -> BaseDestinationWriter:
    """Build a registered destination writer."""
    from datacoolie import destination_registry

    return destination_registry.get(fmt, engine=engine)


def execute_etl_pipeline(
    dataflow: DataFlow,
    dataflow_run_id: str,
    *,
    column_name_mode: Any,
    watermark_start: Optional[Dict[str, Any]],
    watermark_end: Optional[Dict[str, Any]],
    save_watermark: bool,
    watermark_start_operator: Optional[str] = None,
    watermark_end_operator: Optional[str] = None,
    read_range: Optional[SourceReadRange] = None,
    watermark_manager: BaseWatermarkManager | None,
    job_id: str,
    create_source_reader: Callable[[str], BaseSourceReader],
    create_transformer_pipeline: Callable[..., TransformerPipeline],
    create_destination_writer: Callable[[str], BaseDestinationWriter],
    validate_watermark_storage: Callable[..., None],
) -> PipelineAttemptResult:
    """Run one read-transform-write attempt and return partial phase state."""
    logger.debug(
        "Starting %s: %s → %s",
        dataflow.name,
        dataflow.source.full_table_name or dataflow.source.path,
        dataflow.destination.full_table_name or dataflow.destination.path,
    )

    # Replay adapters may retain legacy bound fields for observability while
    # carrying an authoritative SourceReadRange. Never forward both to the
    # public reader contract.
    requested_watermark_start = None if read_range is not None else watermark_start
    requested_watermark_end = None if read_range is not None else watermark_end
    watermark = requested_watermark_start

    source_runtime: Optional[SourceRuntimeInfo] = None
    transform_runtime: Optional[TransformRuntimeInfo] = None
    destination_runtime: Optional[DestinationRuntimeInfo] = None
    reader: Optional[BaseSourceReader] = None
    pipeline: Optional[TransformerPipeline] = None
    writer: Optional[BaseDestinationWriter] = None
    watermark_to_save: Optional[Dict[str, Any]] = None
    source_phase_started = False
    source_runtime_attempted = False
    transform_phase_started = False
    transform_runtime_attempted = False
    destination_phase_started = False
    destination_runtime_attempted = False

    try:
        reader = create_source_reader(dataflow.source.connection.format)
        _require_callable_contract(
            reader,
            "source reader",
            (
                "read",
                "get_runtime_info",
                "get_new_watermark",
                "_validate_watermark_comparison",
                "merge_watermark",
            ),
        )
        if watermark_manager is not None:
            _require_callable_contract(
                watermark_manager,
                "watermark manager",
                ("get_watermark", "save_watermark", "serialize"),
            )

        # Capability validation must precede checkpoint I/O.  A partially
        # implemented reader/manager is a configuration error, not a reason
        # to touch persisted state before failing.
        if (
            watermark is None
            and watermark_manager is not None
            and dataflow.source.has_watermark_state
            and read_range is None
        ):
            watermark = watermark_manager.get_watermark(
                dataflow_id=dataflow.dataflow_id
            )

        _validate_watermark_boundary(reader, watermark, boundary="lower bound")
        _validate_watermark_boundary(
            reader, requested_watermark_end, boundary="upper bound"
        )
        read_kwargs: Dict[str, Any] = {"watermark_end": requested_watermark_end}
        if watermark_start_operator is not None:
            read_kwargs["watermark_start_operator"] = watermark_start_operator
        if watermark_end_operator is not None:
            read_kwargs["watermark_end_operator"] = watermark_end_operator
        if read_range is not None:
            read_kwargs["read_range"] = read_range
        if dataflow.destination.replace_by_watermark and (
            requested_watermark_end is not None or read_range is not None
        ):
            read_kwargs["preserve_empty"] = True
        source_phase_started = True
        frame = reader.read(
            dataflow.source,
            watermark,
            **read_kwargs,
        )
        source_runtime_attempted = True
        source_runtime = _runtime_info(
            reader,
            SourceRuntimeInfo,
            "source reader",
        )

        # The replacement scope belongs to this execution attempt.  Keep it
        # out of ``DataFlow`` so metadata remains immutable and a retry cannot
        # accidentally reuse a prior read's bounds.
        watermark_window = build_watermark_window(
            enabled=dataflow.destination.replace_by_watermark,
            watermark_effective=source_runtime.watermark_effective,
            watermark_after=source_runtime.watermark_after,
            explicit_start=(
                {read_range.column: read_range.start}
                if read_range is not None
                else requested_watermark_start
            ),
            explicit_end=(
                {read_range.column: read_range.end}
                if read_range is not None
                else requested_watermark_end
            ),
            start_operator=(
                read_range.lower_operator
                if read_range is not None
                else (
                    watermark_start_operator
                    if watermark_start_operator is not None
                    else source_runtime.watermark_start_operator
                )
            ),
            end_operator=(
                read_range.upper_operator
                if read_range is not None
                else (
                    watermark_end_operator
                    if watermark_end_operator is not None
                    else source_runtime.watermark_end_operator
                )
            ),
            watermark_kind=source_runtime.watermark_kind,
        )

        # A normal empty read is a no-op.  An explicitly bounded replay is
        # different: an empty source window can still mean that rows removed
        # from the source must be removed from the destination window.
        confirmed_empty_replay = (
            frame is not None
            and source_runtime.rows_read == 0
            and (requested_watermark_end is not None or read_range is not None)
        )

        if frame is None or (source_runtime.rows_read == 0 and not confirmed_empty_replay):
            logger.debug("No data to process")
            return PipelineAttemptResult(
                status=DataFlowStatus.SKIPPED.value,
                source=source_runtime,
                message="Source returned no data to process",
            )

        if save_watermark and watermark_manager:
            # Persist the source observation, never the requested upper bound.
            # A replay range can be independent of the source watermark, and
            # the bound may not represent the maximum value actually read.
            watermark_to_save = reader.get_new_watermark()
            # A nonempty dict of null maxima is not a source observation.
            # Decide before merging so existing state cannot authorize a save.
            if watermark_to_save and not any(
                value is not None for value in watermark_to_save.values()
            ):
                watermark_to_save = None
            if watermark_to_save:
                existing_watermark = watermark_manager.get_watermark(
                    dataflow_id=dataflow.dataflow_id
                )
                # Ordering and opaque-token semantics belong to the reader
                # that produced the observation. The manager never performs a
                # second, generic merge.
                watermark_to_save = reader.merge_watermark(
                    existing_watermark,
                    watermark_to_save,
                )
                _validate_watermark_boundary(
                    reader, watermark_to_save, boundary="new watermark"
                )
                # Prepare the exact checkpoint representation before any
                # destination mutation.  Storage validation alone cannot
                # catch unsupported scalar values such as a non-finite
                # Decimal, which would otherwise fail after a successful
                # write and make a retry duplicate data.
                watermark_manager.serialize(watermark_to_save)
                validate_watermark_storage(
                    dataflow,
                    operation_type=ExecutionType.ETL.value,
                    watermark_start=requested_watermark_start,
                    watermark_end=watermark_to_save,
                    save_watermark=save_watermark,
                )

        pipeline = create_transformer_pipeline(
            dataflow_run_id=dataflow_run_id,
            column_name_mode=column_name_mode,
        )
        transform_phase_started = True
        frame = pipeline.transform(frame, dataflow)
        transform_runtime_attempted = True
        transform_runtime = _runtime_info(
            pipeline,
            TransformRuntimeInfo,
            "transformer pipeline",
        )
        watermark_window = map_watermark_window(
            watermark_window,
            column_mapping=pipeline.get_column_mapping(),
            output_columns=pipeline.get_output_columns(),
        )

        writer = create_destination_writer(dataflow.destination.connection.format)
        _require_callable_contract(
            writer,
            "destination writer",
            ("write", "get_runtime_info"),
        )
        destination_phase_started = True
        writer.write(frame, dataflow, watermark_window=watermark_window)
        destination_runtime_attempted = True
        destination_runtime = _runtime_info(
            writer,
            DestinationRuntimeInfo,
            "destination writer",
        )

        if watermark_to_save and watermark_manager:
            watermark_manager.save_watermark(
                dataflow_id=dataflow.dataflow_id,
                watermark=watermark_to_save,
                job_id=job_id,
                dataflow_run_id=dataflow_run_id,
            )
    except Exception as exc:
        if source_runtime is None:
            source_runtime = _safe_runtime_capture(
                reader,
                SourceRuntimeInfo,
                phase_started=source_phase_started,
                runtime_attempted=source_runtime_attempted,
            )
        if transform_runtime is None:
            transform_runtime = _safe_runtime_capture(
                pipeline,
                TransformRuntimeInfo,
                phase_started=transform_phase_started,
                runtime_attempted=transform_runtime_attempted,
            )
        if destination_runtime is None:
            destination_runtime = _safe_runtime_capture(
                writer,
                DestinationRuntimeInfo,
                phase_started=destination_phase_started,
                runtime_attempted=destination_runtime_attempted,
            )
        raise PipelineError(
            str(exc) or type(exc).__name__,
            partial_result=PipelineAttemptResult(
                status=DataFlowStatus.FAILED.value,
                source=source_runtime,
                transform=transform_runtime,
                destination=destination_runtime,
            ),
        ) from exc

    logger.debug(
        "Complete — Read: %d, Written: %d",
        source_runtime.rows_read if source_runtime else 0,
        destination_runtime.rows_written if destination_runtime else 0,
    )
    return PipelineAttemptResult(
        status=DataFlowStatus.SUCCEEDED.value,
        source=source_runtime,
        transform=transform_runtime,
        destination=destination_runtime,
    )


def execute_maintenance_pipeline(
    dataflow: DataFlow,
    *,
    create_destination_writer: Callable[[str], BaseDestinationWriter],
    retention_hours: int,
    do_compact: bool,
    do_cleanup: bool,
) -> PipelineAttemptResult:
    """Run one maintenance attempt and preserve destination partial state."""
    logger.debug(
        "Starting maintenance: %s",
        dataflow.destination.full_table_name or dataflow.destination.path,
    )
    destination_runtime: Optional[DestinationRuntimeInfo] = None
    writer: Optional[BaseDestinationWriter] = None
    destination_phase_started = False
    destination_runtime_attempted = False
    try:
        writer = create_destination_writer(dataflow.destination.connection.format)
        _require_callable_contract(
            writer,
            "destination writer",
            ("run_maintenance", "get_runtime_info"),
        )
        destination_phase_started = True
        candidate = writer.run_maintenance(
            dataflow=dataflow,
            do_compact=do_compact,
            do_cleanup=do_cleanup,
            retention_hours=retention_hours,
        )
        destination_runtime_attempted = True
        if not isinstance(candidate, DestinationRuntimeInfo):
            raise PipelineError(
                "destination writer.run_maintenance() must return "
                "DestinationRuntimeInfo"
            )
        destination_runtime = candidate
    except Exception as exc:
        if destination_runtime is None:
            candidate = _safe_runtime_capture(
                writer,
                DestinationRuntimeInfo,
                phase_started=destination_phase_started,
                runtime_attempted=destination_runtime_attempted,
            )
        else:
            candidate = destination_runtime
        if candidate is not None:
            if candidate.operation_type == ExecutionType.MAINTENANCE.value:
                if candidate.status == DataFlowStatus.RUNNING.value:
                    candidate.end_time = utc_now()
                    candidate.status = DataFlowStatus.FAILED.value
                    candidate.message = str(exc) or type(exc).__name__
                destination_runtime = candidate
        raise PipelineError(
            str(exc) or type(exc).__name__,
            partial_result=PipelineAttemptResult(
                status=DataFlowStatus.FAILED.value,
                destination=destination_runtime,
            ),
        ) from exc

    message = None
    if destination_runtime.status == DataFlowStatus.SKIPPED.value:
        if not do_compact and not do_cleanup:
            message = "No maintenance operations enabled"
        else:
            skip_messages = [
                entry["message"]
                for entry in destination_runtime.operation_details
                if entry.get("status") == DataFlowStatus.SKIPPED.value
                and entry.get("message")
            ]
            message = (
                "; ".join(dict.fromkeys(skip_messages))
                or "Maintenance performed no operations"
            )

    attempt_result = PipelineAttemptResult(
        status=destination_runtime.status,
        destination=destination_runtime,
        message=message,
    )
    if attempt_result.status == DataFlowStatus.FAILED.value:
        raise PipelineError(
            destination_runtime.message or "Maintenance failed",
            partial_result=attempt_result,
        )

    logger.debug(
        "Maintenance %s — files_added=%d, files_removed=%d, bytes_added=%d, bytes_removed=%d, duration=%.1fs",
        destination_runtime.status,
        destination_runtime.files_added,
        destination_runtime.files_removed,
        destination_runtime.bytes_added,
        destination_runtime.bytes_removed,
        destination_runtime.duration_seconds,
    )
    return attempt_result
