"""Replay coordination for bounded, sequential chunk execution."""

from __future__ import annotations

import copy
import logging
from typing import Callable, List, Optional

from datacoolie.core.constants import ColumnCaseMode, DataFlowStatus, ExecutionType, Format
from datacoolie.core.exceptions import DataCoolieError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.orchestration.execution.runtime import DataFlowRuntimeInfo
from datacoolie.orchestration.execution.runtime import ReplayRuntimeInfo
from datacoolie.core.models.metrics import MetricAccumulator
from datacoolie.core.models.run_config import ReplayConfig
from datacoolie.logging.configuration.constants import LogEvent
from datacoolie.logging.runtime.context import dataflow_context
from datacoolie.logging.runtime.diagnostics import emit_safely
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.orchestration.execution.lifecycle import (
    format_dataflow_message,
    finish_inactive_execution,
    log_result_safely,
    execution_observation_attempted,
)
from datacoolie.orchestration.preparation import PreparedDataFlow
from datacoolie.sources import SourceReadRange
from datacoolie.sources.api_reader import validate_api_bounded_range
from datacoolie.utils.chunking import generate_chunk_boundaries, normalize_chunk_range
from datacoolie.utils.time import utc_now
from datacoolie.watermark.base import BaseWatermarkManager

logger = get_logger(__name__)


def _outer_observation(runtime: ReplayRuntimeInfo) -> DataFlowRuntimeInfo:
    """An early exit is one no-attempt observation with the outer identity."""
    return DataFlowRuntimeInfo(
        dataflow_run_id=runtime.dataflow_run_id, dataflow_id=runtime.dataflow_id,
        operation_type=runtime.operation_type, start_time=runtime.start_time,
        end_time=runtime.end_time, status=runtime.status, message=runtime.message,
    )


def _runtime_duration(runtime: ReplayRuntimeInfo) -> object:
    try:
        return runtime.duration_seconds or 0.0
    except BaseException:
        return 0.0


def validate_replay_chunk_column(dataflow: DataFlow, column: str) -> None:
    """Validate that the source can execute a bounded read for *column*.

    Replay owns the range and chunk scheduling, while each source owns how
    that range is pushed down.  API sources therefore need a canonical
    per-field mapping with both bounds; the selected field does not need to be
    one of the columns whose watermark is persisted.
    """

    source = dataflow.source
    if source.connection.format != Format.API.value:
        return

    validate_api_bounded_range(source, column)


def _notify_chunk(
    callback: Callable[[DataFlowRuntimeInfo], None],
    metadata: DataFlow,
    runtime: DataFlowRuntimeInfo,
) -> None:
    try:
        callback(copy.deepcopy(runtime))
    except Exception as exc:
        emit_safely(
            logger,
            logging.WARNING,
            format_dataflow_message(
                metadata,
                "chunk callback failed: dataflow_id={}, cause={}",
                runtime.dataflow_id,
                exc,
            ),
            exc_info=(type(exc), exc, exc.__traceback__),
            catch_base=True,
        )


def process_replay(
    dataflow: DataFlow,
    replay: ReplayConfig,
    *,
    column_name_mode: ColumnCaseMode,
    prepare_execution_dataflow: Callable[..., PreparedDataFlow],
    validate_watermark_storage: Callable[..., None],
    watermark_manager: BaseWatermarkManager | None,
    run_single_pipeline: Callable[..., DataFlowRuntimeInfo],
    log_result: Callable[[DataFlow, DataFlowRuntimeInfo], None],
    on_chunk_complete: Callable[[DataFlowRuntimeInfo], None],
) -> ReplayRuntimeInfo:
    """Prepare and execute one replay dataflow, stopping on first bad chunk."""
    started = utc_now()
    runtime = ReplayRuntimeInfo(
        dataflow_id=dataflow.dataflow_id,
        operation_type=ExecutionType.REPLAY.value,
        start_time=started,
        status=DataFlowStatus.RUNNING.value,
    )
    # Replay has an outer execution scope.  Chunk execution temporarily binds
    # its own existing runtime ID and restores this scope on return.
    with dataflow_context(dataflow.dataflow_id, runtime.dataflow_run_id):
        emit_safely(
            logger,
            logging.INFO,
            format_dataflow_message(dataflow, "operation=replay"),
            extra={"event_name": LogEvent.REPLAY_STARTED.value},
            catch_base=True,
        )
        observation = _outer_observation(runtime)
        if finish_inactive_execution(
            dataflow,
            observation,
            log_result=log_result,
        ):
            runtime.end_time = observation.end_time
            runtime.status = observation.status
            runtime.message = observation.message
            emit_safely(
                logger,
                logging.INFO,
                format_dataflow_message(
                    dataflow,
                    "status={}, reason={}",
                    runtime.status,
                    runtime.message,
                ),
                extra={"event_name": LogEvent.REPLAY_FINISHED.value},
                catch_base=True,
            )
            return runtime
        return _process_replay_body(
            dataflow,
            replay,
            runtime=runtime,
            authored_metadata=dataflow,
            column_name_mode=column_name_mode,
            prepare_execution_dataflow=prepare_execution_dataflow,
            validate_watermark_storage=validate_watermark_storage,
            watermark_manager=watermark_manager,
            run_single_pipeline=run_single_pipeline,
            log_result=log_result,
            on_chunk_complete=on_chunk_complete,
        )


def _process_replay_body(
    dataflow: DataFlow,
    replay: ReplayConfig,
    *,
    runtime: ReplayRuntimeInfo,
    authored_metadata: DataFlow,
    column_name_mode: ColumnCaseMode,
    prepare_execution_dataflow: Callable[..., PreparedDataFlow],
    validate_watermark_storage: Callable[..., None],
    watermark_manager: BaseWatermarkManager | None,
    run_single_pipeline: Callable[..., DataFlowRuntimeInfo],
    log_result: Callable[[DataFlow, DataFlowRuntimeInfo], None],
    on_chunk_complete: Callable[[DataFlowRuntimeInfo], None],
) -> ReplayRuntimeInfo:
    """Execute the body while the caller owns the outer replay context."""
    metadata_snapshot: Optional[DataFlow] = None
    metadata = authored_metadata
    try:
        metadata_snapshot = dataflow.model_copy(deep=True)
        metadata = metadata_snapshot
        prepared = prepare_execution_dataflow(
            dataflow,
            operation_type=ExecutionType.REPLAY.value,
        )
        execution_baseline = prepared.execution
        validate_watermark_storage(
            execution_baseline,
            operation_type=ExecutionType.REPLAY.value,
            watermark_start=None,
            watermark_end=None,
            save_watermark=replay.save_watermark,
        )

        column = replay.chunk_column
        if column is None:
            watermark_columns = dataflow.source.watermark_columns
            if not watermark_columns:
                raise DataCoolieError(
                    f"Cannot auto-resolve chunk_column: dataflow {dataflow.dataflow_id!r} "
                    "has no watermark_columns. Set replay.chunk_column explicitly."
                )
            column = watermark_columns[0]

        validate_replay_chunk_column(dataflow, column)

        normalized_start, normalized_end, _ = normalize_chunk_range(
            replay.start, replay.end
        )
        chunks = (
            generate_chunk_boundaries(
                start=normalized_start,
                end=normalized_end,
                interval=replay.chunk_interval,
            )
            if replay.chunk_interval is not None
            else [(normalized_start, normalized_end)]
        )

    except Exception as exc:
        runtime.end_time = utc_now()
        runtime.status = DataFlowStatus.FAILED.value
        runtime.message = str(exc) or type(exc).__name__
        emit_safely(
            logger,
            logging.ERROR,
            format_dataflow_message(
                metadata,
                "status=failed, reason={}",
                runtime.message,
            ),
            extra={"event_name": LogEvent.REPLAY_FINISHED.value},
            exc_info=(type(exc), exc, exc.__traceback__),
            catch_base=True,
        )
        log_result_safely(log_result, metadata_snapshot or dataflow, _outer_observation(runtime))
        return runtime

    emit_safely(
        logger,
        logging.DEBUG,
        "Replaying %s — %d chunk(s), range [%s, %s), column=%s",
        dataflow.name,
        len(chunks),
        chunks[0][0],
        chunks[-1][1],
        column,
        catch_base=True,
    )
    chunk_results: List[DataFlowRuntimeInfo] = []
    metric_accumulator = MetricAccumulator()
    try:
        for index, (lower, upper) in enumerate(chunks, 1):
            emit_safely(
                logger,
                logging.DEBUG,
                "Replay chunk %d/%d: [%s, %s)",
                index,
                len(chunks),
                lower,
                upper,
                catch_base=True,
            )
            # A chunk is admitted at this boundary.  If an unexpected
            # exception escapes the normal execution helper, materialize one
            # failed chunk runtime with the same operation identity instead of
            # returning only an unlogged replay aggregate.
            chunk_started = utc_now()
            try:
                chunk_dataflow: DataFlow = execution_baseline.model_copy(deep=True)

                chunk_runtime = run_single_pipeline(
                    PreparedDataFlow(
                        metadata=metadata_snapshot.model_copy(deep=True),
                        execution=chunk_dataflow,
                    ),
                    column_name_mode=column_name_mode,
                    watermark_start=None,
                    watermark_end=None,
                    save_watermark=replay.save_watermark,
                    read_range=SourceReadRange(
                        column=column,
                        start=lower,
                        end=upper,
                        lower_operator=">=",
                        upper_operator="<",
                    ),
                    operation_type=ExecutionType.REPLAY.value,
                )
                if not isinstance(chunk_runtime, DataFlowRuntimeInfo):
                    raise TypeError("replay chunk must return DataFlowRuntimeInfo")
                if chunk_runtime.status not in {
                    DataFlowStatus.SUCCEEDED.value, DataFlowStatus.FAILED.value,
                    DataFlowStatus.SKIPPED.value,
                }:
                    raise ValueError("replay chunk must return a terminal status")
            except Exception as exc:
                chunk_runtime = DataFlowRuntimeInfo(
                    dataflow_id=dataflow.dataflow_id,
                    operation_type=ExecutionType.REPLAY.value,
                    start_time=chunk_started,
                    end_time=utc_now(),
                    status=DataFlowStatus.FAILED.value,
                    message=str(exc) or type(exc).__name__,
                )
                emit_safely(
                    logger,
                    logging.ERROR,
                    format_dataflow_message(
                        metadata,
                        "status=failed, dataflow_id={}, chunk={}/{}",
                        dataflow.dataflow_id,
                        index,
                        len(chunks),
                    ),
                    extra={
                        "event_name": LogEvent.REPLAY_FINISHED.value,
                    },
                    exc_info=(type(exc), exc, exc.__traceback__),
                    catch_base=True,
                )
            chunk_results.append(chunk_runtime)
            if not execution_observation_attempted(chunk_runtime):
                log_result_safely(log_result, metadata_snapshot or dataflow, chunk_runtime)
            runtime.chunk_run_ids.append(chunk_runtime.dataflow_run_id)
            # Each terminal chunk contributes exactly once.  Missing values
            # remain unobserved at the replay aggregate boundary.
            metric_accumulator.add(chunk_runtime.metrics)
            runtime.metrics = metric_accumulator.metrics
            runtime.retry_attempts += chunk_runtime.retry_attempts
            # The callback belongs to the chunk completion boundary.  Keep
            # its existing chunk identity visible even though the outer
            # replay scope is restored immediately afterwards.
            with dataflow_context(
                chunk_runtime.dataflow_id,
                chunk_runtime.dataflow_run_id,
            ):
                _notify_chunk(on_chunk_complete, metadata, chunk_runtime)

            if chunk_runtime.status == DataFlowStatus.FAILED.value:
                emit_safely(
                    logger,
                    logging.DEBUG,
                    "Replay stopped for %s at chunk %d/%d due to failure",
                    dataflow.name,
                    index,
                    len(chunks),
                    catch_base=True,
                )
                break
    except Exception as exc:
        # A chunk that returns FAILED is already represented by its own
        # terminal dataflow event.  This branch is only for an unexpected
        # orchestration exception outside that normal result contract.
        runtime.end_time = utc_now()
        runtime.status = DataFlowStatus.FAILED.value
        runtime.message = str(exc) or type(exc).__name__
        emit_safely(
            logger,
            logging.ERROR,
            format_dataflow_message(
                metadata,
                "status=failed, reason={}",
                runtime.message,
            ),
            extra={"event_name": LogEvent.REPLAY_FINISHED.value},
            exc_info=(type(exc), exc, exc.__traceback__),
            catch_base=True,
        )
        return runtime

    failed = next(
        (result for result in chunk_results if result.status == DataFlowStatus.FAILED.value),
        None,
    )
    all_skipped = bool(chunk_results) and all(
        result.status == DataFlowStatus.SKIPPED.value for result in chunk_results
    )
    runtime.end_time = utc_now()
    runtime.status = (
        DataFlowStatus.FAILED.value
        if failed
        else DataFlowStatus.SKIPPED.value
        if all_skipped
        else DataFlowStatus.SUCCEEDED.value
    )
    runtime.message = failed.message if failed else None
    if all_skipped:
        runtime.message = "All replay chunks were skipped"
    finish_message = format_dataflow_message(
        metadata,
        "status={}, chunks={}, duration={:.3f}s",
        runtime.status,
        len(chunk_results),
        _runtime_duration(runtime),
    )
    if runtime.status == DataFlowStatus.FAILED.value:
        emit_safely(
            logger,
            logging.ERROR,
            finish_message,
            extra={"event_name": LogEvent.REPLAY_FINISHED.value},
            catch_base=True,
        )
    else:
        emit_safely(
            logger,
            logging.INFO,
            finish_message,
            extra={"event_name": LogEvent.REPLAY_FINISHED.value},
            catch_base=True,
        )
    return runtime
