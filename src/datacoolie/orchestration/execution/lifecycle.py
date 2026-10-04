"""Shared lifecycle for one orchestration dataflow execution.

This module deliberately depends on narrow callables.  It owns runtime
finalization, retry-attempt accounting and context cleanup, while pipeline
implementations retain ownership of readers, writers and backend I/O.
"""

from __future__ import annotations

import copy
import logging
import unicodedata
from dataclasses import fields
from typing import Any, Callable, Dict, Optional

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import PipelineError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.orchestration.execution.runtime import AttemptRuntimeInfo
from datacoolie.orchestration.execution.runtime import DataFlowRuntimeInfo
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.sources.runtime import SourceRuntimeInfo
from datacoolie.transformers.runtime import TransformRuntimeInfo
from datacoolie.watermark.runtime import WatermarkRuntimeInfo
from datacoolie.orchestration.execution.read_baseline import ReadBaseline
from datacoolie.sources.contracts import SourceReadRequest
from datacoolie.logging.configuration.constants import LogEvent
from datacoolie.logging.runtime.diagnostics import emit_safely
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.logging.runtime.context import (
    attempt_context,
    clear_dataflow_context,
    set_dataflow_context,
)
from datacoolie.orchestration.execution.activation import inactive_reason
from datacoolie.orchestration.preparation import PreparedDataFlow
from datacoolie.utils.retry import RetryHandler
from datacoolie.utils.time import utc_now

logger = get_logger(__name__)


# ``ParallelExecutor`` receives a completion callback after the process
# callable returns.  Normal execution boundaries persist their observation
# before returning; scheduler fallbacks do not.  Keep this marker private to
# the in-memory runtime so callbacks can distinguish those two cases without
# adding a persisted schema field or a global deduplication registry.
_EXECUTION_OBSERVATION_ATTEMPTED = "_datacoolie_execution_observation_attempted"

_UNNAMED_DATAFLOW = "<unnamed>"
_UNKNOWN_ROUTE = "<unknown>"
_QUERY_ROUTE = "<query>"
_PYTHON_FUNCTION_ROUTE = "<python_function>"
_RENDER_FALLBACK_DETAILS = "details unavailable"
_RENDER_FALLBACK_MESSAGE = (
    f"{_UNNAMED_DATAFLOW} | {_UNKNOWN_ROUTE} → {_UNKNOWN_ROUTE} | "
    f"{_RENDER_FALLBACK_DETAILS}"
)


def execution_observation_attempted(runtime: DataFlowRuntimeInfo) -> bool:
    """Return whether the runtime crossed the execution log boundary."""

    return bool(getattr(runtime, _EXECUTION_OBSERVATION_ATTEMPTED, False))


def _escape_display_text(value: object) -> str:
    """Escape terminal control characters without changing other Unicode."""
    text = str(value)
    escaped: list[str] = []
    for character in text:
        codepoint = ord(character)
        if character == "\n":
            escaped.append(r"\n")
        elif character == "\r":
            escaped.append(r"\r")
        elif character == "\t":
            escaped.append(r"\t")
        elif unicodedata.category(character) == "Cc":
            if codepoint <= 0xFF:
                escaped.append(f"\\x{codepoint:02x}")
            else:
                escaped.append(f"\\u{codepoint:04x}")
        else:
            escaped.append(character)
    return "".join(escaped)


def _display_name(dataflow: DataFlow) -> str:
    try:
        value = dataflow.name
        if value:
            return _escape_display_text(value)
    except BaseException:
        pass
    return _UNNAMED_DATAFLOW


def _display_source(dataflow: DataFlow) -> str:
    try:
        source = dataflow.source
        full_table_name = source.full_table_name
        if full_table_name:
            return _escape_display_text(full_table_name)
    except BaseException:
        source = None

    try:
        if source is not None and source.query:
            return _QUERY_ROUTE
        if source is not None and source.python_function:
            return _PYTHON_FUNCTION_ROUTE
    except BaseException:
        pass
    return _UNKNOWN_ROUTE


def _display_destination(dataflow: DataFlow) -> str:
    try:
        full_table_name = dataflow.destination.full_table_name
        if full_table_name:
            return _escape_display_text(full_table_name)
    except BaseException:
        pass
    return _UNKNOWN_ROUTE


def _runtime_duration(runtime: DataFlowRuntimeInfo) -> object:
    try:
        return runtime.duration_seconds or 0.0
    except BaseException:
        return 0.0


def format_dataflow_message(
    dataflow: DataFlow,
    details: object,
    *details_args: object,
) -> str:
    """Build the authored dataflow message used by lifecycle and replay logs.

    This helper deliberately reads only metadata properties.  It has a safe
    fallback because diagnostics must not replace an execution or finalization
    exception when metadata rendering is malformed.
    """
    try:
        if details_args:
            details = str(details).format(*details_args)
        rendered_details = _escape_display_text(
            details if details is not None else _RENDER_FALLBACK_DETAILS
        )
    except BaseException:
        rendered_details = _RENDER_FALLBACK_DETAILS
    try:
        return (
            f"{_display_name(dataflow)} | {_display_source(dataflow)} → "
            f"{_display_destination(dataflow)} | {rendered_details}"
        )
    except BaseException:
        return _RENDER_FALLBACK_MESSAGE


def _log_dataflow_started(
    metadata: DataFlow,
    runtime: DataFlowRuntimeInfo,
) -> None:
    """Emit the single lifecycle start anchor for one execution scope."""
    emit_safely(
        logger,
        logging.INFO,
        format_dataflow_message(
            metadata,
            "operation={}",
            runtime.operation_type,
        ),
        extra={"event_name": LogEvent.DATAFLOW_STARTED.value},
        catch_base=True,
    )


def _log_dataflow_finished(
    metadata: DataFlow,
    runtime: DataFlowRuntimeInfo,
    *,
    error: Optional[Exception] = None,
) -> None:
    """Emit one terminal lifecycle anchor without duplicating lower stacks."""
    message = format_dataflow_message(
        metadata,
        "operation={}, status={}, duration={:.3f}s",
        runtime.operation_type,
        runtime.status,
        _runtime_duration(runtime),
    )
    fields = {"event_name": LogEvent.DATAFLOW_FINISHED.value}
    if runtime.status == DataFlowStatus.FAILED.value:
        emit_safely(
            logger,
            logging.ERROR,
            message,
            extra=fields,
            exc_info=(
                (type(error), error, error.__traceback__)
                if error is not None
                else False
            ),
            catch_base=True,
        )
    else:
        emit_safely(logger, logging.INFO, message, extra=fields, catch_base=True)


def _observe_retry(
    metadata: DataFlow,
    attempt: int,
    max_attempts: int,
    error: Exception,
    delay: float,
) -> None:
    """Emit retry diagnostics at the orchestration boundary only."""

    emit_safely(
        logger,
        logging.WARNING,
        format_dataflow_message(
            metadata,
            "attempt={}/{}, delay={:.1f}s, cause={}",
            attempt,
            max_attempts,
            delay,
            error,
        ),
        extra={"event_name": LogEvent.RETRY_SCHEDULED.value},
        catch_base=True,
    )


def mark_preparation_failure(runtime: DataFlowRuntimeInfo, exc: Exception) -> None:
    """Set the terminal state for work that never entered a pipeline attempt."""
    runtime.end_time = utc_now()
    runtime.status = DataFlowStatus.FAILED.value
    runtime.message = str(exc) or type(exc).__name__


def finish_inactive_execution(
    dataflow: DataFlow,
    runtime: DataFlowRuntimeInfo,
    *,
    log_result: Callable[[DataFlow, DataFlowRuntimeInfo], None],
) -> bool:
    """Record one terminal skip before preparation or business I/O."""
    reason = inactive_reason(dataflow)
    if reason is None:
        return False
    metadata_snapshot = dataflow.model_copy(deep=True)
    runtime.end_time = utc_now()
    runtime.status = DataFlowStatus.SKIPPED.value
    runtime.message = reason
    emit_safely(
        logger,
        logging.INFO,
        format_dataflow_message(dataflow, "reason={}", reason),
        catch_base=True,
    )
    _log_dataflow_finished(dataflow, runtime)
    log_result_safely(log_result, metadata_snapshot, runtime)
    return True


def _copy_phase_evidence(phase: Any) -> Any:
    """Detach diagnostics, retaining valid fields if a custom payload fails."""
    if phase is None:
        return None
    try:
        return copy.deepcopy(phase)
    except Exception:
        phase_type = next(
            (
                kind
                for kind in (
                    SourceRuntimeInfo,
                    TransformRuntimeInfo,
                    DestinationRuntimeInfo,
                    WatermarkRuntimeInfo,
                )
                if isinstance(phase, kind)
            ),
            None,
        )
        if phase_type is None:
            return None
        snapshot = phase_type()
        for item in fields(snapshot):
            try:
                setattr(snapshot, item.name, copy.deepcopy(getattr(phase, item.name)))
            except Exception:
                # The default explicitly omits this field's malformed payload.
                continue
        return snapshot


def apply_attempt_result(
    runtime: DataFlowRuntimeInfo,
    attempt_result: AttemptRuntimeInfo,
) -> None:
    """Retain a detached terminal snapshot; run fields project its phases."""
    try:
        snapshot = copy.deepcopy(attempt_result)
    except Exception:
        # Custom diagnostic payloads must not turn completed writes into retries.
        snapshot = AttemptRuntimeInfo(
            status=attempt_result.status,
            message=attempt_result.message,
            attempt_number=attempt_result.attempt_number,
            start_time=attempt_result.start_time,
            end_time=attempt_result.end_time,
            source=_copy_phase_evidence(attempt_result.source),
            transform=_copy_phase_evidence(attempt_result.transform),
            destination=_copy_phase_evidence(attempt_result.destination),
            watermark=_copy_phase_evidence(attempt_result.watermark),
        )
        emit_safely(
            logger,
            logging.WARNING,
            "Attempt phase snapshot capture failed",
            exc_info=True,
            catch_base=True,
        )
    runtime.attempts.append(snapshot)


def log_result_safely(
    log_result: Callable[[DataFlow, DataFlowRuntimeInfo], None],
    metadata: DataFlow,
    runtime: DataFlowRuntimeInfo,
) -> None:
    """Keep log sinks observational after business execution has completed."""
    try:
        # Mark before invoking the sink.  A sink failure must not cause the
        # scheduler callback to submit a second observation for the same
        # terminal runtime; persistence remains best-effort and is not
        # advertised as exactly-once delivery.
        setattr(runtime, _EXECUTION_OBSERVATION_ATTEMPTED, True)
        log_result(
            metadata.model_copy(deep=True),
            copy.deepcopy(runtime),
        )
    except Exception:
        emit_safely(
            logger,
            logging.WARNING,
            "Execution result logging failed for %s",
            runtime.dataflow_id,
            exc_info=True,
            catch_base=True,
        )


def _run_prepared_attempts(
    prepared_dataflow: PreparedDataFlow,
    *,
    metadata: DataFlow,
    runtime: DataFlowRuntimeInfo,
    retry_handler: RetryHandler,
    preflight: Callable[[DataFlow], None],
    attempt_runner: Callable[..., AttemptRuntimeInfo],
    attempt_kwargs: Optional[Dict[str, Any]] = None,
    include_dataflow_run_id: bool = True,
) -> DataFlowRuntimeInfo:
    """Run preflight and retryable pipeline attempts for an existing runtime."""
    status = runtime.status
    message: Optional[str] = None
    attempt_result: Optional[AttemptRuntimeInfo] = None
    terminal_error: Optional[Exception] = None
    attempts_started = 0
    kwargs = dict(attempt_kwargs or {})
    read_baseline = ReadBaseline()
    if runtime.operation_type in ("etl", "replay"):
        exact_range = kwargs.get("read_range")
        try:
            runtime.read_request = SourceReadRequest(
                reference_time=read_baseline.reference_time,
                watermark_start=kwargs.get("watermark_start"),
                watermark_end=kwargs.get("watermark_end"),
                watermark_start_operator=kwargs.get("watermark_start_operator"),
                watermark_end_operator=kwargs.get("watermark_end_operator"),
                read_range=exact_range,
            )
        except Exception as exc:
            mark_preparation_failure(runtime, exc)
            _log_dataflow_finished(metadata, runtime, error=exc)
            return runtime
    if include_dataflow_run_id:
        kwargs.setdefault("dataflow_run_id", runtime.dataflow_run_id)

    try:
        try:
            preflight(prepared_dataflow.execution)
        except Exception as exc:
            status = DataFlowStatus.FAILED.value
            message = str(exc) or type(exc).__name__
            terminal_error = exc
        else:

            def execute_attempt(
                baseline: DataFlow,
                **retry_kwargs: Any,
            ) -> AttemptRuntimeInfo:
                nonlocal attempts_started
                attempts_started += 1
                started = utc_now()
                result = AttemptRuntimeInfo(
                    attempt_number=attempts_started,
                    start_time=started,
                    status=DataFlowStatus.RUNNING.value,
                )
                with attempt_context(attempts_started):
                    try:
                        attempt_dataflow = baseline.model_copy(deep=True)
                        returned = attempt_runner(
                            attempt_dataflow,
                            **copy.deepcopy(retry_kwargs),
                            baseline=read_baseline,
                            attempt=result,
                        )
                        if returned is not result:
                            raise TypeError(
                                "attempt_runner must return the lifecycle-owned attempt"
                            )
                        return result
                    except Exception as exc:
                        if isinstance(exc, PipelineError) and isinstance(
                            exc.attempt, AttemptRuntimeInfo
                        ):
                            result = exc.attempt
                        result.status = DataFlowStatus.FAILED.value
                        result.message = str(exc) or type(exc).__name__
                        raise
                    finally:
                        if result is not None:
                            result.attempt_number = attempts_started
                            result.start_time = started
                            try:
                                result.end_time = utc_now()
                            except Exception:
                                emit_safely(logger, logging.WARNING,
                                            "Attempt end-time capture unavailable",
                                            exc_info=True, catch_base=True)
                            apply_attempt_result(runtime, result)

            def observe_retry(
                attempt: int,
                max_attempts: int,
                error: Exception,
                delay: float,
            ) -> None:
                with attempt_context(attempt):
                    _observe_retry(metadata, attempt, max_attempts, error, delay)

            attempt_result, _successful_attempt = retry_handler.execute(
                execute_attempt,
                prepared_dataflow.execution,
                on_retry=observe_retry,
                **kwargs,
            )
            status = attempt_result.status
    except PipelineError as exc:
        status = DataFlowStatus.FAILED.value
        message = str(exc) or type(exc).__name__
        terminal_error = exc
        if isinstance(exc.attempt, AttemptRuntimeInfo):
            attempt_result = exc.attempt
    except Exception as exc:
        status = DataFlowStatus.FAILED.value
        message = str(exc) or type(exc).__name__
        terminal_error = exc

    if attempt_result is not None:
        if message is None:
            message = attempt_result.message
    runtime.end_time = utc_now()
    runtime.status = status
    if status == DataFlowStatus.SKIPPED.value and not message:
        message = "Execution returned skipped without a detailed reason"
    runtime.message = message
    if runtime.status == DataFlowStatus.FAILED.value:
        _log_dataflow_finished(metadata, runtime, error=terminal_error)
    return runtime


def run_dataflow_execution(
    dataflow: DataFlow,
    *,
    operation_type: str,
    prepare_execution_dataflow: Callable[..., PreparedDataFlow],
    retry_handler: RetryHandler,
    preflight: Callable[[DataFlow], None],
    attempt_runner: Callable[..., AttemptRuntimeInfo],
    log_result: Callable[[DataFlow, DataFlowRuntimeInfo], None],
    attempt_kwargs: Optional[Dict[str, Any]] = None,
    include_dataflow_run_id: bool = True,
) -> DataFlowRuntimeInfo:
    """Run one dataflow from preparation through terminal execution.

    The runtime starts before metadata snapshot and preparation. Preparation is
    performed once and remains outside retryable pipeline attempts.
    """
    started = utc_now()
    runtime = DataFlowRuntimeInfo(
        dataflow_id=dataflow.dataflow_id,
        operation_type=operation_type,
        start_time=started,
        status=DataFlowStatus.RUNNING.value,
    )
    ctx_token = set_dataflow_context(
        dataflow.dataflow_id,
        runtime.dataflow_run_id,
    )
    metadata_snapshot: Optional[DataFlow] = None
    try:
        _log_dataflow_started(dataflow, runtime)
        if finish_inactive_execution(dataflow, runtime, log_result=log_result):
            return runtime
        try:
            metadata_snapshot = dataflow.model_copy(deep=True)
            prepared = prepare_execution_dataflow(
                dataflow,
                operation_type=operation_type,
            )
        except Exception as exc:
            mark_preparation_failure(runtime, exc)
            _log_dataflow_finished(metadata_snapshot or dataflow, runtime, error=exc)
        else:
            _run_prepared_attempts(
                prepared,
                metadata=metadata_snapshot or dataflow,
                runtime=runtime,
                retry_handler=retry_handler,
                preflight=preflight,
                attempt_runner=attempt_runner,
                attempt_kwargs=attempt_kwargs,
                include_dataflow_run_id=include_dataflow_run_id,
            )
            if runtime.status != DataFlowStatus.FAILED.value:
                _log_dataflow_finished(metadata_snapshot or dataflow, runtime)
        if metadata_snapshot is not None:
            log_result_safely(log_result, metadata_snapshot, runtime)
    finally:
        clear_dataflow_context(ctx_token)
    return runtime


def run_dry_run_execution(
    dataflow: DataFlow,
    *,
    operation_type: str,
    validate: Callable[[DataFlow], None],
    log_result: Callable[[DataFlow, DataFlowRuntimeInfo], None],
) -> DataFlowRuntimeInfo:
    """Validate one dataflow through the normal runtime lifecycle.

    Dry-run deliberately has no retryable pipeline attempt, but it still owns
    the same dataflow-level timing, context, lifecycle anchors, and one
    terminal execution observation as a normal run.  The validation callback
    receives an isolated execution copy so callers cannot mutate the
    declarative metadata used for logging.

    Only ordinary validation errors are converted to a failed runtime.  A
    process interruption (for example ``KeyboardInterrupt``) is allowed to
    propagate to the Driver boundary, where session failure and cleanup are
    coordinated.
    """
    started = utc_now()
    runtime = DataFlowRuntimeInfo(
        dataflow_id=dataflow.dataflow_id,
        operation_type=operation_type,
        start_time=started,
        status=DataFlowStatus.RUNNING.value,
    )
    ctx_token = set_dataflow_context(
        dataflow.dataflow_id,
        runtime.dataflow_run_id,
    )
    metadata_snapshot: Optional[DataFlow] = None
    try:
        _log_dataflow_started(dataflow, runtime)
        if finish_inactive_execution(dataflow, runtime, log_result=log_result):
            return runtime
        try:
            metadata_snapshot = dataflow.model_copy(deep=True)
            execution_snapshot = dataflow.model_copy(deep=True)
            validate(execution_snapshot)
        except Exception as exc:
            mark_preparation_failure(runtime, exc)
            _log_dataflow_finished(metadata_snapshot or dataflow, runtime, error=exc)
        else:
            runtime.end_time = utc_now()
            runtime.status = DataFlowStatus.SKIPPED.value
            runtime.message = (
                "Dry-run validation passed; pipeline execution was not performed"
            )
            _log_dataflow_finished(metadata_snapshot or dataflow, runtime)

        if metadata_snapshot is not None:
            log_result_safely(log_result, metadata_snapshot, runtime)
    finally:
        clear_dataflow_context(ctx_token)
    return runtime


def run_prepared_execution(
    prepared_dataflow: PreparedDataFlow,
    *,
    operation_type: str,
    retry_handler: RetryHandler,
    preflight: Callable[[DataFlow], None],
    attempt_runner: Callable[..., AttemptRuntimeInfo],
    log_result: Callable[[DataFlow, DataFlowRuntimeInfo], None],
    attempt_kwargs: Optional[Dict[str, Any]] = None,
    include_dataflow_run_id: bool = True,
) -> DataFlowRuntimeInfo:
    """Run one prepared dataflow through preflight, retry and final logging.

    Preparation is intentionally outside this function.  The execution copy
    is cloned for every attempt, and ``attempts_started`` is incremented before
    invoking the attempt so terminal retry exhaustion is represented correctly.
    """
    started = utc_now()
    runtime = DataFlowRuntimeInfo(
        dataflow_id=prepared_dataflow.metadata.dataflow_id,
        operation_type=operation_type,
        start_time=started,
        status=DataFlowStatus.RUNNING.value,
    )
    ctx_token = set_dataflow_context(
        runtime.dataflow_id,
        runtime.dataflow_run_id,
    )
    metadata_snapshot: Optional[DataFlow] = None
    try:
        _log_dataflow_started(prepared_dataflow.metadata, runtime)
        try:
            metadata_snapshot = prepared_dataflow.metadata.model_copy(deep=True)
        except Exception as exc:
            mark_preparation_failure(runtime, exc)
            _log_dataflow_finished(
                metadata_snapshot or prepared_dataflow.metadata,
                runtime,
                error=exc,
            )
        else:
            _run_prepared_attempts(
                prepared_dataflow,
                metadata=metadata_snapshot or prepared_dataflow.metadata,
                runtime=runtime,
                retry_handler=retry_handler,
                preflight=preflight,
                attempt_runner=attempt_runner,
                attempt_kwargs=attempt_kwargs,
                include_dataflow_run_id=include_dataflow_run_id,
            )
            if runtime.status != DataFlowStatus.FAILED.value:
                _log_dataflow_finished(
                    metadata_snapshot or prepared_dataflow.metadata,
                    runtime,
                )
        if metadata_snapshot is not None:
            log_result_safely(log_result, metadata_snapshot, runtime)
    finally:
        clear_dataflow_context(ctx_token)
    return runtime
