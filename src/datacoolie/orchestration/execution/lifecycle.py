"""Shared lifecycle for one orchestration dataflow execution.

This module deliberately depends on narrow callables.  It owns runtime
finalization, retry-attempt accounting and context cleanup, while pipeline
implementations retain ownership of readers, writers and backend I/O.
"""

from __future__ import annotations

import copy
import logging
from typing import Any, Callable, Dict, Optional

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import PipelineError
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DataFlowRuntimeInfo, PipelineAttemptResult
from datacoolie.logging.configuration.constants import LogEvent
from datacoolie.logging.runtime.diagnostics import emit_safely
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.logging.runtime.context import (
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


def execution_observation_attempted(runtime: DataFlowRuntimeInfo) -> bool:
    """Return whether the runtime crossed the execution log boundary."""

    return bool(getattr(runtime, _EXECUTION_OBSERVATION_ATTEMPTED, False))


def _log_dataflow_started(runtime: DataFlowRuntimeInfo) -> None:
    """Emit the single lifecycle start anchor for one execution scope."""
    emit_safely(
        logger,
        logging.INFO,
        "Dataflow started: operation=%s",
        runtime.operation_type,
        extra={"event_name": LogEvent.DATAFLOW_STARTED.value},
        catch_base=True,
    )


def _log_dataflow_finished(
    runtime: DataFlowRuntimeInfo,
    *,
    error: Optional[Exception] = None,
) -> None:
    """Emit one terminal lifecycle anchor without duplicating lower stacks."""
    message = "Dataflow finished: operation=%s, status=%s, duration=%.3fs"
    args = (
        runtime.operation_type,
        runtime.status,
        runtime.duration_seconds or 0.0,
    )
    fields = {"event_name": LogEvent.DATAFLOW_FINISHED.value}
    if runtime.status == DataFlowStatus.FAILED.value:
        emit_safely(
            logger,
            logging.ERROR,
            message,
            *args,
            extra=fields,
            exc_info=(
                (type(error), error, error.__traceback__)
                if error is not None
                else False
            ),
            catch_base=True,
        )
    else:
        emit_safely(logger, logging.INFO, message, *args, extra=fields, catch_base=True)


def _observe_retry(
    attempt: int,
    max_attempts: int,
    error: Exception,
    delay: float,
) -> None:
    """Emit retry diagnostics at the orchestration boundary only."""

    emit_safely(
        logger,
        logging.WARNING,
        "Attempt %d/%d failed (%s). Retrying in %.1f s …",
        attempt,
        max_attempts,
        error,
        delay,
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
        "Dataflow skipped: %s",
        reason,
        catch_base=True,
    )
    _log_dataflow_finished(runtime)
    log_result_safely(log_result, metadata_snapshot, runtime)
    return True


def apply_attempt_result(
    runtime: DataFlowRuntimeInfo,
    attempt_result: PipelineAttemptResult,
) -> None:
    """Copy only phases that actually participated in an attempt."""
    if attempt_result.source is not None:
        runtime.source = attempt_result.source
    if attempt_result.transform is not None:
        runtime.transform = attempt_result.transform
    if attempt_result.destination is not None:
        runtime.destination = attempt_result.destination


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
    runtime: DataFlowRuntimeInfo,
    retry_handler: RetryHandler,
    preflight: Callable[[DataFlow], None],
    attempt_runner: Callable[..., PipelineAttemptResult],
    attempt_kwargs: Optional[Dict[str, Any]] = None,
    include_dataflow_run_id: bool = True,
) -> DataFlowRuntimeInfo:
    """Run preflight and retryable pipeline attempts for an existing runtime."""
    status = runtime.status
    message: Optional[str] = None
    attempt_result: Optional[PipelineAttemptResult] = None
    terminal_error: Optional[Exception] = None
    attempts_started = 0
    kwargs = dict(attempt_kwargs or {})
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
            ) -> PipelineAttemptResult:
                nonlocal attempts_started
                attempts_started += 1
                attempt_dataflow = baseline.model_copy(deep=True)
                return attempt_runner(attempt_dataflow, **retry_kwargs)

            attempt_result, _successful_attempt = retry_handler.execute(
                execute_attempt,
                prepared_dataflow.execution,
                on_retry=_observe_retry,
                **kwargs,
            )
            status = attempt_result.status
    except PipelineError as exc:
        status = DataFlowStatus.FAILED.value
        message = str(exc) or type(exc).__name__
        terminal_error = exc
        if isinstance(exc.partial_result, PipelineAttemptResult):
            attempt_result = exc.partial_result
    except Exception as exc:
        status = DataFlowStatus.FAILED.value
        message = str(exc) or type(exc).__name__
        terminal_error = exc

    if attempt_result is not None:
        apply_attempt_result(runtime, attempt_result)
        if message is None:
            message = attempt_result.message
    runtime.end_time = utc_now()
    runtime.status = status
    if status == DataFlowStatus.SKIPPED.value and not message:
        message = "Execution returned skipped without a detailed reason"
    runtime.message = message
    runtime.retry_attempts = max(0, attempts_started - 1)
    if runtime.status == DataFlowStatus.FAILED.value:
        _log_dataflow_finished(runtime, error=terminal_error)
    return runtime


def run_dataflow_execution(
    dataflow: DataFlow,
    *,
    operation_type: str,
    prepare_execution_dataflow: Callable[..., PreparedDataFlow],
    retry_handler: RetryHandler,
    preflight: Callable[[DataFlow], None],
    attempt_runner: Callable[..., PipelineAttemptResult],
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
        _log_dataflow_started(runtime)
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
            _log_dataflow_finished(runtime, error=exc)
        else:
            _run_prepared_attempts(
                prepared,
                runtime=runtime,
                retry_handler=retry_handler,
                preflight=preflight,
                attempt_runner=attempt_runner,
                attempt_kwargs=attempt_kwargs,
                include_dataflow_run_id=include_dataflow_run_id,
            )
            if runtime.status != DataFlowStatus.FAILED.value:
                _log_dataflow_finished(runtime)
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
        _log_dataflow_started(runtime)
        if finish_inactive_execution(dataflow, runtime, log_result=log_result):
            return runtime
        try:
            metadata_snapshot = dataflow.model_copy(deep=True)
            execution_snapshot = dataflow.model_copy(deep=True)
            validate(execution_snapshot)
        except Exception as exc:
            mark_preparation_failure(runtime, exc)
            _log_dataflow_finished(runtime, error=exc)
        else:
            runtime.end_time = utc_now()
            runtime.status = DataFlowStatus.SKIPPED.value
            runtime.message = "Dry-run validation passed; pipeline execution was not performed"
            _log_dataflow_finished(runtime)

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
    attempt_runner: Callable[..., PipelineAttemptResult],
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
        _log_dataflow_started(runtime)
        try:
            metadata_snapshot = prepared_dataflow.metadata.model_copy(deep=True)
        except Exception as exc:
            mark_preparation_failure(runtime, exc)
            _log_dataflow_finished(runtime, error=exc)
        else:
            _run_prepared_attempts(
                prepared_dataflow,
                runtime=runtime,
                retry_handler=retry_handler,
                preflight=preflight,
                attempt_runner=attempt_runner,
                attempt_kwargs=attempt_kwargs,
                include_dataflow_run_id=include_dataflow_run_id,
            )
            if runtime.status != DataFlowStatus.FAILED.value:
                _log_dataflow_finished(runtime)
        if metadata_snapshot is not None:
            log_result_safely(log_result, metadata_snapshot, runtime)
    finally:
        clear_dataflow_context(ctx_token)
    return runtime
