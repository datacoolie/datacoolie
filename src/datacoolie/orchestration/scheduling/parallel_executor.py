"""Bounded orchestration scheduler for dataflow operations.

The executor owns admission, ordering and result accounting.  Pipeline code is
kept outside this module and is supplied through ``process_fn``.  A single
coordinator pool is used for normal grouped execution so ``max_workers`` is a
global limit for one executor invocation.
"""

from __future__ import annotations

import copy
import logging
import os
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DataFlowRuntimeInfo
from datacoolie.logging.configuration.constants import LogEvent
from datacoolie.logging.runtime.diagnostics import emit_safely
from datacoolie.logging.runtime.manager import get_logger
from datacoolie.utils.time import utc_now

logger = get_logger(__name__)


@dataclass
class ExecutionResult:
    """Aggregate result for one executor invocation.

    ``pending`` means work that was never admitted.  A result is counted once,
    when its process callable completes; observer callback failures do not
    mutate these counters.
    """

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    running: int = 0
    pending: int = 0
    errors: Dict[str, str] = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.succeeded / self.total) * 100

    @property
    def has_failures(self) -> bool:
        return self.failed > 0


@dataclass(frozen=True)
class _Task:
    dataflow: DataFlow
    group_key: Optional[int] = None
    bucket_index: Optional[int] = None
    # The task is admitted when it enters the ready queue.  If the future
    # itself fails outside ``_safe_execute``, this timestamp keeps the
    # synthesized terminal runtime anchored to that admission boundary.
    started_at: datetime = field(default_factory=utc_now)


class ParallelExecutor:
    """Execute dataflows with bounded global concurrency.

    Independent items are ready immediately. Grouped items are released by
    ascending ``execution_order`` buckets; ties in one bucket may run in
    parallel. With ``stop_on_error`` enabled, no new item is admitted after a
    failed terminal result is observed, while already admitted work is drained.
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        stop_on_error: bool = False,
    ) -> None:
        if max_workers is not None and max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self._max_workers = max_workers
        self._stop_on_error = stop_on_error

    def _get_max_workers(self, item_count: int) -> int:
        if item_count <= 0:
            return 0
        if self._max_workers is not None:
            return min(self._max_workers, item_count)
        cpu_count = os.cpu_count() or 4
        return min(item_count, cpu_count)

    def _update_counters(self, agg: ExecutionResult, item: DataFlowRuntimeInfo) -> None:
        status = item.status
        if status == DataFlowStatus.SUCCEEDED.value:
            agg.succeeded += 1
        elif status == DataFlowStatus.SKIPPED.value:
            agg.skipped += 1
        else:
            agg.failed += 1
            if item.message and item.dataflow_id:
                agg.errors[item.dataflow_id] = item.message

    @staticmethod
    def _notify(
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]],
        result: DataFlowRuntimeInfo,
    ) -> None:
        """Invoke an observational callback without changing business status."""
        if callback is None:
            return
        try:
            callback(copy.deepcopy(result))
        except Exception:
            emit_safely(
                logger,
                logging.WARNING,
                "Completion callback failed for %s",
                result.dataflow_id,
                exc_info=True,
                catch_base=True,
            )

    def _finish(self, agg: ExecutionResult, started, *, pending: int) -> ExecutionResult:
        agg.running = 0
        agg.pending = max(0, pending)
        agg.duration_seconds = (utc_now() - started).total_seconds()
        return agg

    def _execute_single(
        self,
        dataflow: DataFlow,
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]],
        *,
        operation_type: Optional[str],
    ) -> ExecutionResult:
        """Run one item on the caller thread.

        Native dataframe backends can keep thread-affine state even after a
        task returns.  A one-item execution gains no parallelism from a
        worker thread, and running it here keeps native reads/writes on the
        same thread as the surrounding driver lifecycle.  The fallback path
        preserves the scheduler's existing handling for a future-level
        failure when a test or adapter replaces ``_safe_execute``.
        """

        started = utc_now()
        task = _Task(dataflow, started_at=started)
        aggregate = ExecutionResult(total=1)
        try:
            if operation_type is None:
                result = self._safe_execute(dataflow, process_fn)
            else:
                result = self._safe_execute(
                    dataflow,
                    process_fn,
                    operation_type=operation_type,
                )
        except Exception as exc:
            future: Future[DataFlowRuntimeInfo] = Future()
            future.set_exception(exc)
            result = self._future_result(task, future, operation_type)
        self._update_counters(aggregate, result)
        self._notify(callback, result)
        if result.status == DataFlowStatus.FAILED.value and self._stop_on_error:
            emit_safely(
                logger,
                logging.WARNING,
                "Scheduler stopped admitting work after dataflow %s failed; "
                "already-running tasks will drain",
                result.dataflow_id,
                extra={"event_name": LogEvent.SCHEDULER_ADMISSION_STOPPED.value},
                catch_base=True,
            )
        return self._finish(aggregate, started, pending=0)

    def execute(
        self,
        dataflows: List[DataFlow],
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]] = None,
        *,
        operation_type: Optional[str] = None,
    ) -> ExecutionResult:
        """Execute independent dataflows with one bounded worker pool."""
        if not dataflows:
            return ExecutionResult()
        if len(dataflows) == 1:
            return self._execute_single(
                dataflows[0],
                process_fn,
                callback,
                operation_type=operation_type,
            )
        return self._execute_scheduled(
            {None: list(dataflows)}, process_fn, callback, operation_type
        )

    def execute_sequential(
        self,
        dataflows: List[DataFlow],
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]] = None,
        *,
        operation_type: Optional[str] = None,
    ) -> ExecutionResult:
        """Execute items in input order, optionally stopping after failure."""
        if not dataflows:
            return ExecutionResult()
        started = utc_now()
        agg = ExecutionResult(total=len(dataflows))
        processed = 0
        for dataflow in dataflows:
            if operation_type is None:
                result = self._safe_execute(dataflow, process_fn)
            else:
                result = self._safe_execute(
                    dataflow,
                    process_fn,
                    operation_type=operation_type,
                )
            processed += 1
            self._update_counters(agg, result)
            self._notify(callback, result)
            if self._stop_on_error and result.status == DataFlowStatus.FAILED.value:
                emit_safely(
                    logger,
                    logging.WARNING,
                    "Scheduler stopped admitting work after dataflow %s failed; "
                    "already-running tasks will drain",
                    result.dataflow_id,
                    extra={
                        "event_name": LogEvent.SCHEDULER_ADMISSION_STOPPED.value
                    },
                    catch_base=True,
                )
                break
        return self._finish(agg, started, pending=len(dataflows) - processed)

    def execute_with_groups(
        self,
        groups: Dict[Optional[int], List[DataFlow]],
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]] = None,
        *,
        operation_type: Optional[str] = None,
    ) -> ExecutionResult:
        """Execute independent and ordered grouped dataflows.

        Normal execution always uses the coordinator below, avoiding nested
        pools and preserving the global cap.
        """
        total = sum(len(items) for items in groups.values())
        if total == 0:
            return ExecutionResult()
        if total == 1:
            dataflow = next(
                dataflow
                for items in groups.values()
                for dataflow in items
            )
            return self._execute_single(
                dataflow,
                process_fn,
                callback,
                operation_type=operation_type,
            )
        return self._execute_scheduled(
            groups,
            process_fn,
            callback,
            operation_type,
        )

    def _execute_scheduled(
        self,
        groups: Dict[Optional[int], List[DataFlow]],
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]],
        operation_type: Optional[str] = None,
    ) -> ExecutionResult:
        """Run a ready-set scheduler with one global worker pool."""
        started = utc_now()
        total = sum(len(items) for items in groups.values())
        agg = ExecutionResult(total=total)
        ready: List[_Task] = []
        grouped_buckets: Dict[int, List[List[DataFlow]]] = {}
        group_completed: Dict[tuple[int, int], int] = {}

        for dataflow in groups.get(None, []):
            ready.append(_Task(dataflow))
        for raw_key, dataflows in groups.items():
            if raw_key is None:
                continue
            buckets: Dict[int, List[DataFlow]] = {}
            for dataflow in dataflows:
                order = dataflow.execution_order or 0
                buckets.setdefault(order, []).append(dataflow)
            ordered = [buckets[key] for key in sorted(buckets)]
            grouped_buckets[raw_key] = ordered
            if ordered:
                group_completed[(raw_key, 0)] = 0
                ready.extend(
                    _Task(dataflow, raw_key, 0) for dataflow in ordered[0]
                )

        max_workers = self._get_max_workers(total)
        running: Dict[Future[DataFlowRuntimeInfo], _Task] = {}
        stop_admission = False
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            while ready or running:
                while ready and len(running) < max_workers and not stop_admission:
                    task = ready.pop(0)
                    if operation_type is None:
                        future = pool.submit(
                            self._safe_execute,
                            task.dataflow,
                            process_fn,
                        )
                    else:
                        future = pool.submit(
                            self._safe_execute,
                            task.dataflow,
                            process_fn,
                            operation_type=operation_type,
                        )
                    running[future] = task

                if not running:
                    break

                done, _ = wait(tuple(running), return_when=FIRST_COMPLETED)
                for future in done:
                    task = running.pop(future)
                    result = self._future_result(task, future, operation_type)
                    self._update_counters(agg, result)
                    self._notify(callback, result)
                    if result.status == DataFlowStatus.FAILED.value:
                        if self._stop_on_error and not stop_admission:
                            emit_safely(
                                logger,
                                logging.WARNING,
                                "Scheduler stopped admitting work after dataflow %s failed; "
                                "already-running tasks will drain",
                                result.dataflow_id,
                                extra={
                                    "event_name": LogEvent.SCHEDULER_ADMISSION_STOPPED.value
                                },
                                catch_base=True,
                            )
                        stop_admission = stop_admission or self._stop_on_error

                    if task.group_key is not None and task.bucket_index is not None:
                        marker = (task.group_key, task.bucket_index)
                        group_completed[marker] = group_completed.get(marker, 0) + 1
                        bucket = grouped_buckets[task.group_key][task.bucket_index]
                        if (
                            group_completed[marker] == len(bucket)
                            and not stop_admission
                            and task.bucket_index + 1 < len(grouped_buckets[task.group_key])
                        ):
                            next_index = task.bucket_index + 1
                            group_completed[(task.group_key, next_index)] = 0
                            ready.extend(
                                _Task(dataflow, task.group_key, next_index)
                                for dataflow in grouped_buckets[task.group_key][next_index]
                            )

                if stop_admission:
                    ready.clear()

        processed = agg.succeeded + agg.failed + agg.skipped
        return self._finish(agg, started, pending=total - processed)

    @staticmethod
    def _future_result(
        task: _Task,
        future: Future[DataFlowRuntimeInfo],
        operation_type: Optional[str],
    ) -> DataFlowRuntimeInfo:
        try:
            return future.result()
        except Exception as exc:
            emit_safely(
                logger,
                logging.ERROR,
                "Scheduler worker failed for dataflow %s",
                task.dataflow.dataflow_id,
                extra={"event_name": LogEvent.SCHEDULER_EXECUTION_FAILED.value},
                exc_info=(type(exc), exc, exc.__traceback__),
                catch_base=True,
            )
            now = utc_now()
            return DataFlowRuntimeInfo(
                dataflow_id=task.dataflow.dataflow_id,
                start_time=task.started_at,
                end_time=now,
                status=DataFlowStatus.FAILED.value,
                message=str(exc) or type(exc).__name__,
                operation_type=operation_type,
            )

    def _safe_execute(
        self,
        dataflow: DataFlow,
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        *,
        operation_type: Optional[str] = None,
    ) -> DataFlowRuntimeInfo:
        """Convert process exceptions into exactly one failed runtime."""
        started_at = utc_now()
        try:
            result = process_fn(dataflow)
            if not isinstance(result, DataFlowRuntimeInfo):
                raise TypeError(
                    "process_fn must return DataFlowRuntimeInfo, "
                    f"got {type(result).__name__}"
                )
            return result
        except Exception as exc:
            emit_safely(
                logger,
                logging.ERROR,
                "Scheduler execution failed for dataflow %s",
                dataflow.dataflow_id,
                extra={"event_name": LogEvent.SCHEDULER_EXECUTION_FAILED.value},
                exc_info=(type(exc), exc, exc.__traceback__),
                catch_base=True,
            )
            now = utc_now()
            return DataFlowRuntimeInfo(
                dataflow_id=dataflow.dataflow_id,
                start_time=started_at,
                end_time=now,
                status=DataFlowStatus.FAILED.value,
                message=str(exc) or type(exc).__name__,
                operation_type=operation_type,
            )
