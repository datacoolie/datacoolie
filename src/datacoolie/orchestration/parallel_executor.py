"""Parallel executor for dataflow and maintenance processing.

``ParallelExecutor`` uses :class:`concurrent.futures.ThreadPoolExecutor`
to run dataflows in parallel, respecting group ordering and
stop-on-error semantics.
"""

from __future__ import annotations

import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.models import DataFlow, DataFlowRuntimeInfo
from datacoolie.logging.base import get_logger
from datacoolie.utils.helpers import utc_now

logger = get_logger(__name__)


@dataclass
class ExecutionResult:
    """Lightweight statistics container for a parallel execution run.

    Detailed per-dataflow logs are written by the ETLLogger; this class
    only tracks counters and errors.
    """

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    errors: Dict[str, str] = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        """Percentage of runs that succeeded."""
        if self.total == 0:
            return 0.0
        return (self.succeeded / self.total) * 100

    @property
    def has_failures(self) -> bool:
        return self.failed > 0


class ParallelExecutor:
    """Thread-based parallel executor for dataflows.

    Features:
        * Configurable ``max_workers``.
        * ``stop_on_error`` cancels remaining work on first failure.
        * ``execute_with_groups`` respects sequential ordering within groups.

    Example::

        executor = ParallelExecutor(max_workers=4)
        result = executor.execute(dataflows, process_fn)
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        stop_on_error: bool = False,
    ) -> None:
        self._max_workers = max_workers
        self._stop_on_error = stop_on_error

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _get_max_workers(self, item_count: int) -> int:
        if self._max_workers:
            return min(self._max_workers, item_count)
        cpu_count = os.cpu_count() or 4
        return min(item_count, cpu_count)

    @staticmethod
    def _status_of(result: DataFlowRuntimeInfo) -> str:
        return result.status

    def _update_counters(
        self,
        agg: ExecutionResult,
        item: DataFlowRuntimeInfo,
    ) -> None:
        status = self._status_of(item)
        if status == DataFlowStatus.SUCCEEDED.value:
            agg.succeeded += 1
        elif status == DataFlowStatus.SKIPPED.value:
            agg.skipped += 1
        else:
            agg.failed += 1
            if item.error_message and item.dataflow_id:
                agg.errors[item.dataflow_id] = item.error_message

    # ------------------------------------------------------------------
    # Execution — flat (no groups)
    # ------------------------------------------------------------------

    def execute(
        self,
        dataflows: List[DataFlow],
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]] = None,
    ) -> ExecutionResult:
        """Execute *dataflows* in parallel.

        Args:
            dataflows: Items to process.
            process_fn: Callable receiving a :class:`DataFlow` and returning
                :class:`DataFlowRuntimeInfo`.
            callback: Optional per-item completion callback.

        Returns:
            Aggregated execution statistics.
        """
        if not dataflows:
            return ExecutionResult()

        start = utc_now()
        agg = ExecutionResult(total=len(dataflows))
        max_w = self._get_max_workers(len(dataflows))

        with ThreadPoolExecutor(max_workers=max_w) as pool:
            future_to_df: Dict[Future[DataFlowRuntimeInfo], DataFlow] = {}
            for df in dataflows:
                future = pool.submit(self._safe_execute, df, process_fn)
                future_to_df[future] = df

            for future in as_completed(future_to_df):
                df = future_to_df[future]
                try:
                    df_result = future.result()
                    self._update_counters(agg, df_result)
                    if callback:
                        callback(df_result)
                except Exception as exc:
                    agg.failed += 1
                    agg.errors[df.dataflow_id] = str(exc)
                    if self._stop_on_error:
                        for f in future_to_df:
                            f.cancel()
                        break

        agg.duration_seconds = (utc_now() - start).total_seconds()
        return agg

    # ------------------------------------------------------------------
    # Execution — sequential (single group or small list)
    # ------------------------------------------------------------------

    def execute_sequential(
        self,
        dataflows: List[DataFlow],
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]] = None,
    ) -> ExecutionResult:
        """Execute *dataflows* one by one (for grouped / ordered execution)."""
        if not dataflows:
            return ExecutionResult()

        start = utc_now()
        agg = ExecutionResult(total=len(dataflows))

        for df in dataflows:
            df_result = self._safe_execute(df, process_fn)
            self._update_counters(agg, df_result)
            if callback:
                callback(df_result)
            if self._stop_on_error and self._status_of(df_result) == DataFlowStatus.FAILED.value:
                break

        agg.duration_seconds = (utc_now() - start).total_seconds()
        return agg

    # ------------------------------------------------------------------
    # Execution — grouped
    # ------------------------------------------------------------------

    def execute_with_groups(
        self,
        groups: Dict[Optional[int], List[DataFlow]],
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]] = None,
    ) -> ExecutionResult:
        """Execute respecting group ordering.

        * ``None`` group → individual items run in parallel.
        * Numbered groups → items within a group run sequentially;
          different groups run in parallel.
        """
        independent = groups.get(None, [])
        grouped = {k: v for k, v in groups.items() if k is not None}

        total = len(independent) + sum(len(g) for g in grouped.values())
        if total == 0:
            return ExecutionResult()

        start = utc_now()
        agg = ExecutionResult(total=total)

        # Build task list: each entry is either ("single", DataFlow) or
        # ("group", (group_num, [DataFlow, ...]))
        tasks: List[tuple] = []
        for df in independent:
            tasks.append(("single", df))
        for gnum, dfs in grouped.items():
            tasks.append(("group", (gnum, dfs)))

        max_w = self._get_max_workers(len(tasks))

        with ThreadPoolExecutor(max_workers=max_w) as pool:
            futures: List[tuple] = []
            for task_type, task_data in tasks:
                if task_type == "single":
                    fut = pool.submit(self._safe_execute, task_data, process_fn)
                else:
                    _gnum, dfs = task_data
                    fut = pool.submit(
                        self._execute_group_sequential, dfs, process_fn, callback
                    )
                futures.append((task_type, fut))

            for task_type, fut in futures:
                try:
                    if task_type == "single":
                        df_result = fut.result()
                        self._update_counters(agg, df_result)
                        if callback:
                            callback(df_result)
                    else:
                        group_results: List[DataFlowRuntimeInfo] = fut.result()
                        for gr in group_results:
                            self._update_counters(agg, gr)
                except Exception as exc:
                    if self._stop_on_error:
                        raise

        agg.duration_seconds = (utc_now() - start).total_seconds()
        return agg

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _safe_execute(
        self,
        dataflow: DataFlow,
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
    ) -> DataFlowRuntimeInfo:
        """Execute *process_fn* with error isolation."""
        try:
            start = utc_now()
            return process_fn(dataflow)
        except Exception as exc:
            end = utc_now()
            return DataFlowRuntimeInfo(
                dataflow_id=dataflow.dataflow_id,
                start_time=start,
                end_time=end,
                status=DataFlowStatus.FAILED.value,
                error_message=str(exc),
            )

    def _execute_group_sequential(
        self,
        dataflows: List[DataFlow],
        process_fn: Callable[[DataFlow], DataFlowRuntimeInfo],
        callback: Optional[Callable[[DataFlowRuntimeInfo], None]] = None,
    ) -> List[DataFlowRuntimeInfo]:
        """Execute a group respecting execution_order.

        Dataflows are bucketed by ``execution_order``.  Each bucket is
        processed in ascending order.  Within a bucket, dataflows run
        **in parallel** when there is more than one item; a single-item
        bucket is executed directly to avoid thread-pool overhead.
        """
        # Bucket by execution_order (preserve original insertion order within
        # each bucket — the list is already sorted by execution_order from
        # JobDistributor.group_dataflows).
        buckets: Dict[int, List[DataFlow]] = {}
        for df in dataflows:
            key = df.execution_order or 0
            buckets.setdefault(key, []).append(df)

        results: List[DataFlowRuntimeInfo] = []
        stop = False

        for _order in sorted(buckets):
            if stop:
                break
            bucket = buckets[_order]

            if len(bucket) == 1:
                # Single item — run directly.
                result = self._safe_execute(bucket[0], process_fn)
                results.append(result)
                if callback:
                    callback(result)
                if self._stop_on_error and self._status_of(result) == DataFlowStatus.FAILED.value:
                    stop = True
            else:
                # Multiple items with the same execution_order — run in parallel.
                max_w = self._get_max_workers(len(bucket))
                with ThreadPoolExecutor(max_workers=max_w) as pool:
                    future_to_df: Dict[Future[DataFlowRuntimeInfo], DataFlow] = {
                        pool.submit(self._safe_execute, df, process_fn): df
                        for df in bucket
                    }
                    for future in as_completed(future_to_df):
                        result = future.result()
                        results.append(result)
                        if callback:
                            callback(result)
                        if self._stop_on_error and self._status_of(result) == DataFlowStatus.FAILED.value:
                            for f in future_to_df:
                                f.cancel()
                            stop = True
                            break

        return results
