"""Tests for datacoolie.orchestration.parallel_executor."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.models import (
    Connection,
    DataFlow,
    DataFlowRuntimeInfo,
    Destination,
    Source,
)
from datacoolie.orchestration.parallel_executor import ExecutionResult, ParallelExecutor


# ============================================================================
# Helpers
# ============================================================================


def _make_df(dataflow_id: str = "df-1", group_number: int | None = None, execution_order: int | None = None) -> DataFlow:
    conn = Connection(name="c", format="delta", configure={"base_path": "/tmp"})
    return DataFlow(
        dataflow_id=dataflow_id,
        group_number=group_number,
        execution_order=execution_order,
        source=Source(connection=conn, table="src"),
        destination=Destination(connection=conn, table="dst"),
    )


def _ok_runtime(df: DataFlow) -> DataFlowRuntimeInfo:
    now = datetime.now(timezone.utc)
    return DataFlowRuntimeInfo(
        dataflow_id=df.dataflow_id,
        start_time=now,
        end_time=now,
        status=DataFlowStatus.SUCCEEDED.value,
    )


def _skip_runtime(df: DataFlow) -> DataFlowRuntimeInfo:
    now = datetime.now(timezone.utc)
    return DataFlowRuntimeInfo(
        dataflow_id=df.dataflow_id,
        start_time=now,
        end_time=now,
        status=DataFlowStatus.SKIPPED.value,
    )


def _fail_runtime(df: DataFlow) -> DataFlowRuntimeInfo:
    now = datetime.now(timezone.utc)
    return DataFlowRuntimeInfo(
        dataflow_id=df.dataflow_id,
        start_time=now,
        end_time=now,
        status=DataFlowStatus.FAILED.value,
        error_message="boom",
    )


# ============================================================================
# ExecutionResult
# ============================================================================


class TestExecutionResult:
    def test_defaults(self):
        r = ExecutionResult()
        assert r.total == 0
        assert r.succeeded == 0
        assert r.failed == 0
        assert r.skipped == 0
        assert r.success_rate == 0.0
        assert r.has_failures is False

    def test_success_rate(self):
        r = ExecutionResult(total=10, succeeded=7, failed=2, skipped=1)
        assert r.success_rate == 70.0
        assert r.has_failures is True


# ============================================================================
# execute
# ============================================================================


class TestExecute:
    def test_empty_list(self):
        ex = ParallelExecutor(max_workers=2)
        result = ex.execute([], _ok_runtime)
        assert result.total == 0

    def test_all_succeed(self):
        ex = ParallelExecutor(max_workers=2)
        dfs = [_make_df(f"df-{i}") for i in range(5)]
        result = ex.execute(dfs, _ok_runtime)
        assert result.total == 5
        assert result.succeeded == 5
        assert result.failed == 0

    def test_mixed_results(self):
        results_map = {
            "df-ok": DataFlowStatus.SUCCEEDED.value,
            "df-skip": DataFlowStatus.SKIPPED.value,
            "df-fail": DataFlowStatus.FAILED.value,
        }

        def process(df):
            now = datetime.now(timezone.utc)
            s = results_map[df.dataflow_id]
            return DataFlowRuntimeInfo(
                dataflow_id=df.dataflow_id,
                start_time=now,
                end_time=now,
                status=s,
                error_message="err" if s == DataFlowStatus.FAILED.value else None,
            )

        ex = ParallelExecutor(max_workers=4)
        dfs = [_make_df("df-ok"), _make_df("df-skip"), _make_df("df-fail")]
        result = ex.execute(dfs, process)
        assert result.total == 3
        assert result.succeeded == 1
        assert result.skipped == 1
        assert result.failed == 1
        assert "df-fail" in result.errors

    def test_callback_invoked(self):
        cb = MagicMock()
        ex = ParallelExecutor(max_workers=1)
        dfs = [_make_df("a"), _make_df("b")]
        ex.execute(dfs, _ok_runtime, callback=cb)
        assert cb.call_count == 2

    def test_exception_in_process_fn_caught(self):
        """Exceptions in process_fn produce FAILED runtime via _safe_execute."""
        def exploding(df):
            raise RuntimeError("kaboom")

        ex = ParallelExecutor(max_workers=1)
        result = ex.execute([_make_df("x")], exploding)
        assert result.total == 1
        assert result.failed == 1

    def test_duration_tracked(self):
        ex = ParallelExecutor(max_workers=1)
        result = ex.execute([_make_df()], _ok_runtime)
        assert result.duration_seconds >= 0


# ============================================================================
# execute_sequential
# ============================================================================


class TestExecuteSequential:
    def test_empty(self):
        ex = ParallelExecutor()
        result = ex.execute_sequential([], _ok_runtime)
        assert result.total == 0

    def test_ordered_execution(self):
        """Items are processed in order."""
        order = []

        def track(df):
            order.append(df.dataflow_id)
            return _ok_runtime(df)

        ex = ParallelExecutor()
        dfs = [_make_df("a"), _make_df("b"), _make_df("c")]
        ex.execute_sequential(dfs, track)
        assert order == ["a", "b", "c"]

    def test_stop_on_error(self):
        call_count = 0

        def fail_second(df):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                return _fail_runtime(df)
            return _ok_runtime(df)

        ex = ParallelExecutor(stop_on_error=True)
        dfs = [_make_df("a"), _make_df("b"), _make_df("c")]
        result = ex.execute_sequential(dfs, fail_second)
        assert result.succeeded == 1
        assert result.failed == 1
        assert call_count == 2  # stopped before c


# ============================================================================
# execute_with_groups
# ============================================================================


class TestExecuteWithGroups:
    def test_empty_groups(self):
        ex = ParallelExecutor(max_workers=2)
        result = ex.execute_with_groups({}, _ok_runtime)
        assert result.total == 0

    def test_independent_only(self):
        ex = ParallelExecutor(max_workers=2)
        dfs = [_make_df("a"), _make_df("b")]
        groups = {None: dfs}
        result = ex.execute_with_groups(groups, _ok_runtime)
        assert result.total == 2
        assert result.succeeded == 2

    def test_grouped_only(self):
        ex = ParallelExecutor(max_workers=2)
        groups = {
            1: [_make_df("g1a", group_number=1, execution_order=1),
                _make_df("g1b", group_number=1, execution_order=2)],
        }
        result = ex.execute_with_groups(groups, _ok_runtime)
        assert result.total == 2
        assert result.succeeded == 2

    def test_mixed(self):
        ex = ParallelExecutor(max_workers=4)
        groups = {
            None: [_make_df("ind1"), _make_df("ind2")],
            1: [_make_df("g1a", group_number=1)],
            2: [_make_df("g2a", group_number=2), _make_df("g2b", group_number=2)],
        }
        result = ex.execute_with_groups(groups, _ok_runtime)
        assert result.total == 5
        assert result.succeeded == 5

    def test_group_sequential_order(self):
        """Within a group, items are processed sequentially by execution_order."""
        order = []

        def track(df):
            order.append(df.dataflow_id)
            return _ok_runtime(df)

        ex = ParallelExecutor(max_workers=1)
        groups = {
            1: [_make_df("a", group_number=1, execution_order=1),
                _make_df("b", group_number=1, execution_order=2)],
        }
        ex.execute_with_groups(groups, track)
        assert order == ["a", "b"]

    def test_same_execution_order_runs_parallel(self):
        """Dataflows with the same execution_order in a group run in parallel."""
        order = []

        def slow_track(df):
            """Sleep briefly so parallel execution overlaps."""
            time.sleep(0.05)
            order.append(df.dataflow_id)
            return _ok_runtime(df)

        ex = ParallelExecutor(max_workers=4)
        groups = {
            1: [
                _make_df("a1", group_number=1, execution_order=1),
                _make_df("a2", group_number=1, execution_order=1),
                _make_df("a3", group_number=1, execution_order=1),
                _make_df("b1", group_number=1, execution_order=2),
            ],
        }
        result = ex.execute_with_groups(groups, slow_track)
        assert result.total == 4
        assert result.succeeded == 4
        # b1 (order=2) must come after all order=1 items
        assert order.index("b1") == 3

    def test_execution_order_buckets_sequential(self):
        """Different execution_orders are processed sequentially."""
        timestamps = {}

        def timed(df):
            timestamps[df.dataflow_id] = time.monotonic()
            time.sleep(0.05)
            return _ok_runtime(df)

        ex = ParallelExecutor(max_workers=4)
        groups = {
            1: [
                _make_df("o1", group_number=1, execution_order=1),
                _make_df("o2", group_number=1, execution_order=2),
                _make_df("o3", group_number=1, execution_order=3),
            ],
        }
        ex.execute_with_groups(groups, timed)
        # Each order bucket must start after the previous one finishes.
        assert timestamps["o1"] < timestamps["o2"]
        assert timestamps["o2"] < timestamps["o3"]


# ============================================================================
# _safe_execute
# ============================================================================


class TestSafeExecute:
    def test_propagates_success(self):
        ex = ParallelExecutor()
        df = _make_df()
        result = ex._safe_execute(df, _ok_runtime)
        assert result.status == DataFlowStatus.SUCCEEDED.value

    def test_catches_exception(self):
        ex = ParallelExecutor()
        df = _make_df("bad")

        def boom(d):
            raise RuntimeError("fail!")

        result = ex._safe_execute(df, boom)
        assert result.status == DataFlowStatus.FAILED.value
        assert result.error_message == "fail!"
        assert result.dataflow_id == "bad"


# ============================================================================
# Additional edge cases (merged from edge-case module)
# ============================================================================


class TestParallelExecutorEdgeCases:
    def test_get_max_workers_uses_fallback_when_cpu_count_none(self):
        ex = ParallelExecutor(max_workers=None)
        with patch("datacoolie.orchestration.parallel_executor.os.cpu_count", return_value=None):
            assert ex._get_max_workers(3) == 3

    def test_update_counters_failed_without_error_payload(self):
        ex = ParallelExecutor()
        agg = ex.execute([], _ok_runtime)
        bad = DataFlowRuntimeInfo(
            dataflow_id="x",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            status=DataFlowStatus.FAILED.value,
            error_message=None,
        )
        ex._update_counters(agg, bad)
        assert agg.failed == 1
        assert agg.errors == {}

    def test_execute_handles_future_exception_and_continues(self):
        ex = ParallelExecutor(max_workers=2, stop_on_error=False)
        dfs = [_make_df("a"), _make_df("b")]

        def flaky(df, _process_fn):
            if df.dataflow_id == "a":
                raise RuntimeError("future boom")
            return _ok_runtime(df)

        with patch.object(ex, "_safe_execute", side_effect=flaky):
            result = ex.execute(dfs, _ok_runtime)

        assert result.total == 2
        assert result.failed == 1
        assert result.succeeded == 1
        assert result.errors["a"] == "future boom"

    def test_execute_stop_on_error_cancels_and_breaks(self):
        ex = ParallelExecutor(max_workers=2, stop_on_error=True)
        dfs = [_make_df("a"), _make_df("b")]

        def flaky(df, _process_fn):
            raise RuntimeError("boom")

        with patch.object(ex, "_safe_execute", side_effect=flaky):
            result = ex.execute(dfs, _ok_runtime)

        assert result.failed >= 1

    def test_execute_sequential_callback_invoked(self):
        cb = MagicMock()
        ex = ParallelExecutor(stop_on_error=False)
        dfs = [_make_df("a"), _make_df("b")]

        ex.execute_sequential(dfs, _ok_runtime, callback=cb)
        assert cb.call_count == 2

    def test_execute_with_groups_raises_when_group_future_fails_and_stop_on_error(self):
        ex = ParallelExecutor(stop_on_error=True)
        groups = {1: [_make_df("g1", execution_order=1)]}

        with patch.object(ex, "_execute_group_sequential", side_effect=RuntimeError("group fail")):
            with pytest.raises(RuntimeError, match="group fail"):
                ex.execute_with_groups(groups, _ok_runtime)

    def test_execute_with_groups_swallow_group_failure_when_not_stop_on_error(self):
        ex = ParallelExecutor(stop_on_error=False)
        groups = {
            None: [_make_df("single")],
            1: [_make_df("g1", execution_order=1)],
        }

        with patch.object(ex, "_execute_group_sequential", side_effect=RuntimeError("group fail")):
            result = ex.execute_with_groups(groups, _ok_runtime)

        assert result.total == 2
        assert result.succeeded == 1

    def test_group_sequential_single_bucket_callback_and_stop(self):
        cb = MagicMock()
        ex = ParallelExecutor(stop_on_error=True)
        dfs = [_make_df("a", execution_order=1), _make_df("b", execution_order=2)]

        def proc(df):
            if df.dataflow_id == "a":
                return _fail_runtime(df)
            return _ok_runtime(df)

        results = ex._execute_group_sequential(dfs, proc, callback=cb)
        assert len(results) == 1
        assert results[0].status == DataFlowStatus.FAILED.value
        cb.assert_called_once()

    def test_group_sequential_parallel_bucket_callback_and_stop(self):
        cb = MagicMock()
        ex = ParallelExecutor(stop_on_error=True)
        dfs = [
            _make_df("a", execution_order=1),
            _make_df("b", execution_order=1),
            _make_df("c", execution_order=2),
        ]

        def proc(df):
            if df.dataflow_id == "a":
                return _fail_runtime(df)
            return _ok_runtime(df)

        results = ex._execute_group_sequential(dfs, proc, callback=cb)
        assert any(r.status == DataFlowStatus.FAILED.value for r in results)
        assert cb.call_count >= 1
