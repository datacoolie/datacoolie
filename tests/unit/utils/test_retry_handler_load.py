"""Retry scheduling, bounded execution, nesting and concurrent load checks.

Tests for datacoolie.utils.retry under:
  - Ordered exponential backoff requests
  - Bounded retry attempts and delays
  - Cascading retries (job retry triggers step retry)
  - Multiple concurrent job retries under load
"""

from __future__ import annotations

import concurrent.futures
from threading import Lock
import time
from types import SimpleNamespace
import pytest

from datacoolie.utils import retry as retry_module
from datacoolie.utils.retry import RetryHandler


@pytest.fixture
def retry_events(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, object]]:
    """Record this module's sleep boundary without patching the shared clock."""
    events: list[tuple[str, object]] = []
    monkeypatch.setattr(
        retry_module,
        "time",
        SimpleNamespace(sleep=lambda delay: events.append(("sleep", delay))),
    )
    return events


# ============================================================================
# Exponential backoff timing validation
# ============================================================================


class TestRetryHandlerBackoffTiming:
    """Verify exponential backoff delays are accurate."""

    @pytest.mark.unit
    def test_backoff_capped_at_max_delay(self) -> None:
        """Verify delays cap at max_delay."""
        h = RetryHandler(
            retry_count=5,
            retry_delay=10.0,
            backoff_multiplier=2.0,
            max_delay=50.0,
        )
        
        # 10, 20, 40, 80→50, 100→50, 200→50
        expected_delays = [10.0, 20.0, 40.0, 50.0, 50.0, 50.0]
        
        for attempt, expected in enumerate(expected_delays):
            actual = h.compute_delay(attempt)
            assert actual == expected

    @pytest.mark.unit
    def test_backoff_with_custom_multiplier(self) -> None:
        """Verify different multipliers (3x backoff)."""
        h = RetryHandler(
            retry_count=4,
            retry_delay=1.0,
            backoff_multiplier=3.0,
            max_delay=100.0,
        )
        
        # 1, 3, 9, 27, 81
        expected = [1.0, 3.0, 9.0, 27.0, 81.0]
        
        for attempt, exp_delay in enumerate(expected):
            assert h.compute_delay(attempt) == exp_delay


# ============================================================================
# Retry scheduling without wall-clock sleeps
# ============================================================================


class TestRetryHandlerActualTiming:
    """Verify execution requests the expected sleeps between attempts."""

    @pytest.mark.unit
    def test_actual_sleep_time_matches_backoff(
        self, retry_events: list[tuple[str, object]]
    ) -> None:
        """Backoff sleeps occur in order, only between failed attempts."""
        h = RetryHandler(
            retry_count=2,
            retry_delay=0.1,  # 100ms
            backoff_multiplier=2.0,
            max_delay=10.0,
        )
        
        call_count = 0
        
        def failing_then_success() -> str:
            nonlocal call_count
            call_count += 1
            retry_events.append(("call", call_count))
            if call_count < 3:
                raise ValueError(f"Attempt {call_count}")
            return "success"
        
        result, attempts = h.execute(failing_then_success)

        assert result == "success"
        assert attempts == 3
        assert call_count == 3
        assert retry_events == [
            ("call", 1), ("sleep", 0.1), ("call", 2), ("sleep", 0.2), ("call", 3)
        ]

    @pytest.mark.unit
    def test_retry_with_long_delays(self, retry_events: list[tuple[str, object]]) -> None:
        """A long configured delay is forwarded before retrying, without real waits."""
        h = RetryHandler(
            retry_count=2,
            retry_delay=30.0,
            backoff_multiplier=1.0,
            max_delay=60.0,
        )
        
        call_count = 0
        
        def eventually_succeeds() -> int:
            nonlocal call_count
            call_count += 1
            retry_events.append(("call", call_count))
            if call_count < 2:
                raise RuntimeError("Fail once")
            return call_count
        
        result, attempts = h.execute(eventually_succeeds)
        
        assert result == 2
        assert attempts == 2
        assert call_count == 2
        assert retry_events == [("call", 1), ("sleep", 30.0), ("call", 2)]


# ============================================================================
# Cascading retries (multi-level)
# ============================================================================


class TestRetryCascading:
    """Test cascading failures: job fails, step within job retries."""

    @pytest.mark.unit
    def test_nested_retry_scenario(self, retry_events: list[tuple[str, object]]) -> None:
        """Inner exhaustion retries the outer job, whose inner retry then succeeds."""
        job_handler = RetryHandler(retry_count=1, retry_delay=0.01)  # 1 retry = 2 attempts
        step_handler = RetryHandler(retry_count=2, retry_delay=0.01)  # 2 retries = 3 attempts
        
        job_calls = 0
        steps_by_job: dict[int, int] = {}

        def job_function() -> bool:
            nonlocal job_calls
            job_calls += 1
            job_attempt = job_calls
            retry_events.append(("job", job_attempt))
            steps_by_job[job_attempt] = 0

            def step_function() -> bool:
                steps_by_job[job_attempt] += 1
                step_attempt = steps_by_job[job_attempt]
                retry_events.append(("step", (job_attempt, step_attempt)))
                if job_attempt == 1 or step_attempt == 1:
                    raise RuntimeError("Step fail")
                return True

            result, step_attempts = step_handler.execute(step_function)
            assert step_attempts == steps_by_job[job_attempt]
            return result

        success, job_attempts = job_handler.execute(job_function)

        assert success is True
        assert job_attempts == job_calls == 2
        assert steps_by_job == {1: 3, 2: 2}
        assert retry_events == [
            ("job", 1), ("step", (1, 1)), ("sleep", 0.01),
            ("step", (1, 2)), ("sleep", 0.02), ("step", (1, 3)),
            ("sleep", 0.01), ("job", 2), ("step", (2, 1)),
            ("sleep", 0.01), ("step", (2, 2)),
        ]

    @pytest.mark.unit
    def test_retry_propagation_up_call_stack(
        self, retry_events: list[tuple[str, object]]
    ) -> None:
        """Errors at step level propagate correctly through retry levels.
        
        Validates:
          - Step fails 10 times (exceeds max_attempts of 3)
          - Error propagates to job level
          - Job retry catches and retries entire job
        """
        step_handler = RetryHandler(retry_count=2, retry_delay=0.01)  # 3 max attempts
        job_handler = RetryHandler(retry_count=1, retry_delay=0.01)   # 2 max attempts
        
        call_count = 0
        job_call_count = 0
        
        def always_fails_step() -> None:
            nonlocal call_count
            call_count += 1
            raise ValueError(f"Step fail #{call_count}")
        
        def job_with_step() -> None:
            nonlocal job_call_count
            job_call_count += 1
            _, _ = step_handler.execute(always_fails_step)
        
        # Job should fail after exhausting its retries
        with pytest.raises(ValueError):
            job_handler.execute(job_with_step)
        
        # verify job was attempted twice (initial + 1 retry)
        assert job_call_count == 2
        # Step was attempted 3 times per job × 2 jobs = 6 calls total
        assert call_count == 3 * job_call_count


# ============================================================================
# Concurrent retries under load
# ============================================================================


class TestRetryConcurrentLoad:
    """Multiple jobs retrying concurrently."""

    @pytest.mark.unit
    def test_concurrent_job_retries_no_interference(self) -> None:
        """10 jobs retrying concurrently don't interfere.
        
        Validates:
          - Each job has its own retry state
          - Concurrent retries don't block each other
                    - All jobs eventually succeed
        """
        num_jobs = 10
        handler = RetryHandler(retry_count=2, retry_delay=0.01)
        
        results: list = []
        
        def job_task(job_id: int) -> None:
            fail_count = [0]  # Closure counter
            
            def flaky_work() -> str:
                fail_count[0] += 1
                if fail_count[0] < 2:
                    raise RuntimeError(f"Job {job_id} fail {fail_count[0]}")
                return f"job-{job_id}-success"
            
            result, attempts = handler.execute(flaky_work)
            results.append((job_id, result, attempts))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_jobs) as executor:
            futures = [executor.submit(job_task, j) for j in range(num_jobs)]
            for f in futures:
                f.result()
        
        # All jobs should succeed
        assert len(results) == num_jobs
        for job_id, result, attempts in results:
            assert "success" in result
            assert attempts == 2  # 1 fail + 1 success

    @pytest.mark.unit
    def test_high_concurrency_retry_storm(self) -> None:
        """50 concurrent jobs with one retry budget = potential retry storm.
        
        Validates:
          - No deadlocks
          - All jobs complete
          - Timing stays reasonable (<10s for 50 jobs)
        """
        num_jobs = 50
        handler = RetryHandler(retry_count=1, retry_delay=0.01)
        
        attempts_by_job: dict[int, int] = {}
        outcomes: dict[int, tuple[str, object]] = {}
        state_lock = Lock()
        
        def quick_job(job_id: int) -> None:
            def work() -> bool:
                with state_lock:
                    attempts_by_job[job_id] = attempts_by_job.get(job_id, 0) + 1
                if job_id % 3 == 0:
                    raise RuntimeError("Retry")
                return True
            
            try:
                result, attempts = handler.execute(work)
            except RuntimeError:
                with state_lock:
                    outcomes[job_id] = ("failed", None)
            else:
                with state_lock:
                    outcomes[job_id] = ("succeeded", (result, attempts))
        
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(quick_job, j) for j in range(num_jobs)]
            for f in futures:
                f.result()
        elapsed = time.time() - start
        
        expected_failed = {job_id for job_id in range(num_jobs) if job_id % 3 == 0}
        expected_succeeded = set(range(num_jobs)) - expected_failed

        assert set(outcomes) == set(range(num_jobs))
        assert {
            job_id for job_id, (status, _detail) in outcomes.items() if status == "failed"
        } == expected_failed
        assert {
            job_id
            for job_id, (status, _detail) in outcomes.items()
            if status == "succeeded"
        } == expected_succeeded
        assert attempts_by_job == {
            job_id: 2 if job_id in expected_failed else 1
            for job_id in range(num_jobs)
        }
        assert all(
            detail == (True, 1)
            for status, detail in outcomes.values()
            if status == "succeeded"
        )
        assert elapsed < 10.0  # Should complete in reasonable time


# ============================================================================
# Bounded retry execution
# ============================================================================


class TestRetryHandlerBudget:
    """Keep retry execution bounded by the configured attempt and delay budget."""

    @pytest.mark.unit
    def test_total_time_bounded_by_max_delay_and_attempts(
        self, retry_events: list[tuple[str, object]]
    ) -> None:
        """All budgeted attempts execute, with capped sleeps and none after success."""
        h = RetryHandler(
            retry_count=3,
            retry_delay=0.05,
            backoff_multiplier=2.0,
            max_delay=0.1,
        )
        
        call_count = [0]
        
        def work() -> bool:
            call_count[0] += 1
            retry_events.append(("call", call_count[0]))
            if call_count[0] < 4:  # Fail 3 times, succeed on 4th
                raise RuntimeError("Fail")
            return True
        
        result, attempts = h.execute(work)

        assert result is True
        assert attempts == call_count[0] == 4
        assert retry_events == [
            ("call", 1), ("sleep", 0.05), ("call", 2), ("sleep", 0.1),
            ("call", 3), ("sleep", 0.1), ("call", 4),
        ]

