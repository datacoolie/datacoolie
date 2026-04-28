"""Tests for datacoolie.orchestration.retry_handler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datacoolie.orchestration.retry_handler import RetryHandler


# ============================================================================
# Construction / validation
# ============================================================================


class TestRetryHandlerInit:
    """Test RetryHandler constructor and validation of parameters."""

    def test_defaults(self) -> None:
        """Verify default parameters: retry_count=0, retry_delay=5s, backoff=2x, max_delay=60s."""
        h = RetryHandler()
        assert h.retry_count == 0
        assert h.retry_delay == 5.0
        assert h.backoff_multiplier == 2.0
        assert h.max_delay == 60.0
        assert h.max_attempts == 1

    def test_custom_params(self) -> None:
        """Verify custom parameters are stored and max_attempts = retry_count + 1."""
        h = RetryHandler(retry_count=3, retry_delay=1.0, backoff_multiplier=3.0, max_delay=30.0)
        assert h.retry_count == 3
        assert h.retry_delay == 1.0
        assert h.backoff_multiplier == 3.0
        assert h.max_delay == 30.0
        assert h.max_attempts == 4

    def test_negative_retry_count(self) -> None:
        """Verify ValueError raised for negative retry_count."""
        with pytest.raises(ValueError, match="retry_count"):
            RetryHandler(retry_count=-1)

    def test_negative_retry_delay(self) -> None:
        """Verify ValueError raised for negative retry_delay."""
        with pytest.raises(ValueError, match="retry_delay"):
            RetryHandler(retry_delay=-1)

    def test_low_backoff_multiplier(self) -> None:
        """Verify ValueError raised for backoff_multiplier < 1.0 (no exponential growth)."""
        with pytest.raises(ValueError, match="backoff_multiplier"):
            RetryHandler(backoff_multiplier=0.5)

    def test_negative_max_delay(self) -> None:
        """Verify ValueError raised for negative max_delay."""
        with pytest.raises(ValueError, match="max_delay"):
            RetryHandler(max_delay=-1)

    def test_repr(self) -> None:
        """Verify __repr__ includes class name and key parameters."""
        h = RetryHandler(retry_count=2, retry_delay=1.0)
        text = repr(h)
        assert "RetryHandler" in text
        assert "retry_count=2" in text


# ============================================================================
# compute_delay
# ============================================================================


class TestComputeDelay:
    """Test exponential backoff delay calculation."""

    def test_first_retry(self) -> None:
        """Verify first retry uses the base retry_delay (attempt 0)."""
        h = RetryHandler(retry_delay=2.0, backoff_multiplier=2.0)
        assert h.compute_delay(0) == 2.0

    def test_exponential_growth(self) -> None:
        """Verify delay doubles with each attempt: 1s, 2s, 4s, 8s."""
        h = RetryHandler(retry_delay=1.0, backoff_multiplier=2.0, max_delay=100)
        assert h.compute_delay(0) == 1.0
        assert h.compute_delay(1) == 2.0
        assert h.compute_delay(2) == 4.0
        assert h.compute_delay(3) == 8.0

    def test_capped_by_max_delay(self) -> None:
        """Verify delays are capped at max_delay (e.g., 30s capped to 25s)."""
        h = RetryHandler(retry_delay=10.0, backoff_multiplier=3.0, max_delay=25.0)
        assert h.compute_delay(0) == 10.0
        assert h.compute_delay(1) == 25.0  # 30 capped to 25
        assert h.compute_delay(5) == 25.0


# ============================================================================
# execute
# ============================================================================


class TestExecute:
    """Test execute() method: success, retry, and failure scenarios."""

    def test_success_on_first_attempt(self) -> None:
        """Verify success on first call returns (result, 1 attempt)."""
        h = RetryHandler(retry_count=3)
        fn = MagicMock(return_value=42)
        result, attempts = h.execute(fn)
        assert result == 42
        assert attempts == 1
        fn.assert_called_once()

    def test_no_retry_when_count_zero(self) -> None:
        """Verify exception raised immediately when retry_count=0."""
        h = RetryHandler(retry_count=0)
        fn = MagicMock(side_effect=RuntimeError("fail"))
        with pytest.raises(RuntimeError):
            h.execute(fn)
        assert fn.call_count == 1

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_retries_on_any_exception(self, mock_sleep: MagicMock) -> None:
        """Verify retries occur with exponential delay: fail1, fail2, success."""
        h = RetryHandler(retry_count=2, retry_delay=1.0, backoff_multiplier=2.0)
        fn = MagicMock(side_effect=[ValueError("t1"), RuntimeError("t2"), 99])
        result, attempts = h.execute(fn)
        assert result == 99
        assert attempts == 3
        assert fn.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1.0)
        mock_sleep.assert_any_call(2.0)

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_raises_after_all_retries_exhausted(self, mock_sleep: MagicMock) -> None:
        """Verify exception raised after retry_count retries (1 initial + 2 retries = 3 calls)."""
        h = RetryHandler(retry_count=2, retry_delay=0.1)
        fn = MagicMock(side_effect=RuntimeError("always fails"))
        with pytest.raises(RuntimeError, match="always fails"):
            h.execute(fn)
        assert fn.call_count == 3  # 1 initial + 2 retries

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_forwards_args_and_kwargs(self, mock_sleep):

        def fn(a, b, c=10):
            return a + b + c

        h = RetryHandler(retry_count=1, retry_delay=0.1)
        result, attempts = h.execute(fn, 1, 2, c=3)
        assert result == 6
        assert attempts == 1


# ============================================================================
# attempts counting (returned from execute)
# ============================================================================


class TestAttemptsCounting:
    def test_one_on_first_success(self):
        h = RetryHandler(retry_count=3)
        _, attempts = h.execute(lambda: "ok")
        assert attempts == 1

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_counts_retries(self, mock_sleep):
        h = RetryHandler(retry_count=3, retry_delay=0.1)
        fn = MagicMock(side_effect=[RuntimeError("t"), ValueError("t"), "ok"])
        _, attempts = h.execute(fn)
        assert attempts == 3

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_counts_after_exhaustion(self, mock_sleep):
        h = RetryHandler(retry_count=2, retry_delay=0.1)
        fn = MagicMock(side_effect=RuntimeError("always"))
        with pytest.raises(RuntimeError):
            h.execute(fn)


# ============================================================================
# Additional edge cases (merged from edge-case module)
# ============================================================================


class TestRetryHandlerEdgeCases:
    @patch("datacoolie.orchestration.retry_handler.range", return_value=[])
    def test_execute_impossible_fallback_assertion(self, _mock_range):
        h = RetryHandler(retry_count=0)
        with pytest.raises(RuntimeError, match="without result"):
            h.execute(lambda: "never")

    @patch("datacoolie.orchestration.retry_handler.time.sleep")
    def test_execute_fallback_raises_last_error(self, _mock_sleep):
        h = RetryHandler(retry_count=0)

        def fake_range(_max_attempts):
            h._retry_count = 99
            return [0]

        with patch("datacoolie.orchestration.retry_handler.range", side_effect=fake_range):
            with pytest.raises(ValueError, match="boom"):
                h.execute(lambda: (_ for _ in ()).throw(ValueError("boom")))
