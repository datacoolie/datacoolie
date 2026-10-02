"""Small dependency-free retry utility used by runtime components."""

from __future__ import annotations

import time
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class RetryHandler:
    """Retry a callable with bounded exponential backoff."""

    def __init__(
        self,
        retry_count: int = 0,
        retry_delay: float = 5.0,
        backoff_multiplier: float = 2.0,
        max_delay: float = 60.0,
    ) -> None:
        if retry_count < 0:
            raise ValueError("retry_count must be non-negative")
        if retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")
        if backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")
        if max_delay < 0:
            raise ValueError("max_delay must be non-negative")

        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._backoff_multiplier = backoff_multiplier
        self._max_delay = max_delay

    @property
    def retry_count(self) -> int:
        return self._retry_count

    @property
    def retry_delay(self) -> float:
        return self._retry_delay

    @property
    def backoff_multiplier(self) -> float:
        return self._backoff_multiplier

    @property
    def max_delay(self) -> float:
        return self._max_delay

    @property
    def max_attempts(self) -> int:
        """Total attempts, including the initial call."""
        return 1 + self._retry_count

    def compute_delay(self, attempt: int) -> float:
        """Return the delay before retry *attempt* (zero-based)."""
        delay = self._retry_delay * (self._backoff_multiplier ** attempt)
        return min(delay, self._max_delay)

    def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        on_retry: Callable[[int, int, Exception, float], None] | None = None,
        **kwargs: Any,
    ) -> tuple[T, int]:
        """Execute *func*, retrying failures up to the configured budget.

        ``on_retry`` is an optional caller-owned observation hook.  The retry
        utility itself deliberately does not log or emit framework events so
        metadata/database retries cannot accidentally appear as dataflow
        lifecycle events.
        """
        last_error: Exception | None = None

        for attempt in range(self.max_attempts):
            try:
                return func(*args, **kwargs), attempt + 1
            except Exception as exc:
                last_error = exc
                if attempt >= self._retry_count:
                    raise
                delay = self.compute_delay(attempt)
                if on_retry is not None:
                    try:
                        on_retry(attempt + 1, self.max_attempts, exc, delay)
                    except Exception:
                        # Diagnostics must never alter retry behavior or hide
                        # the original operation exception.
                        pass
                time.sleep(delay)

        if last_error is None:
            raise RuntimeError("Retry handler exited without result")
        raise last_error

    def __repr__(self) -> str:
        return (
            f"RetryHandler(retry_count={self._retry_count}, "
            f"retry_delay={self._retry_delay}, "
            f"backoff_multiplier={self._backoff_multiplier}, "
            f"max_delay={self._max_delay})"
        )
