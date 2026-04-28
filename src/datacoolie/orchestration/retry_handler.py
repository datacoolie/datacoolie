"""Retry handler with exponential backoff.

``RetryHandler`` wraps a callable and retries it on *any* exception up to
the configured number of attempts.  If all attempts fail the last exception
is re-raised.

Example::

    handler = RetryHandler(retry_count=3, retry_delay=2.0)
    result = handler.execute(my_function, arg1, arg2)
"""

from __future__ import annotations

import time
from typing import Any, Callable, TypeVar

from datacoolie.logging.base import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class RetryHandler:
    """Retry logic with configurable exponential backoff.

    Parameters:
        retry_count: Maximum number of retries (0 = no retry).
        retry_delay: Base delay in seconds between retries.
        backoff_multiplier: Multiplier applied each retry (exponential).
        max_delay: Upper cap on computed delay.

    The actual delay for attempt *n* is::

        min(retry_delay * backoff_multiplier ** n, max_delay)
    """

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

    # -- properties --------------------------------------------------------

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
        """Total attempts = 1 (initial) + retry_count."""
        return 1 + self._retry_count

    # -- core logic --------------------------------------------------------

    def compute_delay(self, attempt: int) -> float:
        """Compute the delay before the given retry *attempt* (0-based).

        Args:
            attempt: The retry attempt number (0 = first retry).

        Returns:
            Sleep duration in seconds.
        """
        delay = self._retry_delay * (self._backoff_multiplier ** attempt)
        return min(delay, self._max_delay)

    def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> tuple[T, int]:
        """Execute *func* with retry logic.

        Args:
            func: Callable to execute.
            *args: Positional arguments forwarded to *func*.
            **kwargs: Keyword arguments forwarded to *func*.

        Returns:
            ``(result, attempts)`` — the return value of *func* and the
            number of attempts used (1 = first-try success).

        Raises:
            Exception: The last exception once all attempts are exhausted.
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
                logger.warning(
                    "Attempt %d/%d failed (%s). Retrying in %.1f s …",
                    attempt + 1,
                    self.max_attempts,
                    exc,
                    delay,
                )
                time.sleep(delay)

        # Defensive fallback for impossible control flow.
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
