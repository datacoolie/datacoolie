"""Shared lifecycle for persistent framework loggers."""

from __future__ import annotations

import copy
import logging
import threading
from uuid import uuid4
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Sequence

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging.configuration.config import LogConfig as _LogConfig
from datacoolie.logging.configuration.constants import (
    INTERNAL_LOGGER_NAME,
    FlushResult as _FlushResult,
    PersistenceMode as _PersistenceMode,
)
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.time import utc_now

_diagnostic_logger = logging.getLogger(INTERNAL_LOGGER_NAME)


@dataclass(frozen=True)
class _FlushOperation:
    """One terminal sink attempt built from immutable inputs."""

    name: str
    execute: Callable[[], object]


@dataclass(frozen=True)
class _FlushOutcome:
    """Observed result of one terminal sink attempt."""

    name: str
    status: str
    error: Optional[BaseException] = None


@dataclass
class _TrackedOperation:
    """One bounded startup operation that may outlive its caller's wait."""

    name: str
    event: threading.Event
    result: object = None
    error: Optional[BaseException] = None
    timeout_reported: bool = False
    timeout_error: Optional[BaseException] = None
    observed: bool = False


class BaseLogger(ABC):
    """Abstract base for persistent loggers (system, execution).

    Provides configuration, explicit activation, periodic scheduling,
    bounded terminal sink attempts, failure isolation, and cleanup ordering.
    Children define what periodic and terminal operations persist.
    """

    _periodic_sink_name = "periodic"

    def __init__(self, config: _LogConfig, platform: Optional[BasePlatform] = None) -> None:
        self._config = copy.deepcopy(config)
        # Dataclass fields remain mutable after construction; validate the
        # detached snapshot at the logger boundary so activation never starts
        # with an invalid persistence contract.
        self._config.__post_init__()
        self._platform = platform
        self._is_active = False
        self._is_closed = False
        self._is_closing = False
        self._run_config: Optional[DataCoolieRunConfig] = None
        # One immutable identity ties the system, job, and dataflow streams
        # emitted by a single logger lifetime together.  The Driver overwrites
        # this with its shared session identity for the two session loggers;
        # standalone loggers still get an isolated identity by default.
        self._log_session_id = uuid4().hex
        self._started_at: Optional[datetime] = None
        self._flush_lock = threading.RLock()
        self._close_lock = threading.Lock()
        self._last_flush_error: Optional[BaseException] = None
        self._flush_errors: dict[str, BaseException] = {}
        self._terminal_outcomes: tuple[_FlushOutcome, ...] = ()
        self._stop_event = threading.Event()
        self._flush_wakeup = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None
        self._periodic_time_due = False
        self._startup_operation: Optional[_TrackedOperation] = None

    # ------------------------------------------------------------------
    # Periodic flush timer
    # ------------------------------------------------------------------

    def _should_start_timer(self) -> bool:
        """Whether periodic flushing is enabled."""
        return (
            (
                self._config.flush_interval_seconds > 0
                or self._config.persistence_mode == _PersistenceMode.BATCH.value
            )
            and self._config.output_path is not None
            and self._platform is not None
        )

    def _request_periodic_flush(self) -> None:
        """Wake a batch writer without performing remote I/O on a producer."""
        self._flush_wakeup.set()

    def activate(self, *, started_at: Optional[datetime] = None) -> None:
        """Activate persistence for one caller-owned job lifetime.

        Driver passes its constructor timestamp so provider startup and
        preparation are included in the same JobRuntime interval. Standalone
        callers may omit it and use the activation timestamp.
        """
        with self._close_lock:
            if self._is_active or self._is_closing or self._is_closed:
                return
            try:
                if started_at is not None:
                    if not isinstance(started_at, datetime):
                        raise ConfigurationError("started_at must be a datetime")
                    if started_at.tzinfo is None or started_at.utcoffset() is None:
                        raise ConfigurationError(
                            "started_at must be timezone-aware"
                        )
                    started_at = started_at.astimezone(timezone.utc)
                if self._run_config is None:
                    self._run_config = DataCoolieRunConfig()
                    self._apply_run_config()
                if self._started_at is not None and started_at is not None:
                    if started_at != self._started_at:
                        raise ConfigurationError(
                            "started_at cannot change for a logger lifetime"
                        )
                self._started_at = self._started_at or started_at or utc_now()
                self._activate()
                self._is_active = True
                if not self._should_start_timer():
                    return
                self._stop_event.clear()
                self._flush_thread = threading.Thread(
                    target=self._flush_loop,
                    name=f"{type(self).__name__}-flush",
                    daemon=True,
                )
                self._flush_thread.start()
            except Exception:
                # A child may have started a timer before a later activation
                # step failed.  Stop that worker first, then let the child
                # release any other state it acquired.  Cleanup failures are
                # secondary: preserve the original activation exception.
                try:
                    self._stop_periodic_flush()
                except Exception as cleanup_exc:
                    _diagnostic_logger.warning(
                        "%s activation timer cleanup failed: %s",
                        type(self).__name__,
                        cleanup_exc,
                        exc_info=True,
                    )
                try:
                    self._activation_cleanup()
                except Exception as cleanup_exc:
                    _diagnostic_logger.warning(
                        "%s activation cleanup failed: %s",
                        type(self).__name__,
                        cleanup_exc,
                        exc_info=True,
                    )
                finally:
                    self._is_active = False
                raise

    def _stop_periodic_flush(self, *, timeout: Optional[float] = None) -> bool:
        """Stop scheduling and report whether the worker actually exited."""
        self._stop_event.set()
        self._flush_wakeup.set()
        thread = self._flush_thread
        if thread is None:
            return True
        if not thread.is_alive():
            self._flush_thread = None
            return True
        join_timeout = min(
            2.0,
            self._config.close_timeout_seconds,
        ) if timeout is None else max(0.0, timeout)
        thread.join(timeout=join_timeout)
        if not thread.is_alive():
            self._flush_thread = None
            return True
        return False

    def _flush_loop(self) -> None:
        """Single daemon thread — sleeps until interval elapses or stop is signalled."""
        interval = self._config.flush_interval_seconds
        deadline = time.monotonic() + interval if interval > 0 else None
        while not self._stop_event.is_set():
            # A wake-up is a size notification.  The monotonic deadline is a
            # separate time trigger, so a small batch cannot wait forever for
            # its byte target and frequent wake-ups cannot postpone the timer.
            timeout = (
                max(0.0, deadline - time.monotonic())
                if deadline is not None
                else None
            )
            triggered = self._flush_wakeup.wait(timeout)
            self._flush_wakeup.clear()
            if self._stop_event.is_set():
                return
            time_due = deadline is not None and time.monotonic() >= deadline
            if time_due and deadline is not None:
                now = time.monotonic()
                while deadline <= now:
                    deadline += interval
            if triggered or time_due:
                self._on_periodic_flush(time_due=time_due)

    def _on_periodic_flush(self, *, time_due: bool = False) -> None:
        """Run the child periodic hook through the common failure boundary."""
        if self._is_closing or self._is_closed:
            return
        previous_time_due = self._periodic_time_due
        self._periodic_time_due = time_due
        try:
            self._execute_flush(self._flush_periodic, reason="periodic")
        finally:
            self._periodic_time_due = previous_time_due

    def _flush_periodic(self) -> None:
        """Persist a periodic checkpoint.

        Children override this when they have a periodic sink.
        """
        return

    def _on_periodic_timeout(self) -> None:
        """Hook for child writers to record an ambiguous timed-out flush."""
        return

    def _on_terminal_timeout(self, sink_name: str) -> None:
        """Hook for child writers to record an ambiguous terminal flush."""
        return

    def _on_startup_timeout(self, sink_name: str) -> None:
        """Hook for a child to mark its startup writer as ambiguous."""
        return

    def _periodic_timeout_sinks(self) -> tuple[str, ...]:
        """Return the concrete streams blocked by a periodic worker."""
        return (self._periodic_sink_name,)

    def _run_bounded_startup(
        self,
        operation: Callable[[], object],
        *,
        sink_name: str,
    ) -> object:
        """Run startup I/O with a bounded caller wait and tracked ownership.

        A timeout does not cancel the worker.  The handle stays attached to
        the logger so later lifecycle operations cannot overtake its write.
        """

        handle = _TrackedOperation(sink_name, threading.Event())
        self._startup_operation = handle

        def run() -> None:
            try:
                handle.result = operation()
            except Exception as exc:
                handle.error = exc
            finally:
                handle.event.set()

        threading.Thread(
            target=run,
            name=f"{type(self).__name__}-{sink_name}-startup",
            daemon=True,
        ).start()
        if not handle.event.wait(timeout=self._config.close_timeout_seconds):
            handle.timeout_reported = True
            self._on_startup_timeout(sink_name)
            timeout_error = TimeoutError(
                f"{sink_name} startup checkpoint did not finish within "
                f"{self._config.close_timeout_seconds:g} seconds"
            )
            handle.timeout_error = timeout_error
            self._set_flush_error(sink_name, timeout_error)
            _diagnostic_logger.warning(
                "%s startup persistence timed out for %s after %.3g seconds",
                type(self).__name__,
                sink_name,
                self._config.close_timeout_seconds,
            )
            return _FlushResult.IN_FLIGHT
        if handle.error is not None:
            handle.observed = True
            self._set_flush_error(sink_name, handle.error)
            _diagnostic_logger.warning(
                "%s startup persistence failed for %s: %s",
                type(self).__name__,
                sink_name,
                handle.error,
                exc_info=(
                    type(handle.error),
                    handle.error,
                    handle.error.__traceback__,
                ),
            )
            return _FlushResult.NO_WORK
        handle.observed = True
        return handle.result

    def _wait_startup_operation(
        self,
        *,
        deadline: float,
    ) -> tuple[_FlushOutcome, ...]:
        """Wait for startup I/O within the shared close deadline."""

        handle = self._startup_operation
        if handle is None:
            return ()
        if handle.event.is_set():
            if handle.observed:
                return ()
            handle.observed = True
            if handle.error is not None:
                self._set_flush_error(handle.name, handle.error)
                _diagnostic_logger.warning(
                    "%s startup persistence failed late for %s: %s",
                    type(self).__name__,
                    handle.name,
                    handle.error,
                    exc_info=(
                        type(handle.error),
                        handle.error,
                        handle.error.__traceback__,
                    ),
                )
                return (_FlushOutcome(handle.name, "failed", handle.error),)
            if handle.timeout_error is not None:
                self._clear_flush_error(handle.name)
                return (_FlushOutcome(handle.name, "succeeded_late"),)
            return ()
        handle.event.wait(timeout=max(0.0, deadline - time.monotonic()))
        if handle.event.is_set():
            return self._wait_startup_operation(deadline=deadline)
        if not handle.timeout_reported:
            handle.timeout_reported = True
            self._on_startup_timeout(handle.name)
        error = TimeoutError(
            f"{handle.name} startup checkpoint remained in flight during close"
        )
        handle.timeout_error = handle.timeout_error or error
        self._set_flush_error(handle.name, handle.timeout_error)
        return (_FlushOutcome(handle.name, "timed_out", error),)

    def _set_flush_error(self, sink_name: str, error: BaseException) -> None:
        """Record the latest unresolved error for one persistence sink."""
        self._flush_errors[sink_name] = error
        self._last_flush_error = error

    def _clear_flush_error(self, sink_name: str) -> None:
        self._flush_errors.pop(sink_name, None)
        self._last_flush_error = next(reversed(self._flush_errors.values()), None)

    def _refresh_flush_error(self) -> None:
        self._last_flush_error = next(reversed(self._flush_errors.values()), None)

    @staticmethod
    def _result_status(result: object) -> Optional[str]:
        if isinstance(result, _FlushResult):
            return result.value
        return None

    def _execute_flush(
        self,
        operation: Callable[[], object],
        *,
        reason: str,
    ) -> bool:
        """Serialize a flush and keep logging failures out of pipeline control."""
        with self._flush_lock:
            if self._is_closed:
                return False
            try:
                result = operation()
            except Exception as exc:
                if not self._is_closed:
                    self._set_flush_error(reason, exc)
                _diagnostic_logger.warning(
                    "%s %s flush failed: %s",
                    type(self).__name__,
                    reason,
                    exc,
                    exc_info=True,
                )
                return False
            result_status = self._result_status(result)
            if result_status in {
                _FlushResult.NO_WORK.value,
                _FlushResult.IN_FLIGHT.value,
            }:
                # Neither state proves a remote write completed.  In
                # particular, an in-flight result must not clear a previous
                # persistence error or be treated as a successful heartbeat.
                return False
            if not self._is_closed:
                self._clear_flush_error(reason)
            return True

    # ------------------------------------------------------------------
    # Properties / lifecycle
    # ------------------------------------------------------------------

    @property
    def config(self) -> _LogConfig:
        return copy.deepcopy(self._config)

    @property
    def run_config(self) -> Optional[DataCoolieRunConfig]:
        return self._run_config.model_copy(deep=True) if self._run_config is not None else None

    @property
    def started_at(self) -> Optional[datetime]:
        """Timestamp supplied by the Driver or captured at activation."""
        return self._started_at

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def last_flush_error(self) -> Optional[BaseException]:
        """Most recent flush failure, cleared after a successful flush."""
        return self._last_flush_error

    @property
    def terminal_outcomes(self) -> tuple[_FlushOutcome, ...]:
        """Internal per-sink results retained for post-close diagnostics."""
        return self._terminal_outcomes

    def validate_session_eligibility(self) -> None:
        """Validate that this logger can be transferred to one Driver session."""
        with self._close_lock:
            if self._is_closed or self._is_closing:
                raise ConfigurationError(
                    f"{type(self).__name__} is closed or closing"
                )
            if self._is_active:
                raise ConfigurationError(
                    f"{type(self).__name__} is already active and cannot be reused"
                )

    def set_run_config(self, run_config: DataCoolieRunConfig) -> None:
        with self._close_lock:
            if self._is_active or self._is_closing or self._is_closed:
                raise ConfigurationError(
                    f"{type(self).__name__} run configuration cannot change after activation"
                )
            if not isinstance(run_config, DataCoolieRunConfig):
                raise ConfigurationError("run_config must be a DataCoolieRunConfig instance")
            self._run_config = DataCoolieRunConfig(
                **copy.deepcopy(run_config.model_dump())
            )
            self._apply_run_config()

    @property
    def log_session_id(self) -> str:
        """Return the immutable identity shared by this logger lifetime."""

        return self._log_session_id

    def set_log_session_id(self, log_session_id: str) -> None:
        """Bind a Driver-owned session identity before activation."""

        with self._close_lock:
            if self._is_active or self._is_closing or self._is_closed:
                raise ConfigurationError(
                    f"{type(self).__name__} log session identity cannot change after activation"
                )
            if not isinstance(log_session_id, str) or not log_session_id.strip():
                raise ConfigurationError("log_session_id must be a non-empty string")
            self._log_session_id = log_session_id.strip()

    def _apply_run_config(self) -> None:
        """Hook for concrete loggers to project the shared run config."""

    @abstractmethod
    def _build_final_operations(
        self,
        *,
        periodic_in_flight: bool,
    ) -> Sequence[_FlushOperation]:
        """Build terminal sink attempts from child-owned immutable payloads."""

    def _activate(self) -> None:
        """Hook for child activation setup."""

    def _activation_cleanup(self) -> None:
        """Release state acquired by a failed activation attempt."""

    def _execute_terminal_operations(
        self,
        operations: Sequence[_FlushOperation],
        *,
        deadline: Optional[float] = None,
    ) -> tuple[_FlushOutcome, ...]:
        """Attempt all terminal sinks and wait no longer than one common deadline."""
        if not operations:
            return ()

        deadline = (
            time.monotonic() + self._config.close_timeout_seconds
            if deadline is None
            else deadline
        )
        results: list[Optional[_FlushOutcome]] = [None] * len(operations)
        completed = [threading.Event() for _ in operations]

        # A close deadline is a hard upper bound for terminal I/O.  Do not
        # start new sink work once the caller has already exhausted it (for
        # example, after waiting for a periodic or startup operation).
        if time.monotonic() >= deadline:
            outcomes = []
            for operation in operations:
                self._on_terminal_timeout(operation.name)
                outcome = _FlushOutcome(
                    operation.name,
                    "timed_out",
                    TimeoutError(
                        f"{operation.name} did not finish within "
                        f"{self._config.close_timeout_seconds:g} seconds"
                    ),
                )
                outcomes.append(outcome)
                self._set_flush_error(operation.name, outcome.error)  # type: ignore[arg-type]
                _diagnostic_logger.warning(
                    "%s terminal sink %s %s: %s",
                    type(self).__name__,
                    outcome.name,
                    outcome.status,
                    outcome.error,
                )
            self._refresh_flush_error()
            return tuple(outcomes)

        def run(index: int, operation: _FlushOperation) -> None:
            try:
                result = operation.execute()
            except BaseException as exc:
                results[index] = _FlushOutcome(operation.name, "failed", exc)
            else:
                result_status = self._result_status(result)
                if result_status == _FlushResult.IN_FLIGHT.value:
                    results[index] = _FlushOutcome(
                        operation.name,
                        "in_flight",
                        RuntimeError(f"{operation.name} write is already in flight"),
                    )
                elif result_status == _FlushResult.NO_WORK.value:
                    results[index] = _FlushOutcome(operation.name, "no_work")
                else:
                    # Existing lifecycle callbacks return None; that remains
                    # a successful operation for the abstract base contract.
                    results[index] = _FlushOutcome(operation.name, "succeeded")
            finally:
                completed[index].set()

        for index, operation in enumerate(operations):
            threading.Thread(
                target=run,
                args=(index, operation),
                name=f"{type(self).__name__}-{operation.name}-close",
                daemon=True,
            ).start()

        for event in completed:
            event.wait(timeout=max(0.0, deadline - time.monotonic()))

        outcomes: list[_FlushOutcome] = []
        for index, operation in enumerate(operations):
            outcome = results[index]
            if outcome is None:
                self._on_terminal_timeout(operation.name)
                outcome = _FlushOutcome(
                    operation.name,
                    "timed_out",
                    TimeoutError(
                        f"{operation.name} did not finish within "
                        f"{self._config.close_timeout_seconds:g} seconds"
                    ),
                )
            outcomes.append(outcome)
            if outcome.error is not None:
                self._set_flush_error(operation.name, outcome.error)
                _diagnostic_logger.warning(
                    "%s terminal sink %s %s: %s",
                    type(self).__name__,
                    outcome.name,
                    outcome.status,
                    outcome.error,
                )
            elif outcome.status == "succeeded":
                self._clear_flush_error(operation.name)

        self._refresh_flush_error()
        return tuple(outcomes)

    def close(self) -> None:
        """Attempt terminal sinks within a bound, then release all logger state."""
        with self._close_lock:
            if self._is_closed:
                return
            self._is_closing = True
            deadline = time.monotonic() + self._config.close_timeout_seconds
            periodic_stopped = self._stop_periodic_flush(
                timeout=max(0.0, deadline - time.monotonic())
            )
            try:
                periodic_outcomes: tuple[_FlushOutcome, ...] = ()
                if not periodic_stopped:
                    self._on_periodic_timeout()
                    timeout_sinks = self._periodic_timeout_sinks()
                    periodic_outcomes = tuple(
                        _FlushOutcome(
                            sink_name,
                            "timed_out",
                            TimeoutError(
                                f"{sink_name} remained in flight during close"
                            ),
                        )
                        for sink_name in timeout_sinks
                    )
                    for outcome in periodic_outcomes:
                        self._set_flush_error(
                            outcome.name,
                            outcome.error,  # type: ignore[arg-type]
                        )
                        _diagnostic_logger.warning(
                            "%s periodic sink %s timed_out: %s",
                            type(self).__name__,
                            outcome.name,
                            outcome.error,
                        )
                startup_outcomes = self._wait_startup_operation(deadline=deadline)
                try:
                    operations = self._build_final_operations(
                        periodic_in_flight=not periodic_stopped,
                    )
                    self._terminal_outcomes = (
                        periodic_outcomes
                        + startup_outcomes
                        + self._execute_terminal_operations(
                            operations,
                            deadline=deadline,
                        )
                    )
                    self._refresh_flush_error()
                    terminal_interrupt = next(
                        (
                            outcome.error
                            for outcome in self._terminal_outcomes
                            if isinstance(outcome.error, BaseException)
                            and not isinstance(outcome.error, Exception)
                        ),
                        None,
                    )
                    if terminal_interrupt is not None:
                        raise terminal_interrupt
                except Exception as terminal_exc:
                    # Final-operation construction/drain is observational
                    # persistence work.  Make it visible without letting it
                    # replace an active Driver/context-manager exception.
                    self._set_flush_error("terminal", terminal_exc)
                    self._terminal_outcomes = (
                        periodic_outcomes
                        + startup_outcomes
                        + (_FlushOutcome("terminal", "failed", terminal_exc),)
                    )
                    _diagnostic_logger.warning(
                        "%s terminal preparation failed: %s",
                        type(self).__name__,
                        terminal_exc,
                        exc_info=True,
                    )
            finally:
                try:
                    self._cleanup()
                except Exception as cleanup_exc:
                    # Terminal sink failures are already represented in
                    # ``terminal_outcomes``.  Cleanup must still make the
                    # logger terminal and must not hide those observations.
                    if self._last_flush_error is None:
                        self._set_flush_error("cleanup", cleanup_exc)
                    _diagnostic_logger.warning(
                        "%s cleanup failed: %s",
                        type(self).__name__,
                        cleanup_exc,
                        exc_info=True,
                    )
                finally:
                    self._is_active = False
                    self._is_closed = True
                    self._is_closing = False

    def _cleanup(self) -> None:
        """Stop the periodic flush thread and release resources.

        Subclasses should call ``super()._cleanup()``.
        """
        self._stop_event.set()
        self._flush_thread = None

    def __enter__(self) -> "BaseLogger":
        self.activate()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            self.close()
        except BaseException:
            # Never replace an exception raised by the caller's work with a
            # persistence/cleanup failure.  With no active exception the
            # cleanup failure remains visible to the caller.
            if exc_type is None:
                raise
            _diagnostic_logger.exception(
                "%s cleanup failed while preserving the active exception",
                type(self).__name__,
            )
