"""Tests for persistent logger activation, flushing, and close lifecycle."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging.base import BaseLogger, _FlushOperation
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.configuration.constants import FlushResult
from datacoolie.logging.persistence.layout import format_partition_path


class _StubLogger(BaseLogger):
    """Minimal concrete implementation for testing the ABC."""

    def __init__(self, config, platform=None):
        super().__init__(config, platform)
        self.flushed = False

    def _build_final_operations(self, *, periodic_in_flight):
        return (_FlushOperation("stub", self._mark_flushed),)

    def _mark_flushed(self):
        self.flushed = True


class _StartupLogger(BaseLogger):
    def __init__(self, operation, *, timeout=0.05):
        super().__init__(
            LogConfig(flush_interval_seconds=0, close_timeout_seconds=timeout)
        )
        self.operation = operation

    def _activate(self):
        self._run_bounded_startup(self.operation, sink_name="startup")

    def _build_final_operations(self, *, periodic_in_flight):
        return ()


class TestBaseLogger:
    def test_properties(self):
        cfg = LogConfig()
        lgr = _StubLogger(cfg)
        assert lgr.config == cfg
        assert lgr.config is not cfg
        assert lgr.is_closed is False
        assert lgr.run_config is None

    def test_set_run_config(self):
        cfg = LogConfig()
        lgr = _StubLogger(cfg)
        rc = DataCoolieRunConfig(job_id="job-1")
        lgr.set_run_config(rc)
        assert lgr.run_config == rc
        assert lgr.run_config is not rc

    def test_run_config_cannot_change_after_activation(self):
        lgr = _StubLogger(LogConfig())
        lgr.set_run_config(DataCoolieRunConfig(job_id="before"))
        lgr.activate()
        with pytest.raises(ConfigurationError, match="after activation"):
            lgr.set_run_config(DataCoolieRunConfig(job_id="after"))
        lgr.close()

    def test_close_flushes(self):
        lgr = _StubLogger(LogConfig())
        lgr.close()
        assert lgr.flushed is True
        assert lgr.is_closed is True

    def test_close_idempotent(self):
        lgr = _StubLogger(LogConfig())
        lgr.close()
        lgr.flushed = False  # reset
        lgr.close()
        assert lgr.flushed is False  # not flushed again

    def test_context_manager(self):
        cfg = LogConfig()
        with _StubLogger(cfg) as lgr:
            pass
        assert lgr.flushed is True
        assert lgr.is_closed is True

    def test_partition_path(self):
        dt = datetime(2024, 1, 15, tzinfo=timezone.utc)
        path = format_partition_path("/base", run_date=dt)
        assert path == "/base/__run_date=2024-01-15"

    def test_partition_path_defaults_to_now(self):
        path = format_partition_path("/base")
        assert path.startswith("/base/__run_date=")

    def test_partition_path_strips_trailing_slash(self):
        dt = datetime(2024, 3, 5, tzinfo=timezone.utc)
        path = format_partition_path("/base/", run_date=dt)
        assert path == "/base/__run_date=2024-03-05"

    def test_partition_path_custom_pattern_with_hour(self):
        dt = datetime(2024, 6, 7, 9, 30, tzinfo=timezone.utc)
        path = format_partition_path("/base", run_date=dt, pattern="year={year}/month={month}/day={day}/hour={hour}")
        assert path == "/base/year=2024/month=06/day=07/hour=09"

    def test_partition_path_custom_pattern_year_month(self):
        dt = datetime(2024, 1, 5, tzinfo=timezone.utc)
        path = format_partition_path("/base", run_date=dt, pattern="dt={year}-{month}")
        assert path == "/base/dt=2024-01"

    def test_partition_path_hour_zero_padded(self):
        dt = datetime(2024, 12, 31, 3, tzinfo=timezone.utc)
        path = format_partition_path("/base", run_date=dt, pattern="run_date={year}-{month}-{day}/hour={hour}")
        assert path == "/base/run_date=2024-12-31/hour=03"

    def test_timer_starts_when_configured(self):
        """Timer starts when flush_interval > 0, output_path, and platform are set."""
        from unittest.mock import MagicMock

        cfg = LogConfig(output_path="/logs", flush_interval_seconds=60)
        lgr = _StubLogger(cfg, platform=MagicMock())
        assert lgr._flush_thread is None
        lgr.activate()
        assert lgr._flush_thread is not None
        assert lgr._flush_thread.daemon is True
        lgr.close()

    def test_timer_not_started_without_output_path(self):
        from unittest.mock import MagicMock

        cfg = LogConfig(output_path=None, flush_interval_seconds=60)
        lgr = _StubLogger(cfg, platform=MagicMock())
        lgr.activate()
        assert lgr._flush_thread is None
        lgr.close()

    def test_timer_not_started_without_platform(self):
        cfg = LogConfig(output_path="/logs", flush_interval_seconds=60)
        lgr = _StubLogger(cfg, platform=None)
        lgr.activate()
        assert lgr._flush_thread is None
        lgr.close()

    def test_timer_not_started_when_interval_zero(self):
        from unittest.mock import MagicMock

        cfg = LogConfig(output_path="/logs", flush_interval_seconds=0)
        lgr = _StubLogger(cfg, platform=MagicMock())
        lgr.activate()
        assert lgr._flush_thread is None
        lgr.close()

    def test_cleanup_stops_timer(self):
        from unittest.mock import MagicMock

        cfg = LogConfig(output_path="/logs", flush_interval_seconds=60)
        lgr = _StubLogger(cfg, platform=MagicMock())
        lgr.activate()
        assert lgr._flush_thread is not None
        lgr.close()
        assert lgr._stop_event.is_set()

    def test_on_periodic_flush_is_noop_by_default(self):
        from unittest.mock import MagicMock

        cfg = LogConfig(flush_interval_seconds=0)
        lgr = _StubLogger(cfg, platform=MagicMock())
        lgr._on_periodic_flush()
        assert lgr.flushed is False
        lgr.close()

    def test_close_does_not_wait_for_blocked_periodic_flush(self):
        class BlockingLogger(BaseLogger):
            def __init__(self):
                super().__init__(
                    LogConfig(
                        output_path="/logs",
                        flush_interval_seconds=0.01,
                        close_timeout_seconds=0.05,
                    ),
                    platform=MagicMock(),
                )
                self.periodic_started = threading.Event()
                self.release_periodic = threading.Event()
                self.periodic_in_flight = None

            def _flush_periodic(self):
                self.periodic_started.set()
                assert self.release_periodic.wait(timeout=2)

            def _build_final_operations(self, *, periodic_in_flight):
                self.periodic_in_flight = periodic_in_flight
                return ()

        logger = BlockingLogger()
        logger.activate()
        assert logger.periodic_started.wait(timeout=2)

        started = time.monotonic()
        logger.close()
        elapsed = time.monotonic() - started
        assert elapsed < 0.3
        assert logger.periodic_in_flight is True
        assert logger.is_closed
        assert logger.terminal_outcomes[0].status == "timed_out"
        logger.release_periodic.set()

    def test_flush_failure_is_fail_open_and_observable(self):
        class FailingLogger(BaseLogger):
            def _build_final_operations(self, *, periodic_in_flight):
                return (
                    _FlushOperation(
                        "failing",
                        lambda: (_ for _ in ()).throw(
                            RuntimeError("storage unavailable")
                        ),
                    ),
                )

        logger = FailingLogger(LogConfig(flush_interval_seconds=0))
        logger.close()

        assert isinstance(logger.last_flush_error, RuntimeError)
        assert str(logger.last_flush_error) == "storage unavailable"
        assert logger.terminal_outcomes[0].status == "failed"

    def test_terminal_base_exception_is_reported_and_propagated_after_cleanup(self):
        class InterruptedLogger(BaseLogger):
            def _build_final_operations(self, *, periodic_in_flight):
                def interrupt():
                    raise KeyboardInterrupt()

                return (_FlushOperation("interrupted", interrupt),)

        logger = InterruptedLogger(LogConfig(flush_interval_seconds=0))
        with pytest.raises(KeyboardInterrupt):
            logger.close()

        assert logger.is_closed
        assert logger.terminal_outcomes[0].status == "failed"
        assert isinstance(logger.terminal_outcomes[0].error, KeyboardInterrupt)

    def test_terminal_operations_share_one_close_deadline(self):
        release = threading.Event()

        class SlowLogger(BaseLogger):
            def _build_final_operations(self, *, periodic_in_flight):
                def wait_forever():
                    release.wait(timeout=2)

                return (
                    _FlushOperation("one", wait_forever),
                    _FlushOperation("two", wait_forever),
                )

        logger = SlowLogger(
            LogConfig(flush_interval_seconds=0, close_timeout_seconds=0.05)
        )
        started = time.monotonic()
        logger.close()
        elapsed = time.monotonic() - started
        release.set()

        assert elapsed < 0.3
        assert [item.status for item in logger.terminal_outcomes] == [
            "timed_out",
            "timed_out",
        ]

    def test_expired_terminal_deadline_does_not_start_sink_work(self):
        started = threading.Event()

        class SinkLogger(BaseLogger):
            def _build_final_operations(self, *, periodic_in_flight):
                return (_FlushOperation("sink", started.set),)

        logger = SinkLogger(LogConfig(flush_interval_seconds=0))
        outcomes = logger._execute_terminal_operations(
            logger._build_final_operations(periodic_in_flight=False),
            deadline=time.monotonic() - 1,
        )

        assert [item.status for item in outcomes] == ["timed_out"]
        assert not started.is_set()

    def test_no_work_and_in_flight_do_not_clear_unresolved_error(self):
        logger = _StubLogger(LogConfig(flush_interval_seconds=0))
        error = RuntimeError("storage unavailable")
        logger._set_flush_error("periodic", error)

        assert logger._execute_flush(
            lambda: FlushResult.NO_WORK,
            reason="periodic",
        ) is False
        assert logger.last_flush_error is error
        assert logger._execute_flush(
            lambda: FlushResult.IN_FLIGHT,
            reason="periodic",
        ) is False
        assert logger.last_flush_error is error
        assert logger._execute_flush(
            lambda: FlushResult.WRITTEN,
            reason="periodic",
        ) is True
        assert logger.last_flush_error is None
        logger.close()

    def test_late_startup_failure_is_observed_on_close(self):
        started = threading.Event()
        release = threading.Event()
        done = threading.Event()

        def operation():
            started.set()
            release.wait(timeout=2)
            done.set()
            raise RuntimeError("late startup failure")

        logger = _StartupLogger(operation)
        logger.activate()
        assert started.wait(timeout=1)
        assert isinstance(logger.last_flush_error, TimeoutError)
        release.set()
        assert done.wait(timeout=1)

        logger.close()

        assert any(
            outcome.name == "startup" and outcome.status == "failed"
            for outcome in logger.terminal_outcomes
        )
        assert isinstance(logger.last_flush_error, RuntimeError)
        assert str(logger.last_flush_error) == "late startup failure"

    def test_late_startup_success_resolves_timeout_health(self):
        started = threading.Event()
        release = threading.Event()
        done = threading.Event()

        def operation():
            started.set()
            release.wait(timeout=2)
            done.set()

        logger = _StartupLogger(operation)
        logger.activate()
        assert started.wait(timeout=1)
        assert isinstance(logger.last_flush_error, TimeoutError)
        release.set()
        assert done.wait(timeout=1)

        logger.close()

        assert any(
            outcome.name == "startup" and outcome.status == "succeeded_late"
            for outcome in logger.terminal_outcomes
        )
        assert logger.last_flush_error is None
