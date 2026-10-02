"""Python system-log capture backed by the shared JSON Lines writer."""

from __future__ import annotations

import copy
import logging
from datetime import datetime
from typing import Optional, Sequence

from datacoolie.logging.base import (
    BaseLogger,
    _FlushOperation,
)
from datacoolie.logging.runtime.capture import LogRecord
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.configuration.constants import (
    FlushResult,
    INTERNAL_LOGGER_NAME,
    LogLevel,
    LogType,
    StorageMode,
)
from datacoolie.logging.persistence.layout import build_job_stem, format_partition_path
from datacoolie.logging.runtime.manager import LogManager
from datacoolie.logging.persistence.writer import JsonLogWriter, PersistenceStats
from datacoolie.logging.persistence.projection import build_system_entry
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.time import utc_now


_diagnostic_logger = logging.getLogger(INTERNAL_LOGGER_NAME)
_SYSTEM_SINK_NAME = LogType.SYSTEM_LOG.value


class SystemLogger(BaseLogger):
    """Capture framework Python logs and persist them as JSON Lines."""

    _periodic_sink_name = _SYSTEM_SINK_NAME

    def __init__(self, config: LogConfig, platform: Optional[BasePlatform] = None) -> None:
        super().__init__(config, platform)
        self._log_manager = LogManager.get_instance()
        self._capture_claimed = False
        self._writer: Optional[JsonLogWriter] = None
        self._job_stem: Optional[str] = None
        self._job_id: Optional[str] = None

    def _activate(self) -> None:
        self._log_manager.claim_capture(
            self,
            level=self._config.log_level,
            file_level=self._config.file_level,
            storage_mode=self._config.storage_mode,
            console_color=self._config.console_color,
            max_buffer_bytes=self._config.spool_max_bytes,
        )
        self._capture_claimed = True
        self._job_id = (
            self._run_config.job_id
            if self._run_config is not None
            else "system"
        )
        if self._platform and self._config.output_path and self._run_config is not None:
            started_at = self.started_at or utc_now()
            self._job_stem = self._build_job_stem(started_at)
            log_dir = self._partition_path("", started_at)
            target = f"{log_dir}/system_{self._job_stem}.json"

            def batch_path(sequence: int) -> str:
                return (
                    f"{self._partition_path('', utc_now())}/"
                    f"system_{self._job_stem}_part_{sequence:08d}.json"
                )

            self._writer = JsonLogWriter(
                self._platform,
                target,
                self._config,
                batch_path_factory=batch_path,
                name="system",
            )
        if self._log_manager.capture_handler is not None:
            self._log_manager.capture_handler.set_record_callback(
                self._on_capture_record
            )

    def _on_capture_record(self, record: LogRecord) -> bool:
        """Admit a captured record into the bounded writer.

        This callback deliberately performs only local encoding/buffering and
        wakes the flush worker.  Uploads remain outside the application
        logging thread.  Returning ``True`` also acknowledges capacity drops
        so CaptureHandler does not build a second unbounded queue.
        """
        if self._writer is None:
            return False
        # Match ExecutionLogger's lifecycle boundary: records emitted after
        # close has started are outside this session and must not be admitted
        # into a writer that is about to be finalized.
        if self._is_closing or self._is_closed:
            return True
        payload = self._capture_payload(record)
        self._writer.append(payload)
        if (
            self._config.persistence_mode == "batch"
            and self._writer.should_flush
        ):
            self._request_periodic_flush()
        return True

    def _activation_cleanup(self) -> None:
        self._release_capture()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        self._job_id = None

    @property
    def persistence_stats(self) -> Optional[PersistenceStats]:
        return self._writer.stats if self._writer is not None else None

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def _drain_capture(self) -> int:
        """Hand off fallback records while restoring an unhandled suffix."""

        if not self._capture_claimed or self._writer is None:
            return 0
        attempted = 0
        while True:
            batch = self._log_manager.drain_captured_records(owner=self)
            if not batch:
                return attempted
            for index, record in enumerate(batch):
                try:
                    # ``JsonLogWriter.append`` accounts for intentional
                    # capacity drops itself.  Once it returns, the capture
                    # record has been handled and must not be requeued.
                    self._append_capture_record(record)
                    attempted += 1
                except Exception as exc:
                    # An exception before writer admission is different from
                    # a deliberate drop: restore this record and the
                    # untouched suffix so fallback capture remains inspectable.
                    self._log_manager.restore_captured_records(batch[index:], owner=self)
                    _diagnostic_logger.warning(
                        "System capture handoff failed; restored %d records: %s",
                        len(batch) - index,
                        exc,
                        exc_info=True,
                    )
                    return attempted

    def _capture_payload(self, record: LogRecord) -> dict[str, object]:
        """Build the system record shared by callback and fallback drain."""

        return build_system_entry(
            record,
            job_id=self._job_id,
            job_num=self._run_config.job_num if self._run_config else None,
            job_index=self._run_config.job_index if self._run_config else None,
            log_session_id=self.log_session_id,
        )

    def _append_capture_record(self, record: LogRecord) -> bool:
        """Append a detached record during activation/fallback-drain paths."""
        if self._writer is None:
            return False
        payload = self._capture_payload(record)
        return self._writer.append(payload)

    def _flush_periodic(self) -> FlushResult:
        if not self._platform or not self._config.output_path or self._writer is None:
            return FlushResult.NO_WORK
        try:
            self._drain_capture()
            if self._writer.pending and (
                self._config.persistence_mode == "snapshot"
                or self._writer.should_flush
                or self._periodic_time_due
            ):
                result = self._writer.flush(force=True)
                if result is FlushResult.WRITTEN:
                    # Startup/previous periodic failures are tracked by the
                    # concrete sink, not by the shared timer label.  A
                    # completed write therefore resolves only this stream.
                    self._clear_flush_error(_SYSTEM_SINK_NAME)
                return result
            return FlushResult.NO_WORK
        except Exception as exc:
            self._set_flush_error(_SYSTEM_SINK_NAME, exc)
            raise

    def _on_periodic_timeout(self) -> None:
        if self._writer is not None and self._writer.in_flight:
            self._writer.mark_timed_out()

    def _on_terminal_timeout(self, sink_name: str) -> None:
        if sink_name == _SYSTEM_SINK_NAME and self._writer is not None:
            self._writer.mark_timed_out()

    def _build_final_operations(
        self,
        *,
        periodic_in_flight: bool,
    ) -> Sequence[_FlushOperation]:
        writer = self._writer
        if periodic_in_flight or writer is None:
            return ()
        self._drain_capture()
        if not writer.pending:
            return ()
        return (
            _FlushOperation(
                _SYSTEM_SINK_NAME,
                lambda writer=writer: self._flush_terminal(writer),
            ),
        )

    @staticmethod
    def _flush_terminal(writer: JsonLogWriter) -> FlushResult:
        """Drain all admitted system records before releasing the writer."""

        result = FlushResult.NO_WORK
        while writer.pending:
            result = writer.flush(force=True)
            if result is not FlushResult.WRITTEN:
                return result
        return result

    # ------------------------------------------------------------------
    # Path and cleanup
    # ------------------------------------------------------------------

    def _build_job_stem(self, started_at: datetime) -> str:
        rc = self._run_config
        job_id = rc.job_id if rc else self._job_id
        job_num = rc.job_num if rc else 1
        job_index = rc.job_index if rc else 0
        return build_job_stem(
            started_at,
            job_id=job_id,
            job_num=job_num,
            job_index=job_index,
        )

    def _partition_path(self, log_type: str, run_date: datetime) -> str:
        base = (self._config.output_path or "").rstrip("/")
        path = f"{base}/{log_type}" if log_type else base
        if self._config.partition_by_date:
            path = format_partition_path(
                path,
                run_date,
                pattern=self._config.partition_pattern,
            )
        return path

    def _release_capture(self) -> None:
        if self._capture_claimed:
            self._log_manager.release_capture(self)
            self._capture_claimed = False

    def _cleanup(self) -> None:
        super()._cleanup()
        self._release_capture()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        self._job_id = None
        self._job_stem = None


def create_system_logger(
    output_path: Optional[str] = None,
    log_level: str = LogLevel.INFO.value,
    file_level: str = LogLevel.DEBUG.value,
    platform: Optional[BasePlatform] = None,
    storage_mode: str = StorageMode.MEMORY.value,
    config: Optional[LogConfig] = None,
) -> SystemLogger:
    """Create a system logger with framework defaults."""

    effective = (
        copy.deepcopy(config)
        if config is not None
        else LogConfig(
            log_level=log_level,
            file_level=file_level,
            storage_mode=storage_mode,
            output_path=output_path,
        )
    )
    if config is not None:
        if output_path is not None:
            effective.output_path = output_path
    return SystemLogger(
        effective,
        platform,
    )
