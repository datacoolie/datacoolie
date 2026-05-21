"""System logger — captures and persists Python log output.

``SystemLogger`` is **not** a logger itself; it configures the global
:class:`LogManager` to capture logs and periodically appends them to a
plain-text ``.log`` file in storage via the platform.

Two independent log levels are supported:

* ``log_level`` — controls the console stream (what operators see live).
* ``file_level`` — controls what is captured to the file (defaults to
  ``DEBUG``, capturing everything regardless of console level).

Flushing is periodic (background timer) plus a final flush on :meth:`close`.
Each flush appends only new captured records to the remote file via
:meth:`~datacoolie.platforms.base.BasePlatform.append_file`, which allows
crash-safe incremental persistence without re-uploading the whole file.

Usage::

    from datacoolie.logging.base import get_logger
    from datacoolie.logging.system_logger import SystemLogger

    logger = get_logger(__name__)

    with SystemLogger(config, platform) as sys_log:
        logger.info("Processing started")   # printed at INFO, captured at DEBUG+
    # all remaining logs appended on close
"""

from __future__ import annotations

from typing import Optional

from datacoolie.logging.base import (
    LogLevel,
    BaseLogger,
    LogConfig,
    LogManager,
    StorageMode,
    format_partition_path,
    get_logger,
)
from datacoolie.platforms.base import BasePlatform

_logger = get_logger(__name__)


class SystemLogger(BaseLogger):
    """Captures all framework Python logs and appends them to a ``.log`` file.

    The file is a plain-text log (one line per record) so operators can read
    it directly without a JSON parser.  The capture level defaults to
    ``DEBUG`` so every framework message is recorded regardless of the
    console level set by the Driver.

    Periodic flushing is driven by the :class:`BaseLogger` daemon timer.
    Each tick atomically drains the in-memory capture buffer and appends
    the text to the remote file via :meth:`~BasePlatform.append_file`.
    A final flush is performed on :meth:`close`.

    Args:
        config: Logging configuration (output_path, log_level, file_level,
            flush_interval_seconds, etc.).
        platform: Platform for file operations.
    """

    def __init__(self, config: LogConfig, platform: Optional[BasePlatform] = None) -> None:
        super().__init__(config, platform)
        self._log_manager = LogManager.get_instance()
        self._log_manager.configure(
            level=config.log_level,
            file_level=config.file_level,
            capture_logs=True,
            storage_mode=config.storage_mode,
            console_output=True,
            force=True,
        )
        self._remote_path: Optional[str] = None

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def _ensure_remote_path(self) -> str:
        """Compute (once) the single remote log file path for this session."""
        if self._remote_path is None:
            output_path = self._config.output_path or ""
            if self._config.partition_by_date:
                output_path = format_partition_path(
                    output_path, pattern=self._config.partition_pattern
                )
            rc = self._run_config
            job_id = (rc.job_id if rc else None) or "default"
            self._remote_path = f"{output_path}/system_log_{job_id}.log"
        return self._remote_path

    def _do_flush(self) -> None:
        """Drain captured logs and append them to the remote file."""
        text = self._log_manager.get_and_clear_captured_logs(include_location=True)
        if not text or not self._config.output_path or not self._platform:
            return
        try:
            full_path = self._ensure_remote_path()
            self._platform.append_file(full_path, text + "\n")
        except Exception as exc:
            _logger.error("Failed to flush system logs: %s", exc)

    def _on_periodic_flush(self) -> None:  # noqa: D102
        self._do_flush()

    def flush(self) -> None:
        """Flush remaining captured logs to storage."""
        self._do_flush()
        if self._remote_path and self._config.output_path and self._platform:
            _logger.info("System logs saved: %s", self._remote_path)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _cleanup(self) -> None:
        super()._cleanup()
        self._log_manager.clear_captured_logs()


def create_system_logger(
    output_path: Optional[str] = None,
    log_level: str = LogLevel.INFO.value,
    file_level: str = LogLevel.DEBUG.value,
    platform: Optional[BasePlatform] = None,
    storage_mode: str = StorageMode.MEMORY.value,
) -> SystemLogger:
    """Factory for :class:`SystemLogger`.

    Args:
        output_path: Base directory for the log file.
        log_level: Console stream level (default ``INFO``).
        file_level: Capture / file level (default ``DEBUG`` — records
            everything regardless of console level).
        platform: Platform for file I/O.
        storage_mode: In-memory or file-backed capture buffer.
    """
    config = LogConfig(
        log_level=log_level,
        file_level=file_level,
        storage_mode=storage_mode,
        output_path=output_path,
        partition_by_date=True,
    )
    return SystemLogger(config, platform)
