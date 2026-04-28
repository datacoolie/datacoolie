"""System logger — captures and persists Python log output.

``SystemLogger`` is **not** a logger itself; it configures the global
:class:`LogManager` to capture logs, then on :meth:`flush` / :meth:`close`
uploads the captured content to datalake storage via the platform.

Usage::

    from datacoolie.logging.base import get_logger
    from datacoolie.logging.system_logger import SystemLogger

    logger = get_logger(__name__)

    with SystemLogger(config, platform) as log_mgr:
        logger.info("Processing started")
    # logs uploaded on close
"""

from __future__ import annotations

import os
import tempfile
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
from datacoolie.utils.helpers import utc_now

_logger = get_logger(__name__)


class SystemLogger(BaseLogger):
    """Captures all framework Python logs and persists them to storage.

    On initialization it re-configures the global :class:`LogManager` to
    capture logs.  On :meth:`flush` the captured content is written as a
    single text file to the platform.

    Args:
        config: Logging configuration (output_path, level, etc.).
        platform: Platform for file operations.
    """

    def __init__(self, config: LogConfig, platform: Optional[BasePlatform] = None) -> None:
        super().__init__(config, platform)
        self._log_manager = LogManager.get_instance()
        self._log_manager.configure(
            level=config.log_level,
            capture_logs=True,
            storage_mode=config.storage_mode,
            console_output=True,
            force=True,
        )

    def flush(self) -> None:
        """Write captured logs to storage as JSONL."""
        jsonl_content = self._log_manager.get_captured_jsonl_logs()
        if not self._config.output_path or not self._platform or not jsonl_content:
            return

        try:
            output_path = self._config.output_path
            if self._config.partition_by_date:
                output_path = format_partition_path(output_path, pattern=self._config.partition_pattern)
            ts = utc_now().strftime("%Y%m%d_%H%M%S")
            rc = self._run_config
            job_id = (rc.job_id if rc else None) or "default"
            job_info = f"{rc.job_num if rc else 1}_{rc.job_index if rc else 0}"
            full_path = f"{output_path}/system_log_{ts}_{job_info}_{job_id}.jsonl"

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
            ) as tmp:
                tmp.write(jsonl_content)
                tmp_path = tmp.name
            try:
                self._platform.upload_file(tmp_path, full_path, overwrite=True)
            finally:
                os.remove(tmp_path)
            _logger.info("System logs written to: %s", full_path)
        except Exception as exc:
            _logger.error("Failed to flush system logs: %s", exc)

    def _cleanup(self) -> None:
        super()._cleanup()
        self._log_manager.clear_captured_logs()


def create_system_logger(
    output_path: Optional[str] = None,
    log_level: str = LogLevel.INFO.value,
    platform: Optional[BasePlatform] = None,
    storage_mode: str = StorageMode.MEMORY.value,
) -> SystemLogger:
    """Factory for :class:`SystemLogger`."""
    config = LogConfig(
        log_level=log_level,
        storage_mode=storage_mode,
        output_path=output_path,
        partition_by_date=True,
    )
    return SystemLogger(config, platform)
