"""Base logging infrastructure for the DataCoolie framework.

Provides:

* :class:`LogManager` — singleton that configures Python logging with
  a capture handler for later persistence to datalake.
* :class:`CaptureHandler` — a :class:`logging.Handler` that buffers
  :class:`LogRecord` objects in memory or a temp file.
* :class:`BaseLogger` — ABC for persistent loggers (system, ETL).
* :class:`LogConfig` — configuration dataclass.
* :func:`get_logger` — module-level convenience to create child loggers.

Usage::

    from datacoolie.logging.base import get_logger

    logger = get_logger(__name__)
    logger.info("Processing started")
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from datacoolie.core.constants import DEFAULT_AUTHOR, DEFAULT_PARTITION_PATTERN
from datacoolie.core.models import DataCoolieRunConfig
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.helpers import utc_now


class DataflowContextFilter(logging.Filter):
    """Inject the current ``dataflow_id`` from :mod:`contextvars` into every log record.

    Attach to handlers (not loggers) so it applies to all propagated messages.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        from datacoolie.logging.context import get_dataflow_id

        record.dataflow_id = get_dataflow_id()  # type: ignore[attr-defined]
        return True


# ============================================================================
# Enums
# ============================================================================


class LogLevel(str, Enum):
    """Standard logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class StorageMode(str, Enum):
    """Temporary storage mode for log buffering."""

    MEMORY = "memory"
    FILE = "file"


def format_partition_path(
    base_path: str,
    run_date: Optional[datetime] = None,
    pattern: str = DEFAULT_PARTITION_PATTERN,
) -> str:
    """Append a partition folder to *base_path* using *pattern*.

    Supported placeholders: ``{year}``, ``{month}``, ``{day}``, ``{hour}``.
    """
    dt = run_date or utc_now()
    folder = pattern.format(
        year=dt.year,
        month=f"{dt.month:02d}",
        day=f"{dt.day:02d}",
        hour=f"{dt.hour:02d}",
    )
    return f"{base_path.rstrip('/')}/{folder}"


# ============================================================================
# LogConfig
# ============================================================================


@dataclass
class LogConfig:
    """Configuration dataclass for loggers."""

    log_level: str = LogLevel.INFO.value
    file_level: str = LogLevel.DEBUG.value
    storage_mode: str = StorageMode.MEMORY.value
    output_path: Optional[str] = None
    partition_by_date: bool = True
    partition_pattern: str = DEFAULT_PARTITION_PATTERN
    flush_interval_seconds: int = 60

    def __post_init__(self) -> None:
        self.log_level = self.log_level.upper()
        self.file_level = self.file_level.upper()


# ============================================================================
# LogRecord (framework-level, not Python's logging.LogRecord)
# ============================================================================


@dataclass
class LogRecord:
    """Captured log entry."""

    timestamp: datetime
    level: str
    logger_name: str
    message: str
    module: Optional[str] = None
    func_name: Optional[str] = None
    line_no: Optional[int] = None
    exc_info: Optional[str] = None
    dataflow_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        d: Dict[str, Any] = {
            "ts": self.timestamp.isoformat(),
            "level": self.level,
            "logger": self.logger_name,
            "msg": self.message,
        }
        if self.dataflow_id:
            d["dataflow_id"] = self.dataflow_id
        if self.module:
            d["module"] = self.module
        if self.func_name:
            d["func"] = self.func_name
        if self.line_no is not None:
            d["line"] = self.line_no
        if self.exc_info:
            d["exc_info"] = self.exc_info
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LogRecord":
        """Reconstruct a LogRecord from a dict produced by :meth:`to_dict`."""
        return cls(
            timestamp=datetime.fromisoformat(d["ts"]),
            level=d["level"],
            logger_name=d["logger"],
            message=d["msg"],
            module=d.get("module"),
            func_name=d.get("func"),
            line_no=d.get("line"),
            exc_info=d.get("exc_info"),
            dataflow_id=d.get("dataflow_id"),
        )

    def format(self, include_location: bool = False) -> str:
        ts = self.timestamp.isoformat()
        df_part = f"[{self.dataflow_id}]" if self.dataflow_id else ""
        if include_location and self.func_name:
            loc = f" [{self.func_name}"
            if self.line_no:
                loc += f":{self.line_no}"
            loc += "]"
        else:
            loc = ""
        base = f"{ts} - {self.level} - {self.logger_name} {loc} - {df_part} - {self.message}"
        if self.exc_info:
            base += f"\n{self.exc_info}"
        return base


# ============================================================================
# CaptureHandler
# ============================================================================


class CaptureHandler(logging.Handler):
    """Captures Python log records for later persistence.

    Uses the handler's built-in ``self.lock`` (RLock) for thread safety —
    no separate lock needed since ``logging.Handler.handle()`` already
    acquires it before calling :meth:`emit`.
    """

    def __init__(
        self,
        level: int = logging.DEBUG,
        storage_mode: str = StorageMode.MEMORY.value,
    ) -> None:
        super().__init__(level)
        self._storage_mode = storage_mode
        self._records: List[LogRecord] = []
        self._temp_file: Optional[str] = None
        if storage_mode == StorageMode.FILE.value:
            self._setup_temp_file()

    def _setup_temp_file(self) -> None:
        temp_dir = tempfile.gettempdir()
        ts = utc_now().strftime("%Y%m%d_%H%M%S")
        self._temp_file = os.path.join(
            temp_dir, f"datacoolie_capture_{ts}_{os.getpid()}.tmp"
        )

    def emit(self, record: logging.LogRecord) -> None:
        # NOTE: self.lock is already held by Handler.handle() when this runs.
        try:
            exc_text: Optional[str] = None
            if record.exc_info:
                exc_text = self.format(record)

            lr = LogRecord(
                timestamp=datetime.fromtimestamp(record.created, tz=timezone.utc),
                level=record.levelname,
                logger_name=record.name,
                message=record.getMessage(),
                module=record.module,
                func_name=record.funcName,
                line_no=record.lineno,
                exc_info=exc_text if record.exc_info else None,
                dataflow_id=getattr(record, "dataflow_id", None) or None,
            )

            if self._storage_mode == StorageMode.MEMORY.value:
                self._records.append(lr)
            else:
                self._write_to_file(lr)
        except Exception:
            self.handleError(record)

    def _write_to_file(self, record: LogRecord) -> None:
        if self._temp_file:
            try:
                with open(self._temp_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record.to_dict(), default=str) + "\n")
            except Exception:
                self._records.append(record)

    def get_records(self) -> List[LogRecord]:
        with self.lock:
            if self._storage_mode == StorageMode.FILE.value:
                return self._load_from_file()
            return list(self._records)

    def _load_from_file(self) -> List[LogRecord]:
        records = list(self._records)
        if self._temp_file and os.path.exists(self._temp_file):
            try:
                with open(self._temp_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                records.append(LogRecord.from_dict(json.loads(line)))
                            except (json.JSONDecodeError, KeyError):
                                records.append(
                                    LogRecord(
                                        timestamp=utc_now(),
                                        level="INFO",
                                        logger_name="file",
                                        message=line,
                                    )
                                )
            except Exception:
                pass
        return records

    def get_formatted_logs(self, include_location: bool = False) -> str:
        with self.lock:
            if self._storage_mode == StorageMode.FILE.value:
                records = self._load_from_file()
                return "\n".join(r.format(include_location) for r in records)
            return "\n".join(r.format(include_location) for r in self._records)

    def get_and_clear_formatted_logs(self, include_location: bool = False) -> str:
        """Atomically drain captured logs and return as formatted text.

        Minimises lock hold time: swaps/clears the buffer under the lock,
        then formats the text outside the lock.
        """
        with self.lock:
            if self._storage_mode == StorageMode.FILE.value:
                records = self._load_from_file()
                self._records.clear()
                if self._temp_file and os.path.exists(self._temp_file):
                    try:
                        os.remove(self._temp_file)
                        self._setup_temp_file()
                    except Exception:
                        pass
            else:
                # Swap list — O(1) under the lock.
                records = self._records
                self._records = []
        # Format outside the lock — no contention during string building.
        return "\n".join(r.format(include_location) for r in records)

    def clear(self) -> None:
        with self.lock:
            self._records.clear()
            if self._temp_file and os.path.exists(self._temp_file):
                try:
                    os.remove(self._temp_file)
                    self._setup_temp_file()
                except Exception:
                    pass

    def cleanup(self) -> None:
        with self.lock:
            self._records.clear()
            if self._temp_file and os.path.exists(self._temp_file):
                try:
                    os.remove(self._temp_file)
                except Exception:
                    pass
            self._temp_file = None


# ============================================================================
# LogManager (Singleton)
# ============================================================================


class LogManager:
    """Singleton that configures Python logging with capture support."""

    _instance: Optional["LogManager"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._level = LogLevel.INFO.value
        self._file_level = LogLevel.DEBUG.value
        self._capture_handler: Optional[CaptureHandler] = None
        self._console_handler: Optional[logging.Handler] = None
        self._context_filter: Optional[DataflowContextFilter] = None
        self._loggers: Dict[str, logging.Logger] = {}
        self._root_logger_name = DEFAULT_AUTHOR
        self._configured = False

    @classmethod
    def get_instance(cls) -> "LogManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (primarily for testing)."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.cleanup()
            cls._instance = None

    def configure(
        self,
        level: str = LogLevel.INFO.value,
        file_level: Optional[str] = None,
        capture_logs: bool = True,
        storage_mode: str = StorageMode.MEMORY.value,
        console_output: bool = True,
        format_string: Optional[str] = None,
        force: bool = False,
    ) -> None:
        """Configure the global logging system.

        If already configured, this is a no-op unless *force* is ``True``.
        Pass ``force=True`` (as ``SystemLogger`` does) to apply new settings
        and replace existing handlers.

        Args:
            level: Console log level (controls what is printed to stderr).
            file_level: Capture log level for file persistence.  Defaults to
                ``level`` when not provided.  Set to ``"DEBUG"`` to capture all
                framework messages regardless of the console level.
            capture_logs: Enable :class:`CaptureHandler`.
            storage_mode: ``"memory"`` or ``"file"``.
            console_output: Emit to stderr.
            format_string: Custom ``logging.Formatter`` pattern.
            force: Re-configure even if already configured.
        """
        if self._configured and not force:
            return
        self._level = level.upper()
        self._file_level = (file_level or level).upper()

        console_int = getattr(logging, self._level, logging.INFO)
        file_int = getattr(logging, self._file_level, logging.DEBUG)
        # Root logger must pass records needed by either handler.
        root_int = min(console_int, file_int)

        root = logging.getLogger(self._root_logger_name)
        root.setLevel(root_int)
        root.propagate = False

        for h in root.handlers[:]:
            root.removeHandler(h)

        fmt = format_string or "%(asctime)s [%(levelname)s] %(name)s - [%(dataflow_id)s] - %(message)s"
        formatter = logging.Formatter(fmt)

        if console_output:
            self._console_handler = logging.StreamHandler()
            self._console_handler.setLevel(console_int)
            self._console_handler.setFormatter(formatter)
            root.addHandler(self._console_handler)

        if capture_logs:
            self._capture_handler = CaptureHandler(
                level=file_int,
                storage_mode=storage_mode,
            )
            self._capture_handler.setFormatter(formatter)
            root.addHandler(self._capture_handler)

        # Inject dataflow_id context into every propagated message.
        self._context_filter = DataflowContextFilter()
        for h in root.handlers:
            h.addFilter(self._context_filter)

        for lgr in self._loggers.values():
            lgr.setLevel(root_int)

        self._configured = True

    def get_logger(self, name: str) -> logging.Logger:
        """Create (or reuse) a child logger under the framework root."""
        if not self._configured:
            self.configure()

        if not name.startswith(self._root_logger_name):
            full_name = f"{self._root_logger_name}.{name}"
        else:
            full_name = name

        if full_name not in self._loggers:
            lgr = logging.getLogger(full_name)
            # Use the minimum of console/file levels so the child does not
            # filter out records that the capture handler needs.
            console_int = getattr(logging, self._level, logging.INFO)
            file_int = getattr(logging, self._file_level, logging.DEBUG)
            lgr.setLevel(min(console_int, file_int))
            self._loggers[full_name] = lgr

        return self._loggers[full_name]

    @property
    def capture_handler(self) -> Optional[CaptureHandler]:
        return self._capture_handler

    def get_captured_logs(self, include_location: bool = False) -> str:
        if self._capture_handler:
            return self._capture_handler.get_formatted_logs(include_location)
        return ""

    def get_and_clear_captured_logs(self, include_location: bool = False) -> str:
        """Atomically return captured logs as plain text and clear the buffer."""
        if self._capture_handler:
            return self._capture_handler.get_and_clear_formatted_logs(include_location)
        return ""

    def clear_captured_logs(self) -> None:
        if self._capture_handler:
            self._capture_handler.clear()

    def cleanup(self) -> None:
        if self._capture_handler:
            self._capture_handler.cleanup()


# Module-level convenience -------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """Get a framework logger (convenience wrapper).

    All loggers are children of the ``DataCoolie`` root logger and inherit
    its handlers (console + capture).

    Args:
        name: Typically ``__name__``.

    Returns:
        Configured :class:`logging.Logger`.
    """
    return LogManager.get_instance().get_logger(name)



# ============================================================================
# BaseLogger ABC
# ============================================================================


class BaseLogger(ABC):
    """Abstract base for persistent loggers (system, ETL).

    Provides configuration, lifecycle management, periodic flush timer,
    and context-manager support.  Subclasses implement :meth:`flush` and
    optionally override :meth:`_on_periodic_flush` for incremental behavior.

    The periodic timer is started automatically when *flush_interval_seconds*
    is positive **and** both *output_path* and *platform* are configured.
    """

    def __init__(self, config: LogConfig, platform: Optional[BasePlatform] = None) -> None:
        self._config = config
        self._platform = platform
        self._is_closed = False
        self._run_config: Optional[DataCoolieRunConfig] = None
        # Periodic flush: single daemon thread using Event.wait(timeout)
        self._stop_event = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None
        if self._should_start_timer():
            self._flush_thread = threading.Thread(
                target=self._flush_loop, daemon=True,
            )
            self._flush_thread.start()

    # ------------------------------------------------------------------
    # Periodic flush timer
    # ------------------------------------------------------------------

    def _should_start_timer(self) -> bool:
        """Whether periodic flushing is enabled."""
        return (
            self._config.flush_interval_seconds > 0
            and self._config.output_path is not None
            and self._platform is not None
        )

    def _flush_loop(self) -> None:
        """Single daemon thread — sleeps until interval elapses or stop is signalled."""
        interval = self._config.flush_interval_seconds
        while not self._stop_event.wait(interval):
            self._on_periodic_flush()

    def _on_periodic_flush(self) -> None:
        """Called by the periodic timer.

        The default implementation calls :meth:`flush`.  Override in
        subclasses that need incremental behavior distinct from the
        final flush (e.g. appending only new bytes).
        """
        self.flush()

    # ------------------------------------------------------------------
    # Properties / lifecycle
    # ------------------------------------------------------------------

    @property
    def config(self) -> LogConfig:
        return self._config

    @property
    def run_config(self) -> Optional[DataCoolieRunConfig]:
        return self._run_config

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    def set_run_config(self, run_config: DataCoolieRunConfig) -> None:
        self._run_config = run_config

    @abstractmethod
    def flush(self) -> None:
        """Flush buffered entries to persistent storage."""

    def close(self) -> None:
        """Flush and release resources."""
        if self._is_closed:
            return
        try:
            self.flush()
        finally:
            self._cleanup()
            self._is_closed = True

    def _cleanup(self) -> None:
        """Stop the periodic flush thread and release resources.

        Subclasses should call ``super()._cleanup()``.
        """
        self._stop_event.set()
        if self._flush_thread is not None:
            self._flush_thread.join(timeout=2.0)
            self._flush_thread = None

    def __enter__(self) -> "BaseLogger":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
