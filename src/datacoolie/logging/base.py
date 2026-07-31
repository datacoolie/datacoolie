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
import math
import os
import tempfile
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence

from datacoolie.core.constants import DEFAULT_AUTHOR, DEFAULT_PARTITION_PATTERN
from datacoolie.core.models import DataCoolieRunConfig
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.helpers import utc_now
from datacoolie.utils.path_utils import normalize_path


_diagnostic_logger = logging.getLogger("datacoolie.logging.internal")
_diagnostic_logger.propagate = False


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
    close_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        self.log_level = self.log_level.upper()
        self.file_level = self.file_level.upper()
        # Canonicalise storage paths to forward-slash separators so that
        # OS-native inputs (e.g. Windows backslashes) do not produce mixed
        # separators when child paths are appended downstream.
        if self.output_path:
            self.output_path = normalize_path(self.output_path)
        if (
            not math.isfinite(self.close_timeout_seconds)
            or self.close_timeout_seconds <= 0
        ):
            raise ValueError(
                "close_timeout_seconds must be a positive finite number"
            )


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
            loc = f"{self.func_name}"
            if self.line_no:
                loc += f":{self.line_no}"
        else:
            loc = ""
        base = f"{ts} - {self.level} - {self.logger_name}:{loc} - {df_part} - {self.message}"
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
            temp_dir,
            f"datacoolie_capture_{ts}_{os.getpid()}_{uuid.uuid4().hex}.tmp",
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

    def _load_from_file(
        self,
        *,
        raise_on_error: bool = False,
    ) -> List[LogRecord]:
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
                if raise_on_error:
                    raise
        return records

    def get_formatted_logs(self, include_location: bool = False) -> str:
        with self.lock:
            if self._storage_mode == StorageMode.FILE.value:
                records = self._load_from_file()
                return "\n".join(r.format(include_location) for r in records)
            return "\n".join(r.format(include_location) for r in self._records)

    def begin_jsonl_batch(self) -> List[LogRecord]:
        """Atomically detach a batch for transactional remote delivery.

        The caller must invoke :meth:`rollback_batch` when delivery fails.
        File-backed records are detached only after the active spool can be
        removed; an inability to rotate the spool is surfaced to the caller so
        records are never acknowledged prematurely.
        """
        with self.lock:
            if self._storage_mode == StorageMode.FILE.value:
                records = self._load_from_file(raise_on_error=True)
                if self._temp_file and os.path.exists(self._temp_file):
                    os.remove(self._temp_file)
                    self._setup_temp_file()
                self._records.clear()
                return records

            records = self._records
            self._records = []
            return records

    def rollback_batch(self, records: List[LogRecord]) -> None:
        """Restore a failed delivery batch ahead of newer records."""
        if not records:
            return
        with self.lock:
            self._records = list(records) + self._records

    @staticmethod
    def batch_to_jsonl(records: List[LogRecord]) -> str:
        """Serialize a detached batch using the persisted JSONL contract."""
        return "\n".join(
            json.dumps(record.to_dict(), default=str) for record in records
        )

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
            if isinstance(h, CaptureHandler):
                h.cleanup()
            h.close()
        self._capture_handler = None
        self._console_handler = None

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

    def begin_captured_jsonl_batch(self) -> List[LogRecord]:
        """Detach the current capture batch for transactional delivery."""
        if self._capture_handler:
            return self._capture_handler.begin_jsonl_batch()
        return []

    def rollback_captured_batch(self, records: List[LogRecord]) -> None:
        """Restore a failed transactional capture batch."""
        if self._capture_handler:
            self._capture_handler.rollback_batch(records)

    @staticmethod
    def captured_batch_to_jsonl(records: List[LogRecord]) -> str:
        """Serialize a detached capture batch as persisted JSONL."""
        return CaptureHandler.batch_to_jsonl(records)

    def clear_captured_logs(self) -> None:
        if self._capture_handler:
            self._capture_handler.clear()

    def cleanup(self) -> None:
        root = logging.getLogger(self._root_logger_name)
        owned_handlers = (
            self._capture_handler,
            self._console_handler,
        )
        for handler in owned_handlers:
            if handler is None:
                continue
            root.removeHandler(handler)
            if isinstance(handler, CaptureHandler):
                handler.cleanup()
            handler.close()
        self._capture_handler = None
        self._console_handler = None


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


@dataclass(frozen=True)
class _FlushOperation:
    """One terminal sink attempt built from immutable inputs."""

    name: str
    execute: Callable[[], None]


@dataclass(frozen=True)
class _FlushOutcome:
    """Observed result of one terminal sink attempt."""

    name: str
    status: str
    error: Optional[Exception] = None


class BaseLogger(ABC):
    """Abstract base for persistent loggers (system, ETL).

    Provides configuration, explicit activation, periodic scheduling,
    bounded terminal sink attempts, failure isolation, and cleanup ordering.
    Children define what periodic and terminal operations persist.
    """

    _periodic_sink_name = "periodic"

    def __init__(self, config: LogConfig, platform: Optional[BasePlatform] = None) -> None:
        self._config = config
        self._platform = platform
        self._is_active = False
        self._is_closed = False
        self._is_closing = False
        self._run_config: Optional[DataCoolieRunConfig] = None
        self._flush_lock = threading.RLock()
        self._close_lock = threading.Lock()
        self._last_flush_error: Optional[Exception] = None
        self._terminal_outcomes: tuple[_FlushOutcome, ...] = ()
        self._stop_event = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None

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

    def activate(self) -> None:
        """Activate periodic persistence after Driver configuration is complete."""
        with self._close_lock:
            if self._is_active or self._is_closing or self._is_closed:
                return
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

    def _stop_periodic_flush(self) -> bool:
        """Stop scheduling and report whether the worker actually exited."""
        self._stop_event.set()
        thread = self._flush_thread
        if thread is None:
            return True
        thread.join(timeout=min(2.0, self._config.close_timeout_seconds))
        if not thread.is_alive():
            self._flush_thread = None
            return True
        return False

    def _flush_loop(self) -> None:
        """Single daemon thread — sleeps until interval elapses or stop is signalled."""
        interval = self._config.flush_interval_seconds
        while not self._stop_event.wait(interval):
            self._on_periodic_flush()

    def _on_periodic_flush(self) -> None:
        """Run the child periodic hook through the common failure boundary."""
        if self._is_closing or self._is_closed:
            return
        self._execute_flush(self._flush_periodic, reason="periodic")

    def _flush_periodic(self) -> None:
        """Persist a periodic checkpoint.

        Children override this when they have a periodic sink.
        """
        return

    def _execute_flush(
        self,
        operation: Callable[[], None],
        *,
        reason: str,
    ) -> bool:
        """Serialize a flush and keep logging failures out of pipeline control."""
        with self._flush_lock:
            if self._is_closed:
                return False
            try:
                operation()
            except Exception as exc:
                if not self._is_closed:
                    self._last_flush_error = exc
                _diagnostic_logger.warning(
                    "%s %s flush failed: %s",
                    type(self).__name__,
                    reason,
                    exc,
                    exc_info=True,
                )
                return False
            if not self._is_closed:
                self._last_flush_error = None
            return True

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

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def last_flush_error(self) -> Optional[Exception]:
        """Most recent flush failure, cleared after a successful flush."""
        return self._last_flush_error

    @property
    def terminal_outcomes(self) -> tuple[_FlushOutcome, ...]:
        """Internal per-sink results retained for post-close diagnostics."""
        return self._terminal_outcomes

    def set_run_config(self, run_config: DataCoolieRunConfig) -> None:
        self._run_config = run_config

    @abstractmethod
    def _build_final_operations(
        self,
        *,
        periodic_in_flight: bool,
    ) -> Sequence[_FlushOperation]:
        """Build terminal sink attempts from child-owned immutable payloads."""

    def _execute_terminal_operations(
        self,
        operations: Sequence[_FlushOperation],
    ) -> tuple[_FlushOutcome, ...]:
        """Attempt all terminal sinks and wait no longer than one common deadline."""
        if not operations:
            return ()

        results: list[Optional[_FlushOutcome]] = [None] * len(operations)
        completed = [threading.Event() for _ in operations]

        def run(index: int, operation: _FlushOperation) -> None:
            try:
                operation.execute()
            except Exception as exc:
                results[index] = _FlushOutcome(operation.name, "failed", exc)
            else:
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

        deadline = time.monotonic() + self._config.close_timeout_seconds
        for event in completed:
            event.wait(timeout=max(0.0, deadline - time.monotonic()))

        outcomes: list[_FlushOutcome] = []
        for index, operation in enumerate(operations):
            outcome = results[index]
            if outcome is None:
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
                _diagnostic_logger.warning(
                    "%s terminal sink %s %s: %s",
                    type(self).__name__,
                    outcome.name,
                    outcome.status,
                    outcome.error,
                )

        errors = [outcome.error for outcome in outcomes if outcome.error]
        self._last_flush_error = errors[-1] if errors else None
        return tuple(outcomes)

    def close(self) -> None:
        """Attempt terminal sinks within a bound, then release all logger state."""
        with self._close_lock:
            if self._is_closed:
                return
            self._is_closing = True
            periodic_stopped = self._stop_periodic_flush()
            try:
                periodic_outcomes: tuple[_FlushOutcome, ...] = ()
                if not periodic_stopped:
                    error = TimeoutError(
                        f"{self._periodic_sink_name} remained in flight during close"
                    )
                    periodic_outcomes = (
                        _FlushOutcome(
                            self._periodic_sink_name,
                            "timed_out",
                            error,
                        ),
                    )
                    _diagnostic_logger.warning(
                        "%s periodic sink %s timed_out: %s",
                        type(self).__name__,
                        self._periodic_sink_name,
                        error,
                    )
                operations = self._build_final_operations(
                    periodic_in_flight=not periodic_stopped,
                )
                self._terminal_outcomes = (
                    periodic_outcomes
                    + self._execute_terminal_operations(operations)
                )
                errors = [
                    outcome.error
                    for outcome in self._terminal_outcomes
                    if outcome.error is not None
                ]
                self._last_flush_error = errors[-1] if errors else None
            finally:
                self._cleanup()
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
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
