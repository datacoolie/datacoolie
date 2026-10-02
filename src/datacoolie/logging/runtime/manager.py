"""Explicit Python logging configuration and capture ownership."""

from __future__ import annotations

import logging
import threading
from typing import Dict, List, Optional

from datacoolie.logging.runtime.capture import CaptureHandler, LogRecord
from datacoolie.logging.configuration.config import (
    normalize_console_color,
    normalize_log_level,
    normalize_storage_mode,
)
from datacoolie.logging.configuration.constants import INTERNAL_LOGGER_NAME, LogLevel, StorageMode
from datacoolie.logging.runtime.context import DataflowContextFilter
from datacoolie.logging.presentation.console import ConsoleFormatter, should_colorize
from datacoolie.logging.presentation.formatting import ContextFormatter, DEFAULT_LOG_FORMAT

_diagnostic_logger = logging.getLogger(INTERNAL_LOGGER_NAME)


class LogManager:
    """Singleton that configures Python logging with capture support."""

    _instance: Optional["LogManager"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._state_lock = threading.RLock()
        self._level = LogLevel.INFO.value
        self._file_level = LogLevel.DEBUG.value
        self._capture_handler: Optional[CaptureHandler] = None
        self._console_handler: Optional[logging.Handler] = None
        self._context_filter: Optional[DataflowContextFilter] = None
        self._loggers: Dict[str, logging.Logger] = {}
        self._root_logger_name = "datacoolie"
        self._configured = False
        self._capture_owner: Optional[object] = None
        # Configuration changes are reversible, but only while the host has
        # not changed the value again. This prevents a session from leaking
        # root-logger state into an embedding application after close.
        self._saved_root_state: Optional[tuple[int, bool]] = None
        self._saved_diagnostic_propagate: Optional[bool] = None
        self._saved_logger_levels: Dict[str, int] = {}
        self._managed_root_state: Optional[tuple[int, bool]] = None
        self._managed_diagnostic_propagate: Optional[bool] = None
        self._managed_logger_levels: Dict[str, int] = {}

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
        console_color: str = "auto",
        max_buffer_bytes: Optional[int] = None,
        force: bool = False,
    ) -> None:
        """Validate and configure logging as one serialized state change."""
        self._validate_bool(capture_logs, "capture_logs")
        self._validate_bool(console_output, "console_output")
        self._validate_bool(force, "force")
        requested_level = normalize_log_level(level, field_name="level")
        requested_file_level = normalize_log_level(
            file_level if file_level is not None else requested_level,
            field_name="file_level",
        )
        requested_storage_mode = normalize_storage_mode(storage_mode)
        requested_console_color = normalize_console_color(console_color)
        with self._state_lock:
            self._assert_mutation_owner_locked()
            if max_buffer_bytes is not None and (
                isinstance(max_buffer_bytes, bool)
                or not isinstance(max_buffer_bytes, int)
                or max_buffer_bytes <= 0
            ):
                raise ValueError("max_buffer_bytes must be a positive integer")
            self._configure_locked(
                level=requested_level,
                file_level=requested_file_level,
                capture_logs=capture_logs,
                storage_mode=requested_storage_mode,
                console_output=console_output,
                format_string=format_string,
                console_color=requested_console_color,
                max_buffer_bytes=max_buffer_bytes,
                force=force,
            )

    def claim_capture(
        self,
        owner: object,
        *,
        level: str = LogLevel.INFO.value,
        file_level: Optional[str] = None,
        storage_mode: str = StorageMode.MEMORY.value,
        console_color: str = "auto",
        max_buffer_bytes: Optional[int] = None,
    ) -> None:
        """Claim the global capture session for one active SystemLogger."""
        requested_level = normalize_log_level(level, field_name="level")
        requested_file_level = normalize_log_level(
            file_level if file_level is not None else requested_level,
            field_name="file_level",
        )
        requested_storage_mode = normalize_storage_mode(storage_mode)
        requested_console_color = normalize_console_color(console_color)
        if max_buffer_bytes is not None and (
            isinstance(max_buffer_bytes, bool)
            or not isinstance(max_buffer_bytes, int)
            or max_buffer_bytes <= 0
        ):
            raise ValueError("max_buffer_bytes must be a positive integer")
        with self._state_lock:
            if self._capture_owner is not None and self._capture_owner is not owner:
                raise RuntimeError("A system log capture session is already active")
            # Explicit standalone capture may precede the session. Those
            # records belong to no SystemLogger and must not be attributed to
            # the logger that claims capture now.
            if self._capture_owner is None and self._capture_handler is not None:
                self._capture_handler.clear()
            self._configure_locked(
                level=requested_level,
                file_level=requested_file_level,
                capture_logs=True,
                storage_mode=requested_storage_mode,
                console_output=True,
                console_color=requested_console_color,
                max_buffer_bytes=max_buffer_bytes,
                force=True,
            )
            self._capture_owner = owner

    def release_capture(self, owner: object) -> None:
        """Release and clean the capture session owned by *owner*."""
        with self._state_lock:
            if self._capture_owner is not owner:
                return
            self._cleanup_locked()

    @staticmethod
    def _validate_bool(value: object, field_name: str) -> None:
        if not isinstance(value, bool):
            raise ValueError(f"{field_name} must be a boolean")

    def _assert_mutation_owner_locked(self, owner: Optional[object] = None) -> None:
        """Reject state mutation by a caller that does not own capture."""
        if self._capture_owner is not None and self._capture_owner is not owner:
            raise RuntimeError("An active system log capture session owns LogManager state")

    def _remember_host_state_locked(self) -> None:
        """Capture host logger state once, before the first manager mutation."""
        if self._saved_root_state is not None:
            return
        root = logging.getLogger(self._root_logger_name)
        self._saved_root_state = (root.level, root.propagate)
        self._saved_diagnostic_propagate = _diagnostic_logger.propagate

    def _remember_logger_level_locked(self, logger: logging.Logger) -> None:
        if logger.name not in self._saved_logger_levels:
            self._saved_logger_levels[logger.name] = logger.level

    def _configure_locked(
        self,
        level: str = LogLevel.INFO.value,
        file_level: Optional[str] = None,
        capture_logs: bool = True,
        storage_mode: str = StorageMode.MEMORY.value,
        console_output: bool = True,
        format_string: Optional[str] = None,
        console_color: str = "auto",
        max_buffer_bytes: Optional[int] = None,
        force: bool = False,
    ) -> None:
        """Configure the global logging system.

        If already configured, this is a no-op unless *force* is ``True``.
        Pass ``force=True`` (as ``SystemLogger`` does) to apply new settings.
        An enabled capture handler is reconfigured in place so accepted records
        are never exposed to a detach/transfer window.

        Args:
            level: Console log level (controls what is printed to stderr).
            file_level: Capture log level for file persistence.  Defaults to
                ``level`` when not provided.  Set to ``"DEBUG"`` to capture all
                framework messages regardless of the console level.
            capture_logs: Enable :class:`CaptureHandler`.
            storage_mode: ``"memory"`` or ``"file"``.
            console_output: Emit to stderr.
            format_string: Custom ``logging.Formatter`` pattern.
            console_color: ``auto``, ``always`` or ``never`` ANSI policy.
            max_buffer_bytes: Maximum encoded bytes retained by capture
                fallback storage.
            force: Re-configure even if already configured.
        """
        if self._configured and not force:
            return

        self._remember_host_state_locked()

        requested_level = level
        requested_file_level = file_level if file_level is not None else requested_level
        console_int = logging.getLevelNamesMapping()[requested_level]
        file_int = logging.getLevelNamesMapping()[requested_file_level]

        root = logging.getLogger(self._root_logger_name)
        _diagnostic_logger.propagate = False
        self._managed_diagnostic_propagate = False

        fmt = format_string or DEFAULT_LOG_FORMAT
        formatter = ContextFormatter(fmt)

        if capture_logs and self._capture_handler is not None:
            try:
                self._capture_handler.reconfigure(
                    level=file_int,
                    storage_mode=storage_mode,
                    formatter=formatter,
                    max_buffer_bytes=max_buffer_bytes,
                )
            except Exception as exc:
                file_int = self._capture_handler.level
                requested_file_level = logging.getLevelName(file_int)
                _diagnostic_logger.warning(
                    "Could not reconfigure captured-log storage; preserving existing state: %s",
                    type(exc).__name__,
                )
        elif capture_logs:
            self._capture_handler = CaptureHandler(
                level=file_int,
                storage_mode=storage_mode,
                max_buffer_bytes=max_buffer_bytes or 512 * 1024 * 1024,
            )
            self._capture_handler.setFormatter(formatter)
            root.addHandler(self._capture_handler)
        elif self._capture_handler is not None:
            root.removeHandler(self._capture_handler)
            self._capture_handler.cleanup()
            self._capture_handler.close()
            self._capture_handler = None

        if self._console_handler is not None:
            root.removeHandler(self._console_handler)
            _diagnostic_logger.removeHandler(self._console_handler)
            self._console_handler.close()
            self._console_handler = None

        if console_output:
            self._console_handler = logging.StreamHandler()
            self._console_handler.setLevel(console_int)
            self._console_handler.setFormatter(
                ConsoleFormatter(
                    fmt,
                    colorize=should_colorize(console_color),
                )
            )
            root.addHandler(self._console_handler)
            _diagnostic_logger.addHandler(self._console_handler)

        # Inject execution correlation only into handlers owned by this manager.  Host
        # handlers may be shared with unrelated applications and must not be
        # mutated or have filters removed during cleanup.
        if self._context_filter is None:
            self._context_filter = DataflowContextFilter()
        for handler in (self._capture_handler, self._console_handler):
            if handler is None:
                continue
            if self._context_filter not in handler.filters:
                handler.addFilter(self._context_filter)

        self._level = requested_level
        self._file_level = str(requested_file_level)
        root_int = min(console_int, file_int) if capture_logs else console_int
        root.setLevel(root_int)
        root.propagate = False
        self._managed_root_state = (root_int, False)

        for lgr in self._loggers.values():
            self._remember_logger_level_locked(lgr)
            lgr.setLevel(root_int)
            self._managed_logger_levels[lgr.name] = root_int

        self._configured = True

    def get_logger(self, name: str) -> logging.Logger:
        """Create (or reuse) a child logger under the framework root."""
        if not isinstance(name, str) or not name.strip():
            raise ValueError("logger name must be a non-empty string")
        name = name.strip()
        if name != self._root_logger_name and not name.startswith(
            f"{self._root_logger_name}."
        ):
            name = f"{self._root_logger_name}.{name}"
        with self._state_lock:
            if name not in self._loggers:
                lgr = logging.getLogger(name)
                if self._configured:
                    # A configured child must not filter records needed by
                    # either the console or capture handler.
                    levels = logging.getLevelNamesMapping()
                    self._remember_logger_level_locked(lgr)
                    lgr.setLevel(min(levels[self._level], levels[self._file_level]))
                    self._managed_logger_levels[name] = lgr.level
                self._loggers[name] = lgr

            return self._loggers[name]

    @property
    def capture_handler(self) -> Optional[CaptureHandler]:
        with self._state_lock:
            return self._capture_handler

    def get_captured_logs(self, include_location: bool = False) -> str:
        with self._state_lock:
            if self._capture_handler:
                return self._capture_handler.get_formatted_logs(include_location)
            return ""

    def drain_captured_records(
        self,
        *,
        max_records: int = 4096,
        max_bytes: int = 8 * 1024 * 1024,
        owner: Optional[object] = None,
    ) -> List[LogRecord]:
        """Detach one bounded fallback chunk for local handoff."""
        with self._state_lock:
            self._assert_mutation_owner_locked(owner)
            if self._capture_handler:
                return self._capture_handler.drain_records(
                    max_records=max_records,
                    max_bytes=max_bytes,
                )
            return []

    def restore_captured_records(
        self,
        records: List[LogRecord],
        *,
        owner: Optional[object] = None,
    ) -> None:
        """Restore records that were not handed to the system writer."""
        with self._state_lock:
            self._assert_mutation_owner_locked(owner)
            if self._capture_handler:
                self._capture_handler.restore_records(records)

    def clear_captured_logs(self, *, owner: Optional[object] = None) -> None:
        with self._state_lock:
            self._assert_mutation_owner_locked(owner)
            if self._capture_handler:
                self._capture_handler.clear()

    def cleanup(self, *, owner: Optional[object] = None) -> None:
        with self._state_lock:
            self._assert_mutation_owner_locked(owner)
            self._cleanup_locked()

    def _cleanup_locked(self) -> None:
        root = logging.getLogger(self._root_logger_name)
        owned_handlers = (
            self._capture_handler,
            self._console_handler,
        )
        for handler in owned_handlers:
            if handler is None:
                continue
            if self._context_filter is not None:
                handler.removeFilter(self._context_filter)
            root.removeHandler(handler)
            _diagnostic_logger.removeHandler(handler)
            if isinstance(handler, CaptureHandler):
                handler.cleanup()
            handler.close()

        # Restore values that still look like the manager's last writes. If
        # the embedding application changed a value while the session was
        # active, its current value remains authoritative.
        if self._saved_root_state is not None:
            saved_level, saved_propagate = self._saved_root_state
            if self._managed_root_state is not None:
                managed_level, managed_propagate = self._managed_root_state
                if root.level == managed_level:
                    root.setLevel(saved_level)
                if root.propagate == managed_propagate:
                    root.propagate = saved_propagate
        if (
            self._saved_diagnostic_propagate is not None
            and self._managed_diagnostic_propagate is not None
            and _diagnostic_logger.propagate == self._managed_diagnostic_propagate
        ):
            _diagnostic_logger.propagate = self._saved_diagnostic_propagate
        for name, saved_level in self._saved_logger_levels.items():
            logger = self._loggers.get(name)
            managed_level = self._managed_logger_levels.get(name)
            if logger is not None and managed_level is not None and logger.level == managed_level:
                logger.setLevel(saved_level)
        self._capture_handler = None
        self._console_handler = None
        self._context_filter = None
        self._capture_owner = None
        # Handler cleanup is a reversible lifecycle boundary.  Leaving this
        # flag set makes a later ``configure(force=False)`` silently skip
        # rebuilding the handlers it just removed.
        self._configured = False
        self._level = LogLevel.INFO.value
        self._file_level = LogLevel.DEBUG.value
        self._saved_root_state = None
        self._saved_diagnostic_propagate = None
        self._saved_logger_levels.clear()
        self._managed_root_state = None
        self._managed_diagnostic_propagate = None
        self._managed_logger_levels.clear()


# Module-level convenience -------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """Return a reusable framework logger without configuring handlers.

    Framework loggers are children of the ``datacoolie`` logger and inherit
    its handlers (console + capture).

    Args:
        name: Typically ``__name__``.

    Returns:
        The named :class:`logging.Logger`.
    """
    return LogManager.get_instance().get_logger(name)
