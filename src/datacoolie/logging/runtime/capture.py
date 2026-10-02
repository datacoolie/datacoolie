"""Captured Python logging records and local fallback storage."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from datacoolie.logging.configuration.config import normalize_storage_mode
from datacoolie.logging.configuration.constants import INTERNAL_LOGGER_NAME, StorageMode
from datacoolie.logging.presentation.formatting import format_context_suffix
from datacoolie.utils.time import utc_now

_diagnostic_logger = logging.getLogger(INTERNAL_LOGGER_NAME)


def _optional_text(value: object) -> Optional[str]:
    """Return a non-empty string field or omit malformed persisted metadata."""
    return value if isinstance(value, str) and value else None


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
    dataflow_run_id: Optional[str] = None
    event_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        d: Dict[str, Any] = {
            "ts": self.timestamp.isoformat(),
            "level": self.level,
            "logger": self.logger_name,
            "msg": self.message,
        }
        if isinstance(self.dataflow_id, str) and self.dataflow_id:
            d["dataflow_id"] = self.dataflow_id
        if isinstance(self.dataflow_run_id, str) and self.dataflow_run_id:
            d["dataflow_run_id"] = self.dataflow_run_id
        if isinstance(self.event_name, str) and self.event_name:
            d["event_name"] = self.event_name
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
            dataflow_id=_optional_text(d.get("dataflow_id")),
            dataflow_run_id=_optional_text(d.get("dataflow_run_id")),
            event_name=_optional_text(d.get("event_name")),
        )

    def format(self, include_location: bool = False) -> str:
        ts = self.timestamp.isoformat()
        if include_location and self.func_name:
            loc = f"{self.func_name}"
            if self.line_no:
                loc += f":{self.line_no}"
        else:
            loc = ""
        logger_part = f"{self.logger_name}:{loc}" if loc else self.logger_name
        context = format_context_suffix(
            self.dataflow_id,
            self.dataflow_run_id,
            self.event_name,
        )
        parts = [ts, self.level, logger_part]
        if context:
            parts.append(context)
        parts.append(self.message)
        base = " - ".join(parts)
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
        max_buffer_bytes: int = 512 * 1024 * 1024,
    ) -> None:
        super().__init__(level)
        storage_mode = normalize_storage_mode(storage_mode)
        if (
            isinstance(max_buffer_bytes, bool)
            or not isinstance(max_buffer_bytes, int)
            or max_buffer_bytes <= 0
        ):
            raise ValueError("max_buffer_bytes must be a positive integer")
        self._storage_mode = storage_mode
        self._max_buffer_bytes = max_buffer_bytes
        self._records: List[LogRecord] = []
        self._buffered_bytes = 0
        self._dropped_records = 0
        self._dropped_bytes = 0
        self._last_drop_notice = 0.0
        self._temp_file: Optional[str] = None
        # An active SystemLogger can consume records directly into its shared
        # bounded writer. Returning ``True`` from the callback means the
        # record was handled (including an intentional capacity drop), so the
        # fallback capture buffer does not become a second unbounded queue.
        self._record_callback: Optional[Callable[[LogRecord], bool]] = None
        if storage_mode == StorageMode.FILE.value:
            self._setup_temp_file()

    def _setup_temp_file(self) -> None:
        self._temp_file = self._new_temp_file_path()

    @staticmethod
    def _new_temp_file_path() -> str:
        temp_dir = tempfile.gettempdir()
        ts = utc_now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(
            temp_dir,
            f"datacoolie_capture_{ts}_{os.getpid()}_{uuid.uuid4().hex}.tmp",
        )

    def reconfigure(
        self,
        *,
        level: int,
        storage_mode: str,
        formatter: logging.Formatter,
        max_buffer_bytes: Optional[int] = None,
    ) -> None:
        """Atomically update capture settings without detaching the handler."""
        storage_mode = normalize_storage_mode(storage_mode)
        if max_buffer_bytes is not None and (
            isinstance(max_buffer_bytes, bool)
            or not isinstance(max_buffer_bytes, int)
            or max_buffer_bytes <= 0
        ):
            raise ValueError("max_buffer_bytes must be a positive integer")

        with self.lock:
            candidate_max = (
                self._max_buffer_bytes
                if max_buffer_bytes is None
                else max_buffer_bytes
            )
            if storage_mode != self._storage_mode:
                records = (
                    self._load_from_file(raise_on_error=True)
                    if self._storage_mode == StorageMode.FILE.value
                    else list(self._records)
                )
                records, dropped = self._bounded_prefix(records, candidate_max)

                if storage_mode == StorageMode.FILE.value:
                    new_temp_file = self._new_temp_file_path()
                    try:
                        with open(new_temp_file, "w", encoding="utf-8") as handle:
                            for record in records:
                                handle.write(json.dumps(record.to_dict(), default=str) + "\n")
                    except Exception:
                        try:
                            if os.path.exists(new_temp_file):
                                os.remove(new_temp_file)
                        except Exception:
                            pass
                        raise
                    old_temp_file = self._temp_file
                    self._records = []
                    self._buffered_bytes = sum(self._record_size(r) for r in records)
                    self._temp_file = new_temp_file
                    self._storage_mode = storage_mode
                    self._max_buffer_bytes = candidate_max
                    if old_temp_file and os.path.exists(old_temp_file):
                        self._safe_remove(old_temp_file)
                else:
                    old_temp_file = self._temp_file
                    if old_temp_file and os.path.exists(old_temp_file):
                        self._safe_remove(old_temp_file)
                    self._records = records
                    self._buffered_bytes = sum(self._record_size(r) for r in records)
                    self._temp_file = None
                    self._storage_mode = storage_mode
                    self._max_buffer_bytes = candidate_max

                for record in dropped:
                    self._record_drop(self._record_size(record), "reconfigure capacity")
            elif max_buffer_bytes is not None:
                if candidate_max < self._max_buffer_bytes:
                    records = (
                        self._load_from_file(raise_on_error=True)
                        if self._storage_mode == StorageMode.FILE.value
                        else list(self._records)
                    )
                    records, dropped = self._bounded_prefix(records, candidate_max)
                    if self._storage_mode == StorageMode.FILE.value:
                        new_temp_file = self._new_temp_file_path()
                        try:
                            with open(new_temp_file, "w", encoding="utf-8") as handle:
                                for record in records:
                                    handle.write(
                                        json.dumps(
                                            record.to_dict(),
                                            ensure_ascii=False,
                                            separators=(",", ":"),
                                        )
                                        + "\n"
                                    )
                        except Exception:
                            self._safe_remove(new_temp_file)
                            raise
                        old_temp_file = self._temp_file
                        self._temp_file = new_temp_file
                        self._records = []
                        if old_temp_file and os.path.exists(old_temp_file):
                            self._safe_remove(old_temp_file)
                    else:
                        self._records = records
                    self._buffered_bytes = sum(self._record_size(r) for r in records)
                    self._max_buffer_bytes = candidate_max
                    for record in dropped:
                        self._record_drop(self._record_size(record), "reconfigure capacity")
                else:
                    self._max_buffer_bytes = candidate_max

            self.setLevel(level)
            self.setFormatter(formatter)

    def _bounded_prefix(
        self,
        records: List[LogRecord],
        max_bytes: int,
    ) -> tuple[List[LogRecord], List[LogRecord]]:
        """Keep the oldest records that fit and return the dropped suffix."""
        retained: List[LogRecord] = []
        dropped: List[LogRecord] = []
        retained_bytes = 0
        for index, record in enumerate(records):
            size = self._record_size(record)
            if retained and retained_bytes + size > max_bytes:
                dropped.extend(records[index:])
                break
            if not retained and size > max_bytes:
                dropped.extend(records[index:])
                break
            retained.append(record)
            retained_bytes += size
        return retained, dropped

    def emit(self, record: logging.LogRecord) -> None:
        # NOTE: self.lock is already held by Handler.handle() when this runs.
        try:
            exc_text: Optional[str] = None
            if record.exc_info:
                # Persist only the traceback body. The structured ``message``
                # already contains the human explanation and the formatter
                # adds it exactly once during presentation.
                formatter = self.formatter or logging.Formatter()
                exc_text = formatter.formatException(record.exc_info)

            lr = LogRecord(
                timestamp=datetime.fromtimestamp(record.created, tz=timezone.utc),
                level=record.levelname,
                logger_name=record.name,
                message=record.getMessage(),
                module=record.module,
                func_name=record.funcName,
                line_no=record.lineno,
                exc_info=exc_text if record.exc_info else None,
                dataflow_id=_optional_text(getattr(record, "dataflow_id", None)),
                dataflow_run_id=_optional_text(
                    getattr(record, "dataflow_run_id", None)
                ),
                event_name=_optional_text(getattr(record, "event_name", None)),
            )

            callback = self._record_callback
            consumed = False
            if callback is not None:
                try:
                    consumed = bool(callback(lr))
                except Exception as exc:
                    # A persistence callback must never break application
                    # logging.  Retain the record in the capture fallback so
                    # a transient local writer error remains inspectable.
                    _diagnostic_logger.warning(
                        "Capture callback failed; retaining record locally: %s",
                        exc,
                    )
            if not consumed:
                self._retain_fallback(lr)
        except Exception:
            self.handleError(record)

    @staticmethod
    def _record_size(record: LogRecord) -> int:
        return len(
            (
                json.dumps(
                    record.to_dict(),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
        )

    def _record_drop(self, size: int, reason: str) -> None:
        self._dropped_records += 1
        self._dropped_bytes += size
        now = time.monotonic()
        if now - self._last_drop_notice >= 60.0:
            self._last_drop_notice = now
            _diagnostic_logger.warning(
                "Captured log record dropped (%s); fallback storage remains bounded",
                reason,
            )

    def _retain_fallback(self, record: LogRecord) -> bool:
        """Retain a record in the bounded local fallback store."""
        size = self._record_size(record)
        if self._buffered_bytes + size > self._max_buffer_bytes:
            self._record_drop(size, "capture capacity")
            return False
        if self._storage_mode == StorageMode.FILE.value and self._temp_file:
            try:
                with open(self._temp_file, "a", encoding="utf-8") as handle:
                    handle.write(
                        json.dumps(
                            record.to_dict(),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
                self._buffered_bytes += size
                return True
            except OSError as exc:
                _diagnostic_logger.warning(
                    "Capture file fallback failed; retaining in memory: %s",
                    exc,
                )
        self._records.append(record)
        self._buffered_bytes += size
        return True

    def _write_to_file(self, record: LogRecord) -> None:
        self._retain_fallback(record)

    def get_records(self) -> List[LogRecord]:
        with self.lock:
            if self._storage_mode == StorageMode.FILE.value:
                return self._load_from_file()
            return list(self._records)

    @property
    def dropped_records(self) -> int:
        with self.lock:
            return self._dropped_records

    @property
    def dropped_bytes(self) -> int:
        with self.lock:
            return self._dropped_bytes

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
                            records.append(self._parse_file_line(line))
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

    def drain_records(
        self,
        *,
        max_records: int = 4096,
        max_bytes: int = 8 * 1024 * 1024,
    ) -> List[LogRecord]:
        """Detach one bounded prefix of the fallback capture store.

        Bounded handoff prevents a long-running process from materialising a
        full file-backed spool in memory just before persistence.  A failed
        handoff can put the returned records back with ``restore_records``.
        """
        if (
            isinstance(max_records, bool)
            or not isinstance(max_records, int)
            or max_records <= 0
        ):
            raise ValueError("max_records must be a positive integer")
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or max_bytes <= 0
        ):
            raise ValueError("max_bytes must be a positive integer")

        with self.lock:
            if self._storage_mode == StorageMode.FILE.value:
                return self._drain_file_chunk_locked(
                    max_records=max_records,
                    max_bytes=max_bytes,
                )

            records: List[LogRecord] = []
            drained_bytes = 0
            for candidate in self._records:
                size = self._record_size(candidate)
                if len(records) >= max_records or (
                    records and drained_bytes + size > max_bytes
                ):
                    break
                records.append(candidate)
                drained_bytes += size
            if records:
                del self._records[: len(records)]
            self._buffered_bytes = max(0, self._buffered_bytes - drained_bytes)
            return records

    def _drain_file_chunk_locked(
        self,
        *,
        max_records: int,
        max_bytes: int,
    ) -> List[LogRecord]:
        """Rotate one bounded prefix while retaining the unconsumed suffix."""
        selected: List[LogRecord] = []
        selected_bytes = 0
        remaining_memory: List[LogRecord] = []

        def take(record: LogRecord) -> bool:
            nonlocal selected_bytes
            size = self._record_size(record)
            if selected and (
                len(selected) >= max_records or selected_bytes + size > max_bytes
            ):
                return False
            selected.append(record)
            selected_bytes += size
            return True

        for record in self._records:
            if not take(record):
                remaining_memory.append(record)

        old_temp_file = self._temp_file
        remainder_path: Optional[str] = None
        remainder_handle = None
        remainder_bytes = 0
        try:
            if old_temp_file and os.path.exists(old_temp_file):
                remainder_path = self._new_temp_file_path()
                remainder_handle = open(remainder_path, "w", encoding="utf-8")
                with open(old_temp_file, "r", encoding="utf-8") as source:
                    for line in source:
                        stripped = line.strip()
                        if not stripped:
                            continue
                        record = self._parse_file_line(stripped)
                        if take(record):
                            continue
                        remainder_handle.write(
                            line if line.endswith("\n") else line + "\n"
                        )
                        remainder_bytes += self._record_size(record)
            if remainder_handle is not None:
                remainder_handle.close()
                remainder_handle = None

            if remainder_path is not None:
                old_path = self._temp_file
                self._temp_file = remainder_path
                if old_path and os.path.exists(old_path):
                    self._safe_remove(old_path)
            elif self._temp_file is not None and os.path.exists(self._temp_file):
                old_path = self._temp_file
                self._temp_file = self._new_temp_file_path()
                self._safe_remove(old_path)

            self._records = remaining_memory
            self._buffered_bytes = (
                sum(self._record_size(record) for record in remaining_memory)
                + remainder_bytes
            )
            return selected
        except Exception:
            if remainder_handle is not None:
                remainder_handle.close()
            if remainder_path is not None:
                self._safe_remove(remainder_path)
            raise

    @staticmethod
    def _parse_file_line(line: str) -> LogRecord:
        try:
            return LogRecord.from_dict(json.loads(line))
        except (json.JSONDecodeError, KeyError):
            return LogRecord(
                timestamp=utc_now(),
                level="INFO",
                logger_name="file",
                message=line,
            )

    def set_record_callback(
        self,
        callback: Optional[Callable[[LogRecord], bool]],
    ) -> None:
        """Install a consumer for newly captured records.

        The consumer runs while the handler lock is held and must not perform
        remote I/O.  Returning ``True`` acknowledges the record to the
        consumer; returning ``False`` keeps it in the normal capture store.
        """
        with self.lock:
            self._record_callback = callback

    def restore_records(self, records: List[LogRecord]) -> None:
        """Restore unhandled records ahead of newer fallback records."""
        if not records:
            return
        with self.lock:
            restored: List[LogRecord] = []
            for record in records:
                size = self._record_size(record)
                if self._buffered_bytes + size > self._max_buffer_bytes:
                    self._record_drop(size, "restore capacity")
                    continue
                restored.append(record)
                self._buffered_bytes += size
            self._records = restored + self._records

    def clear(self) -> None:
        with self.lock:
            self._records.clear()
            self._buffered_bytes = 0
            if self._temp_file and os.path.exists(self._temp_file):
                try:
                    os.remove(self._temp_file)
                    self._setup_temp_file()
                except Exception:
                    pass

    def cleanup(self) -> None:
        with self.lock:
            self._record_callback = None
            self._records.clear()
            self._buffered_bytes = 0
            if self._temp_file and os.path.exists(self._temp_file):
                try:
                    os.remove(self._temp_file)
                except Exception:
                    pass
            self._temp_file = None

    @staticmethod
    def _safe_remove(path: str) -> bool:
        """Best-effort removal for rotated fallback files.

        Reconfiguration and bounded drains have already committed their new
        in-memory/file pointer before the old file is removed.  A local
        permission or transient filesystem error must therefore not turn a
        successful state transition into a misleading exception; the stale
        file is left for the host cleanup policy to handle.
        """
        try:
            os.remove(path)
        except FileNotFoundError:
            return True
        except OSError as exc:
            _diagnostic_logger.warning(
                "Could not remove stale capture fallback %s: %s",
                path,
                exc,
            )
            return False
        return True
