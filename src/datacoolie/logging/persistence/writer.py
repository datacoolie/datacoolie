"""Bounded JSON Lines persistence shared by framework loggers.

The writer deliberately owns only encoded records and publication state.  It
does not know anything about execution or Python logging semantics; callers decide
which records are admitted and which remote path represents a stream.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Deque, Dict, Optional

from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.configuration.constants import (
    INTERNAL_LOGGER_NAME,
    FlushResult,
    PersistenceMode,
)
from datacoolie.platforms.base import BasePlatform
from datacoolie.utils.converters import custom_json_encoder


_diagnostic_logger = logging.getLogger(INTERNAL_LOGGER_NAME)

# A retained record needs one encoded copy in the active spool and one
# reservation for the temporary upload materialisation.  Reserving both at
# admission prevents an accepted record becoming unflushable when the writer
# later needs to create that second copy.
_MATERIALIZATION_RESERVATION_MULTIPLIER = 2


def _reservation_bytes(encoded_bytes: int) -> int:
    return encoded_bytes * _MATERIALIZATION_RESERVATION_MULTIPLIER


def encode_json_line(record: Dict[str, object]) -> bytes:
    """Encode one compact UTF-8 JSON Lines record with a final newline."""

    return (
        json.dumps(
            record,
            default=custom_json_encoder,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


@dataclass(frozen=True)
class PersistenceStats:
    """Writer-local health counters, distinct from JobRuntimeInfo metrics."""

    pending_records: int
    pending_bytes: int
    dropped_records: int
    dropped_bytes: int
    failed_writes: int
    timed_out_writes: int
    last_error: Optional[str]


class SharedByteBudget:
    """Thread-safe encoded-byte budget shared by streams in one logger."""

    def __init__(self, limit: int, *, protected_limit: int = 0) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        if (
            isinstance(protected_limit, bool)
            or not isinstance(protected_limit, int)
            or protected_limit < 0
            or protected_limit > limit
        ):
            raise ValueError("protected_limit must be between zero and limit")
        self._limit = limit
        self._protected_limit = protected_limit
        self._protected_remaining = protected_limit
        self._used = 0
        self._lock = threading.Lock()

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used

    def try_reserve(self, amount: int, *, protected: bool = False) -> bool:
        if amount < 0:
            raise ValueError("amount must be non-negative")
        with self._lock:
            available_limit = (
                self._limit
                if protected
                else self._limit - self._protected_remaining
            )
            if self._used + amount > available_limit:
                return False
            self._used += amount
            if protected:
                self._protected_remaining = max(
                    0,
                    self._protected_remaining - amount,
                )
            return True

    def release(self, amount: int, *, protected: bool = False) -> None:
        if amount < 0:
            raise ValueError("amount must be non-negative")
        with self._lock:
            self._used = max(0, self._used - amount)
            if protected:
                self._protected_remaining = min(
                    self._protected_limit,
                    self._protected_remaining + amount,
                )


class JsonLogWriter:
    """Publish bounded JSON Lines records using snapshot or batch semantics.

    Snapshot mode retains accepted history in an owned spool and replaces one
    remote file on every successful flush.  Batch mode freezes the current
    records into one immutable part and releases them only after upload
    succeeds.  Both modes use ``upload_file``; remote append/read-modify-write
    is intentionally outside this writer.
    """

    def __init__(
        self,
        platform: BasePlatform,
        target_path: str,
        config: LogConfig,
        *,
        batch_path_factory: Optional[Callable[[int], str]] = None,
        name: str = "json",
        capacity_budget: Optional[SharedByteBudget] = None,
        protected_budget: bool = False,
    ) -> None:
        self._platform = platform
        self._target_path = target_path
        self._config = config
        self._batch_path_factory = batch_path_factory
        self._name = name
        self._capacity_budget = capacity_budget
        self._protected_budget = protected_budget
        self._lock = threading.RLock()
        self._memory: Deque[bytes] = deque()
        self._memory_bytes = 0
        self._pending_bytes = 0
        self._pending_records = 0
        self._dropped_records = 0
        self._dropped_bytes = 0
        self._last_drop_notice = 0.0
        self._failed_writes = 0
        self._timed_out_writes = 0
        self._last_error: Optional[str] = None
        self._sequence = 1
        self._inflight = False
        self._inflight_payload: Optional[str] = None
        self._retry_destination: Optional[str] = None
        self._retry_payload: Optional[str] = None
        self._retry_records = 0
        self._retry_bytes = 0
        self._retry_revision = 0
        self._inflight_records = 0
        self._inflight_bytes = 0
        self._inflight_revision = 0
        self._closed = False
        self._replacement_payload: Optional[bytes] = None
        self._replacement_bytes = 0
        self._snapshot_dirty = False
        self._snapshot_revision = 0
        self._spool_path: Optional[str] = None
        self._spool_dir = config.spool_directory or tempfile.gettempdir()
        self._owned_temp_paths: set[str] = set()

    @property
    def target_path(self) -> str:
        return self._target_path

    @property
    def pending(self) -> bool:
        with self._lock:
            if self._config.persistence_mode == PersistenceMode.SNAPSHOT.value:
                return self._snapshot_dirty or self._retry_payload is not None
            return self._pending_records > 0 or self._replacement_payload is not None

    @property
    def replacement_pending(self) -> bool:
        """Whether a latest replacement waits behind an in-flight snapshot."""

        with self._lock:
            return self._replacement_payload is not None

    @property
    def in_flight(self) -> bool:
        """Whether an upload is currently outside the writer lock."""

        with self._lock:
            return self._inflight

    @property
    def should_flush(self) -> bool:
        with self._lock:
            return (
                self._config.persistence_mode == PersistenceMode.BATCH.value
                and (
                    self._retry_payload is not None
                    or self._pending_bytes - self._retry_bytes
                    >= self._config.flush_batch_bytes
                )
            )

    @property
    def stats(self) -> PersistenceStats:
        with self._lock:
            pending_records = self._pending_records
            pending_bytes = self._pending_bytes + self._replacement_bytes
            if (
                self._config.persistence_mode == PersistenceMode.SNAPSHOT.value
                and not self._snapshot_dirty
            ):
                pending_records = 0
                pending_bytes = 0
            return PersistenceStats(
                pending_records=pending_records,
                pending_bytes=pending_bytes,
                dropped_records=self._dropped_records,
                dropped_bytes=self._dropped_bytes,
                failed_writes=self._failed_writes,
                timed_out_writes=self._timed_out_writes,
                last_error=self._last_error,
            )

    def append(self, record: Dict[str, object]) -> bool:
        """Admit one record, returning ``False`` when capacity is exhausted."""

        payload = encode_json_line(record)
        with self._lock:
            if self._closed:
                return False
            payload_bytes = len(payload)
            total_encoded = (
                self._pending_bytes + self._replacement_bytes + payload_bytes
            )
            if _reservation_bytes(total_encoded) > self._config.spool_max_bytes:
                self._record_drop_locked(payload_bytes, "writer capacity")
                return False
            reservation = _reservation_bytes(payload_bytes)
            if self._capacity_budget is not None and not self._capacity_budget.try_reserve(
                reservation,
                protected=self._protected_budget,
            ):
                self._record_drop_locked(payload_bytes, "shared capacity")
                return False
            self._pending_records += 1
            self._pending_bytes += payload_bytes
            try:
                if self._memory_bytes + payload_bytes <= self._config.buffer_memory_bytes:
                    self._memory.append(payload)
                    self._memory_bytes += payload_bytes
                else:
                    self._spill_memory_locked()
                    self._append_spool_locked(payload)
            except OSError as exc:
                self._pending_records -= 1
                self._pending_bytes -= payload_bytes
                if self._capacity_budget is not None:
                    self._capacity_budget.release(
                        reservation,
                        protected=self._protected_budget,
                    )
                self._record_drop_locked(payload_bytes, "spool unavailable")
                self._last_error = str(exc)
                return False
            self._snapshot_dirty = True
            self._snapshot_revision += 1
            return True

    def replace(self, record: Dict[str, object]) -> bool:
        """Replace the single retained record used by job-runtime snapshots."""

        payload = encode_json_line(record)
        with self._lock:
            if self._closed:
                return False
            payload_bytes = len(payload)
            if payload_bytes > self._config.spool_max_bytes:
                self._record_drop_locked(payload_bytes, "replacement capacity")
                return False
            # An in-flight or failed snapshot owns an immutable payload.  A
            # newer JobRuntime record waits as a coalesced replacement rather
            # than mutating the bytes that may already be visible remotely.
            if self._inflight or self._retry_payload is not None:
                # A snapshot upload owns its immutable payload.  Keep that
                # payload stable and coalesce only the latest replacement for
                # the next flush after the upload completes.
                old_bytes = self._replacement_bytes
                # ``_pending_bytes`` excludes the queued replacement.  The
                # in-flight/active snapshot remains owned while this newer
                # payload waits, so capacity is the active bytes plus the new
                # replacement; subtracting ``old_bytes`` under-counts a
                # coalesced replacement and can exceed the writer quota.
                total_encoded = self._pending_bytes + payload_bytes
                if _reservation_bytes(total_encoded) > self._config.spool_max_bytes:
                    self._record_drop_locked(payload_bytes, "replacement capacity")
                    return False
                delta = _reservation_bytes(payload_bytes) - _reservation_bytes(old_bytes)
                if delta > 0 and (
                    self._capacity_budget is not None
                    and not self._capacity_budget.try_reserve(
                        delta,
                        protected=self._protected_budget,
                    )
                ):
                    self._record_drop_locked(payload_bytes, "replacement capacity")
                    return False
                if delta < 0 and self._capacity_budget is not None:
                    self._capacity_budget.release(
                        -delta,
                        protected=self._protected_budget,
                    )
                self._replacement_payload = payload
                self._replacement_bytes = payload_bytes
                self._snapshot_dirty = True
                self._snapshot_revision += 1
                return True

            old_bytes = self._pending_bytes
            total_encoded = payload_bytes
            if _reservation_bytes(total_encoded) > self._config.spool_max_bytes:
                self._record_drop_locked(payload_bytes, "replacement capacity")
                return False
            delta = _reservation_bytes(payload_bytes) - _reservation_bytes(old_bytes)
            if delta > 0:
                if (
                    self._capacity_budget is not None
                    and not self._capacity_budget.try_reserve(
                        delta,
                        protected=self._protected_budget,
                    )
                ):
                    self._record_drop_locked(payload_bytes, "shared capacity")
                    return False
            elif delta < 0 and self._capacity_budget is not None:
                self._capacity_budget.release(
                    -delta,
                    protected=self._protected_budget,
                )
            self._replacement_payload = None
            self._replacement_bytes = 0
            self._remove_active_storage_locked()
            self._memory.append(payload)
            self._memory_bytes = len(payload)
            self._pending_records = 1
            self._pending_bytes = len(payload)
            self._snapshot_dirty = True
            self._snapshot_revision += 1
            return True

    def _record_drop_locked(self, payload_bytes: int, reason: str) -> None:
        """Count a dropped record and emit a bounded console-only notice."""

        self._dropped_records += 1
        self._dropped_bytes += payload_bytes
        now = time.monotonic()
        if now - self._last_drop_notice >= 60.0:
            self._last_drop_notice = now
            _diagnostic_logger.warning(
                "%s log record dropped (%s); persistence remains bounded",
                self._name,
                reason,
            )

    def flush(self, *, force: bool = False) -> FlushResult:
        """Publish one immutable payload and retain it for a safe retry."""

        frozen: Optional[str] = None
        destination: Optional[str] = None
        frozen_records = 0
        frozen_bytes = 0
        payload_revision = 0
        with self._lock:
            if self._closed:
                return FlushResult.NO_WORK
            if self._inflight:
                # A newer request must never overtake an upload whose outcome
                # may be unknown to the remote backend.
                return FlushResult.IN_FLIGHT

            retrying = self._retry_payload is not None
            if self._config.persistence_mode == PersistenceMode.SNAPSHOT.value:
                if not self._snapshot_dirty and not retrying:
                    return FlushResult.NO_WORK
            elif not self._pending_records:
                return FlushResult.NO_WORK

            active_bytes = self._pending_bytes - self._retry_bytes
            if (
                not force
                and not retrying
                and self._config.persistence_mode == PersistenceMode.BATCH.value
                and active_bytes < self._config.flush_batch_bytes
            ):
                return FlushResult.NO_WORK

            self._inflight = True
            try:
                if retrying:
                    frozen = self._retry_payload
                    frozen_records = self._retry_records
                    frozen_bytes = self._retry_bytes
                    if self._config.persistence_mode == PersistenceMode.BATCH.value:
                        destination = self._retry_destination
                        if destination is None:
                            destination = self._batch_destination_locked()
                    else:
                        destination = self._target_path
                    payload_revision = self._retry_revision
                elif self._config.persistence_mode == PersistenceMode.BATCH.value:
                    frozen_records = self._pending_records - self._retry_records
                    frozen_bytes = self._pending_bytes - self._retry_bytes
                    frozen, frozen_records, frozen_bytes = self._freeze_batch_locked(
                        frozen_records,
                        frozen_bytes,
                    )
                    destination = self._batch_destination_locked()
                else:
                    frozen_bytes = self._pending_bytes
                    payload_revision = self._snapshot_revision
                    frozen = self._materialize_snapshot_locked()
                    destination = self._target_path
                    self._snapshot_dirty = False

                self._inflight_payload = frozen
                self._inflight_records = frozen_records
                self._inflight_bytes = frozen_bytes
                self._inflight_revision = payload_revision
                if self._config.persistence_mode == PersistenceMode.BATCH.value:
                    self._retry_destination = destination
            except Exception as exc:
                # Preparation/path failures happen before remote I/O.  Keep a
                # successfully materialised payload as the retry object; if
                # materialisation itself failed, active records remain intact.
                self._inflight = False
                self._inflight_payload = None
                self._inflight_records = 0
                self._inflight_bytes = 0
                self._inflight_revision = 0
                self._failed_writes += 1
                self._last_error = str(exc)
                if self._config.persistence_mode == PersistenceMode.SNAPSHOT.value:
                    self._snapshot_dirty = True
                if frozen is not None and not self._closed:
                    self._retry_payload = frozen
                    self._retry_records = frozen_records
                    self._retry_bytes = frozen_bytes
                    self._retry_revision = payload_revision
                    self._retry_destination = destination
                raise

        assert frozen is not None
        assert destination is not None
        try:
            self._platform.upload_file(frozen, destination, overwrite=True)
        except Exception as exc:
            retain_for_retry = False
            with self._lock:
                self._failed_writes += 1
                self._last_error = str(exc)
                self._inflight = False
                self._inflight_payload = None
                if not self._closed:
                    # The payload and destination are immutable across every
                    # retry.  Newly admitted records remain in active storage
                    # and will form a later batch/ snapshot.
                    if self._config.persistence_mode == PersistenceMode.SNAPSHOT.value:
                        self._snapshot_dirty = True
                    self._retry_payload = frozen
                    self._retry_records = frozen_records
                    self._retry_bytes = frozen_bytes
                    self._retry_revision = self._inflight_revision
                    self._retry_destination = destination
                    retain_for_retry = True
                else:
                    self._release_inflight_reservation_locked()
                self._inflight_records = 0
                self._inflight_bytes = 0
                self._inflight_revision = 0
            if not retain_for_retry:
                self._safe_remove(frozen)
            raise
        else:
            with self._lock:
                self._last_error = None
                self._inflight = False
                self._inflight_payload = None
                completed_bytes = self._inflight_bytes
                completed_records = self._inflight_records
                completed_revision = self._inflight_revision
                completed_retry = self._retry_payload == frozen

                if self._config.persistence_mode == PersistenceMode.BATCH.value:
                    self._pending_records = max(0, self._pending_records - completed_records)
                    self._pending_bytes = max(0, self._pending_bytes - completed_bytes)
                    self._release_inflight_reservation_locked()
                    self._clear_retry_locked()
                    self._retry_destination = None
                    self._sequence += 1
                elif not self._closed and self._replacement_payload is not None:
                    replacement = self._replacement_payload
                    self._replacement_payload = None
                    # The old active snapshot is no longer retained; its full
                    # reservation can be replaced by the queued record.
                    self._release_inflight_reservation_locked()
                    self._remove_active_storage_locked()
                    self._memory.append(replacement)
                    self._memory_bytes = len(replacement)
                    self._pending_records = 1
                    self._pending_bytes = len(replacement)
                    self._replacement_bytes = 0
                    self._snapshot_dirty = True
                elif self._closed:
                    self._release_inflight_reservation_locked()
                elif self._config.persistence_mode == PersistenceMode.SNAPSHOT.value:
                    # The active snapshot remains retained for the next
                    # replacement/refresh; only its retry copy is retired.
                    self._snapshot_dirty = self._snapshot_revision != completed_revision

                if completed_retry:
                    self._clear_retry_locked()
                self._inflight_records = 0
                self._inflight_bytes = 0
                self._inflight_revision = 0
            self._safe_remove(frozen)
            return FlushResult.WRITTEN

    def mark_timed_out(self) -> None:
        with self._lock:
            self._timed_out_writes += 1

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._replacement_payload = None
            retained_encoded = self._pending_bytes + self._replacement_bytes
            inflight_encoded = self._inflight_bytes if self._inflight else 0
            release_encoded = max(0, retained_encoded - inflight_encoded)
            if self._capacity_budget is not None:
                self._capacity_budget.release(
                    _reservation_bytes(release_encoded),
                    protected=self._protected_budget,
                )
            self._pending_records = 0
            self._pending_bytes = 0
            self._replacement_bytes = 0
            self._retry_payload = None
            self._retry_records = 0
            self._retry_bytes = 0
            self._retry_revision = 0
            self._retry_destination = None
            self._snapshot_dirty = False
            self._remove_active_storage_locked()
            inflight_path = self._inflight_payload if self._inflight else None
            for path in tuple(self._owned_temp_paths):
                if path == inflight_path:
                    continue
                self._safe_remove(path)

    def _spill_memory_locked(self) -> None:
        if not self._memory:
            return
        original_size = (
            os.path.getsize(self._spool_path)
            if self._spool_path is not None and os.path.exists(self._spool_path)
            else 0
        )
        try:
            for payload in self._memory:
                self._append_spool_locked(payload)
        except OSError:
            # A partial spill must not duplicate already moved records when
            # the in-memory deque is retried later.
            if self._spool_path is not None:
                try:
                    with open(self._spool_path, "r+b") as handle:
                        handle.truncate(original_size)
                except OSError:
                    pass
            raise
        self._memory.clear()
        self._memory_bytes = 0

    def _append_spool_locked(self, payload: bytes) -> None:
        if self._spool_path is None:
            Path(self._spool_dir).mkdir(parents=True, exist_ok=True)
            handle = tempfile.NamedTemporaryFile(
                mode="wb",
                prefix="datacoolie-log-",
                suffix=".spool",
                dir=self._spool_dir,
                delete=False,
            )
            self._spool_path = handle.name
            self._owned_temp_paths.add(handle.name)
            handle.close()
        original_size = os.path.getsize(self._spool_path)
        try:
            with open(self._spool_path, "ab") as handle:
                handle.write(payload)
        except OSError:
            try:
                with open(self._spool_path, "r+b") as handle:
                    handle.truncate(original_size)
            except OSError:
                pass
            raise

    def _materialize_snapshot_locked(self) -> str:
        self._spill_memory_locked()
        return self._copy_spool_locked(self._spool_path)

    def _freeze_batch_locked(
        self,
        frozen_records: int,
        frozen_bytes: int,
    ) -> tuple[str, int, int]:
        self._spill_memory_locked()
        frozen = self._copy_spool_locked(self._spool_path)
        self._remove_active_storage_locked()
        self._memory.clear()
        self._memory_bytes = 0
        return frozen, frozen_records, frozen_bytes

    def _clear_retry_locked(self) -> None:
        self._retry_payload = None
        self._retry_records = 0
        self._retry_bytes = 0
        self._retry_revision = 0

    def _release_inflight_reservation_locked(self) -> None:
        """Release the reservation held by a completed upload payload."""

        if self._capacity_budget is not None and self._inflight_bytes:
            self._capacity_budget.release(
                _reservation_bytes(self._inflight_bytes),
                protected=self._protected_budget,
            )

    def _batch_destination_locked(self) -> str:
        if self._batch_path_factory is not None:
            return self._batch_path_factory(self._sequence)
        return self._target_path

    def _copy_spool_locked(self, spool_path: Optional[str]) -> str:
        payload_path = self._new_temp_path(".json")
        try:
            with open(payload_path, "wb") as handle:
                if spool_path is not None and os.path.exists(spool_path):
                    with open(spool_path, "rb") as source:
                        shutil.copyfileobj(source, handle)
                for item in self._memory:
                    handle.write(item)
        except Exception:
            self._safe_remove(payload_path)
            raise
        return payload_path

    def _new_temp_path(self, suffix: str) -> str:
        Path(self._spool_dir).mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="datacoolie-log-payload-",
            suffix=suffix,
            dir=self._spool_dir,
            delete=False,
        )
        path = handle.name
        handle.close()
        self._owned_temp_paths.add(path)
        return path

    def _remove_active_storage_locked(self) -> None:
        if self._spool_path is not None:
            self._safe_remove(self._spool_path)
            self._spool_path = None
        self._memory.clear()
        self._memory_bytes = 0

    def _safe_remove(self, path: str) -> None:
        removed = False
        try:
            os.remove(path)
            removed = True
        except FileNotFoundError:
            removed = True
        except OSError:
            # Cleanup must never hide a remote write result.  Keep ownership
            # recorded so a later completion/cleanup can retry the removal.
            return
        if removed:
            self._owned_temp_paths.discard(path)
