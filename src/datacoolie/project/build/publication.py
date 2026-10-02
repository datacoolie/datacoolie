"""Recoverable publication primitives for project build outputs.

The builder creates immutable build directories first and only then replaces
the ``.builds/current`` projection.  This module owns the small amount of
filesystem coordination required for that replacement; it deliberately does
not know anything about metadata or manifests.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
import errno
import os
from pathlib import Path
import shutil
from typing import BinaryIO, Callable
import uuid

from ..errors import ProjectError


class PublicationBusyError(ProjectError):
    """Another process currently owns the project's publication lock."""


class ProjectPublicationLock(AbstractContextManager["ProjectPublicationLock"]):
    """Serialize publication writers across threads and CLI processes.

    The lock file is intentionally retained after release.  The operating
    system owns the lock state, so a process that exits unexpectedly releases
    it without a cleanup race or a stale lock-file heuristic.
    """

    def __init__(self, project_dir: Path | str) -> None:
        self.project_dir = Path(project_dir).expanduser().resolve()
        self.lock_path = self.project_dir / ".builds" / ".publish.lock"
        self._handle: BinaryIO | None = None

    def __enter__(self) -> "ProjectPublicationLock":
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ProjectError(
                f"Cannot prepare publication lock directory: {self.lock_path.parent}"
            ) from exc
        try:
            handle = self.lock_path.open("a+b")
        except OSError as exc:
            raise ProjectError(f"Cannot open publication lock: {self.lock_path}") from exc
        try:
            if os.name == "nt":
                self._lock_windows(handle)
            else:
                self._lock_posix(handle)
        except PublicationBusyError:
            handle.close()
            raise
        except OSError as exc:
            handle.close()
            raise ProjectError(f"Cannot acquire publication lock: {self.lock_path}") from exc
        self._handle = handle
        return self

    @staticmethod
    def _lock_windows(handle: BinaryIO) -> None:
        import msvcrt

        # ``msvcrt.locking`` locks a byte starting at the current file
        # position.  Keep one byte in the file so the range is stable.
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise PublicationBusyError(
                f"Project publication is busy: {handle.name}"
            ) from exc

    @staticmethod
    def _lock_posix(handle: BinaryIO) -> None:
        import fcntl

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                raise PublicationBusyError(
                    f"Project publication is busy: {handle.name}"
                ) from exc
            raise

    def __exit__(self, _exc_type: object, _exc_value: object, _traceback: object) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return None
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            # Closing the descriptor still releases an OS-owned lock.  An
            # unlock cleanup failure must not turn a successful publication
            # into a reported build failure or mask the operation's error.
            pass
        finally:
            handle.close()
        return None


def _remove_path(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    else:
        try:
            path.unlink()
        except OSError:
            # Cleanup is deliberately best effort.  Callers must not report a
            # failed publication after the replacement is already installed.
            pass


def publish_current(
    project_dir: Path | str,
    build_root: Path | str,
    *,
    verify: Callable[[Path], object] | None = None,
) -> Path:
    """Install a verified build as ``.builds/current`` with rollback.

    Callers must hold :class:`ProjectPublicationLock` while invoking this
    function.  Candidate verification happens before the old current is
    moved, so a malformed candidate cannot disturb a healthy projection.
    """

    project = Path(project_dir).expanduser().resolve()
    source_candidate = Path(build_root).expanduser()
    if source_candidate.is_symlink():
        raise ProjectError(f"Build source must not be a symlink: {source_candidate}")
    source = source_candidate.resolve()
    builds_root = project / ".builds"
    builds_root.mkdir(parents=True, exist_ok=True)
    current = builds_root / "current"
    staging = builds_root / f".current-{uuid.uuid4().hex}"
    try:
        shutil.copytree(source, staging)
        if verify is not None:
            verify(staging)
    except Exception:
        _remove_path(staging)
        raise

    backup = builds_root / f".current-backup-{uuid.uuid4().hex}"
    current_moved = False
    replacement_installed = False
    try:
        if current.exists() or current.is_symlink():
            current.rename(backup)
            current_moved = True
        staging.rename(current)
        replacement_installed = True
    except Exception as exc:
        # Only remove ``current`` when this invocation installed the candidate.
        # If renaming the old projection failed, it is still the user's valid
        # current and must never be deleted during rollback.
        if replacement_installed:
            _remove_path(current)
        if current_moved and backup.exists() and not current.exists():
            try:
                backup.rename(current)
            except OSError as restore_error:
                # Preserve the backup and make its recovery path explicit.  A
                # cleanup failure must not hide the original publication error.
                _remove_path(staging)
                raise ProjectError(
                    f"Current publication failed ({exc}); previous current "
                    f"remains recoverable at {backup}: {restore_error}"
                ) from exc
        _remove_path(staging)
        raise

    # A successful replacement is durable even if best-effort cleanup of the
    # old projection is interrupted.  Leaving a uniquely named backup is safe
    # and preferable to risking the newly published current.
    _remove_path(backup)
    return current


__all__ = [
    "ProjectPublicationLock",
    "PublicationBusyError",
    "publish_current",
]
