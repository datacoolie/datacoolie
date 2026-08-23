"""Local filesystem platform implementation.

Uses ``pathlib``, ``os``, and ``shutil`` for all operations.
Suitable for local development, testing, and single-node environments.
"""

from __future__ import annotations

import os
import shutil
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms.base import BasePlatform, FileInfo
from datacoolie.utils.path_utils import normalize_path


@dataclass(frozen=True, slots=True)
class _ScannedEntry:
    """Metadata captured while a directory entry is still open."""

    path: Path
    name: str
    stat_result: os.stat_result
    is_file: bool
    is_dir: bool
    is_symlink: bool


class LocalPlatform(BasePlatform):
    """Platform backed by the local filesystem.

    Also implements :meth:`_fetch_secret` via ``os.environ``, so it can
    serve as its own secret provider in local / test environments.

    Args:
        base_path: Optional root directory. When set, all *relative*
            paths are resolved against this root.
        cache_ttl: Secret cache time-to-live in seconds (default 300).
            Pass ``0`` to disable caching.
    """

    def __init__(
        self,
        base_path: str | None = None,
        cache_ttl: int = 300,
        **kwargs: Any,
    ) -> None:
        super().__init__(cache_ttl=cache_ttl)
        self._base_path = Path(base_path).resolve() if base_path else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve(self, path: str) -> Path:
        """Resolve *path* against the optional **base_path**.

        When ``base_path`` is set, absolute paths and traversals that
        escape the sandbox are rejected with :class:`PlatformError`.
        """
        p = Path(path)
        if self._base_path is not None:
            if p.is_absolute():
                raise PlatformError(
                    f"Absolute paths are not allowed when base_path is set: {path}"
                )
            # ``os.path.realpath`` canonicalises symlink escapes without
            # routing every operation through ``Path.resolve().stat()``.
            resolved = Path(os.path.realpath(self._base_path / p))
            if not resolved.is_relative_to(self._base_path):
                raise PlatformError(f"Path escapes base_path: {path}")
            return resolved
        return p

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def read_file(self, path: str) -> str:
        resolved = self._resolve(path)
        try:
            return resolved.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise PlatformError(f"File not found: {resolved}") from exc
        except OSError as exc:
            raise PlatformError(f"Cannot read file: {resolved}") from exc

    def read_bytes(self, path: str) -> bytes:
        """Zero-overhead native read \u2014 no temp file."""
        resolved = self._resolve(path)
        try:
            return resolved.read_bytes()
        except FileNotFoundError as exc:
            raise PlatformError(f"File not found: {resolved}") from exc
        except OSError as exc:
            raise PlatformError(f"Cannot read file: {resolved}") from exc

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        """Zero-overhead native write — no temp file."""
        resolved = self._resolve(path)
        try:
            resolved.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise PlatformError(f"Cannot write file: {resolved}") from exc

        try:
            if overwrite:
                resolved.write_bytes(data)
            else:
                with resolved.open("xb") as handle:
                    handle.write(data)
        except FileExistsError as exc:
            raise PlatformError(
                f"File already exists (set overwrite=True): {resolved}"
            ) from exc
        except OSError as exc:
            raise PlatformError(f"Cannot write file: {resolved}") from exc

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        resolved = self._resolve(path)
        try:
            resolved.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise PlatformError(f"Cannot write file: {resolved}") from exc

        try:
            if overwrite:
                resolved.write_text(content, encoding="utf-8")
            else:
                with resolved.open("x", encoding="utf-8") as handle:
                    handle.write(content)
        except FileExistsError as exc:
            raise PlatformError(
                f"File already exists (set overwrite=True): {resolved}"
            ) from exc
        except OSError as exc:
            raise PlatformError(f"Cannot write file: {resolved}") from exc

    def append_file(self, path: str, content: str) -> None:
        resolved = self._resolve(path)
        try:
            resolved.parent.mkdir(parents=True, exist_ok=True)
            with resolved.open("a", encoding="utf-8") as fh:
                fh.write(content)
        except OSError as exc:
            raise PlatformError(f"Cannot append to file: {resolved}") from exc

    def delete_file(self, path: str) -> None:
        resolved = self._resolve(path)
        try:
            resolved.unlink(missing_ok=True)
        except OSError as exc:
            raise PlatformError(f"Cannot delete file: {resolved}") from exc

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def create_folder(self, path: str) -> None:
        resolved = self._resolve(path)
        try:
            resolved.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise PlatformError(f"Cannot create folder: {resolved}") from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        resolved = self._resolve(path)
        if not resolved.exists():
            return  # idempotent
        try:
            if recursive:
                shutil.rmtree(resolved)
            else:
                resolved.rmdir()  # raises if non-empty
        except OSError as exc:
            raise PlatformError(f"Cannot delete folder: {resolved}") from exc

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        resolved = self._resolve(path)
        self._validate_directory(resolved)
        results: list[FileInfo] = []
        for entry in self._walk_entries(resolved, recursive=recursive):
            if not entry.is_file:
                continue
            if extension and not entry.name.endswith(extension):
                continue
            results.append(self._file_info(entry.path, entry.stat_result))
        return results

    def list_folders(
        self,
        path: str,
        *,
        recursive: bool = False,
    ) -> list[str]:
        resolved = self._resolve(path)
        self._validate_directory(resolved)
        results: list[str] = []
        for entry in self._walk_entries(resolved, recursive=recursive):
            if entry.is_dir:
                results.append(normalize_path(str(entry.path)))
        return results

    # ------------------------------------------------------------------
    # Existence checks
    # ------------------------------------------------------------------

    def file_exists(self, path: str) -> bool:
        return self._resolve(path).is_file()

    def folder_exists(self, path: str) -> bool:
        return self._resolve(path).is_dir()

    # ------------------------------------------------------------------
    # File management
    # ------------------------------------------------------------------

    def upload_file(
        self, local_path: str, dest: str, *, overwrite: bool = False
    ) -> None:
        src_p = Path(
            local_path
        )  # always an absolute local OS path — do not resolve via _base_path
        dest_p = self._resolve(dest)
        if not src_p.is_file():
            raise PlatformError(f"Local file not found: {src_p}")
        if dest_p.exists() and not overwrite:
            raise PlatformError(
                f"Destination already exists (set overwrite=True): {dest_p}"
            )
        try:
            dest_p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_p, dest_p)
        except OSError as exc:
            raise PlatformError(f"Cannot upload file: {src_p} → {dest_p}") from exc

    def download_file(self, src: str, dest: str) -> None:
        src_p = self._resolve(src)
        dest_p = Path(dest)  # dest is always an absolute local OS path
        if not src_p.is_file():
            raise PlatformError(f"Source file not found: {src_p}")
        try:
            dest_p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src_p), str(dest_p))
        except OSError as exc:
            raise PlatformError(f"Cannot download file: {src_p} → {dest_p}") from exc

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        src_p = self._resolve(src)
        dest_p = self._resolve(dest)
        if self._same_file(src_p, dest_p):
            return
        if not src_p.is_file():
            raise PlatformError(f"Source file not found: {src_p}")
        if dest_p.exists() and not overwrite:
            raise PlatformError(
                f"Destination already exists (set overwrite=True): {dest_p}"
            )
        try:
            dest_p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_p, dest_p)
        except OSError as exc:
            raise PlatformError(f"Cannot copy file: {src_p} → {dest_p}") from exc

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        src_p = self._resolve(src)
        dest_p = self._resolve(dest)
        if self._same_file(src_p, dest_p):
            return
        if not src_p.is_file():
            raise PlatformError(f"Source file not found: {src_p}")
        if dest_p.exists() and not overwrite:
            raise PlatformError(
                f"Destination already exists (set overwrite=True): {dest_p}"
            )
        try:
            dest_p.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src_p), str(dest_p))
        except OSError as exc:
            raise PlatformError(f"Cannot move file: {src_p} → {dest_p}") from exc

    def get_file_info(self, path: str) -> FileInfo:
        resolved = self._resolve(path)
        try:
            stat_result = resolved.stat()
        except FileNotFoundError as exc:
            raise PlatformError(f"Path does not exist: {resolved}") from exc
        except OSError as exc:
            raise PlatformError(f"Cannot get file info: {resolved}") from exc
        return self._file_info(resolved, stat_result)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _file_info(
        p: Path,
        stat_result: os.stat_result | None = None,
    ) -> FileInfo:
        """Build a :class:`~datacoolie.platforms.base.FileInfo` from a ``Path``."""
        stat_result = stat_result or p.stat()
        mod_time = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc)
        return FileInfo(
            name=p.name,
            path=str(p),
            modification_time=mod_time,
            size=stat_result.st_size,
            is_dir=stat.S_ISDIR(stat_result.st_mode),
        )

    @staticmethod
    def _same_file(src: Path, dest: Path) -> bool:
        """Return whether two paths identify the same existing file."""
        if src == dest:
            return True
        try:
            return os.path.samefile(src, dest)
        except (FileNotFoundError, OSError):
            return False

    @staticmethod
    def _validate_directory(path: Path) -> None:
        try:
            stat_result = os.stat(path)
        except FileNotFoundError as exc:
            raise PlatformError(f"Path is not a directory: {path}") from exc
        except OSError as exc:
            raise PlatformError(f"Cannot list directory: {path}") from exc
        if not stat.S_ISDIR(stat_result.st_mode):
            raise PlatformError(f"Path is not a directory: {path}")

    @staticmethod
    def _walk_entries(
        root: Path,
        *,
        recursive: bool,
    ) -> Iterator[_ScannedEntry]:
        """Yield directory entries without sorting or following directory links."""
        pending = [root]
        while pending:
            directory = pending.pop()
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        entry_path = Path(entry.path)
                        try:
                            link_stat = entry.stat(follow_symlinks=False)
                            is_symlink = stat.S_ISLNK(link_stat.st_mode)
                            stat_result = link_stat
                            if is_symlink:
                                try:
                                    stat_result = entry.stat(follow_symlinks=True)
                                except FileNotFoundError:
                                    # Broken links and entries removed during the
                                    # scan are not part of a stable listing.
                                    continue
                            is_file = stat.S_ISREG(stat_result.st_mode)
                            is_dir = stat.S_ISDIR(stat_result.st_mode)
                        except FileNotFoundError:
                            # A child may disappear between scandir and stat.
                            continue
                        except OSError as exc:
                            raise PlatformError(
                                f"Cannot inspect directory entry: {entry_path}"
                            ) from exc

                        if is_dir and recursive and not is_symlink:
                            pending.append(entry_path)
                        yield _ScannedEntry(
                            path=entry_path,
                            name=entry.name,
                            stat_result=stat_result,
                            is_file=is_file,
                            is_dir=is_dir,
                            is_symlink=is_symlink,
                        )
            except FileNotFoundError:
                if directory == root:
                    raise PlatformError(f"Path is not a directory: {root}") from None
                continue
            except PlatformError:
                raise
            except OSError as exc:
                raise PlatformError(f"Cannot list directory: {directory}") from exc

    # ------------------------------------------------------------------
    # Secrets
    # ------------------------------------------------------------------

    def _fetch_secret(self, key: str, source: str) -> str:
        """Look up *key* (with optional *source* prefix) in ``os.environ``.

        Args:
            key: Environment variable name (without prefix).
            source: Optional env var prefix (e.g. ``"APP_"``).

        Raises:
            PlatformError: If the variable is not set.
        """
        full_key = f"{source}{key}" if source else key
        value = os.environ.get(full_key)
        if value is None:
            raise PlatformError(
                f"Secret not found: environment variable '{full_key}' is not set."
            )
        return value
