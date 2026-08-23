"""Native Databricks backend using dbutils and POSIX Volume I/O."""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Literal
from uuid import uuid4

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._databricks.paths import (
    DatabricksPath,
    canonicalize_output_path,
    ensure_mutable_path,
    parent_path,
    parse_databricks_path,
)
from datacoolie.platforms._databricks.runtime import require_dbutils
from datacoolie.platforms._databricks.traversal import (
    DEFAULT_DBUTILS_LIST_WORKERS,
    list_tree,
)
from datacoolie.platforms._databricks.volume_traversal import list_volume_tree
from datacoolie.platforms.base import FileInfo

_TRANSFER_BUFFER_SIZE = 1024 * 1024
_SPOOL_MEMORY_LIMIT = 8 * 1024 * 1024


class _PathNotFound(PlatformError):
    """Internal exact-path miss distinct from authorization/service failure."""


def _is_not_found(error: BaseException) -> bool:
    if isinstance(error, FileNotFoundError):
        return True
    known_names = {
        "FileNotFoundException",
        "NoSuchFileException",
        "NotFound",
        "ResourceDoesNotExist",
    }
    current: BaseException | None = error
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if type(current).__name__ in known_names:
            return True
        java_error = getattr(current, "java_exception", None)
        try:
            if java_error is not None:
                java_name = str(java_error.getClass().getSimpleName())
                if java_name in known_names:
                    return True
        except Exception:  # noqa: BLE001 - optional Py4J inspection only
            pass
        current = current.__cause__ or current.__context__
    return False


def _is_directory(item: Any) -> bool:
    value = getattr(item, "isDir", False)
    if value is not False:
        return bool(value() if callable(value) else value)
    return bool(getattr(item, "is_dir", False))


def _modification_time(item: Any) -> datetime | None:
    value = getattr(item, "modification_time", None)
    if isinstance(value, datetime):
        return value
    value = getattr(item, "modificationTime", None)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return None


class DbutilsBackend:
    """Implement platform operations in an active Databricks runtime."""

    def __init__(
        self,
        dbutils: Any | None = None,
        *,
        fs: Any | None = None,
        max_list_workers: int = DEFAULT_DBUTILS_LIST_WORKERS,
        # The serverless benchmark showed POSIX traversal is materially faster
        # for mounted UC Volumes.  dbutils remains the explicit fallback for
        # non-FUSE runtimes and the strategy used by raw cloud paths.
        volume_listing: Literal["dbutils", "posix"] = "posix",
    ) -> None:
        self._dbutils = dbutils
        self._fs = fs
        self._max_list_workers = max(1, max_list_workers)
        if volume_listing not in {"dbutils", "posix"}:
            raise ValueError("volume_listing must be 'dbutils' or 'posix'")
        self._volume_listing = volume_listing
        self._last_list_calls = 0
        self._list_call_lock = Lock()

    @property
    def dbutils(self) -> Any:
        if self._dbutils is None:
            self._dbutils = require_dbutils()
        return self._dbutils

    @property
    def fs(self) -> Any:
        if self._fs is None:
            self._fs = self.dbutils.fs
        return self._fs

    @staticmethod
    def _path(path: str) -> DatabricksPath:
        return parse_databricks_path(path, allow_cloud=True)

    @staticmethod
    def _local_path(path: DatabricksPath) -> str:
        return path.canonical_path

    def _volume_stat(self, path: DatabricksPath) -> os.stat_result:
        try:
            return os.stat(self._local_path(path), follow_symlinks=False)
        except FileNotFoundError as exc:
            raise _PathNotFound(f"Path does not exist: {path.canonical_path}") from exc
        except OSError as exc:
            raise PlatformError(
                f"Failed to inspect path: {path.canonical_path}"
            ) from exc

    def fetch_secret(self, key: str, source: str) -> str:
        if not source:
            raise PlatformError(
                "scope is required for DatabricksPlatform secret fetching. "
                "Pass it via secrets_ref as the source key."
            )
        try:
            value: str = self.dbutils.secrets.get(scope=source, key=key)
            if value:
                return value
            return self.dbutils.secrets.getBytes(scope=source, key=key).decode("utf-8")
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(
                f"Failed to fetch Databricks secret '{key}' from scope '{source}' "
                f"({type(exc).__name__})."
            ) from exc

    def read_file(self, path: str) -> str:
        parsed = self._path(path)
        if parsed.is_volume:
            try:
                with open(self._local_path(parsed), encoding="utf-8") as handle:
                    return handle.read()
            except FileNotFoundError as exc:
                raise PlatformError(f"File not found: {parsed.canonical_path}") from exc
            except (OSError, UnicodeError) as exc:
                raise PlatformError(
                    f"Cannot read file: {parsed.canonical_path}"
                ) from exc
        try:
            return self._read_remote_bytes(parsed).decode("utf-8")
        except UnicodeError as exc:
            raise PlatformError(
                f"Cannot decode Databricks file as UTF-8: {parsed.canonical_path}"
            ) from exc

    def _read_remote_bytes(self, path: DatabricksPath) -> bytes:
        descriptor, temporary_path = tempfile.mkstemp(prefix="dc_dbr_read_")
        os.close(descriptor)
        try:
            self.fs.cp(
                path.backend_path,
                f"file:{temporary_path}",
                recurse=False,
            )
            with open(temporary_path, "rb") as handle:
                return handle.read()
        except Exception as exc:
            raise PlatformError(f"Failed to read file: {path.canonical_path}") from exc
        finally:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass

    def _copy_remote_to_spool(
        self,
        path: DatabricksPath,
        spool: Any,
    ) -> None:
        descriptor, temporary_path = tempfile.mkstemp(prefix="dc_dbr_append_")
        os.close(descriptor)
        try:
            try:
                self.fs.cp(
                    path.backend_path,
                    f"file:{temporary_path}",
                    recurse=False,
                )
            except Exception as exc:
                if _is_not_found(exc):
                    raise _PathNotFound(
                        f"Path does not exist: {path.canonical_path}"
                    ) from exc
                raise PlatformError(
                    f"Failed to read file: {path.canonical_path}"
                ) from exc
            with open(temporary_path, "rb") as handle:
                shutil.copyfileobj(handle, spool, _TRANSFER_BUFFER_SIZE)
        finally:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        if not overwrite and self.file_exists(parsed.canonical_path):
            raise PlatformError(
                f"File already exists (set overwrite=True): {parsed.canonical_path}"
            )
        if parsed.is_volume:
            try:
                os.makedirs(parent_path(parsed), exist_ok=True)
                with open(self._local_path(parsed), "w", encoding="utf-8") as handle:
                    handle.write(content)
                return
            except OSError as exc:
                raise PlatformError(
                    f"Failed to write file: {parsed.canonical_path}"
                ) from exc
        try:
            self.fs.mkdirs(parent_path(parsed))
            self.fs.put(parsed.backend_path, content, overwrite=overwrite)
        except Exception as exc:
            raise PlatformError(
                f"Failed to write file: {parsed.canonical_path}"
            ) from exc

    def append_file(self, path: str, content: str) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        if parsed.is_volume:
            try:
                os.makedirs(parent_path(parsed), exist_ok=True)
                local_path = self._local_path(parsed)
                spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_LIMIT)
                try:
                    try:
                        with open(local_path, "rb") as handle:
                            shutil.copyfileobj(handle, spool, _TRANSFER_BUFFER_SIZE)
                    except FileNotFoundError:
                        pass
                    spool.write(content.encode("utf-8"))
                    spool.seek(0)
                    with open(local_path, "wb") as handle:
                        shutil.copyfileobj(spool, handle, _TRANSFER_BUFFER_SIZE)
                finally:
                    spool.close()
                return
            except OSError as exc:
                raise PlatformError(
                    f"Failed to append to file: {parsed.canonical_path}"
                ) from exc
        descriptor, temporary_path = tempfile.mkstemp(prefix="dc_dbr_append_")
        os.close(descriptor)
        spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_LIMIT)
        missing = False
        try:
            try:
                self._copy_remote_to_spool(parsed, spool)
            except _PathNotFound:
                missing = True
            spool.write(content.encode("utf-8"))
            spool.seek(0)
            with open(temporary_path, "wb") as handle:
                shutil.copyfileobj(spool, handle, _TRANSFER_BUFFER_SIZE)
            if missing:
                self.fs.mkdirs(parent_path(parsed))
            self.fs.cp(
                f"file:{temporary_path}",
                parsed.backend_path,
                recurse=False,
            )
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(
                f"Failed to append to file: {parsed.canonical_path}"
            ) from exc
        finally:
            spool.close()
            try:
                os.unlink(temporary_path)
            except OSError:
                pass

    def delete_file(self, path: str) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        if parsed.is_volume:
            try:
                os.remove(self._local_path(parsed))
            except FileNotFoundError:
                return
            except OSError as exc:
                raise PlatformError(
                    f"Failed to delete file: {parsed.canonical_path}"
                ) from exc
            return
        try:
            self.fs.rm(parsed.backend_path, recurse=False)
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise PlatformError(
                f"Failed to delete file: {parsed.canonical_path}"
            ) from exc

    def create_folder(self, path: str) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        try:
            self.fs.mkdirs(parsed.backend_path)
        except Exception as exc:
            raise PlatformError(
                f"Failed to create folder: {parsed.canonical_path}"
            ) from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        if parsed.is_volume:
            try:
                metadata = self._volume_stat(parsed)
            except _PathNotFound:
                return
            if not stat.S_ISDIR(metadata.st_mode):
                raise PlatformError(f"Failed to delete folder: {parsed.canonical_path}")
        try:
            removed = self.fs.rm(parsed.backend_path, recurse=recursive)
            if removed is False and self.folder_exists(parsed.canonical_path):
                raise PlatformError(f"Failed to delete folder: {parsed.canonical_path}")
        except PlatformError:
            raise
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise PlatformError(
                f"Failed to delete folder: {parsed.canonical_path}"
            ) from exc

    def _items(
        self,
        parsed: DatabricksPath,
        *,
        recursive: bool,
        include_item: Any | None = None,
    ) -> list[Any]:
        self._last_list_calls = 0

        def record_list_call(_directory: str) -> None:
            with self._list_call_lock:
                self._last_list_calls += 1

        if parsed.is_volume and self._volume_listing == "posix":
            try:
                return list_volume_tree(
                    self._local_path(parsed),
                    parsed.canonical_path,
                    recursive=recursive,
                    max_workers=self._max_list_workers,
                    include_item=include_item,
                    on_directory_list=record_list_call,
                )
            except FileNotFoundError:
                # Some test doubles and non-FUSE native runtimes expose Volume
                # paths only through dbutils.fs. Let the native API provide its
                # canonical missing-path/error response in that case.
                self._last_list_calls = 0
            except OSError as exc:
                raise PlatformError(
                    f"Failed to list path: {parsed.canonical_path}"
                ) from exc
        try:
            return list_tree(
                parsed.backend_path,
                lambda current: list(self.fs.ls(current)),
                _is_directory,
                lambda item: str(item.path),
                recursive=recursive,
                max_workers=self._max_list_workers,
                include_item=include_item,
                on_directory_list=record_list_call,
            )
        except Exception as exc:
            raise PlatformError(
                f"Failed to list path: {parsed.canonical_path}"
            ) from exc

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        parsed = self._path(path)
        results: list[FileInfo] = []
        try:
            items = self._items(
                parsed,
                recursive=recursive,
                include_item=lambda item: (
                    not _is_directory(item)
                    and (
                        not extension or str(item.name).rstrip("/").endswith(extension)
                    )
                ),
            )
        except PlatformError as exc:
            raise PlatformError(
                f"Failed to list files: {parsed.canonical_path}"
            ) from exc
        for item in items:
            name = str(item.name).rstrip("/")
            results.append(
                FileInfo(
                    name=name,
                    path=canonicalize_output_path(str(item.path)),
                    modification_time=_modification_time(item),
                    size=int(getattr(item, "size", 0) or 0),
                )
            )
        return results

    def list_folders(self, path: str, *, recursive: bool = False) -> list[str]:
        parsed = self._path(path)
        try:
            items = self._items(
                parsed,
                recursive=recursive,
                include_item=_is_directory,
            )
        except PlatformError as exc:
            raise PlatformError(
                f"Failed to list folders: {parsed.canonical_path}"
            ) from exc
        return [canonicalize_output_path(str(item.path)) for item in items]

    def _stat_remote(self, parsed: DatabricksPath) -> FileInfo:
        try:
            items = list(self.fs.ls(parsed.backend_path))
        except Exception as exc:
            if _is_not_found(exc):
                raise _PathNotFound(
                    f"Path does not exist: {parsed.canonical_path}"
                ) from exc
            raise PlatformError(
                f"Failed to inspect path: {parsed.canonical_path}"
            ) from exc
        requested = parsed.canonical_path.rstrip("/")
        if len(items) == 1:
            item = items[0]
            item_path = canonicalize_output_path(str(item.path))
            if item_path == requested:
                return FileInfo(
                    name=str(item.name).rstrip("/"),
                    path=item_path,
                    modification_time=_modification_time(item),
                    size=int(getattr(item, "size", 0) or 0),
                    is_dir=_is_directory(item),
                )
        return FileInfo(
            name=requested.rsplit("/", 1)[-1],
            path=requested,
            modification_time=None,
            size=0,
            is_dir=True,
        )

    def file_exists(self, path: str) -> bool:
        parsed = self._path(path)
        if parsed.is_volume:
            try:
                return stat.S_ISREG(self._volume_stat(parsed).st_mode)
            except _PathNotFound:
                return False
        try:
            return not self._stat_remote(parsed).is_dir
        except _PathNotFound:
            return False

    def folder_exists(self, path: str) -> bool:
        parsed = self._path(path)
        if parsed.is_volume:
            try:
                return stat.S_ISDIR(self._volume_stat(parsed).st_mode)
            except _PathNotFound:
                return False
        try:
            return self._stat_remote(parsed).is_dir
        except _PathNotFound:
            return False

    def upload_file(
        self, local_path: str, dest: str, *, overwrite: bool = False
    ) -> None:
        parsed = self._path(dest)
        ensure_mutable_path(parsed)
        if not os.path.isfile(local_path):
            raise PlatformError(f"Local upload source does not exist: {local_path}")
        if not overwrite and self.file_exists(parsed.canonical_path):
            raise PlatformError(
                f"Destination already exists (set overwrite=True): {parsed.canonical_path}"
            )
        try:
            if parsed.is_volume:
                os.makedirs(parent_path(parsed), exist_ok=True)
                shutil.copy2(local_path, self._local_path(parsed))
            else:
                self.fs.mkdirs(parent_path(parsed))
                self.fs.cp(f"file:{local_path}", parsed.backend_path, recurse=False)
        except Exception as exc:
            raise PlatformError(
                f"Failed to upload file: {local_path} -> {parsed.canonical_path}"
            ) from exc

    def download_file(self, src: str, dest: str) -> None:
        parsed = self._path(src)
        try:
            local_parent = os.path.dirname(dest)
            if local_parent:
                os.makedirs(local_parent, exist_ok=True)
            if parsed.is_volume:
                shutil.copy2(self._local_path(parsed), dest)
            else:
                self.fs.cp(parsed.backend_path, f"file:{dest}", recurse=False)
        except Exception as exc:
            raise PlatformError(
                f"Failed to download file: {parsed.canonical_path} -> {dest}"
            ) from exc

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        source = self._path(src)
        target = self._path(dest)
        ensure_mutable_path(target)
        if source.canonical_path == target.canonical_path:
            return
        if not overwrite and self.file_exists(target.canonical_path):
            raise PlatformError(
                f"Destination already exists (set overwrite=True): {target.canonical_path}"
            )
        try:
            if source.is_volume and target.is_volume:
                os.makedirs(parent_path(target), exist_ok=True)
                shutil.copy2(self._local_path(source), self._local_path(target))
            else:
                self.fs.mkdirs(parent_path(target))
                self.fs.cp(source.backend_path, target.backend_path, recurse=False)
        except Exception as exc:
            raise PlatformError(
                f"Failed to copy file: {source.canonical_path} -> {target.canonical_path}"
            ) from exc

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        source = self._path(src)
        target = self._path(dest)
        ensure_mutable_path(source)
        ensure_mutable_path(target)
        if source.canonical_path == target.canonical_path:
            return
        if not overwrite and self.file_exists(target.canonical_path):
            raise PlatformError(
                f"Destination already exists (set overwrite=True): {target.canonical_path}"
            )
        try:
            if (
                source.is_volume
                and target.is_volume
                and source.volume_identity == target.volume_identity
            ):
                os.makedirs(parent_path(target), exist_ok=True)
                shutil.move(self._local_path(source), self._local_path(target))
            elif source.is_volume and target.is_volume:
                self._move_across_volumes(source, target, overwrite=overwrite)
            else:
                self.fs.mkdirs(parent_path(target))
                self.fs.mv(source.backend_path, target.backend_path, recurse=False)
        except Exception as exc:
            raise PlatformError(
                f"Failed to move file: {source.canonical_path} -> {target.canonical_path}"
            ) from exc

    def _move_across_volumes(
        self,
        source: DatabricksPath,
        target: DatabricksPath,
        *,
        overwrite: bool,
    ) -> None:
        """Move between Volume filesystems while preserving overwrite rollback."""
        self.fs.mkdirs(parent_path(target))
        backup_path: str | None = None
        if overwrite and self.file_exists(target.canonical_path):
            backup_path = f"{target.canonical_path}.dc-backup-{uuid4().hex}"
            self.fs.mv(
                target.backend_path,
                backup_path,
                recurse=False,
            )
        try:
            self.fs.mv(
                source.backend_path,
                target.backend_path,
                recurse=False,
            )
        except Exception:
            if backup_path is not None:
                try:
                    self.fs.mv(backup_path, target.backend_path, recurse=False)
                except Exception as restore_error:
                    raise PlatformError(
                        "Failed to move across Databricks Volumes and restore "
                        f"the previous destination: {target.canonical_path} "
                        f"({type(restore_error).__name__})."
                    ) from restore_error
            raise
        if backup_path is not None:
            try:
                self.fs.rm(backup_path, recurse=False)
            except Exception as exc:
                raise PlatformError(
                    "Cross-Volume move succeeded but its temporary destination "
                    f"backup could not be removed: {backup_path} "
                    f"({type(exc).__name__})."
                ) from exc

    def get_file_info(self, path: str) -> FileInfo:
        parsed = self._path(path)
        if not parsed.is_volume:
            try:
                return self._stat_remote(parsed)
            except _PathNotFound as exc:
                raise PlatformError(
                    f"Path does not exist: {parsed.canonical_path}"
                ) from exc
            except PlatformError as exc:
                raise PlatformError(
                    f"Failed to get file info: {parsed.canonical_path}"
                ) from exc
        try:
            value = os.stat(self._local_path(parsed))
            is_dir = stat.S_ISDIR(value.st_mode)
            return FileInfo(
                name=parsed.canonical_path.rsplit("/", 1)[-1],
                path=parsed.canonical_path,
                modification_time=datetime.fromtimestamp(
                    value.st_mtime, tz=timezone.utc
                ),
                size=0 if is_dir else value.st_size,
                is_dir=is_dir,
            )
        except FileNotFoundError as exc:
            raise PlatformError(
                f"Path does not exist: {parsed.canonical_path}"
            ) from exc
        except OSError as exc:
            raise PlatformError(
                f"Failed to get file info: {parsed.canonical_path}"
            ) from exc

    def read_bytes(self, path: str) -> bytes:
        parsed = self._path(path)
        if not parsed.is_volume:
            return self._read_remote_bytes(parsed)
        try:
            with open(self._local_path(parsed), "rb") as handle:
                return handle.read()
        except FileNotFoundError as exc:
            raise PlatformError(f"File not found: {parsed.canonical_path}") from exc
        except OSError as exc:
            raise PlatformError(
                f"Failed to read bytes: {parsed.canonical_path}"
            ) from exc

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        if not overwrite and self.file_exists(parsed.canonical_path):
            raise PlatformError(
                f"File already exists (set overwrite=True): {parsed.canonical_path}"
            )
        if parsed.is_volume:
            try:
                os.makedirs(parent_path(parsed), exist_ok=True)
                with open(self._local_path(parsed), "wb") as handle:
                    handle.write(data)
                return
            except OSError as exc:
                raise PlatformError(
                    f"Failed to write bytes: {parsed.canonical_path}"
                ) from exc
        descriptor, temporary_path = tempfile.mkstemp(prefix="dc_dbr_write_")
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
            self.fs.mkdirs(parent_path(parsed))
            self.fs.cp(f"file:{temporary_path}", parsed.backend_path, recurse=False)
        except Exception as exc:
            raise PlatformError(
                f"Failed to write bytes: {parsed.canonical_path}"
            ) from exc
        finally:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
