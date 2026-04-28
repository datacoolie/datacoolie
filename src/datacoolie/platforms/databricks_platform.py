"""Databricks platform implementation.

Primary backend: **Unity Catalog Volumes** (paths starting with ``/Volumes/``).
These are accessed via standard Python I/O (``os``, ``open``, ``shutil``),
which is the Databricks-recommended approach for UC Volume paths — lower
latency, standard semantics, no driver-side serialisation overhead.

Fallback backend: **``dbutils.fs``** for all other path schemes (``dbfs:/``,
``abfss://``, ``wasbs://``, ``s3://``, legacy DBFS mounts, etc.).

Secrets always use ``dbutils.secrets`` (no alternative exists on Databricks).
"""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
from datetime import datetime, timezone
from typing import Any

from datacoolie.core.exceptions import PlatformError
from datacoolie.logging.base import get_logger
from datacoolie.platforms.base import BasePlatform, FileInfo

logger = get_logger(__name__)

# Paths under this prefix are Unity Catalog Volume paths and are accessible
# directly via standard OS I/O on any Databricks cluster.
_VOLUME_PREFIX = "/Volumes/"


class DatabricksPlatform(BasePlatform):
    """Platform backed by Databricks.

    Routes file operations based on path type:

    * **Unity Catalog Volumes** (``/Volumes/...``) — uses ``os``, ``open``,
      ``shutil``.  This is the Databricks-recommended approach for UC Volumes.
    * **Other paths** (``dbfs:/``, ``abfss://``, ``wasbs://``, ``s3://``,
      legacy DBFS mounts) — falls back to ``dbutils.fs``.

    Secrets always use ``dbutils.secrets``.

    The full ``dbutils`` handle is available via the :attr:`dbutils` property,
    giving access to all services: ``secrets``, ``widgets``, ``notebook``,
    ``fs``, and more.

    Args:
        dbutils: Pre-existing ``dbutils`` handle.  If ``None``, the
            platform resolves it lazily from the Spark session or IPython.
        cache_ttl: Secret cache time-to-live in seconds (default 300).
            Pass ``0`` to disable caching.
    """

    def __init__(
        self,
        dbutils: Any | None = None,
        cache_ttl: int = 300,
        **kwargs: Any,
    ) -> None:
        super().__init__(cache_ttl=cache_ttl)
        self._dbutils: Any | None = dbutils
        self._fs: Any | None = None

    # ------------------------------------------------------------------
    # SDK access
    # ------------------------------------------------------------------

    @property
    def dbutils(self) -> Any:
        """Return the full ``dbutils`` handle, resolving lazily on first use."""
        if self._dbutils is None:
            self._dbutils = self._resolve_dbutils()
        return self._dbutils

    @property
    def fs(self) -> Any:
        """Return ``dbutils.fs``, resolving lazily on first use."""
        if self._fs is None:
            self._fs = self.dbutils.fs
        return self._fs

    # ------------------------------------------------------------------
    # Path routing
    # ------------------------------------------------------------------

    @staticmethod
    def _is_volume(path: str) -> bool:
        """Return ``True`` if *path* is a Unity Catalog Volume path.

        Volume paths start with ``/Volumes/`` and are accessible directly
        via standard Python I/O on any Databricks cluster.
        """
        return path.startswith(_VOLUME_PREFIX)

    # ------------------------------------------------------------------
    # Secrets — always via dbutils.secrets
    # ------------------------------------------------------------------

    def _fetch_secret(self, key: str, source: str) -> str:
        """Fetch *key* from Databricks Secrets via ``dbutils.secrets``.

        Args:
            key: Secret key name within the scope.
            source: Databricks secret scope name (e.g. ``"my-scope"``).

        Raises:
            PlatformError: If *source* (scope) is empty or the call fails.
        """
        if not source:
            raise PlatformError(
                "scope is required for DatabricksPlatform secret fetching. "
                "Pass it via secrets_ref as the source key, e.g. "
                '{"my-scope": ["key_1", "key_2"]}.'
            )
        try:
            dbutils = self._dbutils if self._dbutils is not None else self._resolve_dbutils()
            result: str = dbutils.secrets.get(scope=source, key=key)
            if result:
                return result
            # Fall back to getBytes for binary-encoded secrets
            return dbutils.secrets.getBytes(scope=source, key=key).decode("utf-8")
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(
                f"Failed to fetch secret '{key}' from Databricks scope '{source}': {exc}"
            ) from exc

    @staticmethod
    def _resolve_dbutils() -> Any:
        """Resolve a full ``dbutils`` handle (not just ``dbutils.fs``).

        Resolution order:

        1. **IPython globals** — In Databricks notebooks ``dbutils`` is
           pre-injected into the kernel namespace (same as ``spark``).
           Checking here first is cheap and covers the most common case.
        2. **SparkSession → DBUtils** — Fallback for job clusters and
           non-notebook contexts where the pre-injected variable is absent.
        """
        # Attempt 1: pre-injected dbutils in Databricks notebook kernel
        try:
            from IPython import get_ipython  # type: ignore[import-untyped]

            ip = get_ipython()
            if ip is not None:
                dbutils = ip.user_ns.get("dbutils")
                if dbutils is not None:
                    return dbutils
        except Exception:  # noqa: BLE001
            pass

        # Attempt 2: construct from active SparkSession (job clusters, tests)
        try:
            from pyspark.sql import SparkSession  # type: ignore[import-untyped]
            from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]

            spark = SparkSession.getActiveSession()
            if spark is not None:
                return DBUtils(spark)
        except Exception:  # noqa: BLE001
            pass

        raise PlatformError(
            "Cannot resolve dbutils. "
            "DatabricksPlatform requires a Databricks notebook/cluster environment."
        )

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def read_file(self, path: str) -> str:
        """Read a text file.

        Volume path → ``open()`` directly.
        Other paths → ``dbutils.fs.cp`` to a temp file, then read locally.
        Using ``fs.cp`` (rather than ``fs.head``) guarantees the full content
        is returned regardless of file size.
        """
        if self._is_volume(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    return fh.read()
            except FileNotFoundError as exc:
                raise PlatformError(f"File not found: {path}") from exc
            except OSError as exc:
                raise PlatformError(f"Cannot read file: {path}") from exc
        # Fallback: materialise via dbutils.fs to a temp file
        tmp_fd, tmp_path = tempfile.mkstemp(prefix="dc_rf_", suffix=".tmp")
        os.close(tmp_fd)
        try:
            self.fs.cp(path, f"file:{tmp_path}", recurse=False)
            with open(tmp_path, encoding="utf-8") as fh:
                return fh.read()
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to read file: {path}") from exc
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        if not overwrite and self.file_exists(path):
            raise PlatformError(f"File already exists (set overwrite=True): {path}")
        if self._is_volume(path):
            try:
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(content)
            except OSError as exc:
                raise PlatformError(f"Failed to write file: {path}") from exc
        else:
            try:
                self.fs.put(path, content, overwrite=overwrite)
            except Exception as exc:
                raise PlatformError(f"Failed to write file: {path}") from exc

    def append_file(self, path: str, content: str) -> None:
        if self._is_volume(path):
            try:
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "a", encoding="utf-8") as fh:
                    fh.write(content)
            except OSError as exc:
                raise PlatformError(f"Failed to append to file: {path}") from exc
        else:
            try:
                existing = self.read_file(path) if self.file_exists(path) else ""
                self.fs.put(path, existing + content, overwrite=True)
            except PlatformError:
                raise
            except Exception as exc:
                raise PlatformError(f"Failed to append to file: {path}") from exc

    def delete_file(self, path: str) -> None:
        if self._is_volume(path):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass  # idempotent
            except OSError as exc:
                raise PlatformError(f"Failed to delete file: {path}") from exc
        else:
            try:
                self.fs.rm(path, recurse=False)
            except Exception:  # noqa: BLE001
                pass  # idempotent

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def create_folder(self, path: str) -> None:
        if self._is_volume(path):
            try:
                os.makedirs(path, exist_ok=True)
            except OSError as exc:
                raise PlatformError(f"Failed to create folder: {path}") from exc
        else:
            try:
                self.fs.mkdirs(path)
            except Exception as exc:
                raise PlatformError(f"Failed to create folder: {path}") from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        if self._is_volume(path):
            try:
                if recursive:
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    os.rmdir(path)
            except OSError as exc:
                raise PlatformError(f"Failed to delete folder: {path}") from exc
        else:
            try:
                self.fs.rm(path, recurse=recursive)
            except Exception as exc:
                raise PlatformError(f"Failed to delete folder: {path}") from exc

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        if self._is_volume(path):
            return self._list_files_volume(path, recursive=recursive, extension=extension)
        return self._list_files_dbutils(path, recursive=recursive, extension=extension)

    def _list_files_volume(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        results: list[FileInfo] = []
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        if recursive:
                            results.extend(
                                self._list_files_volume(
                                    entry.path, recursive=True, extension=extension
                                )
                            )
                        continue
                    if extension and not entry.name.endswith(extension):
                        continue
                    try:
                        st = entry.stat()
                        mod_time = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
                        size = st.st_size
                    except OSError:
                        mod_time = None
                        size = 0
                    results.append(
                        FileInfo(
                            name=entry.name,
                            path=entry.path,
                            modification_time=mod_time,
                            size=size,
                        )
                    )
        except OSError as exc:
            raise PlatformError(f"Failed to list files: {path}") from exc
        return results

    def _list_files_dbutils(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        try:
            items = self.fs.ls(path)
        except Exception as exc:
            raise PlatformError(f"Failed to list files: {path}") from exc

        results: list[FileInfo] = []
        for item in items:
            if item.isDir():
                if recursive:
                    results.extend(
                        self._list_files_dbutils(item.path, recursive=True, extension=extension)
                    )
                continue
            if extension and not item.name.endswith(extension):
                continue
            ms = getattr(item, "modificationTime", None)
            mod_time = (
                datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
                if isinstance(ms, (int, float))
                else None
            )
            results.append(
                FileInfo(
                    name=item.name,
                    path=item.path,
                    modification_time=mod_time,
                    size=item.size,
                )
            )
        return results

    def list_folders(
        self,
        path: str,
        *,
        recursive: bool = False,
    ) -> list[str]:
        if self._is_volume(path):
            return self._list_folders_volume(path, recursive=recursive)
        return self._list_folders_dbutils(path, recursive=recursive)

    def _list_folders_volume(self, path: str, *, recursive: bool = False) -> list[str]:
        results: list[str] = []
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        results.append(entry.path)
                        if recursive:
                            results.extend(
                                self._list_folders_volume(entry.path, recursive=True)
                            )
        except OSError as exc:
            raise PlatformError(f"Failed to list folders: {path}") from exc
        return results

    def _list_folders_dbutils(self, path: str, *, recursive: bool = False) -> list[str]:
        try:
            items = self.fs.ls(path)
        except Exception as exc:
            raise PlatformError(f"Failed to list folders: {path}") from exc

        results: list[str] = []
        for item in items:
            if item.isDir():
                results.append(item.path)
                if recursive:
                    results.extend(self._list_folders_dbutils(item.path, recursive=True))
        return results

    # ------------------------------------------------------------------
    # Existence checks
    # ------------------------------------------------------------------

    def file_exists(self, path: str) -> bool:
        if self._is_volume(path):
            return os.path.isfile(path)
        try:
            items = self.fs.ls(path)
            return len(items) > 0
        except Exception:  # noqa: BLE001
            return False

    def folder_exists(self, path: str) -> bool:
        if self._is_volume(path):
            return os.path.isdir(path)
        try:
            self.fs.ls(path)
            return True
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    # File management
    # ------------------------------------------------------------------

    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        if self._is_volume(dest):
            try:
                parent = os.path.dirname(dest)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                shutil.copy2(local_path, dest)
            except OSError as exc:
                raise PlatformError(f"Failed to upload file: {local_path} → {dest}") from exc
        else:
            try:
                self.fs.cp(f"file:{local_path}", dest)
            except Exception as exc:
                raise PlatformError(f"Failed to upload file: {local_path} → {dest}") from exc

    def download_file(self, src: str, dest: str) -> None:
        """Download a file to the local filesystem.

        Volume path → ``shutil.copy2`` (direct OS copy).
        Other paths → ``dbutils.fs.cp`` with ``file:`` prefix on *dest*.

        Args:
            src: Source path (Unity Catalog Volume or DBFS/ADLS URI).
            dest: Absolute local OS filesystem path to write to.

        Raises:
            PlatformError: On download failure.
        """
        if self._is_volume(src):
            try:
                shutil.copy2(src, dest)
            except OSError as exc:
                raise PlatformError(f"Failed to download file: {src} → {dest}") from exc
        else:
            try:
                self.fs.cp(src, f"file:{dest}", recurse=False)
            except Exception as exc:
                raise PlatformError(f"Failed to download file: {src} → {dest}") from exc

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        if self._is_volume(src) and self._is_volume(dest):
            # Both are Volume paths: use stdlib directly.
            try:
                parent = os.path.dirname(dest)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                shutil.copy2(src, dest)
            except OSError as exc:
                raise PlatformError(f"Failed to copy file: {src} → {dest}") from exc
        else:
            # Mixed-scheme (one Volume, one non-Volume) or both non-Volume:
            # dbutils.fs handles all path scheme combinations uniformly.
            try:
                self.fs.cp(src, dest, recurse=False)
            except Exception as exc:
                raise PlatformError(f"Failed to copy file: {src} → {dest}") from exc

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        if self._is_volume(src) and self._is_volume(dest):
            # Both are Volume paths: use stdlib directly.
            try:
                parent = os.path.dirname(dest)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                shutil.move(src, dest)
            except OSError as exc:
                raise PlatformError(f"Failed to move file: {src} → {dest}") from exc
        else:
            # Mixed-scheme or both non-Volume: delegate to dbutils.fs.
            try:
                self.fs.mv(src, dest, recurse=False)
            except Exception as exc:
                raise PlatformError(f"Failed to move file: {src} → {dest}") from exc

    def get_file_info(self, path: str) -> FileInfo:
        if self._is_volume(path):
            try:
                st = os.stat(path)
                is_dir = stat.S_ISDIR(st.st_mode)
                return FileInfo(
                    name=path.rstrip("/").rsplit("/", 1)[-1],
                    path=path,
                    modification_time=datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
                    size=st.st_size,
                    is_dir=is_dir,
                )
            except FileNotFoundError as exc:
                raise PlatformError(f"Path does not exist: {path}") from exc
            except OSError as exc:
                raise PlatformError(f"Failed to get file info: {path}") from exc
        # Fallback: dbutils.fs.ls
        try:
            items = self.fs.ls(path)
            if not items:
                raise PlatformError(f"Path does not exist: {path}")
            item = items[0]
            ms = getattr(item, "modificationTime", None)
            mod_time = (
                datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
                if isinstance(ms, (int, float))
                else None
            )
            return FileInfo(
                name=item.name,
                path=item.path,
                modification_time=mod_time,
                size=item.size,
                is_dir=item.isDir(),
            )
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to get file info: {path}") from exc

    # ------------------------------------------------------------------
    # Binary I/O
    # ------------------------------------------------------------------

    def read_bytes(self, path: str) -> bytes:
        """Return the entire file contents as ``bytes``.

        Volume path → ``open(path, "rb").read()`` (zero-copy, no temp file).
        Other paths → ``dbutils.fs.cp`` to a temp file then read back.
        """
        if self._is_volume(path):
            try:
                with open(path, "rb") as fh:
                    return fh.read()
            except FileNotFoundError as exc:
                raise PlatformError(f"File not found: {path}") from exc
            except OSError as exc:
                raise PlatformError(f"Failed to read bytes: {path}") from exc
        fd, tmp = tempfile.mkstemp(prefix="dc_rb_")
        os.close(fd)
        try:
            self.fs.cp(path, f"file:{tmp}", recurse=False)
            with open(tmp, "rb") as fh:
                return fh.read()
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to read bytes: {path}") from exc
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        """Write raw ``bytes`` to *path*.

        Volume path → ``makedirs`` + ``open(path, "wb").write(data)`` (zero-copy).
        Other paths → write to a temp file then push via ``dbutils.fs.cp``.
        Using ``fs.put`` is deliberately avoided: it only accepts strings
        and would corrupt arbitrary binary data.
        """
        if not overwrite and self.file_exists(path):
            raise PlatformError(f"File already exists (set overwrite=True): {path}")
        if self._is_volume(path):
            try:
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "wb") as fh:
                    fh.write(data)
            except OSError as exc:
                raise PlatformError(f"Failed to write bytes: {path}") from exc
        else:
            fd, tmp = tempfile.mkstemp(prefix="dc_wb_")
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(data)
                self.fs.cp(f"file:{tmp}", path)
            except Exception as exc:
                raise PlatformError(f"Failed to write bytes: {path}") from exc
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
