"""Microsoft Fabric platform implementation.

Uses ``notebookutils`` for all operations.  The module is lazily imported
so the class can be *defined* outside a Fabric notebook without error
(e.g. for registry registration).  All ``notebookutils`` services are
accessible via the :attr:`notebookutils` property (``fs``, ``credentials``,
``mssparkutils``, etc.).
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
import tempfile
from typing import Any

from datacoolie.core.exceptions import PlatformError
from datacoolie.logging.base import get_logger
from datacoolie.platforms.base import BasePlatform, FileInfo

logger = get_logger(__name__)


class FabricPlatform(BasePlatform):
    """Platform backed by Microsoft Fabric ``notebookutils``.

    Also implements :meth:`_fetch_secret` via ``notebookutils.credentials``,
    so it can serve as its own secret provider inside Fabric notebooks.

    The full ``notebookutils`` module is available via the
    :attr:`notebookutils` property, giving access to all services:
    ``fs``, ``credentials``, ``mssparkutils``, and more.

    Requires execution in a Fabric notebook environment where
    ``notebookutils`` is pre-installed.

    Args:
        cache_ttl: Secret cache time-to-live in seconds (default 300).
            Pass ``0`` to disable caching.
    """

    def __init__(
        self,
        cache_ttl: int = 300,
        **kwargs: Any,
    ) -> None:
        super().__init__(cache_ttl=cache_ttl)
        self._nu: Any | None = None
        self._fs: Any | None = None

    # ------------------------------------------------------------------
    # SDK access
    # ------------------------------------------------------------------

    @property
    def notebookutils(self) -> Any:
        """Return the ``notebookutils`` module, importing lazily on first use.

        Provides access to all notebookutils services: ``fs``,
        ``credentials``, ``mssparkutils``, and more.
        """
        if self._nu is None:
            try:
                import notebookutils as nu  # type: ignore[import-untyped]

                self._nu = nu
            except ImportError as exc:
                raise PlatformError(
                    "notebookutils is not available. "
                    "FabricPlatform requires a Microsoft Fabric notebook environment."
                ) from exc
        return self._nu

    @property
    def fs(self) -> Any:
        """Return ``notebookutils.fs``, resolving lazily on first use."""
        if self._fs is None:
            self._fs = self.notebookutils.fs
        return self._fs

    # ------------------------------------------------------------------
    # Secrets
    # ------------------------------------------------------------------

    def _fetch_secret(self, key: str, source: str) -> str:
        """Fetch *key* from Azure Key Vault via ``notebookutils.credentials``.

        Args:
            key: Secret name in the vault.
            source: Azure Key Vault URL
                (e.g. ``"https://myvault.vault.azure.net/"``).

        Raises:
            PlatformError: If *source* (vault URL) is empty or the call fails.
        """
        if not source:
            raise PlatformError(
                "vault_url is required for FabricPlatform secret fetching. "
                "Pass it via secrets_ref as the source key, e.g. "
                '{"https://myvault.vault.azure.net/": ["key_1", "key_2"]}.'
            )
        try:
            return self.notebookutils.credentials.getSecret(source, key)
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(
                f"Failed to fetch secret '{key}' from Fabric vault '{source}': {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def read_file(self, path: str) -> str:
        """Read a remote file by downloading it to a temp file and reading locally.

        Using :meth:`download_file` (``notebookutils.fs.cp``) guarantees the
        full file content is read regardless of size, unlike ``fs.head`` which
        may truncate large files even with a high ``maxBytes`` limit.
        """
        tmp_fd, tmp_path = tempfile.mkstemp(prefix="dc_rf_", suffix=".tmp")
        os.close(tmp_fd)
        try:
            self.download_file(path, tmp_path)
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
        try:
            self.fs.put(path, content, overwrite=overwrite)
        except Exception as exc:
            raise PlatformError(f"Failed to write file: {path}") from exc

    def append_file(self, path: str, content: str) -> None:
        try:
            self.fs.append(path, content, createFileIfNotExists=True)
        except Exception as exc:
            raise PlatformError(f"Failed to append to file: {path}") from exc

    def delete_file(self, path: str) -> None:
        try:
            self.fs.rm(path, recurse=False)
        except Exception:  # noqa: BLE001
            pass  # idempotent

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def create_folder(self, path: str) -> None:
        try:
            self.fs.mkdirs(path)
        except Exception as exc:
            raise PlatformError(f"Failed to create folder: {path}") from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
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
        try:
            items = self.fs.ls(path)
        except Exception as exc:
            raise PlatformError(f"Failed to list files: {path}") from exc

        results: list[FileInfo] = []
        for item in items:
            if item.isDir:
                if recursive:
                    results.extend(
                        self.list_files(item.path, recursive=True, extension=extension)
                    )
                continue
            if extension and not item.name.endswith(extension):
                continue
            ms = getattr(item, "modifyTime", None)
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
        try:
            items = self.fs.ls(path)
        except Exception as exc:
            raise PlatformError(f"Failed to list folders: {path}") from exc

        results: list[str] = []
        for item in items:
            if item.isDir:
                results.append(item.path)
                if recursive:
                    results.extend(self.list_folders(item.path, recursive=True))
        return results

    # ------------------------------------------------------------------
    # Existence checks
    # ------------------------------------------------------------------

    def file_exists(self, path: str) -> bool:
        try:
            return self.fs.exists(path)
        except Exception:  # noqa: BLE001
            return False

    def folder_exists(self, path: str) -> bool:
        try:
            return self.fs.exists(path)
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    # File management
    # ------------------------------------------------------------------

    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        try:
            self.fs.cp(f"file://{local_path}", dest)
        except Exception as exc:
            raise PlatformError(f"Failed to upload file: {local_path} → {dest}") from exc

    def download_file(self, src: str, dest: str) -> None:
        """Download a file from Fabric storage to the local filesystem.

        Uses ``notebookutils.fs.cp`` to copy *src* to the local path *dest*.

        Args:
            src: Source path on Fabric storage
                (e.g. ``"abfss://container@account.dfs.core.windows.net/path"``).
            dest: Absolute local filesystem path to write to.

        Raises:
            PlatformError: On download failure.
        """
        try:
            self.fs.cp(src, f"file://{dest}")
        except Exception as exc:
            raise PlatformError(f"Failed to download file: {src} → {dest}") from exc

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        try:
            self.fs.cp(src, dest, recurse=False)
        except Exception as exc:
            raise PlatformError(f"Failed to copy file: {src} → {dest}") from exc

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        try:
            self.fs.mv(src, dest, overwrite=overwrite)
        except Exception as exc:
            raise PlatformError(f"Failed to move file: {src} → {dest}") from exc

    def get_file_info(self, path: str) -> FileInfo:
        try:
            items = self.fs.ls(path)
            if not items:
                raise PlatformError(f"Path does not exist: {path}")
            item = items[0]
            ms = getattr(item, "modifyTime", None)
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
                is_dir=item.isDir,
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

        Downloads *path* to a temp file via ``notebookutils.fs.cp``
        (``download_file``) then reads it back — the only reliable
        unbounded path on Fabric (``fs.head`` truncates large files).
        """
        fd, tmp = tempfile.mkstemp(prefix="dc_rb_")
        os.close(fd)
        try:
            self.download_file(path, tmp)
            with open(tmp, "rb") as fh:
                return fh.read()
        except PlatformError:
            raise
        except OSError as exc:
            raise PlatformError(f"Failed to read bytes: {path}") from exc
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        """Write raw ``bytes`` to *path*.

        Writes *data* to a local temp file then pushes it via
        ``notebookutils.fs.cp`` (``upload_file``).
        """
        if not overwrite and self.file_exists(path):
            raise PlatformError(f"File already exists (set overwrite=True): {path}")
        fd, tmp = tempfile.mkstemp(prefix="dc_wb_")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            self.upload_file(tmp, path, overwrite=overwrite)
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to write bytes: {path}") from exc
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
