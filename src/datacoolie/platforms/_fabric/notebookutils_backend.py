"""Native Microsoft Fabric backend powered by NotebookUtils."""

from __future__ import annotations

from datetime import datetime, timezone
import os
import tempfile
from typing import Any
from urllib.parse import urlsplit

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._fabric.paths import ensure_mutable_path, parse_azure_datalake_path
from datacoolie.platforms._fabric.traversal import list_notebookutils_tree
from datacoolie.platforms.base import FileInfo


def _is_not_found(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    if status == 404:
        return True
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    return "notfound" in name or "not found" in message or "does not exist" in message


class NotebookUtilsBackend:
    """Implement the Fabric platform contract with an injected NotebookUtils handle."""

    def __init__(self, notebookutils: Any | None, *, fs: Any | None = None) -> None:
        if notebookutils is None and fs is None:
            raise PlatformError("NotebookUtils or its filesystem handle is required.")
        self.notebookutils = notebookutils
        self.fs = fs if fs is not None else notebookutils.fs

    @staticmethod
    def _protect_qualified_mutation(path: str) -> None:
        try:
            parsed = urlsplit(path)
            host = (parsed.hostname or "").lower()
        except ValueError as exc:
            raise PlatformError("Malformed qualified storage path.") from exc
        if parsed.scheme.lower() in {"abfs", "abfss", "https"} and (
            host.endswith(".dfs.fabric.microsoft.com")
            or host.endswith(".dfs.core.windows.net")
        ):
            ensure_mutable_path(parse_azure_datalake_path(path))

    def fetch_secret(self, key: str, source: str) -> str:
        if not source:
            raise PlatformError(
                "vault_url is required for FabricPlatform secret fetching. "
                "Pass it via secrets_ref as the source key."
        )
        try:
            if self.notebookutils is None:
                raise PlatformError("NotebookUtils credentials are unavailable.")
            value: str = self.notebookutils.credentials.getSecret(source, key)
            return value
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(
                f"Failed to fetch secret '{key}' from the configured Fabric vault."
            ) from exc

    def read_file(self, path: str) -> str:
        fd, temp_path = tempfile.mkstemp(prefix="dc_rf_", suffix=".tmp")
        os.close(fd)
        try:
            self.download_file(path, temp_path)
            with open(temp_path, encoding="utf-8") as handle:
                return handle.read()
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to read file: {path}") from exc
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        self._protect_qualified_mutation(path)
        if not overwrite and self.file_exists(path):
            raise PlatformError(f"File already exists (set overwrite=True): {path}")
        try:
            self.fs.put(path, content, overwrite=overwrite)
        except Exception as exc:
            raise PlatformError(f"Failed to write file: {path}") from exc

    def append_file(self, path: str, content: str) -> None:
        self._protect_qualified_mutation(path)
        try:
            self.fs.append(path, content, createFileIfNotExists=True)
        except Exception as exc:
            raise PlatformError(f"Failed to append to file: {path}") from exc

    def delete_file(self, path: str) -> None:
        self._protect_qualified_mutation(path)
        try:
            self.fs.rm(path, recurse=False)
        except Exception as exc:
            if not _is_not_found(exc):
                raise PlatformError(f"Failed to delete file: {path}") from exc

    def create_folder(self, path: str) -> None:
        self._protect_qualified_mutation(path)
        try:
            self.fs.mkdirs(path)
        except Exception as exc:
            raise PlatformError(f"Failed to create folder: {path}") from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        self._protect_qualified_mutation(path)
        try:
            self.fs.rm(path, recurse=recursive)
        except Exception as exc:
            if not _is_not_found(exc):
                raise PlatformError(f"Failed to delete folder: {path}") from exc

    @staticmethod
    def _file_info(item: Any) -> FileInfo:
        milliseconds = getattr(item, "modifyTime", None)
        modified = (
            datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
            if isinstance(milliseconds, (int, float))
            else None
        )
        return FileInfo(
            name=item.name,
            path=item.path,
            modification_time=modified,
            size=item.size,
            is_dir=item.isDir,
        )

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        try:
            items = list_notebookutils_tree(self.fs, path, recursive=recursive)
        except Exception as exc:
            raise PlatformError(f"Failed to list files: {path}") from exc

        results: list[FileInfo] = []
        for item in items:
            if item.isDir:
                continue
            if extension and not item.name.endswith(extension):
                continue
            results.append(self._file_info(item))
        return results

    def list_folders(self, path: str, *, recursive: bool = False) -> list[str]:
        try:
            items = list_notebookutils_tree(self.fs, path, recursive=recursive)
        except Exception as exc:
            raise PlatformError(f"Failed to list folders: {path}") from exc

        results: list[str] = []
        for item in items:
            if not item.isDir:
                continue
            results.append(item.path)
        return results

    def file_exists(self, path: str) -> bool:
        try:
            return bool(self.fs.exists(path))
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise PlatformError(f"Failed to check file existence: {path}") from exc

    def folder_exists(self, path: str) -> bool:
        try:
            return bool(self.fs.exists(path))
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise PlatformError(f"Failed to check folder existence: {path}") from exc

    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
        self._protect_qualified_mutation(dest)
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        try:
            self.fs.cp(f"file://{local_path}", dest)
        except Exception as exc:
            raise PlatformError(f"Failed to upload file: {local_path} → {dest}") from exc

    def download_file(self, src: str, dest: str) -> None:
        try:
            self.fs.cp(src, f"file://{dest}")
        except Exception as exc:
            raise PlatformError(f"Failed to download file: {src} → {dest}") from exc

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        self._protect_qualified_mutation(dest)
        if src.rstrip("/") == dest.rstrip("/"):
            return
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        try:
            self.fs.cp(src, dest, recurse=False)
        except Exception as exc:
            raise PlatformError(f"Failed to copy file: {src} → {dest}") from exc

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        self._protect_qualified_mutation(src)
        self._protect_qualified_mutation(dest)
        if src.rstrip("/") == dest.rstrip("/"):
            return
        if not overwrite and self.file_exists(dest):
            raise PlatformError(f"Destination already exists (set overwrite=True): {dest}")
        try:
            self.fs.mv(src, dest, create_path=True, overwrite=overwrite)
        except Exception as exc:
            raise PlatformError(f"Failed to move file: {src} → {dest}") from exc

    def get_file_info(self, path: str) -> FileInfo:
        try:
            items = self.fs.ls(path)
            if not items:
                raise PlatformError(f"Path does not exist: {path}")
            return self._file_info(items[0])
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to get file info: {path}") from exc

    def read_bytes(self, path: str) -> bytes:
        fd, temp_path = tempfile.mkstemp(prefix="dc_rb_")
        os.close(fd)
        try:
            self.download_file(path, temp_path)
            with open(temp_path, "rb") as handle:
                return handle.read()
        except PlatformError:
            raise
        except OSError as exc:
            raise PlatformError(f"Failed to read bytes: {path}") from exc
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        self._protect_qualified_mutation(path)
        if not overwrite and self.file_exists(path):
            raise PlatformError(f"File already exists (set overwrite=True): {path}")
        fd, temp_path = tempfile.mkstemp(prefix="dc_wb_")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            self.upload_file(temp_path, path, overwrite=overwrite)
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(f"Failed to write bytes: {path}") from exc
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
