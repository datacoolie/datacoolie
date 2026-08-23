"""External Databricks SDK backend for Unity Catalog Volumes."""

from __future__ import annotations

import hashlib
import io
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import Any, BinaryIO

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._databricks.paths import (
    DatabricksPath,
    canonicalize_output_path,
    ensure_mutable_path,
    parent_path,
    parse_databricks_path,
)
from datacoolie.platforms._databricks.traversal import (
    DEFAULT_SDK_DELETE_WORKERS,
    DEFAULT_SDK_LIST_WORKERS,
    list_tree,
)
from datacoolie.platforms.base import FileInfo

_TRANSFER_BUFFER_SIZE = 1024 * 1024
_SPOOL_MEMORY_LIMIT = 8 * 1024 * 1024


def _create_workspace_client() -> Any:
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise PlatformError(
            "External Databricks access requires databricks-sdk. Install "
            "datacoolie[databricks-external]."
        ) from exc
    return WorkspaceClient()


def _is_not_found(error: BaseException) -> bool:
    if error.__class__.__name__ in {"NotFound", "ResourceDoesNotExist"}:
        return True
    try:
        from databricks.sdk.errors import NotFound, ResourceDoesNotExist
    except ImportError:
        return False
    return isinstance(error, (NotFound, ResourceDoesNotExist))


def _http_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _entry_datetime(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return None


def _copy_and_hash(source: BinaryIO, destination: BinaryIO) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(_TRANSFER_BUFFER_SIZE):
        destination.write(chunk)
        digest.update(chunk)
        size += len(chunk)
    return size, digest.hexdigest()


class DatabricksSdkBackend:
    """Implement Volume operations through ``WorkspaceClient.files``."""

    def __init__(
        self,
        workspace_client: Any | None = None,
        *,
        max_list_workers: int = DEFAULT_SDK_LIST_WORKERS,
        max_delete_workers: int = DEFAULT_SDK_DELETE_WORKERS,
    ) -> None:
        self._workspace_client = workspace_client
        self._max_list_workers = max(1, max_list_workers)
        self._max_delete_workers = max(1, max_delete_workers)
        self._last_list_calls = 0
        self._list_call_lock = Lock()

    @property
    def workspace_client(self) -> Any:
        if self._workspace_client is None:
            self._workspace_client = _create_workspace_client()
        return self._workspace_client

    @property
    def files(self) -> Any:
        return self.workspace_client.files

    @staticmethod
    def _path(path: str) -> DatabricksPath:
        return parse_databricks_path(path, allow_cloud=False)

    @staticmethod
    def _failure(action: str, path: str, error: BaseException) -> PlatformError:
        return PlatformError(
            f"Databricks {action} failed for '{path}' ({type(error).__name__})."
        )

    def fetch_secret(self, key: str, source: str) -> str:
        if not source:
            raise PlatformError(
                "scope is required for DatabricksPlatform secret fetching. "
                "Pass it via secrets_ref as the source key."
            )
        try:
            value: str = self.workspace_client.dbutils.secrets.get(source, key)
            if value:
                return value
            return self.workspace_client.dbutils.secrets.getBytes(source, key).decode(
                "utf-8"
            )
        except PlatformError:
            raise
        except Exception as exc:
            raise PlatformError(
                f"Failed to fetch Databricks secret '{key}' from scope '{source}' "
                f"({type(exc).__name__})."
            ) from exc

    def _download_to_spool(
        self,
        path: DatabricksPath,
    ) -> tuple[BinaryIO, int, str]:
        spool: BinaryIO = tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_LIMIT)
        try:
            response = self.files.download(path.canonical_path)
            stream = response.contents
            if stream is None:
                raise PlatformError(
                    f"Databricks returned no content for '{path.canonical_path}'."
                )
            try:
                size, digest = _copy_and_hash(stream, spool)
            finally:
                stream.close()
            spool.seek(0)
            return spool, size, digest
        except BaseException:
            spool.close()
            raise

    def _verify(self, path: DatabricksPath, size: int, digest: str) -> None:
        downloaded, actual_size, actual_digest = self._download_to_spool(path)
        downloaded.close()
        if actual_size != size or actual_digest != digest:
            raise PlatformError(
                f"Databricks transfer verification failed for '{path.canonical_path}'."
            )

    def _ensure_parent(self, path: DatabricksPath) -> None:
        parent = parent_path(path)
        parent_parts = parent.strip("/").split("/")
        if len(parent_parts) > 4:
            self.files.create_directory(parent)

    def read_bytes(self, path: str) -> bytes:
        parsed = self._path(path)
        try:
            response = self.files.download(parsed.canonical_path)
            if response.contents is None:
                raise PlatformError(
                    f"Databricks returned no content for '{parsed.canonical_path}'."
                )
            try:
                return response.contents.read()
            finally:
                response.contents.close()
        except PlatformError:
            raise
        except Exception as exc:
            raise self._failure("read", parsed.canonical_path, exc) from exc

    def read_file(self, path: str) -> str:
        try:
            return self.read_bytes(path).decode("utf-8")
        except UnicodeError as exc:
            raise PlatformError(
                f"Cannot decode Databricks file as UTF-8: {path}"
            ) from exc

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        try:
            self._ensure_parent(parsed)
            self.files.upload(
                parsed.canonical_path,
                io.BytesIO(data),
                overwrite=overwrite,
            )
        except Exception as exc:
            raise self._failure("write", parsed.canonical_path, exc) from exc

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        self.write_bytes(path, content.encode("utf-8"), overwrite=overwrite)

    def append_file(self, path: str, content: str) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        spool: BinaryIO = tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_LIMIT)
        missing = False
        try:
            try:
                existing, _, _ = self._download_to_spool(parsed)
                try:
                    shutil.copyfileobj(existing, spool, _TRANSFER_BUFFER_SIZE)
                finally:
                    existing.close()
            except Exception as exc:
                if _is_not_found(exc):
                    missing = True
                else:
                    raise
            spool.write(content.encode("utf-8"))
            spool.seek(0)
            if missing:
                self._ensure_parent(parsed)
            self.files.upload(parsed.canonical_path, spool, overwrite=True)
        except Exception as exc:
            if isinstance(exc, PlatformError):
                raise
            raise self._failure("append", parsed.canonical_path, exc) from exc
        finally:
            spool.close()

    def delete_file(self, path: str) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        try:
            self.files.delete(parsed.canonical_path)
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise self._failure("delete", parsed.canonical_path, exc) from exc

    def create_folder(self, path: str) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        try:
            self.files.create_directory(parsed.canonical_path)
        except Exception as exc:
            raise self._failure("create directory", parsed.canonical_path, exc) from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        parsed = self._path(path)
        ensure_mutable_path(parsed)
        try:
            self.files.get_directory_metadata(parsed.canonical_path)
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise self._failure(
                "inspect directory", parsed.canonical_path, exc
            ) from exc

        try:
            if recursive:
                entries = self._items(parsed, recursive=True)
                files = [
                    str(entry.path) for entry in entries if not bool(entry.is_directory)
                ]
                directories = [
                    str(entry.path) for entry in entries if bool(entry.is_directory)
                ]
                self._delete_many(files, self._delete_file_if_present)
                for depth in sorted(
                    {directory.count("/") for directory in directories},
                    reverse=True,
                ):
                    self._delete_many(
                        [
                            directory
                            for directory in directories
                            if directory.count("/") == depth
                        ],
                        self._delete_directory_if_present,
                    )
            self._delete_directory_if_present(parsed.canonical_path)
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise self._failure("delete directory", parsed.canonical_path, exc) from exc

    def _delete_file_if_present(self, path: str) -> None:
        try:
            self.files.delete(path)
        except Exception as exc:
            if not _is_not_found(exc):
                raise

    def _delete_directory_if_present(self, path: str) -> None:
        try:
            self.files.delete_directory(path)
        except Exception as exc:
            if not _is_not_found(exc):
                raise

    def _delete_many(
        self,
        paths: list[str],
        operation: Any,
    ) -> None:
        if not paths:
            return
        if self._max_delete_workers == 1 or len(paths) == 1:
            for path in paths:
                operation(path)
            return

        futures = []
        with ThreadPoolExecutor(
            max_workers=min(self._max_delete_workers, len(paths)),
        ) as executor:
            futures = [executor.submit(operation, path) for path in paths]
            try:
                for future in as_completed(futures):
                    future.result()
            except BaseException:
                for future in futures:
                    future.cancel()
                raise

    def _items(
        self,
        path: DatabricksPath,
        *,
        recursive: bool,
        include_item: Any | None = None,
    ) -> list[Any]:
        self._last_list_calls = 0

        def record_list_call(_directory: str) -> None:
            with self._list_call_lock:
                self._last_list_calls += 1

        return list_tree(
            path.canonical_path,
            lambda current: list(self.files.list_directory_contents(current)),
            lambda item: bool(item.is_directory),
            lambda item: str(item.path),
            recursive=recursive,
            max_workers=self._max_list_workers,
            include_item=include_item,
            on_directory_list=record_list_call,
        )

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        parsed = self._path(path)
        try:
            entries = self._items(
                parsed,
                recursive=recursive,
                include_item=lambda item: (
                    not bool(item.is_directory)
                    and (not extension or str(item.name).endswith(extension))
                ),
            )
        except Exception as exc:
            raise self._failure("list files", parsed.canonical_path, exc) from exc
        results: list[FileInfo] = []
        for entry in entries:
            name = str(entry.name)
            results.append(
                FileInfo(
                    name=name,
                    path=canonicalize_output_path(str(entry.path)),
                    modification_time=_entry_datetime(entry.last_modified),
                    size=int(entry.file_size or 0),
                )
            )
        return results

    def list_folders(self, path: str, *, recursive: bool = False) -> list[str]:
        parsed = self._path(path)
        try:
            entries = self._items(
                parsed,
                recursive=recursive,
                include_item=lambda item: bool(item.is_directory),
            )
        except Exception as exc:
            raise self._failure("list folders", parsed.canonical_path, exc) from exc
        return [canonicalize_output_path(str(entry.path)) for entry in entries]

    def file_exists(self, path: str) -> bool:
        parsed = self._path(path)
        try:
            self.files.get_metadata(parsed.canonical_path)
            return True
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise self._failure("inspect file", parsed.canonical_path, exc) from exc

    def folder_exists(self, path: str) -> bool:
        parsed = self._path(path)
        try:
            self.files.get_directory_metadata(parsed.canonical_path)
            return True
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise self._failure(
                "inspect directory", parsed.canonical_path, exc
            ) from exc

    def upload_file(
        self, local_path: str, dest: str, *, overwrite: bool = False
    ) -> None:
        parsed = self._path(dest)
        ensure_mutable_path(parsed)
        if not os.path.isfile(local_path):
            raise PlatformError(f"Local upload source does not exist: {local_path}")
        try:
            self._ensure_parent(parsed)
            with open(local_path, "rb") as handle:
                self.files.upload(
                    parsed.canonical_path,
                    handle,
                    overwrite=overwrite,
                )
        except Exception as exc:
            raise self._failure("upload", parsed.canonical_path, exc) from exc

    def download_file(self, src: str, dest: str) -> None:
        parsed = self._path(src)
        try:
            response = self.files.download(parsed.canonical_path)
            if response.contents is None:
                raise PlatformError(
                    f"Databricks returned no content for '{parsed.canonical_path}'."
                )
            local_parent = os.path.dirname(dest)
            if local_parent:
                os.makedirs(local_parent, exist_ok=True)
            with open(dest, "wb") as handle:
                try:
                    shutil.copyfileobj(response.contents, handle, _TRANSFER_BUFFER_SIZE)
                finally:
                    response.contents.close()
        except PlatformError:
            raise
        except Exception as exc:
            raise self._failure("download", parsed.canonical_path, exc) from exc

    def _restore_destination(
        self,
        target: DatabricksPath,
        backup: BinaryIO | None,
        backup_size: int,
        backup_digest: str,
    ) -> None:
        if backup is None:
            try:
                self.files.delete(target.canonical_path)
            except Exception as exc:
                if not _is_not_found(exc):
                    raise
            return
        backup.seek(0)
        self.files.upload(target.canonical_path, backup, overwrite=True)
        self._verify(target, backup_size, backup_digest)

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        source = self._path(src)
        target = self._path(dest)
        ensure_mutable_path(target)
        if source.canonical_path == target.canonical_path:
            return

        destination_exists = self.file_exists(target.canonical_path)
        if destination_exists and not overwrite:
            raise PlatformError(
                f"Destination already exists (set overwrite=True): {target.canonical_path}"
            )

        source_spool: BinaryIO | None = None
        backup: BinaryIO | None = None
        backup_size = 0
        backup_digest = ""
        target_modified = False
        try:
            source_spool, source_size, source_digest = self._download_to_spool(source)
            if destination_exists:
                backup, backup_size, backup_digest = self._download_to_spool(target)
            self._ensure_parent(target)
            source_spool.seek(0)
            target_modified = True
            self.files.upload(target.canonical_path, source_spool, overwrite=overwrite)
            self._verify(target, source_size, source_digest)
        except Exception as exc:
            if target_modified:
                try:
                    self._restore_destination(
                        target,
                        backup,
                        backup_size,
                        backup_digest,
                    )
                except Exception as restore_error:
                    raise PlatformError(
                        "Databricks copy failed and the previous destination could "
                        f"not be restored: '{target.canonical_path}' "
                        f"({type(restore_error).__name__})."
                    ) from exc
            if isinstance(exc, PlatformError):
                raise
            raise self._failure("copy", source.canonical_path, exc) from exc
        finally:
            if source_spool is not None:
                source_spool.close()
            if backup is not None:
                backup.close()

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        source = self._path(src)
        target = self._path(dest)
        ensure_mutable_path(source)
        ensure_mutable_path(target)
        if source.canonical_path == target.canonical_path:
            return
        self.copy_file(
            source.canonical_path,
            target.canonical_path,
            overwrite=overwrite,
        )
        try:
            self.files.delete(source.canonical_path)
        except Exception as exc:
            raise self._failure(
                "delete move source", source.canonical_path, exc
            ) from exc

    def get_file_info(self, path: str) -> FileInfo:
        parsed = self._path(path)
        try:
            metadata = self.files.get_metadata(parsed.canonical_path)
            return FileInfo(
                name=parsed.canonical_path.rsplit("/", 1)[-1],
                path=parsed.canonical_path,
                modification_time=_http_datetime(metadata.last_modified),
                size=int(metadata.content_length or 0),
            )
        except Exception as file_error:
            if not _is_not_found(file_error):
                raise self._failure(
                    "get metadata", parsed.canonical_path, file_error
                ) from file_error
        try:
            self.files.get_directory_metadata(parsed.canonical_path)
            return FileInfo(
                name=parsed.canonical_path.rsplit("/", 1)[-1],
                path=parsed.canonical_path,
                modification_time=None,
                size=0,
                is_dir=True,
            )
        except Exception as directory_error:
            raise self._failure(
                "get metadata", parsed.canonical_path, directory_error
            ) from directory_error
