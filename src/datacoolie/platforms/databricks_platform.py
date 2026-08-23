"""Portable Databricks platform facade.

Native Databricks runtimes prefer ``dbutils`` and direct POSIX content I/O for
Unity Catalog Volumes. Other Python runtimes use ``WorkspaceClient.files``
with Databricks unified authentication.
"""

from __future__ import annotations

from typing import Any

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._databricks.dbutils_backend import DbutilsBackend
from datacoolie.platforms._databricks.runtime import (
    DatabricksRuntime,
    EffectiveDatabricksRuntime,
    require_dbutils,
    resolve_runtime,
    validate_runtime,
)
from datacoolie.platforms._databricks.sdk_backend import DatabricksSdkBackend
from datacoolie.platforms.base import BasePlatform, FileInfo

_Backend = DbutilsBackend | DatabricksSdkBackend


class DatabricksPlatform(BasePlatform):
    """Access Databricks storage through the best backend for the runtime.

    Args:
        dbutils: Optional native Databricks utilities handle.
        cache_ttl: Secret cache time-to-live in seconds. Pass ``0`` to disable.
        runtime: ``"auto"`` prefers native Databricks, ``"databricks"``
            requires it, and ``"external"`` always uses the Databricks SDK.
        workspace_client: Optional injected ``WorkspaceClient`` for external
            execution. When omitted, unified authentication is used lazily.
    """

    def __init__(
        self,
        dbutils: Any | None = None,
        cache_ttl: int = 300,
        *,
        runtime: DatabricksRuntime = "auto",
        workspace_client: Any | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(cache_ttl=cache_ttl)
        self._runtime = validate_runtime(runtime)
        self._effective_runtime: EffectiveDatabricksRuntime | None = None
        self._workspace_client = workspace_client
        self._backend: _Backend | None = None

        # Retained as native compatibility surfaces and test injection points.
        self._dbutils = dbutils
        self._fs: Any | None = None

    def _resolve_effective_runtime(self) -> EffectiveDatabricksRuntime:
        if self._effective_runtime is None:
            if self._runtime == "auto" and self._fs is not None:
                self._effective_runtime = "databricks"
            else:
                effective, discovered = resolve_runtime(
                    self._runtime,
                    injected_dbutils=self._dbutils,
                )
                self._effective_runtime = effective
                if discovered is not None:
                    self._dbutils = discovered
        return self._effective_runtime

    def _get_backend(self) -> _Backend:
        if self._backend is not None:
            return self._backend
        if self._resolve_effective_runtime() == "external":
            self._backend = DatabricksSdkBackend(self._workspace_client)
            return self._backend
        self._backend = DbutilsBackend(self._dbutils, fs=self._fs)
        if self._fs is None:
            self._dbutils = self._backend.dbutils
        self._fs = self._backend.fs
        return self._backend

    @staticmethod
    def _resolve_dbutils() -> Any:
        """Compatibility wrapper for callers that explicitly resolve dbutils."""
        return require_dbutils()

    @staticmethod
    def _is_volume(path: str) -> bool:
        """Return whether a path uses the portable Unity Catalog namespace."""
        return path.startswith(("/Volumes/", "dbfs:/Volumes/"))

    @property
    def dbutils(self) -> Any:
        """Return native ``dbutils``; unavailable in external mode."""
        backend = self._get_backend()
        if not isinstance(backend, DbutilsBackend):
            raise PlatformError(
                "dbutils is available only when DatabricksPlatform uses "
                "runtime='databricks'."
            )
        self._dbutils = backend.dbutils
        return self._dbutils

    @property
    def fs(self) -> Any:
        """Return native ``dbutils.fs``; unavailable in external mode."""
        backend = self._get_backend()
        if not isinstance(backend, DbutilsBackend):
            raise PlatformError(
                "dbutils.fs is available only when DatabricksPlatform uses "
                "runtime='databricks'."
            )
        self._fs = backend.fs
        return self._fs

    def _fetch_secret(self, key: str, source: str) -> str:
        return self._get_backend().fetch_secret(key, source)

    def read_file(self, path: str) -> str:
        return self._get_backend().read_file(path)

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        self._get_backend().write_file(path, content, overwrite=overwrite)

    def append_file(self, path: str, content: str) -> None:
        self._get_backend().append_file(path, content)

    def delete_file(self, path: str) -> None:
        self._get_backend().delete_file(path)

    def create_folder(self, path: str) -> None:
        self._get_backend().create_folder(path)

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        self._get_backend().delete_folder(path, recursive=recursive)

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        return self._get_backend().list_files(
            path,
            recursive=recursive,
            extension=extension,
        )

    def list_folders(self, path: str, *, recursive: bool = False) -> list[str]:
        return self._get_backend().list_folders(path, recursive=recursive)

    def file_exists(self, path: str) -> bool:
        return self._get_backend().file_exists(path)

    def folder_exists(self, path: str) -> bool:
        return self._get_backend().folder_exists(path)

    def upload_file(
        self, local_path: str, dest: str, *, overwrite: bool = False
    ) -> None:
        self._get_backend().upload_file(local_path, dest, overwrite=overwrite)

    def download_file(self, src: str, dest: str) -> None:
        self._get_backend().download_file(src, dest)

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        self._get_backend().copy_file(src, dest, overwrite=overwrite)

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        self._get_backend().move_file(src, dest, overwrite=overwrite)

    def get_file_info(self, path: str) -> FileInfo:
        return self._get_backend().get_file_info(path)

    def read_bytes(self, path: str) -> bytes:
        return self._get_backend().read_bytes(path)

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        self._get_backend().write_bytes(path, data, overwrite=overwrite)
