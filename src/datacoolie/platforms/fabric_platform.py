"""Portable Microsoft Fabric platform facade.

NotebookUtils remains the preferred backend inside Fabric. External Python
runtimes use the Azure Data Lake and Key Vault SDKs with Microsoft Entra
credentials while callers keep the same :class:`FabricPlatform` API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._fabric.azure_backend import AzureSdkBackend
from datacoolie.platforms._fabric.notebookutils_backend import NotebookUtilsBackend
from datacoolie.platforms._fabric.runtime import (
    EffectiveFabricRuntime,
    FabricRuntime,
    require_notebookutils,
    resolve_runtime,
    validate_runtime,
)
from datacoolie.platforms.base import BasePlatform, FileInfo

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
else:
    TokenCredential = Any

_Backend = NotebookUtilsBackend | AzureSdkBackend


class FabricPlatform(BasePlatform):
    """Access OneLake or ADLS through the best backend for the current runtime.

    Args:
        cache_ttl: Secret cache time-to-live in seconds. Pass ``0`` to disable it.
        runtime: ``"auto"`` prefers NotebookUtils when it is usable; ``"fabric"``
            requires NotebookUtils; ``"external"`` always uses Azure SDK clients.
        azure_credential: Optional Azure ``TokenCredential`` for external mode.
            When omitted, ``DefaultAzureCredential`` is created lazily.
    """

    def __init__(
        self,
        cache_ttl: int = 300,
        *,
        runtime: FabricRuntime = "auto",
        azure_credential: TokenCredential | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(cache_ttl=cache_ttl)
        self._runtime = validate_runtime(runtime)
        self._effective_runtime: EffectiveFabricRuntime | None = None
        self._azure_credential = azure_credential
        self._backend: _Backend | None = None

        # Retained as native compatibility surfaces and test injection points.
        self._nu: Any | None = None
        self._fs: Any | None = None

    def _resolve_effective_runtime(self) -> EffectiveFabricRuntime:
        if self._effective_runtime is None:
            if self._runtime == "auto" and (self._nu is not None or self._fs is not None):
                self._effective_runtime = "fabric"
            else:
                self._effective_runtime = resolve_runtime(self._runtime)
        return self._effective_runtime

    def _get_backend(self) -> _Backend:
        if self._backend is not None:
            return self._backend

        if self._resolve_effective_runtime() == "external":
            self._backend = AzureSdkBackend(credential=self._azure_credential)
            return self._backend

        if self._nu is None and self._fs is None:
            self._nu = require_notebookutils()
        self._backend = NotebookUtilsBackend(self._nu, fs=self._fs)
        self._fs = self._backend.fs
        return self._backend

    @property
    def notebookutils(self) -> Any:
        """Return native NotebookUtils; unavailable in explicit external mode."""
        backend = self._get_backend()
        if not isinstance(backend, NotebookUtilsBackend):
            raise PlatformError(
                "notebookutils is available only when FabricPlatform uses runtime='fabric'."
            )
        if self._nu is None:
            self._nu = require_notebookutils()
            backend.notebookutils = self._nu
        return self._nu

    @property
    def fs(self) -> Any:
        """Return ``notebookutils.fs`` in the native Fabric runtime."""
        backend = self._get_backend()
        if not isinstance(backend, NotebookUtilsBackend):
            raise PlatformError(
                "notebookutils.fs is available only when FabricPlatform uses runtime='fabric'."
            )
        return backend.fs

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

    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
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
