"""Abstract base class for platform (file system and secrets) operations.

Every concrete platform — Local, Fabric, Databricks, AWS — inherits from
:class:`BasePlatform` and implements all abstract methods.
"""

from __future__ import annotations

import os
import tempfile
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from datacoolie.core.exceptions import PlatformError
from datacoolie.core.secret_provider import BaseSecretProvider
from datacoolie.utils.path_utils import normalize_path


@dataclass(frozen=True, slots=True)
class FileInfo:
    """Metadata for a single file-system entry.

    Returned by :meth:`BasePlatform.list_files` and
    :meth:`BasePlatform.get_file_info`.  ``frozen=True`` makes instances
    immutable and hashable; ``slots=True`` reduces per-instance memory.
    """

    name: str
    path: str
    modification_time: Optional[datetime]  # UTC-aware datetime; None if unavailable
    size: int = field(default=0)
    is_dir: bool = field(default=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", normalize_path(self.path))


class BasePlatform(BaseSecretProvider):
    """Abstract interface for file/directory operations and secret retrieval.

    Platforms encapsulate the storage layer differences (local FS, ADLS,
    S3, DBFS) behind a uniform API so that the rest of the framework
    never touches ``os``, ``shutil``, or cloud SDKs directly.

    Each platform also implements :meth:`_fetch_secret` so it can serve as
    its own :class:`~datacoolie.core.secret_provider.BaseSecretProvider`,
    using its existing SDK handle (env vars / notebookutils / dbutils / boto3).

    **16 abstract methods** across five categories:

    * **File I/O** — read / write / append / delete text content
    * **Directory Ops** — create / delete / list files / list folders
    * **Existence Checks** — file_exists / folder_exists
    * **File Management** — upload / download / copy / move / get_file_info
    * **Secrets** — ``_fetch_secret`` (inherited requirement from BaseSecretProvider)
    """

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    @abstractmethod
    def read_file(self, path: str) -> str:
        """Read the entire content of a text file.

        Args:
            path: Absolute path or URI to the file.

        Returns:
            File content as a string.

        Raises:
            PlatformError: If the file does not exist or cannot be read.
        """

    @abstractmethod
    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        """Write text content to a file.

        Args:
            path: Absolute path or URI for the target file.
            content: Text to write.
            overwrite: If ``True``, overwrite an existing file; otherwise
                raise when the file already exists.

        Raises:
            PlatformError: If the file exists and *overwrite* is ``False``,
                or on I/O failure.
        """

    @abstractmethod
    def append_file(self, path: str, content: str) -> None:
        """Append text content to an existing file.

        If the file does not exist, create it.

        Args:
            path: Absolute path or URI for the target file.
            content: Text to append.

        Raises:
            PlatformError: On I/O failure.
        """

    @abstractmethod
    def delete_file(self, path: str) -> None:
        """Delete a file.

        No-op if the file does not exist (idempotent).

        Args:
            path: Absolute path or URI to delete.

        Raises:
            PlatformError: On I/O failure.
        """

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    @abstractmethod
    def create_folder(self, path: str) -> None:
        """Create a directory (including any missing parents).

        No-op if the directory already exists.

        Args:
            path: Absolute path or URI.

        Raises:
            PlatformError: On failure.
        """

    @abstractmethod
    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        """Delete a directory.

        Args:
            path: Absolute path or URI.
            recursive: If ``True``, delete contents recursively.

        Raises:
            PlatformError: If *recursive* is ``False`` and the directory
                is non-empty, or on I/O failure.
        """

    @abstractmethod
    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        """List files under *path*.

        Each entry is a :class:`FileInfo` instance.

        Args:
            path: Directory path or URI.
            recursive: Descend into sub-directories.
            extension: Filter by suffix (e.g. ``".parquet"``).

        Returns:
            List of :class:`FileInfo` objects (directories excluded).

        Raises:
            PlatformError: If the path does not exist or is not a directory.
        """

    @abstractmethod
    def list_folders(
        self,
        path: str,
        *,
        recursive: bool = False,
    ) -> list[str]:
        """List immediate (or recursive) sub-directories.

        Args:
            path: Directory path or URI.
            recursive: Descend into sub-directories.

        Returns:
            List of directory paths.

        Raises:
            PlatformError: If *path* is not a directory.
        """

    # ------------------------------------------------------------------
    # Existence checks
    # ------------------------------------------------------------------

    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """Return ``True`` if the path refers to an existing file."""

    @abstractmethod
    def folder_exists(self, path: str) -> bool:
        """Return ``True`` if the path refers to an existing directory."""

    # ------------------------------------------------------------------
    # File management
    # ------------------------------------------------------------------

    @abstractmethod
    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
        """Upload a file from the local filesystem to the platform.

        Args:
            local_path: Absolute path on the **local** OS filesystem.
            dest: Destination path on this platform.
            overwrite: Allow overwriting an existing file at *dest*.

        Raises:
            PlatformError: If *local_path* does not exist or *dest* exists
                without *overwrite*.
        """

    @abstractmethod
    def download_file(self, src: str, dest: str) -> None:
        """Download a file from this platform to the local filesystem.

        Args:
            src: Source path on this platform.
            dest: Absolute path on the **local** OS filesystem to write to.

        Raises:
            PlatformError: If the source does not exist or the download fails.
        """

    @abstractmethod
    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        """Copy a file from *src* to *dest*.

        Args:
            src: Source file path.
            dest: Destination file path.
            overwrite: Allow overwriting an existing file at *dest*.

        Raises:
            PlatformError: If source does not exist or dest exists
                without *overwrite*.
        """

    @abstractmethod
    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        """Move (rename) a file from *src* to *dest*.

        Args:
            src: Source file path.
            dest: Destination file path.
            overwrite: Allow overwriting an existing file at *dest*.

        Raises:
            PlatformError: If source does not exist or dest exists
                without *overwrite*.
        """

    @abstractmethod
    def get_file_info(self, path: str) -> FileInfo:
        """Return metadata about a single file or directory.

        Returns:
            A :class:`FileInfo` instance with ``name``, ``path``, ``size``,
            ``modification_time`` (UTC-aware datetime or ``None``), and
            ``is_dir``.

        Raises:
            PlatformError: If the path does not exist.
        """

    # ------------------------------------------------------------------
    # Binary I/O — abstract; every platform must provide an implementation
    # ------------------------------------------------------------------

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Return the entire file contents as ``bytes``.

        Implementations MUST NOT use bounded ``head()``-style APIs that
        truncate large files.  Prefer the platform's fastest native unbounded
        primitive (e.g. ``s3.get_object`` for AWS, direct ``open(path, "rb")``
        for Unity Catalog Volumes, ``fs.cp`` to a temp file for other
        cloud paths).

        Suitable for files that fit comfortably in memory.

        Raises:
            PlatformError: If the file does not exist or cannot be read.
        """

    @abstractmethod
    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        """Write raw ``bytes`` to *path*.

        Symmetric counterpart of :meth:`read_bytes`.  Prefer the platform's
        fastest native upload primitive.  For very large payloads, write to a
        temp file and call :meth:`upload_file` directly.

        Args:
            path: Destination path or URI.
            data: Bytes to write.
            overwrite: Allow overwriting an existing file at *path*.

        Raises:
            PlatformError: If the file exists and *overwrite* is ``False``,
                or on I/O failure.
        """
