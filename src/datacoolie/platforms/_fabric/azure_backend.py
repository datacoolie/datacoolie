"""External OneLake and ADLS Gen2 backend powered by Azure SDK clients."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import PurePosixPath
from typing import Any, Iterable
from urllib.parse import urlsplit
from uuid import uuid4

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._fabric.paths import (
    AzureDataLakePath,
    ensure_mutable_path,
    parse_azure_datalake_path,
)
from datacoolie.platforms.base import FileInfo

_KEY_VAULT_SUFFIXES = (
    ".vault.azure.net",
    ".vault.azure.cn",
    ".vault.usgovcloudapi.net",
    ".vault.microsoftazure.de",
)
_UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class AzureSdkDependencies:
    """Lazily imported Azure classes, injectable for deterministic tests."""

    default_credential: type[Any]
    service_client: type[Any]
    secret_client: type[Any]
    not_found_error: type[Exception]
    resource_exists_error: type[Exception]


def load_azure_sdk() -> AzureSdkDependencies:
    """Import the optional Azure SDK only when the external backend is used."""
    try:
        from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError as exc:
        raise PlatformError(
            "Azure SDK dependencies are required for FabricPlatform external mode. "
            "Install them with: pip install 'datacoolie[fabric-external]'."
        ) from exc

    return AzureSdkDependencies(
        default_credential=DefaultAzureCredential,
        service_client=DataLakeServiceClient,
        secret_client=SecretClient,
        not_found_error=ResourceNotFoundError,
        resource_exists_error=ResourceExistsError,
    )


def _status_code(exc: Exception) -> int | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    response = getattr(exc, "response", None)
    response_status = getattr(response, "status_code", None)
    return response_status if isinstance(response_status, int) else None


def _property(value: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        result = getattr(value, name, None)
        if result is not None:
            return result
        try:
            result = value[name]
        except (KeyError, TypeError):
            continue
        if result is not None:
            return result
    return default


def _utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class AzureSdkBackend:
    """Implement the Fabric platform contract outside Microsoft Fabric."""

    def __init__(
        self,
        credential: Any | None = None,
        *,
        dependencies: AzureSdkDependencies | None = None,
    ) -> None:
        self._credential = credential
        self._dependencies = dependencies
        self._service_clients: dict[str, Any] = {}
        self._file_system_clients: dict[tuple[str, str], Any] = {}
        self._secret_clients: dict[str, Any] = {}

    @property
    def dependencies(self) -> AzureSdkDependencies:
        if self._dependencies is None:
            self._dependencies = load_azure_sdk()
        return self._dependencies

    @property
    def credential(self) -> Any:
        if self._credential is None:
            self._credential = self.dependencies.default_credential()
        return self._credential

    def _is_not_found(self, exc: Exception) -> bool:
        return isinstance(exc, self.dependencies.not_found_error) or _status_code(exc) == 404

    def _is_exists(self, exc: Exception) -> bool:
        return isinstance(exc, self.dependencies.resource_exists_error) or _status_code(exc) == 409

    @staticmethod
    def _failure(operation: str, target: str, exc: Exception) -> PlatformError:
        status = _status_code(exc)
        suffix = f" (status {status})" if status is not None else ""
        return PlatformError(f"Azure {operation} failed for {target}{suffix}.")

    def _service_client(self, location: AzureDataLakePath) -> Any:
        client = self._service_clients.get(location.account_url)
        if client is None:
            client = self.dependencies.service_client(
                account_url=location.account_url,
                credential=self.credential,
            )
            self._service_clients[location.account_url] = client
        return client

    def _file_system_client(self, location: AzureDataLakePath) -> Any:
        key = (location.account_url, location.file_system)
        client = self._file_system_clients.get(key)
        if client is None:
            client = self._service_client(location).get_file_system_client(location.file_system)
            self._file_system_clients[key] = client
        return client

    def _file_client(self, location: AzureDataLakePath) -> Any:
        return self._file_system_client(location).get_file_client(location.path)

    def _directory_client(self, location: AzureDataLakePath) -> Any:
        return self._file_system_client(location).get_directory_client(location.path)

    @staticmethod
    def _parse(path: str) -> AzureDataLakePath:
        return parse_azure_datalake_path(path)

    @staticmethod
    def _is_directory(properties: Any) -> bool:
        marker = _property(properties, "is_directory", "isDir", default=False)
        if isinstance(marker, str):
            return marker.lower() == "true"
        return bool(marker)

    def _info_from_properties(
        self,
        location: AzureDataLakePath,
        properties: Any,
        *,
        is_dir: bool | None = None,
    ) -> FileInfo:
        item_path = str(_property(properties, "name", default=location.path)).strip("/")
        item_location = location.with_path(item_path)
        return FileInfo(
            name=PurePosixPath(item_path).name,
            path=item_location.canonical_uri,
            modification_time=_utc(_property(properties, "last_modified", "lastModified")),
            size=int(_property(properties, "content_length", "size", default=0) or 0),
            is_dir=self._is_directory(properties) if is_dir is None else is_dir,
        )

    @staticmethod
    def _already_exists(target: str) -> PlatformError:
        return PlatformError(f"Destination already exists (set overwrite=True): {target}")

    @staticmethod
    def _validate_vault_url(source: str) -> str:
        if not source:
            raise PlatformError(
                "vault_url is required for FabricPlatform secret fetching. "
                "Pass it via secrets_ref as the source key."
            )
        try:
            parsed = urlsplit(source)
            host = (parsed.hostname or "").lower()
            port = parsed.port
        except ValueError as exc:
            raise PlatformError("Malformed Azure Key Vault URL.") from exc
        if (
            parsed.scheme.lower() != "https"
            or not host
            or parsed.username is not None
            or parsed.password is not None
            or port is not None
            or parsed.query
            or parsed.fragment
            or (parsed.path not in {"", "/"})
            or not any(host.endswith(suffix) and host != suffix[1:] for suffix in _KEY_VAULT_SUFFIXES)
        ):
            raise PlatformError("source must be a qualified Azure Key Vault HTTPS URL.")
        return f"https://{host}"

    def fetch_secret(self, key: str, source: str) -> str:
        vault_url = self._validate_vault_url(source)
        client = self._secret_clients.get(vault_url)
        if client is None:
            client = self.dependencies.secret_client(
                vault_url=vault_url,
                credential=self.credential,
            )
            self._secret_clients[vault_url] = client
        try:
            value = client.get_secret(key).value
            if value is None:
                raise PlatformError(f"Secret '{key}' has no value in the configured Azure vault.")
            return str(value)
        except PlatformError:
            raise
        except Exception as exc:
            raise self._failure("Key Vault secret retrieval", vault_url, exc) from exc

    def read_bytes(self, path: str) -> bytes:
        location = self._parse(path)
        try:
            value = self._file_client(location).download_file().readall()
            if isinstance(value, bytes):
                return value
            if isinstance(value, str):
                return value.encode("utf-8")
            return bytes(value)
        except Exception as exc:
            raise self._failure("file read", location.canonical_uri, exc) from exc

    def read_file(self, path: str) -> str:
        try:
            return self.read_bytes(path).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PlatformError(f"File is not valid UTF-8 text: {path}") from exc

    @staticmethod
    def _upload_chunks(data: Any) -> Iterable[bytes]:
        if isinstance(data, (bytes, bytearray, memoryview)):
            return (bytes(data),)
        if isinstance(data, str):
            return (data.encode("utf-8"),)
        read = getattr(data, "read", None)
        if callable(read):
            def read_chunks() -> Iterable[bytes]:
                while chunk := read(_UPLOAD_CHUNK_SIZE):
                    yield bytes(chunk)

            return read_chunks()
        return (bytes(chunk) for chunk in data)

    def _upload_data(
        self,
        client: Any,
        data: Any,
        *,
        overwrite: bool,
        target: str,
    ) -> None:
        if overwrite:
            client.upload_data(data, overwrite=True)
            return

        client.create_file()
        try:
            offset = 0
            for chunk in self._upload_chunks(data):
                if not chunk:
                    continue
                client.append_data(chunk, offset=offset, length=len(chunk))
                offset += len(chunk)
            if offset:
                client.flush_data(offset)
        except Exception as operation_error:
            try:
                client.delete_file()
            except Exception as cleanup_error:
                raise PlatformError(
                    f"Azure upload failed and its partial file could not be removed: {target}."
                ) from cleanup_error
            raise operation_error

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = False) -> None:
        location = self._parse(path)
        ensure_mutable_path(location)
        try:
            self._upload_data(
                self._file_client(location),
                data,
                overwrite=overwrite,
                target=location.canonical_uri,
            )
        except PlatformError:
            raise
        except Exception as exc:
            if not overwrite and self._is_exists(exc):
                raise PlatformError(f"File already exists (set overwrite=True): {path}") from exc
            raise self._failure("file write", location.canonical_uri, exc) from exc

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        self.write_bytes(path, content.encode("utf-8"), overwrite=overwrite)

    def append_file(self, path: str, content: str) -> None:
        location = self._parse(path)
        ensure_mutable_path(location)
        data = content.encode("utf-8")
        if not data:
            return
        client = self._file_client(location)
        try:
            try:
                offset = int(_property(client.get_file_properties(), "size", default=0) or 0)
            except Exception as exc:
                if not self._is_not_found(exc):
                    raise
                client.create_file()
                offset = 0
            client.append_data(data, offset=offset, length=len(data))
            client.flush_data(offset + len(data))
        except Exception as exc:
            raise self._failure("file append", location.canonical_uri, exc) from exc

    def delete_file(self, path: str) -> None:
        location = self._parse(path)
        ensure_mutable_path(location)
        try:
            self._file_client(location).delete_file()
        except Exception as exc:
            if not self._is_not_found(exc):
                raise self._failure("file delete", location.canonical_uri, exc) from exc

    def create_folder(self, path: str) -> None:
        location = self._parse(path)
        ensure_mutable_path(location)
        try:
            self._directory_client(location).create_directory()
        except Exception as exc:
            if not self._is_exists(exc):
                raise self._failure("folder create", location.canonical_uri, exc) from exc

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        location = self._parse(path)
        ensure_mutable_path(location)
        try:
            if not recursive:
                children = self._file_system_client(location).get_paths(
                    path=location.path,
                    recursive=False,
                    max_results=1,
                )
                if next(iter(children), None) is not None:
                    raise PlatformError(
                        f"Folder is not empty (set recursive=True): {location.canonical_uri}"
                    )
            self._directory_client(location).delete_directory()
        except PlatformError:
            raise
        except Exception as exc:
            if not self._is_not_found(exc):
                raise self._failure("folder delete", location.canonical_uri, exc) from exc

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        location = self._parse(path)
        try:
            paths = self._file_system_client(location).get_paths(
                path=location.path or None,
                recursive=recursive,
            )
            results = [
                self._info_from_properties(location, item)
                for item in paths
                if not self._is_directory(item)
                and (
                    extension is None
                    or str(_property(item, "name", default="")).endswith(extension)
                )
            ]
            return results
        except Exception as exc:
            raise self._failure("file listing", location.canonical_uri, exc) from exc

    def list_folders(self, path: str, *, recursive: bool = False) -> list[str]:
        location = self._parse(path)
        try:
            paths = self._file_system_client(location).get_paths(
                path=location.path or None,
                recursive=recursive,
            )
            results = [
                location.with_path(str(_property(item, "name"))).canonical_uri
                for item in paths
                if self._is_directory(item)
            ]
            return results
        except Exception as exc:
            raise self._failure("folder listing", location.canonical_uri, exc) from exc

    def file_exists(self, path: str) -> bool:
        location = self._parse(path)
        if not location.path:
            return False
        try:
            return bool(self._file_client(location).exists())
        except Exception as exc:
            if self._is_not_found(exc):
                return False
            raise self._failure("file existence check", location.canonical_uri, exc) from exc

    def folder_exists(self, path: str) -> bool:
        location = self._parse(path)
        if not location.path:
            try:
                return bool(self._file_system_client(location).exists())
            except Exception as exc:
                if self._is_not_found(exc):
                    return False
                raise self._failure("folder existence check", location.canonical_uri, exc) from exc
        try:
            return bool(self._directory_client(location).exists())
        except Exception as exc:
            if self._is_not_found(exc):
                return False
            raise self._failure("folder existence check", location.canonical_uri, exc) from exc

    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
        location = self._parse(dest)
        ensure_mutable_path(location)
        try:
            with open(local_path, "rb") as handle:
                self._upload_data(
                    self._file_client(location),
                    handle,
                    overwrite=overwrite,
                    target=location.canonical_uri,
                )
        except PlatformError:
            raise
        except Exception as exc:
            if not overwrite and self._is_exists(exc):
                raise self._already_exists(dest) from exc
            raise self._failure("file upload", location.canonical_uri, exc) from exc

    def download_file(self, src: str, dest: str) -> None:
        location = self._parse(src)
        try:
            with open(dest, "wb") as handle:
                self._file_client(location).download_file().readinto(handle)
        except Exception as exc:
            raise self._failure("file download", location.canonical_uri, exc) from exc

    @staticmethod
    def _download_chunks(downloader: Any) -> Iterable[bytes]:
        chunks = getattr(downloader, "chunks", None)
        if callable(chunks):
            return chunks()
        return (downloader.readall(),)

    def _ensure_parent_directory(self, location: AzureDataLakePath) -> None:
        parent, _, _ = location.path.rpartition("/")
        if not parent:
            return
        parts = parent.split("/")
        first_mutable_part = 0
        if (
            location.provider == "onelake"
            and len(parts) >= 2
            and parts[1].lower() in {"files", "tables"}
        ):
            first_mutable_part = 2
        for end in range(first_mutable_part + 1, len(parts) + 1):
            directory = location.with_path("/".join(parts[:end]))
            try:
                self._directory_client(directory).create_directory()
            except Exception as exc:
                if not self._is_exists(exc):
                    raise

    @staticmethod
    def _sibling_location(location: AzureDataLakePath, label: str) -> AzureDataLakePath:
        parent, _, name = location.path.rpartition("/")
        sibling = f".{name}.datacoolie-{label}-{uuid4().hex}"
        return location.with_path(f"{parent}/{sibling}" if parent else sibling)

    def _rename_path(
        self,
        source: AzureDataLakePath,
        destination: AzureDataLakePath,
    ) -> None:
        self._file_client(source).rename_file(
            new_name=f"{destination.file_system}/{destination.path}"
        )

    def _delete_path_if_present(self, location: AzureDataLakePath) -> None:
        try:
            self._file_client(location).delete_file()
        except Exception as exc:
            if not self._is_not_found(exc):
                raise

    def _replace_path(
        self,
        source: AzureDataLakePath,
        destination: AzureDataLakePath,
        *,
        overwrite: bool,
    ) -> None:
        destination_exists = self.file_exists(destination.canonical_uri)
        if destination_exists and not overwrite:
            raise self._already_exists(destination.canonical_uri)
        if not destination_exists:
            self._rename_path(source, destination)
            return

        backup = self._sibling_location(destination, "backup")
        self._rename_path(destination, backup)
        try:
            self._rename_path(source, destination)
        except Exception as promotion_error:
            try:
                self._rename_path(backup, destination)
            except Exception as restore_error:
                raise PlatformError(
                    "Azure replacement failed and the previous destination could not be "
                    f"restored; backup retained at {backup.canonical_uri}."
                ) from restore_error
            raise promotion_error

        try:
            self._file_client(backup).delete_file()
        except Exception as cleanup_error:
            try:
                self._rename_path(destination, source)
                self._rename_path(backup, destination)
            except Exception as restore_error:
                raise PlatformError(
                    "Azure replacement completed but backup cleanup and rollback failed; "
                    f"inspect {destination.canonical_uri} and {backup.canonical_uri}."
                ) from restore_error
            raise PlatformError(
                "Azure replacement backup cleanup failed; previous source and destination "
                "were restored."
            ) from cleanup_error

    def _copy_and_verify(
        self,
        source: AzureDataLakePath,
        destination: AzureDataLakePath,
        *,
        overwrite: bool,
    ) -> None:
        source_client = self._file_client(source)
        destination_client = self._file_client(destination)
        expected_size = int(
            _property(source_client.get_file_properties(), "size", default=0) or 0
        )
        downloader = source_client.download_file()
        source_digest = sha256()
        source_size = 0

        def source_chunks() -> Iterable[bytes]:
            nonlocal source_size
            for chunk in self._download_chunks(downloader):
                value = bytes(chunk)
                source_digest.update(value)
                source_size += len(value)
                yield value

        self._upload_data(
            destination_client,
            source_chunks(),
            overwrite=overwrite,
            target=destination.canonical_uri,
        )

        destination_digest = sha256()
        destination_size = 0
        for chunk in self._download_chunks(destination_client.download_file()):
            value = bytes(chunk)
            destination_digest.update(value)
            destination_size += len(value)

        if (
            source_size != expected_size
            or destination_size != expected_size
            or destination_digest.digest() != source_digest.digest()
        ):
            raise PlatformError(
                "Azure copy verification failed; source and prior destination were retained."
            )

    def _copy_to_destination(
        self,
        source: AzureDataLakePath,
        destination: AzureDataLakePath,
        *,
        overwrite: bool,
    ) -> None:
        self._ensure_parent_directory(destination)
        temporary = self._sibling_location(destination, "copy")
        try:
            self._copy_and_verify(source, temporary, overwrite=False)
            self._replace_path(temporary, destination, overwrite=overwrite)
        except Exception as operation_error:
            try:
                self._delete_path_if_present(temporary)
            except Exception as cleanup_error:
                raise PlatformError(
                    "Azure copy failed and its temporary file could not be removed: "
                    f"{temporary.canonical_uri}."
                ) from cleanup_error
            raise operation_error

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        source = self._parse(src)
        destination = self._parse(dest)
        ensure_mutable_path(destination)
        if source.canonical_uri == destination.canonical_uri:
            return
        try:
            self._copy_to_destination(source, destination, overwrite=overwrite)
        except PlatformError:
            raise
        except Exception as exc:
            if not overwrite and self._is_exists(exc):
                raise self._already_exists(dest) from exc
            raise self._failure("file copy", destination.canonical_uri, exc) from exc

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        source = self._parse(src)
        destination = self._parse(dest)
        ensure_mutable_path(source)
        ensure_mutable_path(destination)

        if source.canonical_uri == destination.canonical_uri:
            return

        same_file_system = (
            source.account_url == destination.account_url
            and source.file_system == destination.file_system
        )
        try:
            if same_file_system:
                self._ensure_parent_directory(destination)
                self._replace_path(source, destination, overwrite=overwrite)
                return
            self._copy_to_destination(source, destination, overwrite=overwrite)
            self._file_client(source).delete_file()
        except PlatformError:
            raise
        except Exception as exc:
            if not overwrite and self._is_exists(exc):
                raise self._already_exists(dest) from exc
            raise self._failure("file move", destination.canonical_uri, exc) from exc

    def get_file_info(self, path: str) -> FileInfo:
        location = self._parse(path)
        if not location.path:
            try:
                properties = self._file_system_client(location).get_file_system_properties()
                return FileInfo(
                    name=location.file_system,
                    path=location.canonical_uri,
                    modification_time=_utc(_property(properties, "last_modified")),
                    size=0,
                    is_dir=True,
                )
            except Exception as exc:
                raise self._failure("path metadata read", location.canonical_uri, exc) from exc
        try:
            properties = self._file_client(location).get_file_properties()
            return self._info_from_properties(location, properties, is_dir=False)
        except Exception as file_error:
            if not self._is_not_found(file_error):
                raise self._failure(
                    "path metadata read", location.canonical_uri, file_error
                ) from file_error
        try:
            properties = self._directory_client(location).get_directory_properties()
            return self._info_from_properties(location, properties, is_dir=True)
        except Exception as directory_error:
            if self._is_not_found(directory_error):
                raise PlatformError(f"Path does not exist: {location.canonical_uri}") from directory_error
            raise self._failure(
                "path metadata read", location.canonical_uri, directory_error
            ) from directory_error
