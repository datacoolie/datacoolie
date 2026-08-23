"""Contract tests for the external Azure SDK Fabric backend."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any, Iterable

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._fabric.azure_backend import (
    AzureSdkBackend,
    AzureSdkDependencies,
    load_azure_sdk,
)

ROOT = "abfss://raw@account.dfs.core.windows.net"
OTHER_ROOT = "abfss://raw@other.dfs.core.windows.net"
NOW = datetime(2026, 8, 20, tzinfo=timezone.utc)


class FakeNotFound(Exception):
    status_code = 404


class FakeExists(Exception):
    status_code = 409


class FakeCredential:
    created = 0

    def __init__(self) -> None:
        type(self).created += 1


class FakeDownloader:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def readall(self) -> bytes:
        return self.data

    def readinto(self, handle: Any) -> int:
        return handle.write(self.data)

    def chunks(self) -> Iterable[bytes]:
        midpoint = len(self.data) // 2
        yield self.data[:midpoint]
        yield self.data[midpoint:]


def _payload_bytes(data: Any) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode()
    if hasattr(data, "read"):
        return data.read()
    return b"".join(data)


class FakeFileClient:
    def __init__(self, file_system: "FakeFileSystemClient", path: str) -> None:
        self.file_system = file_system
        self.path = path

    def upload_data(self, data: Any, *, overwrite: bool = False) -> None:
        self.file_system.upload_overwrite_values.append(overwrite)
        if self.path in self.file_system.files and not overwrite:
            raise FakeExists
        self.file_system.files[self.path] = _payload_bytes(data)

    def exists(self) -> bool:
        self.file_system.file_exists_calls += 1
        return self.path in self.file_system.files

    def download_file(self) -> FakeDownloader:
        try:
            data = self.file_system.files[self.path]
            if self.file_system.corrupt_downloads and ".datacoolie-copy-" in self.path:
                data = b"corrupt" + data
            return FakeDownloader(data)
        except KeyError as exc:
            raise FakeNotFound from exc

    def get_file_properties(self) -> SimpleNamespace:
        if self.path not in self.file_system.files:
            raise FakeNotFound
        return SimpleNamespace(size=len(self.file_system.files[self.path]), last_modified=NOW)

    def create_file(self) -> None:
        self.file_system.create_file_calls += 1
        if self.path in self.file_system.files:
            raise FakeExists
        self.file_system.files[self.path] = b""

    def append_data(self, data: bytes, *, offset: int, length: int) -> None:
        if self.path in self.file_system.append_failures:
            raise RuntimeError("injected append failure")
        current = self.file_system.files.get(self.path)
        if current is None:
            raise FakeNotFound
        assert offset == len(current)
        assert length == len(data)
        self.file_system.files[self.path] = current + data

    def flush_data(self, offset: int) -> None:
        assert offset == len(self.file_system.files[self.path])

    def delete_file(self) -> None:
        if self.path in self.file_system.delete_failures:
            raise RuntimeError("injected delete failure")
        try:
            del self.file_system.files[self.path]
        except KeyError as exc:
            raise FakeNotFound from exc

    def rename_file(self, *, new_name: str) -> None:
        file_system_name, _, destination = new_name.partition("/")
        assert file_system_name == self.file_system.name
        if (self.path, destination) in self.file_system.rename_failures:
            raise RuntimeError("injected rename failure")
        if destination in self.file_system.files:
            raise FakeExists
        try:
            self.file_system.files[destination] = self.file_system.files.pop(self.path)
        except KeyError as exc:
            raise FakeNotFound from exc


class FakeDirectoryClient:
    def __init__(self, file_system: "FakeFileSystemClient", path: str) -> None:
        self.file_system = file_system
        self.path = path

    def create_directory(self) -> None:
        if self.path in self.file_system.directories:
            raise FakeExists
        self.file_system.directories.add(self.path)

    def exists(self) -> bool:
        self.file_system.directory_exists_calls += 1
        return self.path in self.file_system.directories

    def get_directory_properties(self) -> SimpleNamespace:
        if self.path not in self.file_system.directories:
            raise FakeNotFound
        return SimpleNamespace(name=self.path, last_modified=NOW)

    def delete_directory(self) -> None:
        if self.path not in self.file_system.directories:
            raise FakeNotFound
        prefix = f"{self.path}/"
        self.file_system.files = {
            key: value for key, value in self.file_system.files.items() if not key.startswith(prefix)
        }
        self.file_system.directories = {
            key
            for key in self.file_system.directories
            if key != self.path and not key.startswith(prefix)
        }


class FakeFileSystemClient:
    def __init__(self, name: str) -> None:
        self.name = name
        self.files: dict[str, bytes] = {}
        self.directories: set[str] = set()
        self.file_exists_calls = 0
        self.directory_exists_calls = 0
        self.get_paths_calls = 0
        self.corrupt_downloads = False
        self.create_file_calls = 0
        self.upload_overwrite_values: list[bool] = []
        self.append_failures: set[str] = set()
        self.rename_failures: set[tuple[str, str]] = set()
        self.delete_failures: set[str] = set()

    def get_file_client(self, path: str) -> FakeFileClient:
        return FakeFileClient(self, path)

    def get_directory_client(self, path: str) -> FakeDirectoryClient:
        return FakeDirectoryClient(self, path)

    def get_paths(
        self,
        *,
        path: str | None = None,
        recursive: bool = True,
        max_results: int | None = None,
    ) -> list[SimpleNamespace]:
        self.get_paths_calls += 1
        prefix = f"{path.rstrip('/')}" if path else ""
        child_prefix = f"{prefix}/" if prefix else ""
        values: list[SimpleNamespace] = []
        for name in sorted(self.directories | set(self.files)):
            if child_prefix and not name.startswith(child_prefix):
                continue
            remainder = name[len(child_prefix) :]
            if not recursive and "/" in remainder:
                continue
            is_directory = name in self.directories
            values.append(
                SimpleNamespace(
                    name=name,
                    is_directory=is_directory,
                    content_length=0 if is_directory else len(self.files[name]),
                    last_modified=NOW,
                )
            )
        return values[:max_results] if max_results is not None else values

    def exists(self) -> bool:
        return True

    def get_file_system_properties(self) -> SimpleNamespace:
        return SimpleNamespace(last_modified=NOW)


class FakeServiceClient:
    created: list["FakeServiceClient"] = []

    def __init__(self, *, account_url: str, credential: Any) -> None:
        self.account_url = account_url
        self.credential = credential
        self.file_systems: dict[str, FakeFileSystemClient] = {}
        type(self).created.append(self)

    def get_file_system_client(self, name: str) -> FakeFileSystemClient:
        return self.file_systems.setdefault(name, FakeFileSystemClient(name))


class FakeSecretClient:
    created: list["FakeSecretClient"] = []
    values = {"api-key": "secret-value"}

    def __init__(self, *, vault_url: str, credential: Any) -> None:
        self.vault_url = vault_url
        self.credential = credential
        type(self).created.append(self)

    def get_secret(self, key: str) -> SimpleNamespace:
        if key not in self.values:
            raise FakeNotFound
        return SimpleNamespace(value=self.values[key])


@pytest.fixture(autouse=True)
def reset_fakes() -> None:
    FakeCredential.created = 0
    FakeServiceClient.created.clear()
    FakeSecretClient.created.clear()


@pytest.fixture()
def dependencies() -> AzureSdkDependencies:
    return AzureSdkDependencies(
        default_credential=FakeCredential,
        service_client=FakeServiceClient,
        secret_client=FakeSecretClient,
        not_found_error=FakeNotFound,
        resource_exists_error=FakeExists,
    )


@pytest.fixture()
def backend(dependencies: AzureSdkDependencies) -> AzureSdkBackend:
    return AzureSdkBackend(dependencies=dependencies)


def test_default_credential_is_lazy_and_shared_by_storage_and_vault(
    backend: AzureSdkBackend,
) -> None:
    assert FakeCredential.created == 0
    backend.write_file(f"{ROOT}/folder/a.txt", "a")
    assert backend.fetch_secret("api-key", "https://demo.vault.azure.net/") == "secret-value"
    assert FakeCredential.created == 1
    assert FakeServiceClient.created[0].credential is FakeSecretClient.created[0].credential


def test_injected_credential_is_reused_without_default_creation(
    dependencies: AzureSdkDependencies,
) -> None:
    credential = object()
    backend = AzureSdkBackend(credential, dependencies=dependencies)
    backend.write_file(f"{ROOT}/a.txt", "a")
    backend.fetch_secret("api-key", "https://demo.vault.azure.net")
    assert FakeCredential.created == 0
    assert FakeServiceClient.created[0].credential is credential
    assert FakeSecretClient.created[0].credential is credential


def test_text_binary_append_list_and_metadata(backend: AzureSdkBackend) -> None:
    backend.create_folder(f"{ROOT}/folder")
    backend.write_file(f"{ROOT}/folder/a.txt", "hello")
    backend.append_file(f"{ROOT}/folder/a.txt", " world")
    backend.write_bytes(f"{ROOT}/folder/b.bin", b"binary")

    assert backend.read_file(f"{ROOT}/folder/a.txt") == "hello world"
    assert backend.read_bytes(f"{ROOT}/folder/b.bin") == b"binary"
    assert backend.file_exists(f"{ROOT}/folder/a.txt") is True
    assert backend.folder_exists(f"{ROOT}/folder") is True

    files = backend.list_files(f"{ROOT}/folder")
    assert [item.name for item in files] == ["a.txt", "b.bin"]
    assert backend.list_files(f"{ROOT}/folder", extension=".txt")[0].size == 11
    assert backend.list_folders(ROOT) == [f"{ROOT}/folder"]

    info = backend.get_file_info(f"{ROOT}/folder/a.txt")
    assert info.path == f"{ROOT}/folder/a.txt"
    assert info.modification_time == NOW
    assert info.is_dir is False


def test_exact_existence_and_info_never_scan_parent(backend: AzureSdkBackend) -> None:
    backend.create_folder(f"{ROOT}/folder")
    backend.write_file(f"{ROOT}/folder/a.txt", "value")
    file_system = backend._file_system_client(backend._parse(f"{ROOT}/folder/a.txt"))
    file_system.get_paths_calls = 0

    assert backend.file_exists(f"{ROOT}/folder/a.txt") is True
    assert backend.folder_exists(f"{ROOT}/folder") is True
    assert backend.get_file_info(f"{ROOT}/folder/a.txt").is_dir is False
    assert backend.get_file_info(f"{ROOT}/folder").is_dir is True

    assert file_system.get_paths_calls == 0
    assert file_system.file_exists_calls == 1
    assert file_system.directory_exists_calls == 1


def test_write_conflict_uses_atomic_create_without_exists_preflight(
    backend: AzureSdkBackend,
) -> None:
    path = f"{ROOT}/folder/a.txt"
    backend.write_file(path, "first")
    file_system = backend._file_system_client(backend._parse(path))
    file_system.file_exists_calls = 0

    with pytest.raises(PlatformError, match="already exists"):
        backend.write_file(path, "second")

    assert file_system.file_exists_calls == 0
    assert file_system.create_file_calls == 2
    assert file_system.upload_overwrite_values == []
    assert backend.read_file(path) == "first"


def test_non_overwrite_upload_removes_partial_file_after_append_failure(
    backend: AzureSdkBackend,
) -> None:
    path = f"{ROOT}/folder/a.txt"
    file_system = backend._file_system_client(backend._parse(path))
    file_system.append_failures.add("folder/a.txt")

    with pytest.raises(PlatformError, match="file write"):
        backend.write_file(path, "value")

    assert backend.file_exists(path) is False


def test_non_overwrite_upload_reports_partial_file_when_cleanup_fails(
    backend: AzureSdkBackend,
) -> None:
    path = f"{ROOT}/folder/a.txt"
    file_system = backend._file_system_client(backend._parse(path))
    file_system.append_failures.add("folder/a.txt")
    file_system.delete_failures.add("folder/a.txt")

    with pytest.raises(PlatformError, match="partial file could not be removed"):
        backend.write_file(path, "value")

    assert backend.file_exists(path) is True


def test_upload_download_overwrite_and_delete(
    backend: AzureSdkBackend,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"payload")
    cloud_path = f"{ROOT}/folder/a.bin"

    backend.upload_file(str(source), cloud_path)
    with pytest.raises(PlatformError, match="already exists"):
        backend.upload_file(str(source), cloud_path)
    backend.upload_file(str(source), cloud_path, overwrite=True)
    backend.download_file(cloud_path, str(destination))
    assert destination.read_bytes() == b"payload"

    backend.delete_file(cloud_path)
    backend.delete_file(cloud_path)
    assert backend.file_exists(cloud_path) is False


def test_folder_delete_requires_recursive_for_non_empty(backend: AzureSdkBackend) -> None:
    backend.create_folder(f"{ROOT}/folder")
    backend.write_file(f"{ROOT}/folder/a.txt", "a")
    with pytest.raises(PlatformError, match="not empty"):
        backend.delete_folder(f"{ROOT}/folder")
    backend.delete_folder(f"{ROOT}/folder", recursive=True)
    assert backend.folder_exists(f"{ROOT}/folder") is False


def test_copy_and_same_filesystem_move(backend: AzureSdkBackend) -> None:
    source = f"{ROOT}/folder/source.txt"
    copied = f"{ROOT}/folder/copied.txt"
    moved = f"{ROOT}/folder/moved.txt"
    backend.write_file(source, "payload")

    backend.copy_file(source, copied)
    assert backend.read_file(copied) == "payload"
    assert backend.file_exists(source) is True

    backend.move_file(source, moved)
    assert backend.read_file(moved) == "payload"
    assert backend.file_exists(source) is False


def test_cross_account_move_copies_verifies_then_deletes(backend: AzureSdkBackend) -> None:
    source = f"{ROOT}/folder/source.txt"
    destination = f"{OTHER_ROOT}/folder/moved.txt"
    backend.write_file(source, "payload")
    backend.move_file(source, destination)
    assert backend.read_file(destination) == "payload"
    assert backend.file_exists(source) is False


def test_cross_account_verification_failure_retains_source_and_destination(
    backend: AzureSdkBackend,
) -> None:
    source = f"{ROOT}/folder/source.txt"
    destination = f"{OTHER_ROOT}/new/deep/moved.txt"
    backend.write_file(source, "new-value")
    backend.write_file(destination, "old-value")
    destination_fs = backend._file_system_client(backend._parse(destination))
    destination_fs.corrupt_downloads = True

    with pytest.raises(PlatformError, match="verification failed"):
        backend.move_file(source, destination, overwrite=True)

    destination_fs.corrupt_downloads = False
    assert backend.read_file(source) == "new-value"
    assert backend.read_file(destination) == "old-value"
    assert not any(".datacoolie-" in path for path in destination_fs.files)


def test_same_filesystem_overwrite_restores_destination_when_promotion_fails(
    backend: AzureSdkBackend,
) -> None:
    source = f"{ROOT}/folder/source.txt"
    destination = f"{ROOT}/new/deep/moved.txt"
    backend.write_file(source, "new-value")
    backend.write_file(destination, "old-value")
    file_system = backend._file_system_client(backend._parse(source))
    file_system.rename_failures.add(("folder/source.txt", "new/deep/moved.txt"))

    with pytest.raises(PlatformError, match="file move"):
        backend.move_file(source, destination, overwrite=True)

    assert backend.read_file(source) == "new-value"
    assert backend.read_file(destination) == "old-value"
    assert "new" in file_system.directories
    assert "new/deep" in file_system.directories
    assert not any(".datacoolie-backup-" in path for path in file_system.files)


def test_same_filesystem_overwrite_rolls_back_when_backup_cleanup_fails(
    backend: AzureSdkBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = f"{ROOT}/folder/source.txt"
    destination = f"{ROOT}/new/deep/moved.txt"
    backend.write_file(source, "new-value")
    backend.write_file(destination, "old-value")
    file_system = backend._file_system_client(backend._parse(source))
    original_delete = FakeFileClient.delete_file

    def fail_backup_delete(client: FakeFileClient) -> None:
        if ".datacoolie-backup-" in client.path:
            raise RuntimeError("injected backup cleanup failure")
        original_delete(client)

    monkeypatch.setattr(FakeFileClient, "delete_file", fail_backup_delete)

    with pytest.raises(PlatformError, match="previous source and destination were restored"):
        backend.move_file(source, destination, overwrite=True)

    assert backend.read_file(source) == "new-value"
    assert backend.read_file(destination) == "old-value"
    assert not any(".datacoolie-backup-" in path for path in file_system.files)


def test_copy_verification_failure_retains_source(backend: AzureSdkBackend) -> None:
    source = f"{ROOT}/folder/source.txt"
    destination = f"{OTHER_ROOT}/folder/copied.txt"
    backend.write_file(source, "payload")

    original = backend._copy_and_verify

    def fail_after_copy(*args: Any, **kwargs: Any) -> None:
        original(*args, **kwargs)
        raise PlatformError("verification failed")

    backend._copy_and_verify = fail_after_copy  # type: ignore[method-assign]
    with pytest.raises(PlatformError, match="verification failed"):
        backend.move_file(source, destination)
    assert backend.file_exists(source) is True


def test_managed_onelake_roots_are_protected(backend: AzureSdkBackend) -> None:
    managed_root = (
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Files"
    )
    with pytest.raises(PlatformError, match="only below"):
        backend.delete_folder(managed_root, recursive=True)


def test_vault_validation_and_client_cache(backend: AzureSdkBackend) -> None:
    assert backend.fetch_secret("api-key", "https://demo.vault.azure.net/") == "secret-value"
    assert backend.fetch_secret("api-key", "https://demo.vault.azure.net") == "secret-value"
    assert len(FakeSecretClient.created) == 1
    with pytest.raises(PlatformError, match="qualified Azure Key Vault"):
        backend.fetch_secret("api-key", "https://attacker.example.com")


def test_error_messages_do_not_include_sdk_exception_text(
    backend: AzureSdkBackend,
) -> None:
    client = backend._file_system_client(backend._parse(f"{ROOT}/folder/a.txt"))

    class AuthenticationFailure(Exception):
        status_code = 403

    def fail(_: str) -> FakeFileClient:
        raise AuthenticationFailure("access-token-value")

    client.get_file_client = fail
    with pytest.raises(PlatformError) as captured:
        backend.read_file(f"{ROOT}/folder/a.txt")
    assert "access-token-value" not in str(captured.value)
    assert "status 403" in str(captured.value)


def test_missing_optional_dependency_has_actionable_install_message() -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setitem(sys.modules, "azure.identity", None)
        with pytest.raises(PlatformError, match=r"datacoolie\[fabric-external\]"):
            load_azure_sdk()
