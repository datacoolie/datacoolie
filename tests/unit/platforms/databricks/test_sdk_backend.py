"""External Databricks SDK backend contract tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._databricks.sdk_backend import DatabricksSdkBackend
from tests.unit.platforms.databricks._fakes import (
    FakeWorkspaceClient,
    PermissionDenied,
)

ROOT = "/Volumes/main/default/logs"


def test_external_full_file_contract(tmp_path: Path) -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client, max_list_workers=1)
    source = f"{ROOT}/source.txt"
    copied = f"{ROOT}/copied.txt"
    moved = f"{ROOT}/nested/moved.txt"
    binary = f"{ROOT}/upload.bin"

    backend.write_file(source, "first")
    backend.append_file(source, "-second")
    assert backend.read_file(source) == "first-second"
    assert backend.read_bytes(source) == b"first-second"

    backend.copy_file(source, copied)
    backend.move_file(copied, moved)
    assert backend.file_exists(copied) is False
    assert backend.read_file(moved) == "first-second"

    local_source = tmp_path / "source.bin"
    local_dest = tmp_path / "nested" / "dest.bin"
    local_source.write_bytes(b"binary-payload")
    backend.upload_file(str(local_source), binary)
    backend.download_file(binary, str(local_dest))
    assert local_dest.read_bytes() == b"binary-payload"

    assert {item.name for item in backend.list_files(ROOT)} == {
        "source.txt",
        "upload.bin",
    }
    assert {item.name for item in backend.list_files(ROOT, recursive=True)} == {
        "source.txt",
        "moved.txt",
        "upload.bin",
    }
    assert backend.list_folders(ROOT) == [f"{ROOT}/nested"]
    assert backend.get_file_info(source).size == len(b"first-second")
    assert backend.get_file_info(f"{ROOT}/nested").is_dir is True

    backend.delete_folder(f"{ROOT}/nested", recursive=True)
    assert backend.folder_exists(f"{ROOT}/nested") is False
    backend.delete_file(source)
    backend.delete_file(source)


def test_exists_uses_exact_metadata_and_preserves_permission_errors() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    path = f"{ROOT}/a.txt"
    backend.write_file(path, "a")
    client.files.calls.clear()

    assert backend.file_exists(path) is True
    assert client.files.calls == [("get_metadata", path)]

    with patch.object(
        client.files,
        "get_metadata",
        side_effect=PermissionDenied("forbidden"),
    ):
        with pytest.raises(PlatformError, match="PermissionDenied"):
            backend.file_exists(path)


def test_append_existing_file_skips_metadata_and_parent_calls() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    path = f"{ROOT}/append.txt"
    backend.write_file(path, "first")
    client.files.calls.clear()

    backend.append_file(path, "-second")

    assert client.files.calls == [
        ("download", path),
        ("upload", path),
    ]
    assert backend.read_file(path) == "first-second"


def test_append_missing_file_creates_parent_only_after_exact_not_found() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    path = f"{ROOT}/append/nested/value.txt"

    backend.append_file(path, "first")

    assert client.files.calls == [
        ("download", path),
        ("create_directory", f"{ROOT}/append/nested"),
        ("upload", path),
    ]
    assert backend.read_file(path) == "first"


def test_append_does_not_treat_permission_as_missing() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    path = f"{ROOT}/append/forbidden.txt"
    with patch.object(
        client.files,
        "download",
        side_effect=PermissionDenied("forbidden"),
    ):
        with pytest.raises(PlatformError, match="append"):
            backend.append_file(path, "payload")

    assert client.files.calls == []


def test_volume_alias_outputs_canonical_path() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    alias = "dbfs:/Volumes/main/default/logs/a.txt"
    backend.write_file(alias, "a")
    assert backend.get_file_info(alias).path == f"{ROOT}/a.txt"


def test_external_copy_and_move_to_same_path_are_noops() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    path = f"{ROOT}/same.txt"
    backend.write_file(path, "value")
    calls_before = list(client.files.calls)

    backend.copy_file(path, path)
    backend.move_file(path, path)

    assert client.files.calls == calls_before
    assert backend.read_file(path) == "value"


def test_recursive_delete_removes_files_before_directories() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client, max_delete_workers=2)
    backend.write_file(f"{ROOT}/nested/deep/a.txt", "a")
    backend.write_file(f"{ROOT}/nested/deep/b.txt", "b")
    client.files.calls.clear()

    backend.delete_folder(f"{ROOT}/nested", recursive=True)

    delete_indexes = [
        index
        for index, call in enumerate(client.files.calls)
        if call[0] in {"delete", "delete_directory"}
    ]
    directory_indexes = [
        index
        for index, call in enumerate(client.files.calls)
        if call[0] == "delete_directory"
    ]
    assert delete_indexes
    assert directory_indexes
    assert min(directory_indexes) > 0
    assert client.files.calls[-1] == ("delete_directory", f"{ROOT}/nested")


def test_external_rejects_raw_cloud_uri_and_volume_root_mutation() -> None:
    backend = DatabricksSdkBackend(FakeWorkspaceClient())
    with pytest.raises(PlatformError, match="external mode requires"):
        backend.read_file("s3://bucket/a.txt")
    with pytest.raises(PlatformError, match="Volume root"):
        backend.create_folder(ROOT)


def test_failed_overwrite_copy_restores_previous_destination() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    source = f"{ROOT}/source.txt"
    target = f"{ROOT}/target.txt"
    backend.write_file(source, "new")
    backend.write_file(target, "old")

    with patch.object(
        backend,
        "_verify",
        side_effect=[PlatformError("verification failed"), None],
    ):
        with pytest.raises(PlatformError, match="verification failed"):
            backend.copy_file(source, target, overwrite=True)

    assert backend.read_file(source) == "new"
    assert backend.read_file(target) == "old"


def test_failed_move_source_delete_retains_source() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    source = f"{ROOT}/source.txt"
    target = f"{ROOT}/target.txt"
    backend.write_file(source, "value")

    original_delete = client.files.delete

    def fail_source_delete(path: str) -> None:
        if path == source:
            raise PermissionDenied("forbidden")
        original_delete(path)

    with patch.object(client.files, "delete", side_effect=fail_source_delete):
        with pytest.raises(PlatformError, match="delete move source"):
            backend.move_file(source, target)

    assert backend.file_exists(source) is True
    assert backend.file_exists(target) is True


def test_not_found_is_the_only_idempotent_sdk_error() -> None:
    client = FakeWorkspaceClient()
    backend = DatabricksSdkBackend(client)
    missing = f"{ROOT}/missing.txt"
    assert backend.file_exists(missing) is False
    backend.delete_file(missing)

    with patch.object(
        client.files,
        "delete",
        side_effect=PermissionDenied("forbidden"),
    ):
        with pytest.raises(PlatformError, match="PermissionDenied"):
            backend.delete_file(missing)
