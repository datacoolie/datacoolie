"""Native dbutils backend contract tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, mock_open, patch

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._databricks.dbutils_backend import DbutilsBackend


def _item(
    name: str,
    path: str,
    *,
    directory: bool = False,
    size: int = 10,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        path=path,
        size=size,
        isDir=lambda: directory,
        modificationTime=1735689600000,
    )


def test_volume_full_reads_use_unbounded_python_io() -> None:
    backend = DbutilsBackend(MagicMock())
    with patch("builtins.open", mock_open(read_data=b"full-binary")) as opened:
        assert (
            backend.read_bytes("dbfs:/Volumes/main/default/logs/value.bin")
            == b"full-binary"
        )
    opened.assert_called_once_with(
        "/Volumes/main/default/logs/value.bin",
        "rb",
    )


def test_volume_directory_management_uses_dbutils() -> None:
    fs = MagicMock()
    backend = DbutilsBackend(MagicMock(fs=fs))
    backend.create_folder("/Volumes/main/default/logs/nested")
    fs.mkdirs.assert_called_once_with("/Volumes/main/default/logs/nested")


def test_volume_append_uses_read_modify_write_for_fuse_compatibility(tmp_path) -> None:
    local_file = tmp_path / "value.txt"
    local_file.write_text("first", encoding="utf-8")
    backend = DbutilsBackend(MagicMock())
    with (
        patch.object(backend, "_local_path", return_value=str(local_file)),
        patch(
            "datacoolie.platforms._databricks.dbutils_backend.parent_path",
            return_value=str(tmp_path),
        ),
    ):
        backend.append_file("/Volumes/main/default/logs/value.txt", "-second")

    assert local_file.read_text(encoding="utf-8") == "first-second"


def test_native_raw_cloud_move_creates_parent_and_preserves_fix() -> None:
    fs = MagicMock()
    fs.ls.side_effect = FileNotFoundError("missing")
    backend = DbutilsBackend(MagicMock(fs=fs))
    backend.move_file(
        "s3://bucket/source.txt",
        "s3://bucket/new/deep/destination.txt",
    )
    fs.mkdirs.assert_called_once_with("s3://bucket/new/deep")
    fs.mv.assert_called_once_with(
        "s3://bucket/source.txt",
        "s3://bucket/new/deep/destination.txt",
        recurse=False,
    )


def test_native_raw_cloud_append_creates_parent_only_after_missing_read() -> None:
    fs = MagicMock()
    fs.cp.side_effect = [FileNotFoundError("missing"), None]
    backend = DbutilsBackend(MagicMock(fs=fs))

    backend.append_file("s3://bucket/new/deep/value.txt", "payload")

    fs.mkdirs.assert_called_once_with("s3://bucket/new/deep")
    assert fs.cp.call_count == 2


def test_native_raw_cloud_append_does_not_treat_permission_as_missing() -> None:
    fs = MagicMock()
    fs.cp.side_effect = PermissionError("forbidden")
    backend = DbutilsBackend(MagicMock(fs=fs))

    with pytest.raises(PlatformError, match="Failed to read file"):
        backend.append_file("s3://bucket/value.txt", "payload")

    fs.mkdirs.assert_not_called()


def test_native_copy_and_move_to_same_path_are_noops() -> None:
    fs = MagicMock()
    backend = DbutilsBackend(MagicMock(fs=fs))
    path = "s3://bucket/source.txt"

    backend.copy_file(path, path)
    backend.move_file(path, path)

    fs.assert_not_called()


def test_native_listing_is_iterative_and_canonicalizes_volume_aliases() -> None:
    fs = MagicMock()
    root = _item(
        "nested/",
        "dbfs:/Volumes/main/default/logs/nested/",
        directory=True,
    )
    child = _item(
        "a.json",
        "dbfs:/Volumes/main/default/logs/nested/a.json",
    )
    fs.ls.side_effect = [[root], [child]]
    backend = DbutilsBackend(MagicMock(fs=fs), max_list_workers=1)
    result = backend.list_files(
        "/Volumes/main/default/logs",
        recursive=True,
    )
    assert [item.path for item in result] == [
        "/Volumes/main/default/logs/nested/a.json"
    ]


def test_volume_listing_defaults_to_posix_entries_when_volume_is_mounted(
    tmp_path,
) -> None:
    root = tmp_path / "logs"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / "root.txt").write_text("root", encoding="utf-8")
    (nested / "child.json").write_text("child", encoding="utf-8")
    fs = MagicMock()
    backend = DbutilsBackend(
        MagicMock(fs=fs),
        max_list_workers=1,
    )

    with patch.object(backend, "_local_path", return_value=str(root)):
        files = backend.list_files(
            "/Volumes/main/default/logs",
            recursive=True,
            extension=".json",
        )
        folders = backend.list_folders(
            "/Volumes/main/default/logs",
            recursive=True,
        )

    assert [item.path for item in files] == [
        "/Volumes/main/default/logs/nested/child.json"
    ]
    assert folders == ["/Volumes/main/default/logs/nested"]
    fs.ls.assert_not_called()


def test_volume_exists_returns_false_only_for_missing_path() -> None:
    backend = DbutilsBackend(MagicMock())
    path = "/Volumes/main/default/logs/value.txt"
    with patch.object(
        backend,
        "_local_path",
        return_value="C:/missing/value.txt",
    ):
        assert backend.file_exists(path) is False

    with patch(
        "datacoolie.platforms._databricks.dbutils_backend.os.stat",
        side_effect=PermissionError("forbidden"),
    ):
        with pytest.raises(PlatformError, match="Failed to inspect"):
            backend.file_exists(path)


def test_posix_volume_listing_preserves_permission_errors() -> None:
    backend = DbutilsBackend(MagicMock(), volume_listing="posix")
    with patch(
        "datacoolie.platforms._databricks.volume_traversal.os.scandir",
        side_effect=PermissionError("forbidden"),
    ):
        with pytest.raises(PlatformError, match="Failed to list files"):
            backend.list_files("/Volumes/main/default/logs", recursive=True)


def test_cross_volume_move_uses_dbutils_and_cleans_backup() -> None:
    fs = MagicMock()
    backend = DbutilsBackend(MagicMock(fs=fs))
    source = "/Volumes/main/default/source/a.txt"
    target = "/Volumes/main/other/target/a.txt"
    with patch.object(backend, "file_exists", return_value=True):
        backend.move_file(source, target, overwrite=True)

    fs.mkdirs.assert_called_once_with("/Volumes/main/other/target")
    move_calls = [call for call in fs.mv.call_args_list]
    assert move_calls[0].args[:1] == (target,)
    assert move_calls[1].args == (source, target)
    assert fs.rm.call_count == 1
    assert fs.rm.call_args.args[0].startswith(f"{target}.dc-backup-")


def test_cross_volume_move_restores_overwritten_target_on_failure() -> None:
    fs = MagicMock()
    fs.mv.side_effect = [None, PermissionError("move failed"), None]
    backend = DbutilsBackend(MagicMock(fs=fs))
    source = "/Volumes/main/default/source/a.txt"
    target = "/Volumes/main/other/target/a.txt"

    with patch.object(backend, "file_exists", return_value=True):
        with pytest.raises(PlatformError, match="Failed to move file"):
            backend.move_file(source, target, overwrite=True)

    assert fs.mv.call_count == 3
    assert fs.mv.call_args_list[2].args[0].startswith(f"{target}.dc-backup-")
    fs.rm.assert_not_called()


def test_native_secret_and_errors_do_not_expose_secret_value() -> None:
    dbutils = MagicMock()
    dbutils.secrets.get.return_value = "secret-value"
    backend = DbutilsBackend(dbutils)
    assert backend.fetch_secret("key", "scope") == "secret-value"

    dbutils.secrets.get.side_effect = RuntimeError("secret-value")
    with pytest.raises(PlatformError) as error:
        backend.fetch_secret("key", "scope")
    assert "secret-value" not in str(error.value)


def test_native_rejects_deprecated_dbfs_paths_before_fs_call() -> None:
    fs = MagicMock()
    backend = DbutilsBackend(MagicMock(fs=fs))
    with pytest.raises(PlatformError, match="DBFS root and mounts"):
        backend.read_file("dbfs:/FileStore/a.txt")
    fs.assert_not_called()


def test_native_delete_does_not_swallow_service_errors() -> None:
    fs = MagicMock()
    fs.rm.side_effect = PermissionError("forbidden")
    backend = DbutilsBackend(MagicMock(fs=fs))
    with pytest.raises(PlatformError, match="Failed to delete"):
        backend.delete_file("s3://bucket/a.txt")


def test_native_exists_does_not_swallow_service_errors() -> None:
    fs = MagicMock()
    fs.ls.side_effect = PermissionError("forbidden")
    backend = DbutilsBackend(MagicMock(fs=fs))
    with pytest.raises(PlatformError, match="Failed to inspect"):
        backend.file_exists("s3://bucket/a.txt")
