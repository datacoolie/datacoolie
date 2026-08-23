"""Native NotebookUtils backend safety and compatibility tests."""

from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._fabric.notebookutils_backend import NotebookUtilsBackend


class NotFoundError(Exception):
    status_code = 404


@pytest.fixture()
def fs() -> MagicMock:
    value = MagicMock()
    value.exists.return_value = False
    return value


@pytest.fixture()
def backend(fs: MagicMock) -> NotebookUtilsBackend:
    credentials = SimpleNamespace(getSecret=MagicMock(return_value="value"))
    return NotebookUtilsBackend(SimpleNamespace(fs=fs, credentials=credentials))


def test_relative_paths_remain_native_compatible(
    backend: NotebookUtilsBackend,
    fs: MagicMock,
) -> None:
    backend.write_file("Files/output.txt", "value")
    fs.put.assert_called_once_with("Files/output.txt", "value", overwrite=False)


def test_delete_suppresses_only_not_found(
    backend: NotebookUtilsBackend,
    fs: MagicMock,
) -> None:
    fs.rm.side_effect = NotFoundError
    backend.delete_file("Files/missing.txt")

    fs.rm.side_effect = PermissionError("denied")
    with pytest.raises(PlatformError, match="Failed to delete file"):
        backend.delete_file("Files/protected.txt")


def test_exists_does_not_hide_permission_failure(
    backend: NotebookUtilsBackend,
    fs: MagicMock,
) -> None:
    fs.exists.side_effect = PermissionError("denied")
    with pytest.raises(PlatformError, match="existence"):
        backend.file_exists("Files/protected.txt")


def test_file_and_folder_exists_both_delegate_to_fs_exists(
    backend: NotebookUtilsBackend,
    fs: MagicMock,
) -> None:
    fs.exists.side_effect = [True, False]

    assert backend.file_exists("Files/value") is True
    assert backend.folder_exists("Files/value") is False

    assert fs.exists.call_args_list == [call("Files/value"), call("Files/value")]


def test_recursive_lists_use_shared_tree_traversal(
    backend: NotebookUtilsBackend,
    fs: MagicMock,
) -> None:
    folder = SimpleNamespace(
        name="nested", path="Files/nested", size=0, isDir=True, modifyTime=None
    )
    root_file = SimpleNamespace(
        name="root.jsonl", path="Files/root.jsonl", size=1, isDir=False, modifyTime=None
    )
    nested_file = SimpleNamespace(
        name="nested.jsonl",
        path="Files/nested/nested.jsonl",
        size=1,
        isDir=False,
        modifyTime=None,
    )
    fs.ls.side_effect = lambda path: {
        "Files": [folder, root_file],
        "Files/nested": [nested_file],
    }[path]

    files = backend.list_files("Files", recursive=True, extension=".jsonl")
    folders = backend.list_folders("Files", recursive=True)

    assert {item.path for item in files} == {"Files/root.jsonl", "Files/nested/nested.jsonl"}
    assert folders == ["Files/nested"]


def test_move_explicitly_creates_parent_across_notebook_runtimes(
    backend: NotebookUtilsBackend,
    fs: MagicMock,
) -> None:
    backend.move_file("Files/source.txt", "Files/new/deep/destination.txt")

    fs.mv.assert_called_once_with(
        "Files/source.txt",
        "Files/new/deep/destination.txt",
        create_path=True,
        overwrite=False,
    )


def test_onelake_managed_root_is_protected(
    backend: NotebookUtilsBackend,
    fs: MagicMock,
) -> None:
    root = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files"
    with pytest.raises(PlatformError, match="only below"):
        backend.delete_folder(root, recursive=True)
    fs.rm.assert_not_called()


def test_secret_error_does_not_include_provider_exception_text(
    backend: NotebookUtilsBackend,
) -> None:
    backend.notebookutils.credentials.getSecret.side_effect = RuntimeError("token-value")
    with pytest.raises(PlatformError) as captured:
        backend.fetch_secret("key", "https://demo.vault.azure.net")
    assert "token-value" not in str(captured.value)
