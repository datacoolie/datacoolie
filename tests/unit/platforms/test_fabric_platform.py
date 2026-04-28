"""Tests for datacoolie.platforms.fabric_platform — FabricPlatform.

All tests mock ``notebookutils.fs`` since the real SDK is only
available inside a Fabric notebook environment.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, mock_open, patch

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms.fabric_platform import FabricPlatform


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture()
def mock_fs() -> MagicMock:
    """Create a mock ``notebookutils.fs`` object."""
    return MagicMock()


@pytest.fixture()
def platform(mock_fs: MagicMock) -> FabricPlatform:
    """FabricPlatform with pre-injected mock fs."""
    p = FabricPlatform()
    p._fs = mock_fs
    return p


def _file_item(name: str, path: str, size: int = 100, is_dir: bool = False) -> SimpleNamespace:
    return SimpleNamespace(name=name, path=path, size=size, isDir=is_dir, modifyTime=1735689600000)  # 2025-01-01T00:00:00Z in ms


# =====================================================================
# File I/O
# =====================================================================


class TestReadFile:
    def test_read(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        platform.download_file = MagicMock()  # type: ignore[method-assign]
        m = mock_open(read_data="hello")
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("os.unlink"), \
             patch("builtins.open", m):
            result = platform.read_file("abfss://c@s/file.txt")
        assert result == "hello"
        mock_fs.head.assert_not_called()
        platform.download_file.assert_called_once_with("abfss://c@s/file.txt", "/tmp/t.tmp")

    def test_read_failure(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        platform.download_file = MagicMock(side_effect=PlatformError("fail"))  # type: ignore[method-assign]
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("os.unlink"):
            with pytest.raises(PlatformError):
                platform.read_file("abfss://c@s/file.txt")


class TestWriteFile:
    def test_write_new(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.return_value = False
        platform.write_file("path", "content")
        mock_fs.put.assert_called_once_with("path", "content", overwrite=False)

    def test_write_exists_no_overwrite(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.return_value = True
        with pytest.raises(PlatformError, match="already exists"):
            platform.write_file("path", "content")


class TestAppendFile:
    def test_append(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        platform.append_file("path", "data")
        mock_fs.append.assert_called_once_with("path", "data", createFileIfNotExists=True)


class TestDeleteFile:
    def test_delete(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        platform.delete_file("path")
        mock_fs.rm.assert_called_once_with("path", recurse=False)


# =====================================================================
# Directory operations
# =====================================================================


class TestCreateFolder:
    def test_create(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        platform.create_folder("path")
        mock_fs.mkdirs.assert_called_once_with("path")


class TestDeleteFolder:
    def test_delete_recursive(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        platform.delete_folder("path", recursive=True)
        mock_fs.rm.assert_called_once_with("path", recurse=True)


class TestListFiles:
    def test_flat(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [
            _file_item("a.parquet", "/data/a.parquet"),
            _file_item("sub", "/data/sub", is_dir=True),
        ]
        result = platform.list_files("/data")
        assert len(result) == 1
        assert result[0].name == "a.parquet"

    def test_extension_filter(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [
            _file_item("a.csv", "/data/a.csv"),
            _file_item("b.parquet", "/data/b.parquet"),
        ]
        result = platform.list_files("/data", extension=".parquet")
        assert len(result) == 1


class TestListFolders:
    def test_list(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [
            _file_item("sub", "/data/sub", is_dir=True),
            _file_item("file.txt", "/data/file.txt"),
        ]
        result = platform.list_folders("/data")
        assert result == ["/data/sub"]


# =====================================================================
# Existence checks
# =====================================================================


class TestExistence:
    def test_file_exists(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.return_value = True
        assert platform.file_exists("path") is True

    def test_folder_exists(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.return_value = False
        assert platform.folder_exists("path") is False


# =====================================================================
# File management
# =====================================================================


class TestCopyFile:
    def test_copy(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.return_value = False
        platform.copy_file("src", "dest")
        mock_fs.cp.assert_called_once_with("src", "dest", recurse=False)


class TestMoveFile:
    def test_move(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.return_value = False
        platform.move_file("src", "dest")
        mock_fs.mv.assert_called_once()


class TestGetFileInfo:
    def test_info(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("f.txt", "/data/f.txt", size=42)]
        info = platform.get_file_info("/data/f.txt")
        assert info.name == "f.txt"
        assert info.size == 42


# =====================================================================
# File management — download
# =====================================================================


class TestDownloadFile:
    def test_download(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        platform.download_file("abfss://c@s/file.txt", "/tmp/local.txt")
        mock_fs.cp.assert_called_once_with("abfss://c@s/file.txt", "file:///tmp/local.txt")

    def test_download_failure(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.cp.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="download"):
            platform.download_file("abfss://c@s/file.txt", "/tmp/local.txt")


# =====================================================================
# notebookutils property
# =====================================================================


class TestNotebookutils:
    def test_lazy_import(self) -> None:
        mock_nu = MagicMock()
        with patch.dict("sys.modules", {"notebookutils": mock_nu}):
            p = FabricPlatform()
            assert p.notebookutils is mock_nu

    def test_fs_resolved_from_notebookutils(self) -> None:
        mock_nu = MagicMock()
        with patch.dict("sys.modules", {"notebookutils": mock_nu}):
            p = FabricPlatform()
            _ = p.fs
            assert p._fs is mock_nu.fs


# =====================================================================
# Lazy import guard
# =====================================================================


class TestLazyImport:
    def test_missing_notebookutils_raises(self) -> None:
        p = FabricPlatform()
        with patch.dict("sys.modules", {"notebookutils": None}):
            with pytest.raises(PlatformError, match="notebookutils"):
                _ = p.fs


# =====================================================================
# Registry
# =====================================================================


class TestRegistry:
    def test_fabric_in_registry(self) -> None:
        from datacoolie import platform_registry
        assert platform_registry.is_available("fabric")


# =====================================================================
# Secrets
# =====================================================================


class TestGetSecret:
    def test_fetch_secret_via_notebookutils(self) -> None:
        mock_nu = MagicMock()
        mock_nu.credentials.getSecret.return_value = "tok3n"
        with patch.dict("sys.modules", {"notebookutils": mock_nu}):
            p = FabricPlatform()
            assert p.get_secret("my-secret", "https://myvault.vault.azure.net/") == "tok3n"
            mock_nu.credentials.getSecret.assert_called_once_with(
                "https://myvault.vault.azure.net/", "my-secret"
            )

    def test_missing_vault_url_raises(self) -> None:
        from datacoolie.core.exceptions import DataCoolieError
        p = FabricPlatform()
        with pytest.raises(DataCoolieError, match="vault_url is required"):
            p._fetch_secret("key", "")

    def test_notebookutils_unavailable_raises(self) -> None:
        from datacoolie.core.exceptions import DataCoolieError
        p = FabricPlatform()
        with patch.dict("sys.modules", {"notebookutils": None}):
            with pytest.raises(DataCoolieError, match="notebookutils"):
                p._fetch_secret("key", "https://myvault.vault.azure.net/")

    def test_is_base_secret_provider(self) -> None:
        from datacoolie.core.secret_provider import BaseSecretProvider
        assert isinstance(FabricPlatform(), BaseSecretProvider)


class TestFabricAdvancedPaths:
    def test_read_file_generic_exception_wrapped(self) -> None:
        p = FabricPlatform()
        p.download_file = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("os.unlink"):
            with pytest.raises(PlatformError, match="Failed to read file"):
                p.read_file("abfss://x")

    def test_read_file_cleanup_oserror_ignored(self) -> None:
        p = FabricPlatform()
        p.download_file = MagicMock()  # type: ignore[method-assign]
        m = mock_open(read_data="ok")
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("builtins.open", m), \
             patch("os.unlink", side_effect=OSError("cannot delete")):
            assert p.read_file("abfss://x") == "ok"

    def test_write_append_create_delete_error_wrapped(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.return_value = False
        mock_fs.put.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to write file"):
            platform.write_file("p", "c")

        mock_fs.put.side_effect = None
        mock_fs.append.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to append"):
            platform.append_file("p", "c")

        mock_fs.append.side_effect = None
        mock_fs.mkdirs.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to create folder"):
            platform.create_folder("d")

        mock_fs.mkdirs.side_effect = None
        mock_fs.rm.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to delete folder"):
            platform.delete_folder("d")

    def test_list_files_recursive_and_invalid_modify_time(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        root_dir = _file_item("sub", "/data/sub", is_dir=True)
        root_file = _file_item("a.txt", "/data/a.txt")
        root_file.modifyTime = "bad"
        nested_file = _file_item("b.txt", "/data/sub/b.txt")
        mock_fs.ls.side_effect = [[root_dir, root_file], [nested_file]]
        files = platform.list_files("/data", recursive=True)
        assert {f.name for f in files} == {"a.txt", "b.txt"}

    def test_list_files_and_folders_errors_wrapped(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to list files"):
            platform.list_files("/data")
        with pytest.raises(PlatformError, match="Failed to list folders"):
            platform.list_folders("/data")

    def test_list_folders_recursive(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        d1 = _file_item("sub", "/data/sub", is_dir=True)
        d2 = _file_item("inner", "/data/sub/inner", is_dir=True)
        mock_fs.ls.side_effect = [[d1], [d2], []]
        result = platform.list_folders("/data", recursive=True)
        assert "/data/sub" in result

    def test_exists_exception_paths_return_false(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.side_effect = Exception("boom")
        assert platform.file_exists("x") is False
        assert platform.folder_exists("x") is False

    def test_upload_copy_move_conflict_and_error_paths(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.exists.return_value = True
        with pytest.raises(PlatformError, match="already exists"):
            platform.upload_file("/tmp/a", "abfss://x")
        with pytest.raises(PlatformError, match="already exists"):
            platform.copy_file("abfss://a", "abfss://x")
        with pytest.raises(PlatformError, match="already exists"):
            platform.move_file("abfss://a", "abfss://x")

        mock_fs.exists.return_value = False
        mock_fs.cp.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to upload"):
            platform.upload_file("/tmp/a", "abfss://x")
        with pytest.raises(PlatformError, match="Failed to copy"):
            platform.copy_file("abfss://a", "abfss://x")
        with pytest.raises(PlatformError, match="Failed to download"):
            platform.download_file("abfss://a", "/tmp/a")

        mock_fs.cp.side_effect = None
        mock_fs.mv.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to move"):
            platform.move_file("abfss://a", "abfss://x")

    def test_get_file_info_empty_and_error_paths(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = []
        with pytest.raises(PlatformError, match="does not exist"):
            platform.get_file_info("abfss://x")

        mock_fs.ls.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to get file info"):
            platform.get_file_info("abfss://x")

    def test_notebookutils_cached_branch(self) -> None:
        p = FabricPlatform()
        cached = MagicMock()
        p._nu = cached
        assert p.notebookutils is cached

    def test_fetch_secret_exception_wrapped(self) -> None:
        mock_nu = MagicMock()
        mock_nu.credentials.getSecret.side_effect = RuntimeError("boom")
        with patch.dict("sys.modules", {"notebookutils": mock_nu}):
            p = FabricPlatform()
            with pytest.raises(Exception, match="Failed to fetch secret"):
                p._fetch_secret("k", "https://vault")

    def test_delete_file_exception_is_idempotent(self, platform: FabricPlatform, mock_fs: MagicMock) -> None:
        mock_fs.rm.side_effect = Exception("boom")
        platform.delete_file("abfss://x")
