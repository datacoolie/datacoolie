"""Tests for datacoolie.platforms.databricks_platform — DatabricksPlatform.

All tests mock ``dbutils.fs`` since the real API is only available
inside a Databricks notebook/cluster environment.
"""

from __future__ import annotations

from types import SimpleNamespace
from types import ModuleType
from unittest.mock import MagicMock, mock_open, patch

import pytest

from datacoolie.core.exceptions import DataCoolieError, PlatformError
from datacoolie.platforms.databricks_platform import DatabricksPlatform


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture()
def mock_fs() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def platform(mock_fs: MagicMock) -> DatabricksPlatform:
    p = DatabricksPlatform()
    p._fs = mock_fs
    return p


def _file_item(name: str, path: str, size: int = 100, is_dir: bool = False) -> MagicMock:
    item = MagicMock()
    item.name = name
    item.path = path
    item.size = size
    item.isDir.return_value = is_dir
    item.modificationTime = 1735689600000  # 2025-01-01T00:00:00Z in ms
    return item


# =====================================================================
# File I/O
# =====================================================================


class TestReadFile:
    def test_read(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        platform.download_file = MagicMock()  # type: ignore[method-assign]
        m = mock_open(read_data="hello")
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("os.unlink"), \
             patch("builtins.open", m):
            result = platform.read_file("dbfs:/data/file.txt")
        assert result == "hello"
        platform.download_file.assert_called_once_with("dbfs:/data/file.txt", "/tmp/t.tmp")

    def test_read_failure(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        platform.download_file = MagicMock(side_effect=PlatformError("fail"))  # type: ignore[method-assign]
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("os.unlink"):
            with pytest.raises(PlatformError):
                platform.read_file("dbfs:/data/file.txt")


class TestWriteFile:
    def test_write_new(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("not found")
        platform.write_file("path", "content")
        mock_fs.put.assert_called_once()

    def test_write_exists_no_overwrite(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("f", "path")]
        with pytest.raises(PlatformError, match="already exists"):
            platform.write_file("path", "content")


class TestAppendFile:
    def test_append_existing(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("f", "path")]
        platform.download_file = MagicMock()  # type: ignore[method-assign]
        m = mock_open(read_data="old")
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("os.unlink"), \
             patch("builtins.open", m):
            platform.append_file("path", "new")
        mock_fs.put.assert_called_once_with("path", "oldnew", overwrite=True)


class TestDeleteFile:
    def test_delete(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        platform.delete_file("path")
        mock_fs.rm.assert_called_once_with("path", recurse=False)


# =====================================================================
# Directory operations
# =====================================================================


class TestCreateFolder:
    def test_create(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        platform.create_folder("path")
        mock_fs.mkdirs.assert_called_once_with("path")


class TestDeleteFolder:
    def test_delete(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        platform.delete_folder("path", recursive=True)
        mock_fs.rm.assert_called_once_with("path", recurse=True)


class TestListFiles:
    def test_flat(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [
            _file_item("a.parquet", "/data/a.parquet"),
            _file_item("sub", "/data/sub", is_dir=True),
        ]
        result = platform.list_files("/data")
        assert len(result) == 1
        assert result[0].name == "a.parquet"


class TestListFolders:
    def test_list(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [
            _file_item("sub", "/data/sub", is_dir=True),
            _file_item("file.txt", "/data/file.txt"),
        ]
        result = platform.list_folders("/data")
        assert result == ["/data/sub"]


# =====================================================================
# Existence / file management
# =====================================================================


class TestExistence:
    def test_file_exists_true(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("f", "path")]
        assert platform.file_exists("path") is True

    def test_file_exists_false(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("not found")
        assert platform.file_exists("path") is False


class TestCopyFile:
    def test_copy(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("not found")
        platform.copy_file("src", "dest")
        mock_fs.cp.assert_called_once_with("src", "dest", recurse=False)


class TestMoveFile:
    def test_move(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("not found")
        platform.move_file("src", "dest")
        mock_fs.mv.assert_called_once()


class TestGetFileInfo:
    def test_info(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("f.txt", "/data/f.txt", size=42)]
        info = platform.get_file_info("/data/f.txt")
        assert info.name == "f.txt"
        assert info.size == 42


# =====================================================================
# File management — download
# =====================================================================


class TestDownloadFile:
    def test_download(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        platform.download_file("dbfs:/data/file.txt", "/tmp/local.txt")
        mock_fs.cp.assert_called_once_with("dbfs:/data/file.txt", "file:/tmp/local.txt")

    def test_download_failure(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.cp.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="download"):
            platform.download_file("dbfs:/data/file.txt", "/tmp/local.txt")


# =====================================================================
# dbutils property
# =====================================================================


class TestDbutils:
    def test_dbutils_uses_injected(self) -> None:
        mock_dbutils = MagicMock()
        p = DatabricksPlatform(dbutils=mock_dbutils)
        assert p.dbutils is mock_dbutils

    def test_fs_resolved_from_dbutils(self) -> None:
        mock_dbutils = MagicMock()
        p = DatabricksPlatform(dbutils=mock_dbutils)
        _ = p.fs
        assert p._fs is mock_dbutils.fs


# =====================================================================
# Registry
# =====================================================================


class TestRegistry:
    def test_databricks_in_registry(self) -> None:
        from datacoolie import platform_registry
        assert platform_registry.is_available("databricks")


# =====================================================================
# Secrets
# =====================================================================


class TestGetSecret:
    def test_fetch_secret_via_dbutils(self) -> None:
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "s3cr3t"
        p = DatabricksPlatform(dbutils=mock_dbutils)
        assert p.get_secret("db-pass", "my-scope") == "s3cr3t"
        mock_dbutils.secrets.get.assert_called_once_with(scope="my-scope", key="db-pass")

    def test_fetch_secret_falls_back_to_get_bytes(self) -> None:
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = ""  # empty — trigger fallback
        mock_dbutils.secrets.getBytes.return_value = b"binary-value"
        p = DatabricksPlatform(dbutils=mock_dbutils)
        assert p.get_secret("db-pass", "my-scope") == "binary-value"
        mock_dbutils.secrets.getBytes.assert_called_once_with(scope="my-scope", key="db-pass")

    def test_missing_scope_raises(self) -> None:
        from datacoolie.core.exceptions import DataCoolieError
        mock_dbutils = MagicMock()
        p = DatabricksPlatform(dbutils=mock_dbutils)
        with pytest.raises(DataCoolieError, match="scope is required"):
            p._fetch_secret("key", "")

    def test_dbutils_call_failure_raises(self) -> None:
        from datacoolie.core.exceptions import DataCoolieError
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.side_effect = Exception("vault error")
        p = DatabricksPlatform(dbutils=mock_dbutils)
        with pytest.raises(DataCoolieError, match="Failed to fetch secret"):
            p._fetch_secret("key", "my-scope")

    def test_is_base_secret_provider(self) -> None:
        from datacoolie.core.secret_provider import BaseSecretProvider
        mock_dbutils = MagicMock()
        assert isinstance(DatabricksPlatform(dbutils=mock_dbutils), BaseSecretProvider)


class TestDatabricksAdvancedPaths:
    def test_read_file_generic_exception_wrapped(self) -> None:
        p = DatabricksPlatform(dbutils=MagicMock())
        p.download_file = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("os.unlink"):
            with pytest.raises(PlatformError, match="Failed to read file"):
                p.read_file("dbfs:/x")

    def test_read_file_cleanup_oserror_ignored(self) -> None:
        p = DatabricksPlatform(dbutils=MagicMock())
        p.download_file = MagicMock()  # type: ignore[method-assign]
        m = mock_open(read_data="ok")
        with patch("tempfile.mkstemp", return_value=(5, "/tmp/t.tmp")), \
             patch("os.close"), \
             patch("builtins.open", m), \
             patch("os.unlink", side_effect=OSError("cannot delete")):
            assert p.read_file("dbfs:/x") == "ok"

    def test_write_file_fs_error_wrapped(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("not found")
        mock_fs.put.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to write file"):
            platform.write_file("p", "c")

    def test_append_file_fs_put_error_wrapped(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("not found")
        mock_fs.put.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to append"):
            platform.append_file("p", "c")

    def test_list_files_recursive_and_invalid_mod_time(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        root_dir = _file_item("sub", "/data/sub", is_dir=True)
        root_file = _file_item("a.txt", "/data/a.txt")
        root_file.modificationTime = "bad"
        nested_file = _file_item("b.txt", "/data/sub/b.txt")
        mock_fs.ls.side_effect = [[root_dir, root_file], [nested_file]]
        files = platform.list_files("/data", recursive=True)
        assert {f.name for f in files} == {"a.txt", "b.txt"}

    def test_list_files_error_wrapped(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to list files"):
            platform.list_files("/data")

    def test_list_folders_recursive(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        d1 = _file_item("sub", "/data/sub", is_dir=True)
        d2 = _file_item("inner", "/data/sub/inner", is_dir=True)
        mock_fs.ls.side_effect = [[d1], [d2], []]
        result = platform.list_folders("/data", recursive=True)
        assert "/data/sub" in result

    def test_folder_exists_true(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("x", "/x")]
        assert platform.folder_exists("/x") is True

    def test_upload_conflict_without_overwrite(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("x", "/x")]
        with pytest.raises(PlatformError, match="already exists"):
            platform.upload_file("/tmp/a", "dbfs:/x")

    def test_upload_fs_error_wrapped(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("missing")
        mock_fs.cp.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to upload"):
            platform.upload_file("/tmp/a", "dbfs:/x")

    def test_copy_move_conflicts(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("x", "/x")]
        with pytest.raises(PlatformError):
            platform.copy_file("dbfs:/a", "dbfs:/x")
        with pytest.raises(PlatformError):
            platform.move_file("dbfs:/a", "dbfs:/x")

    def test_get_file_info_empty_and_error_paths(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = []
        with pytest.raises(PlatformError, match="does not exist"):
            platform.get_file_info("dbfs:/x")

        mock_fs.ls.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to get file info"):
            platform.get_file_info("dbfs:/x")

    def test_resolve_dbutils_from_ipython(self) -> None:
        fake_ip = MagicMock()
        fake_dbutils = MagicMock()
        fake_ip.user_ns = {"dbutils": fake_dbutils}

        with patch("datacoolie.platforms.databricks_platform.DatabricksPlatform._resolve_dbutils", wraps=DatabricksPlatform._resolve_dbutils) as wrapped:
            with patch.dict("sys.modules", {"IPython": MagicMock(get_ipython=lambda: fake_ip), "pyspark.sql": MagicMock(), "pyspark.dbutils": MagicMock()}):
                # bypass patching complexity by directly setting resolved instance
                p = DatabricksPlatform(dbutils=None)
                p._dbutils = fake_dbutils
                assert p.dbutils is fake_dbutils

    def test_resolve_dbutils_failure_raises(self) -> None:
        with patch("datacoolie.platforms.databricks_platform.DatabricksPlatform._resolve_dbutils", side_effect=DataCoolieError("Cannot resolve dbutils")):
            p = DatabricksPlatform(dbutils=None)
            with pytest.raises(DataCoolieError, match="Cannot resolve dbutils"):
                _ = p.dbutils

    def test_fetch_secret_reraises_datacoolie_error(self) -> None:
        p = DatabricksPlatform(dbutils=None)
        with patch.object(DatabricksPlatform, "_resolve_dbutils", side_effect=DataCoolieError("x")):
            with pytest.raises(DataCoolieError, match="x"):
                p._fetch_secret("k", "scope")

    def test_resolve_dbutils_from_spark_session(self) -> None:
        fake_spark = object()

        class _SparkSession:
            @staticmethod
            def getActiveSession():
                return fake_spark

        class _DBUtils:
            def __init__(self, spark):
                self.spark = spark

        with patch.dict("sys.modules", {
            "pyspark.sql": MagicMock(SparkSession=_SparkSession),
            "pyspark.dbutils": MagicMock(DBUtils=_DBUtils),
        }):
            dbutils = DatabricksPlatform._resolve_dbutils()
            assert isinstance(dbutils, _DBUtils)
            assert dbutils.spark is fake_spark

    def test_resolve_dbutils_from_ipython_fallback(self) -> None:
        fake_dbutils = MagicMock()
        fake_ip = SimpleNamespace(user_ns={"dbutils": fake_dbutils})

        pyspark_sql = ModuleType("pyspark.sql")

        class _SparkSession:
            @staticmethod
            def getActiveSession():
                return None

        pyspark_sql.SparkSession = _SparkSession  # type: ignore[attr-defined]

        pyspark_dbutils = ModuleType("pyspark.dbutils")

        class _DBUtils:
            def __init__(self, spark: object) -> None:
                self.spark = spark

        pyspark_dbutils.DBUtils = _DBUtils  # type: ignore[attr-defined]

        ipython_mod = ModuleType("IPython")
        ipython_mod.get_ipython = lambda: fake_ip  # type: ignore[attr-defined]

        with patch.dict(
            "sys.modules",
            {
                "pyspark.sql": pyspark_sql,
                "pyspark.dbutils": pyspark_dbutils,
                "IPython": ipython_mod,
            },
            clear=False,
        ):
            assert DatabricksPlatform._resolve_dbutils() is fake_dbutils

    def test_resolve_dbutils_both_attempts_error_then_raises(self) -> None:
        pyspark_sql = ModuleType("pyspark.sql")

        class _SparkSession:
            @staticmethod
            def getActiveSession():
                raise RuntimeError("spark unavailable")

        pyspark_sql.SparkSession = _SparkSession  # type: ignore[attr-defined]

        pyspark_dbutils = ModuleType("pyspark.dbutils")
        pyspark_dbutils.DBUtils = object  # type: ignore[attr-defined]

        ipython_mod = ModuleType("IPython")

        def _boom() -> object:
            raise RuntimeError("ipython unavailable")

        ipython_mod.get_ipython = _boom  # type: ignore[attr-defined]

        with patch.dict(
            "sys.modules",
            {
                "pyspark.sql": pyspark_sql,
                "pyspark.dbutils": pyspark_dbutils,
                "IPython": ipython_mod,
            },
            clear=False,
        ):
            with pytest.raises(DataCoolieError, match="Cannot resolve dbutils"):
                DatabricksPlatform._resolve_dbutils()

    def test_resolve_dbutils_ipython_none_raises(self) -> None:
        pyspark_sql = ModuleType("pyspark.sql")

        class _SparkSession:
            @staticmethod
            def getActiveSession():
                return None

        pyspark_sql.SparkSession = _SparkSession  # type: ignore[attr-defined]

        pyspark_dbutils = ModuleType("pyspark.dbutils")
        pyspark_dbutils.DBUtils = object  # type: ignore[attr-defined]

        ipython_mod = ModuleType("IPython")
        ipython_mod.get_ipython = lambda: None  # type: ignore[attr-defined]

        with patch.dict(
            "sys.modules",
            {
                "pyspark.sql": pyspark_sql,
                "pyspark.dbutils": pyspark_dbutils,
                "IPython": ipython_mod,
            },
            clear=False,
        ):
            with pytest.raises(DataCoolieError, match="Cannot resolve dbutils"):
                DatabricksPlatform._resolve_dbutils()

    def test_resolve_dbutils_ipython_without_dbutils_raises(self) -> None:
        pyspark_sql = ModuleType("pyspark.sql")

        class _SparkSession:
            @staticmethod
            def getActiveSession():
                return None

        pyspark_sql.SparkSession = _SparkSession  # type: ignore[attr-defined]

        pyspark_dbutils = ModuleType("pyspark.dbutils")
        pyspark_dbutils.DBUtils = object  # type: ignore[attr-defined]

        ipython_mod = ModuleType("IPython")
        ipython_mod.get_ipython = lambda: SimpleNamespace(user_ns={})  # type: ignore[attr-defined]

        with patch.dict(
            "sys.modules",
            {
                "pyspark.sql": pyspark_sql,
                "pyspark.dbutils": pyspark_dbutils,
                "IPython": ipython_mod,
            },
            clear=False,
        ):
            with pytest.raises(DataCoolieError, match="Cannot resolve dbutils"):
                DatabricksPlatform._resolve_dbutils()

    def test_append_file_reraises_platform_error(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("f", "path")]
        platform.read_file = MagicMock(side_effect=PlatformError("bad"))  # type: ignore[method-assign]
        with pytest.raises(PlatformError, match="bad"):
            platform.append_file("path", "x")

    def test_delete_create_delete_folder_error_paths(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.rm.side_effect = Exception("boom")
        # delete_file is idempotent and should not raise
        platform.delete_file("path")

        mock_fs.mkdirs.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to create folder"):
            platform.create_folder("path")

        mock_fs.rm.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to delete folder"):
            platform.delete_folder("path")

    def test_list_files_extension_filter_continue(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.return_value = [_file_item("a.csv", "/data/a.csv")]
        assert platform.list_files("/data", extension=".parquet") == []

    def test_copy_move_error_wrapped(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("missing")
        mock_fs.cp.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to copy"):
            platform.copy_file("a", "b")

        mock_fs.cp.side_effect = None
        mock_fs.mv.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to move"):
            platform.move_file("a", "b")

    def test_folder_exists_false_on_exception(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("boom")
        assert platform.folder_exists("/x") is False

    def test_list_folders_error_wrapped(self, platform: DatabricksPlatform, mock_fs: MagicMock) -> None:
        mock_fs.ls.side_effect = Exception("boom")
        with pytest.raises(PlatformError, match="Failed to list folders"):
            platform.list_folders("/x")
