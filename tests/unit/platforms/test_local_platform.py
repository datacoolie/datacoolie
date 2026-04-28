"""Tests for datacoolie.platforms.local_platform — LocalPlatform."""

from __future__ import annotations

from pathlib import Path
import os
from unittest.mock import patch

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms.local_platform import LocalPlatform


@pytest.fixture()
def platform(tmp_path: Path) -> LocalPlatform:
    """LocalPlatform rooted at a temporary directory."""
    return LocalPlatform(base_path=str(tmp_path))


@pytest.fixture()
def root(tmp_path: Path) -> Path:
    """Convenience alias for the tmp_path root."""
    return tmp_path


# =====================================================================
# File I/O
# =====================================================================


class TestReadFile:
    def test_read_existing(self, platform: LocalPlatform, root: Path) -> None:
        (root / "hello.txt").write_text("world", encoding="utf-8")
        assert platform.read_file("hello.txt") == "world"

    def test_read_nonexistent_raises(self, platform: LocalPlatform) -> None:
        with pytest.raises(PlatformError, match="not found"):
            platform.read_file("does_not_exist.txt")


class TestWriteFile:
    def test_write_new(self, platform: LocalPlatform, root: Path) -> None:
        platform.write_file("out.txt", "data")
        assert (root / "out.txt").read_text(encoding="utf-8") == "data"

    def test_write_creates_parents(self, platform: LocalPlatform, root: Path) -> None:
        platform.write_file("a/b/c.txt", "nested")
        assert (root / "a" / "b" / "c.txt").read_text(encoding="utf-8") == "nested"

    def test_overwrite_false_raises(self, platform: LocalPlatform, root: Path) -> None:
        (root / "exist.txt").write_text("old", encoding="utf-8")
        with pytest.raises(PlatformError, match="already exists"):
            platform.write_file("exist.txt", "new")

    def test_overwrite_true(self, platform: LocalPlatform, root: Path) -> None:
        (root / "exist.txt").write_text("old", encoding="utf-8")
        platform.write_file("exist.txt", "new", overwrite=True)
        assert (root / "exist.txt").read_text(encoding="utf-8") == "new"


class TestAppendFile:
    def test_append_existing(self, platform: LocalPlatform, root: Path) -> None:
        (root / "log.txt").write_text("line1\n", encoding="utf-8")
        platform.append_file("log.txt", "line2\n")
        assert (root / "log.txt").read_text(encoding="utf-8") == "line1\nline2\n"

    def test_append_creates_file(self, platform: LocalPlatform, root: Path) -> None:
        platform.append_file("new.txt", "first")
        assert (root / "new.txt").read_text(encoding="utf-8") == "first"


class TestDeleteFile:
    def test_delete_existing(self, platform: LocalPlatform, root: Path) -> None:
        (root / "rm.txt").write_text("x", encoding="utf-8")
        platform.delete_file("rm.txt")
        assert not (root / "rm.txt").exists()

    def test_delete_nonexistent_is_noop(self, platform: LocalPlatform) -> None:
        platform.delete_file("ghost.txt")  # should not raise


# =====================================================================
# Directory operations
# =====================================================================


class TestCreateFolder:
    def test_create(self, platform: LocalPlatform, root: Path) -> None:
        platform.create_folder("sub/dir")
        assert (root / "sub" / "dir").is_dir()

    def test_create_existing_is_noop(self, platform: LocalPlatform, root: Path) -> None:
        (root / "existing_dir").mkdir()
        platform.create_folder("existing_dir")  # no error


class TestDeleteFolder:
    def test_delete_empty(self, platform: LocalPlatform, root: Path) -> None:
        (root / "empty").mkdir()
        platform.delete_folder("empty")
        assert not (root / "empty").exists()

    def test_delete_nonempty_non_recursive_raises(
        self, platform: LocalPlatform, root: Path
    ) -> None:
        d = root / "nonempty"
        d.mkdir()
        (d / "file.txt").write_text("x", encoding="utf-8")
        with pytest.raises(PlatformError):
            platform.delete_folder("nonempty")

    def test_delete_recursive(self, platform: LocalPlatform, root: Path) -> None:
        d = root / "tree"
        (d / "a").mkdir(parents=True)
        (d / "a" / "f.txt").write_text("x", encoding="utf-8")
        platform.delete_folder("tree", recursive=True)
        assert not d.exists()

    def test_delete_nonexistent_is_noop(self, platform: LocalPlatform) -> None:
        platform.delete_folder("ghost_dir")  # no error


class TestListFiles:
    def test_list_empty_dir(self, platform: LocalPlatform, root: Path) -> None:
        (root / "empty").mkdir()
        assert platform.list_files("empty") == []

    def test_list_flat(self, platform: LocalPlatform, root: Path) -> None:
        d = root / "data"
        d.mkdir()
        (d / "a.csv").write_text("a", encoding="utf-8")
        (d / "b.csv").write_text("b", encoding="utf-8")
        result = platform.list_files("data")
        assert len(result) == 2
        names = {f.name for f in result}
        assert names == {"a.csv", "b.csv"}

    def test_list_recursive(self, platform: LocalPlatform, root: Path) -> None:
        d = root / "nested"
        (d / "sub").mkdir(parents=True)
        (d / "top.txt").write_text("t", encoding="utf-8")
        (d / "sub" / "bottom.txt").write_text("b", encoding="utf-8")
        result = platform.list_files("nested", recursive=True)
        assert len(result) == 2

    def test_list_with_extension(self, platform: LocalPlatform, root: Path) -> None:
        d = root / "mixed"
        d.mkdir()
        (d / "a.parquet").write_text("p", encoding="utf-8")
        (d / "b.json").write_text("j", encoding="utf-8")
        result = platform.list_files("mixed", extension=".parquet")
        assert len(result) == 1
        assert result[0].name == "a.parquet"

    def test_not_a_directory_raises(self, platform: LocalPlatform, root: Path) -> None:
        (root / "file.txt").write_text("x", encoding="utf-8")
        with pytest.raises(PlatformError, match="not a directory"):
            platform.list_files("file.txt")

    def test_file_info_structure(self, platform: LocalPlatform, root: Path) -> None:
        d = root / "info"
        d.mkdir()
        f = d / "sample.txt"
        f.write_text("hello", encoding="utf-8")
        result = platform.list_files("info")
        assert len(result) == 1
        info = result[0]
        assert info.name == "sample.txt"
        assert info.size == 5
        assert info.is_dir is False
        assert info.modification_time is not None


class TestListFolders:
    def test_list(self, platform: LocalPlatform, root: Path) -> None:
        d = root / "parent"
        (d / "child_a").mkdir(parents=True)
        (d / "child_b").mkdir(parents=True)
        result = platform.list_folders("parent")
        assert len(result) == 2

    def test_not_a_directory_raises(self, platform: LocalPlatform, root: Path) -> None:
        (root / "file.txt").write_text("x", encoding="utf-8")
        with pytest.raises(PlatformError, match="not a directory"):
            platform.list_folders("file.txt")

    def test_ignores_files_when_listing_folders(self, platform: LocalPlatform, root: Path) -> None:
        d = root / "parent"
        d.mkdir()
        (d / "child").mkdir()
        (d / "file.txt").write_text("x", encoding="utf-8")
        result = platform.list_folders("parent")
        assert len(result) == 1
        assert result[0].endswith("child")


# =====================================================================
# Existence checks
# =====================================================================


class TestFileExists:
    def test_exists(self, platform: LocalPlatform, root: Path) -> None:
        (root / "f.txt").write_text("x", encoding="utf-8")
        assert platform.file_exists("f.txt") is True

    def test_not_exists(self, platform: LocalPlatform) -> None:
        assert platform.file_exists("nope.txt") is False

    def test_dir_returns_false(self, platform: LocalPlatform, root: Path) -> None:
        (root / "mydir").mkdir()
        assert platform.file_exists("mydir") is False


class TestFolderExists:
    def test_exists(self, platform: LocalPlatform, root: Path) -> None:
        (root / "mydir").mkdir()
        assert platform.folder_exists("mydir") is True

    def test_not_exists(self, platform: LocalPlatform) -> None:
        assert platform.folder_exists("nope") is False


# =====================================================================
# File management
# =====================================================================


class TestUploadFile:
    def test_upload(self, platform: LocalPlatform, root: Path, tmp_path: Path) -> None:
        local_file = tmp_path / "upload.txt"
        local_file.write_text("uploaded", encoding="utf-8")
        platform.upload_file(str(local_file), "dest.txt")
        assert (root / "dest.txt").read_text(encoding="utf-8") == "uploaded"

    def test_upload_src_missing_raises(self, platform: LocalPlatform, tmp_path: Path) -> None:
        with pytest.raises(PlatformError, match="not found"):
            platform.upload_file(str(tmp_path / "no.txt"), "dest.txt")

    def test_upload_dest_exists_raises(self, platform: LocalPlatform, root: Path, tmp_path: Path) -> None:
        local_file = tmp_path / "upload.txt"
        local_file.write_text("data", encoding="utf-8")
        (root / "dest.txt").write_text("existing", encoding="utf-8")
        with pytest.raises(PlatformError, match="already exists"):
            platform.upload_file(str(local_file), "dest.txt")

    def test_upload_overwrite(self, platform: LocalPlatform, root: Path, tmp_path: Path) -> None:
        local_file = tmp_path / "upload.txt"
        local_file.write_text("new", encoding="utf-8")
        (root / "dest.txt").write_text("old", encoding="utf-8")
        platform.upload_file(str(local_file), "dest.txt", overwrite=True)
        assert (root / "dest.txt").read_text(encoding="utf-8") == "new"


class TestDownloadFile:
    def test_download(self, platform: LocalPlatform, root: Path, tmp_path: Path) -> None:
        (root / "data.txt").write_text("hello", encoding="utf-8")
        dest = tmp_path / "downloaded.txt"
        platform.download_file("data.txt", str(dest))
        assert dest.read_text(encoding="utf-8") == "hello"

    def test_download_src_missing_raises(self, platform: LocalPlatform, tmp_path: Path) -> None:
        dest = tmp_path / "out.txt"
        with pytest.raises(PlatformError, match="not found"):
            platform.download_file("ghost.txt", str(dest))

    def test_download_creates_parent_dirs(self, platform: LocalPlatform, root: Path, tmp_path: Path) -> None:
        (root / "f.txt").write_text("x", encoding="utf-8")
        dest = tmp_path / "nested" / "dir" / "output.txt"
        platform.download_file("f.txt", str(dest))
        assert dest.read_text(encoding="utf-8") == "x"


class TestCopyFile:
    def test_copy(self, platform: LocalPlatform, root: Path) -> None:
        (root / "src.txt").write_text("data", encoding="utf-8")
        platform.copy_file("src.txt", "dst.txt")
        assert (root / "dst.txt").read_text(encoding="utf-8") == "data"
        assert (root / "src.txt").exists()  # original still there

    def test_copy_src_missing_raises(self, platform: LocalPlatform) -> None:
        with pytest.raises(PlatformError, match="not found"):
            platform.copy_file("no.txt", "dst.txt")

    def test_copy_dest_exists_raises(self, platform: LocalPlatform, root: Path) -> None:
        (root / "src.txt").write_text("a", encoding="utf-8")
        (root / "dst.txt").write_text("b", encoding="utf-8")
        with pytest.raises(PlatformError, match="already exists"):
            platform.copy_file("src.txt", "dst.txt")

    def test_copy_overwrite(self, platform: LocalPlatform, root: Path) -> None:
        (root / "src.txt").write_text("new", encoding="utf-8")
        (root / "dst.txt").write_text("old", encoding="utf-8")
        platform.copy_file("src.txt", "dst.txt", overwrite=True)
        assert (root / "dst.txt").read_text(encoding="utf-8") == "new"


class TestMoveFile:
    def test_move(self, platform: LocalPlatform, root: Path) -> None:
        (root / "src.txt").write_text("data", encoding="utf-8")
        platform.move_file("src.txt", "dst.txt")
        assert (root / "dst.txt").read_text(encoding="utf-8") == "data"
        assert not (root / "src.txt").exists()

    def test_move_src_missing_raises(self, platform: LocalPlatform) -> None:
        with pytest.raises(PlatformError, match="not found"):
            platform.move_file("no.txt", "dst.txt")

    def test_move_dest_exists_raises(self, platform: LocalPlatform, root: Path) -> None:
        (root / "src.txt").write_text("a", encoding="utf-8")
        (root / "dst.txt").write_text("b", encoding="utf-8")
        with pytest.raises(PlatformError, match="already exists"):
            platform.move_file("src.txt", "dst.txt")

    def test_move_overwrite(self, platform: LocalPlatform, root: Path) -> None:
        (root / "src.txt").write_text("new", encoding="utf-8")
        (root / "dst.txt").write_text("old", encoding="utf-8")
        platform.move_file("src.txt", "dst.txt", overwrite=True)
        assert (root / "dst.txt").read_text(encoding="utf-8") == "new"


class TestGetFileInfo:
    def test_info(self, platform: LocalPlatform, root: Path) -> None:
        f = root / "info.txt"
        f.write_text("hello!", encoding="utf-8")
        info = platform.get_file_info("info.txt")
        assert info.name == "info.txt"
        assert info.size == 6
        assert info.is_dir is False
        assert info.modification_time is not None


class TestLocalPlatformErrorPaths:
    def test_read_file_oserror_wrapped(self, platform: LocalPlatform) -> None:
        with patch.object(Path, "read_text", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot read file"):
                platform.read_file("x.txt")

    def test_write_file_oserror_wrapped(self, platform: LocalPlatform) -> None:
        with patch.object(Path, "write_text", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot write file"):
                platform.write_file("x.txt", "data", overwrite=True)

    def test_append_file_oserror_wrapped(self, platform: LocalPlatform) -> None:
        with patch.object(Path, "open", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot append"):
                platform.append_file("x.txt", "data")

    def test_delete_file_oserror_wrapped(self, platform: LocalPlatform) -> None:
        with patch.object(Path, "unlink", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot delete file"):
                platform.delete_file("x.txt")

    def test_create_folder_oserror_wrapped(self, platform: LocalPlatform) -> None:
        with patch.object(Path, "mkdir", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot create folder"):
                platform.create_folder("x")

    def test_upload_copy_oserror_wrapped(self, platform: LocalPlatform, tmp_path: Path) -> None:
        src = tmp_path / "src.txt"
        src.write_text("x", encoding="utf-8")
        with patch("shutil.copy2", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot upload file"):
                platform.upload_file(str(src), "dst.txt", overwrite=True)

    def test_download_copy_oserror_wrapped(self, platform: LocalPlatform, root: Path) -> None:
        (root / "s.txt").write_text("x", encoding="utf-8")
        with patch("shutil.copy2", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot download file"):
                platform.download_file("s.txt", str(root / "out" / "d.txt"))

    def test_copy_file_oserror_wrapped(self, platform: LocalPlatform, root: Path) -> None:
        (root / "s.txt").write_text("x", encoding="utf-8")
        with patch("shutil.copy2", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot copy file"):
                platform.copy_file("s.txt", "d.txt", overwrite=True)

    def test_move_file_oserror_wrapped(self, platform: LocalPlatform, root: Path) -> None:
        (root / "s.txt").write_text("x", encoding="utf-8")
        with patch("shutil.move", side_effect=OSError("boom")):
            with pytest.raises(PlatformError, match="Cannot move file"):
                platform.move_file("s.txt", "d.txt", overwrite=True)

    def test_fetch_secret_from_env(self) -> None:
        p = LocalPlatform()
        with patch.dict(os.environ, {"APP_TOKEN": "abc"}, clear=False):
            assert p.get_secret("TOKEN", "APP_") == "abc"

    def test_fetch_secret_missing_raises(self) -> None:
        p = LocalPlatform()
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(Exception, match="Secret not found"):
                p.get_secret("MISSING", "APP_")

    def test_nonexistent_raises(self, platform: LocalPlatform) -> None:
        with pytest.raises(PlatformError, match="does not exist"):
            platform.get_file_info("nope.txt")


# =====================================================================
# Resolve logic
# =====================================================================


class TestResolve:
    def test_relative_resolved_against_base(self, tmp_path: Path) -> None:
        platform = LocalPlatform(base_path=str(tmp_path))
        resolved = platform._resolve("subdir/file.txt")
        assert resolved == tmp_path / "subdir" / "file.txt"

    def test_absolute_not_changed(self, tmp_path: Path) -> None:
        from datacoolie.core.exceptions import PlatformError
        platform = LocalPlatform(base_path=str(tmp_path))
        abs_path = str(tmp_path / "other.txt")
        with pytest.raises(PlatformError, match="Absolute paths are not allowed"):
            platform._resolve(abs_path)

    def test_no_base_path(self) -> None:
        platform = LocalPlatform()
        resolved = platform._resolve("relative.txt")
        assert resolved == Path("relative.txt")

    def test_traversal_dotdot_raises(self, tmp_path: Path) -> None:
        platform = LocalPlatform(base_path=str(tmp_path))
        with pytest.raises(PlatformError, match="Path escapes base_path"):
            platform._resolve("../escape")

    def test_traversal_nested_raises(self, tmp_path: Path) -> None:
        platform = LocalPlatform(base_path=str(tmp_path))
        with pytest.raises(PlatformError, match="Path escapes base_path"):
            platform._resolve("sub/../../escape")


# =====================================================================
# Secrets
# =====================================================================


class TestGetSecret:
    def test_fetch_secret_from_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_SECRET", "s3cr3t")
        platform = LocalPlatform(base_path=str(tmp_path))
        assert platform.get_secret("MY_SECRET") == "s3cr3t"

    def test_fetch_secret_with_prefix(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("APP_DB_PASS", "pass123")
        platform = LocalPlatform(base_path=str(tmp_path))
        assert platform.get_secret("DB_PASS", "APP_") == "pass123"

    def test_missing_env_var_raises(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        platform = LocalPlatform(base_path=str(tmp_path))
        from datacoolie.core.exceptions import DataCoolieError
        with pytest.raises(DataCoolieError, match="not set"):
            platform.get_secret("MISSING_VAR")

    def test_secret_cached(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CACHE_VAR", "value")
        platform = LocalPlatform(base_path=str(tmp_path), cache_ttl=60)
        # First call fetches; second uses cache even with env removed
        platform.get_secret("CACHE_VAR")
        monkeypatch.delenv("CACHE_VAR")
        assert platform.get_secret("CACHE_VAR") == "value"

    def test_cache_disabled(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("NO_CACHE_VAR", "first")
        platform = LocalPlatform(base_path=str(tmp_path), cache_ttl=0)
        assert platform.get_secret("NO_CACHE_VAR") == "first"
        monkeypatch.setenv("NO_CACHE_VAR", "second")
        assert platform.get_secret("NO_CACHE_VAR") == "second"

    def test_is_base_secret_provider(self, tmp_path: Path) -> None:
        from datacoolie.core.secret_provider import BaseSecretProvider
        platform = LocalPlatform(base_path=str(tmp_path))
        assert isinstance(platform, BaseSecretProvider)
