"""Tests for datacoolie.platforms.base — ABC contract tests."""

from __future__ import annotations

import pytest

from datacoolie.platforms.base import BasePlatform, FileInfo


class DummyPlatform(BasePlatform):
    """Non-abstract subclass that does NOT override anything.

    Every method should raise ``TypeError`` on instantiation or
    ``NotImplementedError`` when called.
    """

    # Must implement all abstract methods to allow instantiation
    def read_file(self, path: str) -> str:  # type: ignore[override]
        raise NotImplementedError

    def write_file(self, path: str, content: str, *, overwrite: bool = False) -> None:
        raise NotImplementedError

    def append_file(self, path: str, content: str) -> None:
        raise NotImplementedError

    def delete_file(self, path: str) -> None:
        raise NotImplementedError

    def create_folder(self, path: str) -> None:
        raise NotImplementedError

    def delete_folder(self, path: str, *, recursive: bool = False) -> None:
        raise NotImplementedError

    def list_files(self, path: str, *, recursive: bool = False, extension: str | None = None) -> list:
        raise NotImplementedError

    def list_folders(self, path: str, *, recursive: bool = False) -> list:
        raise NotImplementedError

    def file_exists(self, path: str) -> bool:
        raise NotImplementedError

    def folder_exists(self, path: str) -> bool:
        raise NotImplementedError

    def upload_file(self, local_path: str, dest: str, *, overwrite: bool = False) -> None:
        raise NotImplementedError

    def download_file(self, src: str, dest: str) -> None:
        raise NotImplementedError

    def copy_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        raise NotImplementedError

    def move_file(self, src: str, dest: str, *, overwrite: bool = False) -> None:
        raise NotImplementedError

    def get_file_info(self, path: str) -> FileInfo:
        raise NotImplementedError

    def _fetch_secret(self, key: str) -> str:
        raise NotImplementedError


class TestBasePlatformContract:
    """Ensure BasePlatform cannot be instantiated directly."""

    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            BasePlatform()  # type: ignore[abstract]

    def test_dummy_subclass_instantiates(self) -> None:
        platform = DummyPlatform()
        assert isinstance(platform, BasePlatform)

    @pytest.mark.parametrize(
        "method_name",
        [
            "read_file",
            "write_file",
            "append_file",
            "delete_file",
            "create_folder",
            "delete_folder",
            "list_files",
            "list_folders",
            "file_exists",
            "folder_exists",
            "upload_file",
            "download_file",
            "copy_file",
            "move_file",
            "get_file_info",
        ],
    )
    def test_abstract_method_not_implemented(self, method_name: str) -> None:
        platform = DummyPlatform()
        with pytest.raises(NotImplementedError):
            method = getattr(platform, method_name)
            # Provide minimal args; positional only
            if method_name in ("upload_file", "download_file", "copy_file", "move_file"):
                method("a", "b")
            elif method_name == "write_file":
                method("/path", "text")
            elif method_name == "append_file":
                method("/path", "text")
            elif method_name == "delete_folder":
                method("/path")
            else:
                method("/path")

    def test_has_16_abstract_methods(self) -> None:
        abstracts = getattr(BasePlatform, "__abstractmethods__", frozenset())
        assert len(abstracts) == 16  # 15 filesystem methods + _fetch_secret
