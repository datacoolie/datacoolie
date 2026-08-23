"""Focused traversal and identity tests for :class:`LocalPlatform`."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from datacoolie.platforms.local_platform import LocalPlatform


def test_relative_base_path_is_stable_after_chdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_cwd = tmp_path / "cwd"
    changed_cwd = tmp_path / "other-cwd"
    original_cwd.mkdir()
    changed_cwd.mkdir()
    monkeypatch.chdir(original_cwd)

    platform = LocalPlatform(base_path="root")
    expected_root = (original_cwd / "root").resolve()
    monkeypatch.chdir(changed_cwd)

    assert platform._resolve("data/file.json") == expected_root / "data/file.json"


def test_listing_uses_direntry_metadata_without_path_stat(
    tmp_path: Path,
) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    (root / "a.json").write_text("{}", encoding="utf-8")
    (root / "nested").mkdir()
    (root / "nested" / "b.json").write_text("{}", encoding="utf-8")
    platform = LocalPlatform(base_path=str(tmp_path))

    with patch.object(Path, "stat", side_effect=AssertionError("Path.stat used")):
        files = platform.list_files("tree", recursive=True)

    assert {item.name for item in files} == {"a.json", "b.json"}


def test_get_file_info_uses_one_path_stat(tmp_path: Path) -> None:
    path = tmp_path / "item.json"
    path.write_text("{}", encoding="utf-8")
    platform = LocalPlatform(base_path=str(tmp_path))

    original_stat = Path.stat

    def count_stat() -> os.stat_result:
        return original_stat(path)

    with patch.object(Path, "stat", side_effect=count_stat) as stat_mock:
        info = platform.get_file_info("item.json")

    assert info.size == 2
    assert stat_mock.call_count == 1


def test_recursive_listing_does_not_follow_directory_symlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "tree"
    target = root / "target"
    target.mkdir(parents=True)
    (target / "inside.txt").write_text("x", encoding="utf-8")
    link = root / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"directory symlinks are unavailable: {exc}")

    platform = LocalPlatform(base_path=str(tmp_path))
    files = platform.list_files("tree", recursive=True)
    folders = platform.list_folders("tree", recursive=True)
    folder_paths = set(folders)

    assert {item.name for item in files} == {"inside.txt"}
    assert os.path.normcase(str(link)).replace("\\", "/") in {
        os.path.normcase(path).replace("\\", "/") for path in folder_paths
    }
    assert not any(item.path.endswith("/link/inside.txt") for item in files)


def test_copy_and_move_treat_relative_absolute_alias_as_same_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "item.txt"
    path.write_text("payload", encoding="utf-8")
    platform = LocalPlatform()

    platform.copy_file("item.txt", str(path))
    platform.move_file("./item.txt", str(path))

    assert path.read_text(encoding="utf-8") == "payload"


def test_copy_and_move_treat_hard_link_as_same_file(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    alias = tmp_path / "alias.txt"
    source.write_text("payload", encoding="utf-8")
    try:
        alias.hardlink_to(source)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"hard links are unavailable: {exc}")
    platform = LocalPlatform()

    platform.copy_file(str(source), str(alias))
    platform.move_file(str(source), str(alias))

    assert source.read_text(encoding="utf-8") == "payload"
    assert alias.read_text(encoding="utf-8") == "payload"


def test_non_overwrite_write_does_not_preflight_exists(tmp_path: Path) -> None:
    platform = LocalPlatform(base_path=str(tmp_path))

    with patch.object(Path, "exists", side_effect=AssertionError("exists used")):
        platform.write_file("nested/new.txt", "payload")
        platform.write_bytes("nested/new.bin", b"payload")

    assert (tmp_path / "nested" / "new.txt").read_text(encoding="utf-8") == "payload"
    assert (tmp_path / "nested" / "new.bin").read_bytes() == b"payload"
