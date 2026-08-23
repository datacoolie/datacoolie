"""Shared live file contract for native and external Databricks backends."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from datacoolie.platforms.base import BasePlatform


def exercise_file_contract(
    platform: BasePlatform,
    configured_root: str,
    prefix: str,
    tmp_path: Path,
) -> None:
    test_root = f"{configured_root}/{prefix}/datacoolie-live-{uuid4().hex}"
    source = f"{test_root}/source.txt"
    copied = f"{test_root}/copied.txt"
    moved = f"{test_root}/nested/deep/moved.txt"
    uploaded = f"{test_root}/uploaded.bin"
    local_source = tmp_path / "source.bin"
    local_download = tmp_path / "download.bin"
    local_source.write_bytes(b"binary-payload")
    created = False

    try:
        platform.create_folder(test_root)
        created = True
        platform.write_file(source, "databricks-")
        platform.append_file(source, "payload")
        assert platform.read_file(source) == "databricks-payload"
        assert platform.read_bytes(source) == b"databricks-payload"

        platform.copy_file(source, copied)
        platform.move_file(copied, moved)
        assert platform.file_exists(copied) is False
        assert platform.read_file(moved) == "databricks-payload"

        platform.write_file(moved, "previous-value", overwrite=True)
        platform.copy_file(source, copied)
        platform.move_file(copied, moved, overwrite=True)
        assert platform.file_exists(copied) is False
        assert platform.read_file(moved) == "databricks-payload"

        platform.upload_file(str(local_source), uploaded)
        platform.download_file(uploaded, str(local_download))
        assert local_download.read_bytes() == b"binary-payload"

        assert {item.name for item in platform.list_files(test_root)} == {
            "source.txt",
            "uploaded.bin",
        }
        assert {
            item.name for item in platform.list_files(test_root, recursive=True)
        } == {"source.txt", "moved.txt", "uploaded.bin"}
        assert platform.get_file_info(source).size == len(b"databricks-payload")
    finally:
        if created:
            platform.delete_folder(test_root, recursive=True)
