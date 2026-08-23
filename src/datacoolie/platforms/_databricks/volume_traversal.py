"""Iterative POSIX traversal for Unity Catalog Volume paths."""

from __future__ import annotations

import os
import posixpath
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from datacoolie.platforms._databricks.traversal import list_tree


@dataclass(frozen=True, slots=True)
class VolumeEntry:
    """A materialized Volume entry safe to use after ``scandir`` closes."""

    name: str
    path: str
    local_path: str
    is_dir: bool
    size: int
    modification_time: datetime | None


def _canonical_child(root: str, local_root: str, local_path: str) -> str:
    relative = os.path.relpath(local_path, local_root)
    if relative == ".":
        return root.rstrip("/")
    return posixpath.join(root.rstrip("/"), relative.replace(os.sep, "/"))


def _scan_directory(
    local_directory: str,
    *,
    local_root: str,
    canonical_root: str,
) -> list[VolumeEntry]:
    entries: list[VolumeEntry] = []
    with os.scandir(local_directory) as iterator:
        for entry in iterator:
            local_path = entry.path
            is_dir = entry.is_dir(follow_symlinks=False)
            if is_dir:
                entries.append(
                    VolumeEntry(
                        name=entry.name,
                        path=_canonical_child(canonical_root, local_root, local_path),
                        local_path=local_path,
                        is_dir=True,
                        size=0,
                        modification_time=None,
                    )
                )
                continue

            metadata = entry.stat(follow_symlinks=False)
            if not stat.S_ISREG(metadata.st_mode):
                continue
            entries.append(
                VolumeEntry(
                    name=entry.name,
                    path=_canonical_child(canonical_root, local_root, local_path),
                    local_path=local_path,
                    is_dir=False,
                    size=metadata.st_size,
                    modification_time=datetime.fromtimestamp(
                        metadata.st_mtime,
                        tz=timezone.utc,
                    ),
                )
            )
    return entries


def list_volume_tree(
    local_root: str,
    canonical_root: str,
    *,
    recursive: bool,
    max_workers: int,
    include_item: Callable[[VolumeEntry], bool] | None = None,
    on_directory_list: Callable[[str], None] | None = None,
) -> list[VolumeEntry]:
    """List a Volume tree without retaining entries rejected by the caller."""

    return list_tree(
        local_root,
        lambda current: _scan_directory(
            current,
            local_root=local_root,
            canonical_root=canonical_root,
        ),
        lambda item: item.is_dir,
        lambda item: item.local_path,
        recursive=recursive,
        max_workers=max_workers,
        include_item=include_item,
        on_directory_list=on_directory_list,
    )
