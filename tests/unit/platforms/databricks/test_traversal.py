"""Tests for bounded Databricks tree traversal."""

from __future__ import annotations

from dataclasses import dataclass

from datacoolie.platforms._databricks.traversal import list_tree


@dataclass(frozen=True)
class _Entry:
    path: str
    directory: bool


def test_include_item_filters_results_but_keeps_directories_as_frontier() -> None:
    tree = {
        "/root": [
            _Entry("/root/folder", True),
            _Entry("/root/root.txt", False),
        ],
        "/root/folder": [_Entry("/root/folder/child.txt", False)],
    }

    result = list_tree(
        "/root",
        lambda path: tree[path],
        lambda item: item.directory,
        lambda item: item.path,
        recursive=True,
        max_workers=1,
        include_item=lambda item: not item.directory,
    )

    assert [item.path for item in result] == [
        "/root/root.txt",
        "/root/folder/child.txt",
    ]
