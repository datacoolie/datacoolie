"""Deterministic tests for bounded native Fabric directory traversal."""

from __future__ import annotations

from collections import Counter
from threading import Event, Lock, Thread
from types import SimpleNamespace
from typing import Any

from datacoolie.platforms._fabric.traversal import list_notebookutils_tree


def _directory(path: str) -> SimpleNamespace:
    return SimpleNamespace(path=path, name=path.rsplit("/", 1)[-1], isDir=True)


def _file(path: str) -> SimpleNamespace:
    return SimpleNamespace(path=path, name=path.rsplit("/", 1)[-1], isDir=False)


class TreeFileSystem:
    def __init__(self, tree: dict[str, list[Any]]) -> None:
        self.tree = tree
        self.calls: Counter[str] = Counter()

    def ls(self, path: str) -> list[Any]:
        self.calls[path] += 1
        return list(self.tree[path])


def test_non_recursive_listing_calls_root_once() -> None:
    fs = TreeFileSystem({"root": [_directory("root/a"), _file("root/x.jsonl")]})

    results = list_notebookutils_tree(fs, "root", recursive=False)

    assert [item.path for item in results] == ["root/a", "root/x.jsonl"]
    assert fs.calls == Counter({"root": 1})


def test_deep_tree_is_iterative_and_visits_each_directory_once() -> None:
    depth = 1_100
    tree: dict[str, list[Any]] = {}
    for index in range(depth):
        current = f"root/{index}" if index else "root"
        child = f"root/{index + 1}"
        tree[current] = [_directory(child)]
    tree[f"root/{depth}"] = [_file(f"root/{depth}/last.jsonl")]
    fs = TreeFileSystem(tree)

    results = list_notebookutils_tree(fs, "root", recursive=True)

    assert results[-1].path == f"root/{depth}/last.jsonl"
    assert len(fs.calls) == depth + 1
    assert set(fs.calls.values()) == {1}


class BlockingFileSystem:
    def __init__(self, width: int) -> None:
        self.width = width
        self.release = Event()
        self.full = Event()
        self.lock = Lock()
        self.active = 0
        self.maximum_active = 0

    def ls(self, path: str) -> list[Any]:
        if path == "root":
            return [_directory(f"root/{index}") for index in range(self.width)]
        with self.lock:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            if self.active == 8:
                self.full.set()
        self.release.wait(timeout=5)
        with self.lock:
            self.active -= 1
        return [_file(f"{path}/value.jsonl")]


class NestedBlockingFileSystem(BlockingFileSystem):
    def ls(self, path: str) -> list[Any]:
        if path == "root":
            return [_directory("root/only")]
        if path == "root/only":
            return [_directory(f"root/only/{index}") for index in range(self.width)]
        return super().ls(path)


def _assert_eight_workers(fs: BlockingFileSystem, *, expected_items: int) -> None:
    captured: list[list[Any]] = []
    worker = Thread(
        target=lambda: captured.append(list_notebookutils_tree(fs, "root", recursive=True))
    )
    worker.start()
    try:
        assert fs.full.wait(timeout=5), "expected eight concurrent directory listings"
        assert fs.maximum_active == 8
    finally:
        fs.release.set()
        worker.join(timeout=5)

    assert not worker.is_alive()
    assert len(captured[0]) == expected_items


def test_recursive_listing_uses_at_most_eight_workers() -> None:
    _assert_eight_workers(BlockingFileSystem(width=16), expected_items=32)


def test_recursive_listing_retains_capacity_after_single_root_child() -> None:
    _assert_eight_workers(NestedBlockingFileSystem(width=16), expected_items=33)
