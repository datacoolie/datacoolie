"""Bounded NotebookUtils directory traversal for metadata and log trees."""

from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any

_MAX_LIST_WORKERS = 8


def list_notebookutils_tree(
    fs: Any,
    root: str,
    *,
    recursive: bool,
    max_workers: int = _MAX_LIST_WORKERS,
) -> list[Any]:
    """List *root*, visiting each discovered directory at most once.

    The public platform API remains synchronous and materialized. Only remote
    directory requests are overlapped, with a small fixed upper bound.
    """
    root_items = list(fs.ls(root))
    if not recursive:
        return root_items

    results = list(root_items)
    pending = deque(str(item.path) for item in root_items if item.isDir)
    seen = set(pending)
    if not pending:
        return results

    # Keep the configured capacity available for fan-out discovered below a
    # single root child (for example year -> month/day partitions).
    worker_count = max(1, max_workers)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        active: dict[Future[list[Any]], str] = {}
        try:
            while pending or active:
                while pending and len(active) < worker_count:
                    directory = pending.popleft()
                    active[executor.submit(lambda path=directory: list(fs.ls(path)))] = directory

                completed, _ = wait(active, return_when=FIRST_COMPLETED)
                for future in completed:
                    active.pop(future)
                    children = future.result()
                    results.extend(children)
                    for child in children:
                        if not child.isDir:
                            continue
                        child_path = str(child.path)
                        if child_path in seen:
                            continue
                        seen.add(child_path)
                        pending.append(child_path)
        except BaseException:
            for future in active:
                future.cancel()
            raise

    return results
