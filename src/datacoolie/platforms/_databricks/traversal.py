"""Bounded iterative traversal for Databricks directory trees."""

from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Callable, TypeVar

T = TypeVar("T")

DEFAULT_DBUTILS_LIST_WORKERS = 8
DEFAULT_SDK_LIST_WORKERS = 16
DEFAULT_SDK_DELETE_WORKERS = 8


def list_tree(
    root: str,
    list_directory: Callable[[str], list[T]],
    is_directory: Callable[[T], bool],
    item_path: Callable[[T], str],
    *,
    recursive: bool,
    max_workers: int = DEFAULT_DBUTILS_LIST_WORKERS,
    include_item: Callable[[T], bool] | None = None,
    on_directory_list: Callable[[str], None] | None = None,
) -> list[T]:
    """List a tree while bounding independent directory requests.

    ``include_item`` filters the returned materialization but never prevents a
    discovered directory from being traversed. This keeps file/folder callers
    from retaining irrelevant entry types while preserving the same traversal
    frontier and error behavior.
    """
    should_include = include_item or (lambda _item: True)

    def fetch(directory: str) -> list[T]:
        if on_directory_list is not None:
            on_directory_list(directory)
        return list_directory(directory)

    root_items = fetch(root)
    if not recursive:
        return [item for item in root_items if should_include(item)]

    results = [item for item in root_items if should_include(item)]
    pending = deque(item_path(item) for item in root_items if is_directory(item))
    seen = set(pending)
    if not pending:
        return results

    worker_count = max(1, max_workers)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        active: dict[Future[list[T]], str] = {}
        try:
            while pending or active:
                while pending and len(active) < worker_count:
                    directory = pending.popleft()
                    active[executor.submit(fetch, directory)] = directory

                completed, _ = wait(active, return_when=FIRST_COMPLETED)
                for future in completed:
                    active.pop(future)
                    children = future.result()
                    results.extend(child for child in children if should_include(child))
                    for child in children:
                        if not is_directory(child):
                            continue
                        child_path = item_path(child)
                        if child_path in seen:
                            continue
                        seen.add(child_path)
                        pending.append(child_path)
        except BaseException:
            for future in active:
                future.cancel()
            raise
    return results
