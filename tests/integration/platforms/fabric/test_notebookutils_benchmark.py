"""Opt-in Fabric benchmark for bounded NotebookUtils recursive listing."""

from __future__ import annotations

import json
import math
from statistics import median
from threading import Lock
from time import perf_counter
from typing import Any
from uuid import uuid4

import pytest

from datacoolie.platforms._fabric.traversal import list_notebookutils_tree
from tests.integration.cloud_config import FabricIntegrationConfig

notebookutils = pytest.importorskip("notebookutils")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.benchmark,
    pytest.mark.cloud_integration,
    pytest.mark.cloud_platform("fabric"),
]


class CountingFileSystem:
    def __init__(self, fs: Any) -> None:
        self._fs = fs
        self._lock = Lock()
        self.ls_calls = 0

    def ls(self, path: str) -> Any:
        with self._lock:
            self.ls_calls += 1
        return self._fs.ls(path)


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[math.ceil(percentile * len(ordered)) - 1]


def test_notebookutils_recursive_listing_worker_caps(
    fabric_integration_config: FabricIntegrationConfig,
) -> None:
    fs = notebookutils.fs
    test_root = (
        f"{fabric_integration_config.benchmark_root}/datacoolie-list-{uuid4().hex}"
    )
    expected_files: set[str] = set()
    results: list[dict[str, Any]] = []

    try:
        # One root child followed by wider month/day/hour fan-out mirrors the
        # partition shape used by metadata and log storage.
        for month in range(1, 7):
            for day in range(1, 8):
                for hour in range(4):
                    folder = (
                        f"{test_root}/year=2026/month={month:02d}/"
                        f"day={day:02d}/hour={hour:02d}"
                    )
                    fs.mkdirs(folder)
                    path = f"{folder}/part.jsonl"
                    fs.put(path, "{}\n", overwrite=False)
                    expected_files.add(path.split(f"{test_root}/", 1)[1])

        # Warm the Fabric runtime and storage route before measuring.
        list_notebookutils_tree(fs, test_root, recursive=True, max_workers=1)

        for cap in (1, 4, 8, 16):
            elapsed: list[float] = []
            failures = 0
            observed_files: set[str] = set()
            observed_folders: set[str] = set()
            ls_calls = 0
            for _ in range(5):
                counting_fs = CountingFileSystem(fs)
                started = perf_counter()
                try:
                    items = list_notebookutils_tree(
                        counting_fs,
                        test_root,
                        recursive=True,
                        max_workers=cap,
                    )
                except Exception:
                    failures += 1
                    continue
                elapsed.append(perf_counter() - started)
                ls_calls = counting_fs.ls_calls
                observed_files = {
                    str(item.path).split(f"{test_root}/", 1)[1]
                    for item in items
                    if not item.isDir
                }
                observed_folders = {str(item.path) for item in items if item.isDir}

            assert failures == 0
            assert observed_files == expected_files
            assert ls_calls == len(observed_folders) + 1
            results.append(
                {
                    "worker_cap": cap,
                    "p50_seconds": round(median(elapsed), 6),
                    "p95_seconds": round(_percentile(elapsed, 0.95), 6),
                    "directory_count": len(observed_folders),
                    "file_count": len(observed_files),
                    "ls_calls": ls_calls,
                    "failures": failures,
                }
            )

        print(json.dumps(results, indent=2), flush=True)
    finally:
        if fs.exists(test_root):
            fs.rm(test_root, recurse=True)
