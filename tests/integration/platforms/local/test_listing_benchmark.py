"""Opt-in local filesystem listing benchmark for deep metadata trees."""

from __future__ import annotations

import json
import math
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Callable

import pytest

from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.utils.path_utils import normalize_path

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.benchmark,
]

_FILE_COUNT = 8_192
_MEASURED_RUNS = 7


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[math.ceil(percentile * len(ordered)) - 1]


def _pathlib_listing(root: Path) -> set[str]:
    return {normalize_path(str(path)) for path in root.rglob("*") if path.is_file()}


def _measure(
    listing: Callable[[], set[str]],
    expected_paths: set[str],
) -> list[float]:
    assert listing() == expected_paths
    samples: list[float] = []
    for _ in range(_MEASURED_RUNS):
        started = perf_counter()
        observed_paths = listing()
        elapsed = perf_counter() - started
        assert observed_paths == expected_paths
        samples.append(elapsed)
    return samples


def test_local_recursive_listing_benchmark(tmp_path: Path) -> None:
    root = tmp_path / "benchmark"
    for partition in range(32):
        for bucket in range(8):
            directory = root / f"partition={partition:02d}" / f"bucket={bucket:02d}"
            directory.mkdir(parents=True, exist_ok=True)
            for part in range(32):
                (directory / f"part-{part:04d}.jsonl").write_text(
                    "{}\n",
                    encoding="utf-8",
                )

    expected_paths = _pathlib_listing(root)
    assert len(expected_paths) == _FILE_COUNT

    platform = LocalPlatform(base_path=str(tmp_path))
    local_paths = {
        item.path for item in platform.list_files("benchmark", recursive=True)
    }
    assert local_paths == expected_paths

    pathlib_samples = _measure(
        lambda: _pathlib_listing(root),
        expected_paths,
    )
    local_samples = _measure(
        lambda: {
            item.path for item in platform.list_files("benchmark", recursive=True)
        },
        expected_paths,
    )
    results = {
        "files": _FILE_COUNT,
        "runs": _MEASURED_RUNS,
        "pathlib": {
            "p50_seconds": round(median(pathlib_samples), 6),
            "p95_seconds": round(_percentile(pathlib_samples, 0.95), 6),
        },
        "local_platform": {
            "p50_seconds": round(median(local_samples), 6),
            "p95_seconds": round(_percentile(local_samples, 0.95), 6),
        },
        "p50_speedup": round(median(pathlib_samples) / median(local_samples), 2),
    }
    print(json.dumps(results, indent=2, sort_keys=True), flush=True)
