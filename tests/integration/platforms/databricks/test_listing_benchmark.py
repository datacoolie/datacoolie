"""Read-only recursive listing benchmark for Databricks backends."""

from __future__ import annotations

import json
import math
from statistics import median
from time import perf_counter
from typing import Any
from uuid import uuid4

import pytest

from datacoolie.core.exceptions import PlatformError
from datacoolie.platforms._databricks.dbutils_backend import DbutilsBackend
from datacoolie.platforms._databricks.sdk_backend import DatabricksSdkBackend
from datacoolie.platforms._databricks.runtime import require_dbutils
from tests.integration.cloud_config import DatabricksIntegrationConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.benchmark,
    pytest.mark.cloud_integration,
    pytest.mark.cloud_platform("databricks"),
]

_WORKER_CAPS = (1, 4, 8, 16)
_MEASURED_RUNS = 5
_DELETE_WORKER_CAPS = (1, 8)
_DELETE_RUNS = 3


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[math.ceil(percentile * len(ordered)) - 1]


def _is_throttled(error: BaseException) -> bool:
    text = f"{type(error).__name__} {error}".lower()
    return any(marker in text for marker in ("429", "throttl", "rate limit"))


def _benchmark(backend_factory, root: str) -> list[dict[str, Any]]:
    measurements: dict[int, dict[str, Any]] = {
        workers: {
            "workers": workers,
            "seconds": [],
            "files": 0,
            "directories": [],
            "directory_list_calls": [],
            "failures": 0,
            "throttled": 0,
        }
        for workers in _WORKER_CAPS
    }
    expected_paths: set[str] | None = None
    backends = {workers: backend_factory(workers) for workers in _WORKER_CAPS}

    for workers, backend in backends.items():
        paths = {item.path for item in backend.list_files(root, recursive=True)}
        if expected_paths is None:
            expected_paths = paths
        assert paths == expected_paths

    for run in range(_MEASURED_RUNS):
        ordered_workers = (
            _WORKER_CAPS[run % len(_WORKER_CAPS) :]
            + _WORKER_CAPS[: run % len(_WORKER_CAPS)]
        )
        for workers in ordered_workers:
            backend = backends[workers]
            started = perf_counter()
            try:
                paths = {item.path for item in backend.list_files(root, recursive=True)}
                assert paths == expected_paths
            except Exception as exc:  # noqa: BLE001 - benchmark failure accounting
                measurements[workers]["failures"] += 1
                if _is_throttled(exc):
                    measurements[workers]["throttled"] += 1
                continue
            measurements[workers]["seconds"].append(perf_counter() - started)
            measurements[workers]["files"] = len(paths)
            measurements[workers]["directory_list_calls"].append(
                int(getattr(backend, "_last_list_calls", 0))
            )
            measurements[workers]["directories"].append(
                max(0, int(getattr(backend, "_last_list_calls", 0)) - 1)
            )

    results: list[dict[str, Any]] = []
    for workers in _WORKER_CAPS:
        measurement = measurements[workers]
        seconds = measurement["seconds"]
        assert measurement["failures"] == 0
        assert len(seconds) == _MEASURED_RUNS
        results.append(
            {
                "workers": workers,
                "p50_seconds": round(median(seconds), 6),
                "p95_seconds": round(_percentile(seconds, 0.95), 6),
                "files": measurement["files"],
                "directories": measurement["directories"],
                "directory_list_calls": measurement["directory_list_calls"],
                "failures": measurement["failures"],
                "throttled": measurement["throttled"],
            }
        )
    print(json.dumps(results, indent=2, sort_keys=True), flush=True)
    return results


def test_external_sdk_recursive_listing_worker_caps(
    databricks_integration_config: DatabricksIntegrationConfig,
) -> None:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(host=databricks_integration_config.host)
    results = _benchmark(
        lambda workers: DatabricksSdkBackend(client, max_list_workers=workers),
        databricks_integration_config.benchmark_root,
    )
    assert len(results) == len(_WORKER_CAPS)


def test_native_recursive_listing_worker_caps(
    databricks_integration_config: DatabricksIntegrationConfig,
) -> None:
    try:
        dbutils = require_dbutils()
    except PlatformError as exc:
        pytest.skip(f"Native Databricks runtime is unavailable: {exc}")
    results = _benchmark(
        lambda workers: DbutilsBackend(
            dbutils,
            max_list_workers=workers,
            volume_listing="dbutils",
        ),
        databricks_integration_config.benchmark_root,
    )
    assert len(results) == len(_WORKER_CAPS)


def test_native_posix_recursive_listing_worker_caps(
    databricks_integration_config: DatabricksIntegrationConfig,
) -> None:
    try:
        dbutils = require_dbutils()
    except PlatformError as exc:
        pytest.skip(f"Native Databricks runtime is unavailable: {exc}")
    results = _benchmark(
        lambda workers: DbutilsBackend(
            dbutils,
            max_list_workers=workers,
            volume_listing="posix",
        ),
        databricks_integration_config.benchmark_root,
    )
    assert len(results) == len(_WORKER_CAPS)


def test_external_recursive_delete_worker_caps(
    databricks_integration_config: DatabricksIntegrationConfig,
) -> None:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(host=databricks_integration_config.host)
    measurements: dict[int, list[float]] = {
        workers: [] for workers in _DELETE_WORKER_CAPS
    }
    for workers in _DELETE_WORKER_CAPS:
        for _ in range(_DELETE_RUNS):
            root = (
                f"{databricks_integration_config.benchmark_root}/"
                f"datacoolie-delete-{workers}-{uuid4().hex}"
            )
            backend = DatabricksSdkBackend(
                client,
                max_delete_workers=workers,
            )
            try:
                for branch in range(4):
                    for part in range(3):
                        backend.write_file(
                            f"{root}/branch={branch}/part={part}.jsonl",
                            "{}\n",
                        )
                started = perf_counter()
                backend.delete_folder(root, recursive=True)
                measurements[workers].append(perf_counter() - started)
                assert backend.folder_exists(root) is False
            finally:
                backend.delete_folder(root, recursive=True)

    results = [
        {
            "workers": workers,
            "p50_seconds": round(median(measurements[workers]), 6),
            "p95_seconds": round(
                _percentile(measurements[workers], 0.95),
                6,
            ),
            "runs": len(measurements[workers]),
        }
        for workers in _DELETE_WORKER_CAPS
    ]
    print(json.dumps(results, indent=2, sort_keys=True), flush=True)
    assert all(item["runs"] == _DELETE_RUNS for item in results)
