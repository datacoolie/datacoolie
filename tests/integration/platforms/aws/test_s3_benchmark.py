"""Opt-in AWS/MinIO S3 filesystem benchmark.

The test is intentionally gated by ``--cloud-integration``.  It creates one
UUID-scoped tree, verifies path sets on every repetition, and removes only
that tree in ``finally``.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from uuid import uuid4

import pytest

from datacoolie.platforms.aws_platform import AWSPlatform
from tests.integration.cloud_config import AwsIntegrationConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.benchmark,
    pytest.mark.cloud_integration,
    pytest.mark.cloud_platform("aws"),
]

_RUNS = 5
_OBJECT_COUNT = 1024


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = min(len(ordered) - 1, round((len(ordered) - 1) * percentile))
    return ordered[index]


def _measure(label: str, operation, *, runs: int = _RUNS) -> dict[str, object]:
    samples: list[float] = []
    failures = 0
    for _ in range(runs):
        started = time.perf_counter()
        try:
            operation()
        except Exception:
            failures += 1
        else:
            samples.append(time.perf_counter() - started)
    return {
        "operation": label,
        "runs": runs,
        "samples": samples,
        "p50_seconds": _percentile(samples, 0.50) if samples else None,
        "p95_seconds": _percentile(samples, 0.95) if samples else None,
        "failures": failures,
    }


def test_s3_filesystem_benchmark(
    aws_integration_config: AwsIntegrationConfig,
    tmp_path: Path,
) -> None:
    platform = AWSPlatform(
        bucket=aws_integration_config.bucket,
        region=aws_integration_config.region,
        endpoint_url=aws_integration_config.endpoint_url,
    )
    benchmark_root = aws_integration_config.benchmark_root.strip("/")
    test_root = (
        f"s3://{aws_integration_config.bucket}/{benchmark_root}/"
        f"aws-benchmark-{uuid4().hex}"
    )
    expected_files = {
        f"{test_root}/level-{index % 16}/group-{index % 64}/item-{index}.jsonl"
        for index in range(_OBJECT_COUNT)
    }
    expected_direct_folders = {f"{test_root}/level-{index}/" for index in range(16)}
    expected_recursive_folders = {
        f"{test_root}/level-{index}/" for index in range(16)
    } | {
        f"{test_root}/level-{index % 16}/group-{index % 64}/"
        for index in range(_OBJECT_COUNT)
    }
    payload = b'{"ok":true}\n'
    measurements: list[dict[str, object]] = []

    try:
        for index, path in enumerate(sorted(expected_files)):
            platform.write_bytes(path, payload)
            if index == 0:
                platform.create_folder(test_root)

        listed_recursive = {
            item.path for item in platform.list_files(test_root, recursive=True)
        }
        assert listed_recursive == expected_files
        listed_direct = platform.list_files(test_root, recursive=False)
        assert listed_direct == []

        # Warm each route once before collecting five comparable samples.
        platform.list_files(test_root, recursive=True)
        platform.list_files(test_root, recursive=False)
        platform.list_folders(test_root, recursive=True)
        platform.list_folders(test_root, recursive=False)
        sample_file = sorted(expected_files)[0]
        platform.read_bytes(sample_file)

        def check_recursive_files() -> None:
            assert {
                item.path for item in platform.list_files(test_root, recursive=True)
            } == expected_files

        def check_direct_files() -> None:
            assert platform.list_files(test_root, recursive=False) == []

        def check_recursive_folders() -> None:
            assert (
                set(platform.list_folders(test_root, recursive=True))
                == expected_recursive_folders
            )

        def check_direct_folders() -> None:
            assert (
                set(platform.list_folders(test_root, recursive=False))
                == expected_direct_folders
            )

        measurements.extend(
            [
                _measure(
                    "list_files_recursive",
                    check_recursive_files,
                ),
                _measure(
                    "list_files_nonrecursive",
                    check_direct_files,
                ),
                _measure(
                    "list_folders_recursive",
                    check_recursive_folders,
                ),
                _measure(
                    "list_folders_nonrecursive",
                    check_direct_folders,
                ),
                _measure("read_1kib", lambda: platform.read_bytes(sample_file)),
            ]
        )

        sizes = {
            "read_64kib": 64 * 1024,
            "read_1mib": 1024 * 1024,
            "read_16mib": 16 * 1024 * 1024,
        }
        for label, size in sizes.items():
            path = f"{test_root}/sizes/{label}.bin"
            platform.write_bytes(path, b"x" * size, overwrite=True)
            measurements.append(
                _measure(label, lambda path=path: platform.read_bytes(path))
            )

        download_path = tmp_path / "s3-benchmark-download.bin"
        managed_source = f"{test_root}/sizes/read_16mib.bin"
        measurements.append(
            _measure(
                "managed_download_16mib",
                lambda: platform.download_file(managed_source, str(download_path)),
            )
        )

        append_path = f"{test_root}/hot/append.jsonl"
        copy_path = f"{test_root}/hot/copy.jsonl"
        platform.write_file(append_path, "seed\n", overwrite=True)
        measurements.append(
            _measure("append_small", lambda: platform.append_file(append_path, "x\n"))
        )
        measurements.append(
            _measure(
                "copy_small",
                lambda: platform.copy_file(append_path, copy_path, overwrite=True),
            )
        )

        print(
            json.dumps(
                {
                    "boto3_version": __import__("boto3").__version__,
                    "region": aws_integration_config.region,
                    "endpoint_kind": "custom"
                    if aws_integration_config.endpoint_url
                    else "aws",
                    "object_count": _OBJECT_COUNT,
                    "measurements": measurements,
                },
                sort_keys=True,
            )
        )
        assert all(item["failures"] == 0 for item in measurements)
    finally:
        platform.delete_folder(test_root, recursive=True)
        assert platform.folder_exists(test_root) is False
