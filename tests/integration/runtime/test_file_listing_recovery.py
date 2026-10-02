"""Persisted recovery when file discovery fails before a read."""

from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.base import FileInfo
from datacoolie.platforms.local_platform import LocalPlatform


pytestmark = pytest.mark.integration


class _ListingFailurePlatform(LocalPlatform):
    """Local platform fixture that can fail file discovery deterministically."""

    def __init__(self, *, fail_list_files: bool = False) -> None:
        super().__init__()
        self.fail_list_files = fail_list_files

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        if self.fail_list_files:
            raise RuntimeError("synthetic list_files failure")
        return super().list_files(path, recursive=recursive, extension=extension)


def _write_metadata(path: Path, input_root: Path, output_root: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "file",
                        "format": "parquet",
                        "configure": {"base_path": str(input_root)},
                    },
                    {
                        "name": "destination",
                        "connection_type": "file",
                        "format": "parquet",
                        "configure": {"base_path": str(output_root)},
                    },
                ],
                "dataflows": [
                    {
                        "name": "listing-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": ["__file_modification_time"],
                        },
                        "destination": {
                            "connection_name": "destination",
                            "table": "events",
                            "load_type": "append",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _run_driver(
    metadata_path: Path,
    platform: LocalPlatform,
    state_root: Path,
    log_root: Path,
):
    provider = FileProvider(config_path=str(metadata_path), platform=platform)
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform),
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(state_root),
        log_base_path=str(log_root),
        config=DataCoolieRunConfig(
            job_id="file-listing-recovery",
            retry_count=0,
            retry_delay=0,
        ),
    ) as driver:
        return driver.run(stage="bronze")


def _read_ids(output_root: Path) -> list[int]:
    return sorted(
        row["id"]
        for path in sorted((output_root / "events").rglob("*.parquet"))
        for row in pl.read_parquet(path).select("id").to_dicts()
    )


def _read_checkpoint(state_root: Path) -> tuple[Path, str]:
    paths = sorted(state_root.rglob("watermark_value.json"))
    assert len(paths) == 1
    path = paths[0]
    return path, path.read_text(encoding="utf-8")


def test_file_listing_failure_preserves_target_and_checkpoint_until_recovery(tmp_path):
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"
    metadata_path = tmp_path / "metadata.json"
    input_table = input_root / "events"
    input_table.mkdir(parents=True)
    _write_metadata(metadata_path, input_root, output_root)

    first = input_table / "first.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(first)
    os.utime(first, (1_700_000_000, 1_700_000_000))

    initial = _run_driver(
        metadata_path,
        _ListingFailurePlatform(),
        state_root,
        log_root,
    )
    assert (initial.total, initial.succeeded, initial.failed) == (1, 1, 0), initial.errors
    assert _read_ids(output_root) == [1]
    checkpoint_path, checkpoint_before = _read_checkpoint(state_root)

    second = input_table / "second.parquet"
    pl.DataFrame({"id": [2]}).write_parquet(second)
    os.utime(second, (1_700_003_600, 1_700_003_600))

    failed = _run_driver(
        metadata_path,
        _ListingFailurePlatform(fail_list_files=True),
        state_root,
        log_root,
    )
    assert (failed.total, failed.succeeded, failed.failed) == (1, 0, 1)
    assert _read_ids(output_root) == [1]
    assert _read_checkpoint(state_root) == (checkpoint_path, checkpoint_before)

    recovered = _run_driver(
        metadata_path,
        _ListingFailurePlatform(),
        state_root,
        log_root,
    )
    assert (recovered.total, recovered.succeeded, recovered.failed) == (1, 1, 0), recovered.errors
    assert _read_ids(output_root) == [1, 2]
    checkpoint_after = _read_checkpoint(state_root)
    assert checkpoint_after[0] == checkpoint_path
    assert checkpoint_after[1] != checkpoint_before
