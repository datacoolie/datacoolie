"""Persisted recovery when date-folder discovery fails before a read."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


pytestmark = pytest.mark.integration


class _FolderFailurePlatform(LocalPlatform):
    """Local platform fixture that can fail date-folder discovery."""

    def __init__(self, *, fail_list_folders: bool = False) -> None:
        super().__init__()
        self.fail_list_folders = fail_list_folders

    def list_folders(
        self,
        path: str,
        *,
        recursive: bool = False,
    ) -> list[str]:
        if self.fail_list_folders:
            raise RuntimeError("synthetic list_folders failure")
        return super().list_folders(path, recursive=recursive)


def _write_metadata(
    path: Path,
    input_root: Path,
    output_root: Path,
    *,
    watermark_columns: list[str] | None = None,
) -> None:
    path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "file",
                        "format": "parquet",
                        "configure": {
                            "base_path": str(input_root),
                            "date_folder_partitions": "{year}/{month}/{day}",
                        },
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
                        "name": "dated-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": (
                                ["updated_at"]
                                if watermark_columns is None
                                else watermark_columns
                            ),
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
            job_id="date-folder-recovery",
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


def test_date_folder_listing_failure_preserves_state_until_recovery(tmp_path):
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"
    metadata_path = tmp_path / "metadata.json"
    _write_metadata(metadata_path, input_root, output_root)

    first_dir = input_root / "events" / "2026" / "01" / "01"
    first_dir.mkdir(parents=True)
    first = first_dir / "first.parquet"
    pl.DataFrame(
        {"id": [1], "updated_at": ["2026-01-01T00:00:00+00:00"]}
    ).write_parquet(first)
    os.utime(first, (1_700_000_000, 1_700_000_000))

    initial = _run_driver(
        metadata_path,
        _FolderFailurePlatform(),
        state_root,
        log_root,
    )
    assert (initial.total, initial.succeeded, initial.failed) == (1, 1, 0), initial.errors
    assert _read_ids(output_root) == [1]
    checkpoint_path, checkpoint_before = _read_checkpoint(state_root)

    second_dir = input_root / "events" / "2026" / "02" / "01"
    second_dir.mkdir(parents=True)
    second = second_dir / "second.parquet"
    pl.DataFrame(
        {"id": [2], "updated_at": ["2026-02-01T00:00:00+00:00"]}
    ).write_parquet(second)
    os.utime(second, (1_700_003_600, 1_700_003_600))

    failed = _run_driver(
        metadata_path,
        _FolderFailurePlatform(fail_list_folders=True),
        state_root,
        log_root,
    )
    assert (failed.total, failed.succeeded, failed.failed) == (1, 0, 1)
    assert _read_ids(output_root) == [1]
    assert _read_checkpoint(state_root) == (checkpoint_path, checkpoint_before)

    recovered = _run_driver(
        metadata_path,
        _FolderFailurePlatform(),
        state_root,
        log_root,
    )
    assert (recovered.total, recovered.succeeded, recovered.failed) == (1, 1, 0), recovered.errors
    assert _read_ids(output_root) == [1, 2]
    checkpoint_after = _read_checkpoint(state_root)
    assert checkpoint_after[0] == checkpoint_path
    assert checkpoint_after[1] != checkpoint_before


def test_date_folder_only_state_is_loaded_on_next_run(tmp_path):
    """Internal folder state prunes older folders without authored row watermarks."""

    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"
    metadata_path = tmp_path / "metadata.json"
    _write_metadata(
        metadata_path,
        input_root,
        output_root,
        watermark_columns=[],
    )

    first_dir = input_root / "events" / "2026" / "01" / "01"
    first_dir.mkdir(parents=True)
    pl.DataFrame({"id": [1]}).write_parquet(first_dir / "first.parquet")

    initial = _run_driver(metadata_path, LocalPlatform(), state_root, log_root)
    assert (initial.total, initial.succeeded, initial.failed) == (1, 1, 0), initial.errors
    assert _read_ids(output_root) == [1]

    # An old folder must be pruned by the internal folder cursor; a later
    # folder must still be discovered on the next ordinary run.
    old_dir = input_root / "events" / "2025" / "12" / "31"
    old_dir.mkdir(parents=True)
    pl.DataFrame({"id": [99]}).write_parquet(old_dir / "old.parquet")
    later_dir = input_root / "events" / "2026" / "02" / "01"
    later_dir.mkdir(parents=True)
    pl.DataFrame({"id": [2]}).write_parquet(later_dir / "later.parquet")

    recovered = _run_driver(metadata_path, LocalPlatform(), state_root, log_root)
    assert (recovered.total, recovered.succeeded, recovered.failed) == (1, 1, 0), recovered.errors
    # The saved folder boundary is intentionally included again; exact row or
    # mtime filtering is a separate source feature. The older folder must not
    # be discovered, while the boundary and later folder remain eligible.
    assert _read_ids(output_root) == [1, 1, 2]


@pytest.mark.parametrize("tracked", [False, True], ids=["untracked-mtime", "tracked-mtime"])
def test_mtime_replay_scans_old_folders_and_uses_half_open_file_bounds(tmp_path, tracked):
    """Folder names only discover files; saved discovery state never narrows replay."""
    input_root, output_root = tmp_path / "input", tmp_path / "output"
    metadata = tmp_path / "metadata.json"
    columns = ["__file_modification_time"] if tracked else []
    _write_metadata(metadata, input_root, output_root, watermark_columns=columns)
    lower, upper = 1_700_000_000, 1_700_000_100
    # The lower-bound file is in an old folder; the upper-bound folder is
    # unrelated to its file's modification date. No folder/mtime correlation.
    for identifier, folder, stamp in [
        (0, "2020/01/01", lower - 1), (1, "2020/01/01", lower),
        (2, "2030/01/01", lower + 50), (3, "2021/01/01", upper),
        (4, "2031/01/01", upper + 1),
    ]:
        path = input_root / "events" / folder / f"{identifier}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"id": [identifier]}).write_parquet(path)
        os.utime(path, (stamp, stamp))

    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata), platform=platform,
                            watermark_base_path=str(tmp_path / "watermarks"))
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform), platform=platform,
        metadata_provider=provider, log_base_path=str(tmp_path / "logs"),
        config=DataCoolieRunConfig(retry_count=0),
    ) as driver:
        flow, = driver.load_dataflows()
        provider.update_watermark(flow.dataflow_id, json.dumps({
            DATE_FOLDER_PARTITION_KEY: "2040-01-01T00:00:00+00:00",
            "__file_modification_time": "2040-01-01T00:00:00+00:00",
            "aux": "kept",
        }))
        replay = ReplayConfig(
            start=datetime.fromtimestamp(lower, timezone.utc),
            end=datetime.fromtimestamp(upper, timezone.utc),
            chunk_column="__file_modification_time", save_watermark=True,
        )
        result = driver.run_replay([flow], replay)
        assert result.succeeded == 1, result.errors
    assert _read_ids(output_root) == [1, 2]
    # Restart the provider to verify that historical observations do not move
    # production discovery/mtime backwards and that auxiliary state survives.
    restarted = FileProvider(config_path=str(metadata), platform=LocalPlatform(),
                             watermark_base_path=str(tmp_path / "watermarks"))
    restarted.initialize()
    try:
        saved = json.loads(restarted.get_watermark(flow.dataflow_id))
    finally:
        restarted.close()
    assert saved == {DATE_FOLDER_PARTITION_KEY: "2040-01-01T00:00:00+00:00",
                     "__file_modification_time": "2040-01-01T00:00:00+00:00", "aux": "kept"}
