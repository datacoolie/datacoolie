"""Opt-in native replacement-window qualification.

The default suite keeps this module skipped.  It exercises a real Polars /
Delta path so the engine-owned delete-then-append boundary is not represented
only by a mock.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.engines.contracts.windows import WindowSpec
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]


def test_polars_delta_replace_window_preserves_out_of_window_rows(
    tmp_path: Path,
) -> None:
    polars = pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    from datacoolie.engines.polars_engine import PolarsEngine

    engine = PolarsEngine()
    path = tmp_path / "orders"
    initial = polars.DataFrame(
        {
            "id": [1, 2, 3],
            "modified_at": [1, 2, 3],
            "value": ["old-1", "old-2", "old-3"],
        }
    ).lazy()
    engine.write_to_path(initial, str(path), mode="overwrite", fmt="delta")

    replacement = polars.DataFrame(
        {
            "id": [2, 3],
            "modified_at": [2, 3],
            "value": ["new-2", "new-3"],
        }
    ).lazy()
    engine.replace_window(
        replacement,
        path=str(path),
        window=WindowSpec(bounds={"modified_at": (1, 3)}),
        fmt="delta",
    )

    rows = (
        engine.read_delta(str(path))
        .collect()
        .sort("id")
        .select("id", "modified_at", "value")
        .to_dicts()
    )
    assert rows == [
        {"id": 1, "modified_at": 1, "value": "old-1"},
        {"id": 2, "modified_at": 2, "value": "new-2"},
        {"id": 3, "modified_at": 3, "value": "new-3"},
    ]


def _write_delta_metadata(
    path: Path,
    input_root: Path,
    output_root: Path,
    *,
    transform: dict[str, object] | None = None,
) -> None:
    """Write the smallest metadata document that exercises Driver replay."""

    path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "lakehouse",
                        "format": "delta",
                        "configure": {"base_path": str(input_root)},
                    },
                    {
                        "name": "destination",
                        "connection_type": "lakehouse",
                        "format": "delta",
                        "configure": {"base_path": str(output_root)},
                    },
                ],
                "dataflows": [
                    {
                        "name": "replace-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": ["Modified At"],
                            "configure": {"backward_days": 1},
                        },
                        "destination": {
                            "connection_name": "destination",
                            "table": "events",
                            "load_type": "merge_overwrite",
                            "configure": {"replace_by_watermark": True},
                        },
                        "transform": transform or {"rename_columns": {"Modified At": "Window-Start"}},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _run_delta_replay(
    metadata_path: Path,
    state_root: Path,
    log_root: Path,
    *,
    job_id: str,
):
    """Run one Driver replay against the local Delta source and target."""

    from datacoolie.engines.polars_engine import PolarsEngine

    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata_path), platform=platform)
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform),
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(state_root),
        log_base_path=str(log_root),
        config=DataCoolieRunConfig(job_id=job_id, retry_count=0, retry_delay=0),
    ) as driver:
        dataflow = provider.get_dataflows(stage="bronze")[0]
        return driver.run_replay(
            dataflow,
            ReplayConfig(
                start=1,
                end=10,
                chunk_column="Modified At",
                save_watermark=True,
            ),
            column_name_mode="snake",
        )


def _delta_rows(path: Path) -> list[dict[str, object]]:
    """Read only business columns so framework audit columns stay irrelevant."""

    pytest.importorskip("polars")
    from datacoolie.engines.polars_engine import PolarsEngine

    return (
        PolarsEngine()
        .read_delta(str(path))
        .collect()
        .select("id", "window_start", "value")
        .sort("window_start")
        .to_dicts()
    )


def _write_polars_delta(engine, path: Path, frame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    engine.write_to_path(frame.lazy(), str(path), mode="overwrite", fmt="delta")


@pytest.mark.runtime_qualification
def test_driver_delta_replace_keeps_requested_upper_and_is_stable_on_rerun(
    tmp_path: Path,
) -> None:
    """A replay replaces the full requested window, including an empty tail."""

    pl = pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    from datacoolie.engines.polars_engine import PolarsEngine

    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    source_path = input_root / "events"
    target_path = output_root / "events"
    source_engine = PolarsEngine()
    _write_polars_delta(
        source_engine,
        source_path,
        pl.DataFrame(
            {
                "id": [2, 3],
                "Modified At": [2, 3],
                "value": ["new-2", "new-3"],
            }
        ),
    )
    _write_polars_delta(
        source_engine,
        target_path,
        pl.DataFrame(
            {
                "id": [100, 101, 102, 103, 104],
                "window_start": [0, 1, 4, 9, 10],
                "value": ["before", "stale-1", "stale-4", "stale-9", "after"],
            }
        ),
    )
    metadata_path = tmp_path / "metadata.json"
    _write_delta_metadata(metadata_path, input_root, output_root)
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"

    first = _run_delta_replay(
        metadata_path,
        state_root,
        log_root,
        job_id="delta-replace-first",
    )
    assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
    expected = [
        {"id": 100, "window_start": 0, "value": "before"},
        {"id": 2, "window_start": 2, "value": "new-2"},
        {"id": 3, "window_start": 3, "value": "new-3"},
        {"id": 104, "window_start": 10, "value": "after"},
    ]
    assert _delta_rows(target_path) == expected
    checkpoint = sorted(state_root.rglob("watermark_value.json"))
    assert len(checkpoint) == 1
    assert json.loads(checkpoint[0].read_text(encoding="utf-8")) == {"Modified At": 3}

    second = _run_delta_replay(
        metadata_path,
        state_root,
        log_root,
        job_id="delta-replace-rerun",
    )
    assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
    assert _delta_rows(target_path) == expected


@pytest.mark.runtime_qualification
def test_driver_delta_typed_empty_replaces_requested_window_without_observed_max(
    tmp_path: Path,
) -> None:
    """A typed empty Delta range still deletes stale rows through the request upper."""

    pl = pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    from datacoolie.engines.polars_engine import PolarsEngine

    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    source_path = input_root / "events"
    target_path = output_root / "events"
    engine = PolarsEngine()
    _write_polars_delta(
        engine,
        source_path,
        pl.DataFrame(
            {
                "id": [200, 201],
                "Modified At": [0, 10],
                "value": ["before", "after"],
            }
        ),
    )
    _write_polars_delta(
        engine,
        target_path,
        pl.DataFrame(
            {
                "id": [100, 101, 102, 103],
                "window_start": [0, 2, 8, 10],
                "value": ["before", "stale-2", "stale-8", "after"],
            }
        ),
    )
    metadata_path = tmp_path / "metadata.json"
    _write_delta_metadata(metadata_path, input_root, output_root)
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"

    first = _run_delta_replay(
        metadata_path,
        state_root,
        log_root,
        job_id="delta-empty-first",
    )
    assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
    expected = [
        {"id": 100, "window_start": 0, "value": "before"},
        {"id": 103, "window_start": 10, "value": "after"},
    ]
    assert _delta_rows(target_path) == expected
    state_files = list(state_root.rglob("watermark_value.json"))
    assert not state_files, [
        (path.as_posix(), path.read_text(encoding="utf-8")) for path in state_files
    ]

    second = _run_delta_replay(
        metadata_path,
        state_root,
        log_root,
        job_id="delta-empty-rerun",
    )
    assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
    assert _delta_rows(target_path) == expected


@pytest.mark.runtime_qualification
@pytest.mark.parametrize(
    "transform",
    [
        {"drop_columns": ["Modified At"]},
        {"select_columns": ["id", "value"]},
    ],
    ids=["drop", "select"],
)
def test_driver_replacement_guard_rejects_missing_watermark_before_write(
    tmp_path: Path,
    transform: dict[str, object],
) -> None:
    """A transformed-away source range column cannot mutate the Delta target."""

    pl = pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    from datacoolie.engines.polars_engine import PolarsEngine

    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    source_path = input_root / "events"
    target_path = output_root / "events"
    engine = PolarsEngine()
    _write_polars_delta(
        engine,
        source_path,
        pl.DataFrame(
            {"id": [2], "Modified At": [2], "value": ["new-2"]}
        ),
    )
    _write_polars_delta(
        engine,
        target_path,
        pl.DataFrame(
            {"id": [100, 101, 102], "window_start": [0, 2, 10], "value": ["before", "old", "after"]}
        ),
    )
    before = _delta_rows(target_path)
    metadata_path = tmp_path / "metadata.json"
    _write_delta_metadata(
        metadata_path,
        input_root,
        output_root,
        transform=transform,
    )

    result = _run_delta_replay(
        metadata_path,
        tmp_path / "state",
        tmp_path / "logs",
        job_id="delta-guard",
    )
    assert (result.total, result.succeeded, result.failed) == (1, 0, 1)
    assert _delta_rows(target_path) == before
    assert not list((tmp_path / "state").rglob("watermark_value.json"))
