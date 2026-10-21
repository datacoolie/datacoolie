"""Opt-in native replacement-window qualification.

The default suite keeps this module skipped.  It exercises a real Polars /
Delta path so the engine-owned delete-then-append boundary is not represented
only by a mock.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.core.models.source import Source
from datacoolie.engines.contracts.windows import WindowSpec
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.watermark.base import BaseWatermarkManager


pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]


@pytest.mark.parametrize("reverse", [False, True])
def test_driver_mixed_case_parquet_lineage_replaces_only_userid(tmp_path, reverse):
    """Distinct source spellings survive defaults and native Delta replacement."""
    pl = pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.core.models.transform import Transform

    input_root = tmp_path / "input"
    (input_root / "events").mkdir(parents=True)
    frame = pl.DataFrame({"UserID": [999], "userid": [5], "value": ["new"]})
    if reverse:
        frame = frame.select("userid", "UserID", "value")
    frame.write_parquet(input_root / "events/part.parquet")
    target = tmp_path / "output/events"
    engine = PolarsEngine()
    _write_polars_delta(engine, target, pl.DataFrame({
        "user_id": [101, 102, 103], "userid": [0, 3, 9],
        "value": ["before", "stale", "after"],
    }))
    flow = DataFlow(name="mixed-case", source=Source(
        connection=Connection(name="source", connection_type="file", format="parquet",
                              configure={"base_path": str(input_root)}),
        table="events", watermark_columns=["userid"], configure={"backward_days": 1},
    ), destination=Destination(
        connection=Connection(name="target", connection_type="lakehouse", format="delta",
                              configure={"base_path": str(target.parent)}),
        table="events", load_type="merge_overwrite", configure={"replace_by_watermark": True},
    ))
    manager = _MemoryWatermarkManager({"userid": 1})
    with DataCoolieDriver(engine=PolarsEngine(), platform=LocalPlatform(), watermark_manager=manager,
        state_base_path=str(tmp_path / "state"), log_base_path=str(tmp_path / "logs"),
        config=DataCoolieRunConfig(job_id="mixed-case", retry_count=0, retry_delay=0)) as driver:
        result = driver.run(dataflows=[flow], column_name_mode="snake")
        assert (result.succeeded, result.failed) == (1, 0), result.errors
        expected = [
            {"user_id": 101, "userid": 0, "value": "before"},
            {"user_id": 999, "userid": 5, "value": "new"},
            {"user_id": 103, "userid": 9, "value": "after"},
        ]
        assert engine.read_delta(str(target)).collect().select("user_id", "userid", "value").sort("userid").to_dicts() == expected
        assert manager.current == {"userid": 5}
        assert manager.saved == [{"userid": 5}]
        before_log = _delta_log_entries(target)
        flow.transform = Transform(select_columns=["value"])
        failed = driver.run_replay(flow, ReplayConfig(start=1, end=10, chunk_column="userid",
                                                    save_watermark=True), column_name_mode="snake")
        assert (failed.succeeded, failed.failed) == (0, 1), failed.errors
        assert engine.read_delta(str(target)).collect().select("user_id", "userid", "value").sort("userid").to_dicts() == expected
        assert _delta_log_entries(target) == before_log
        assert manager.current == {"userid": 5}
        assert manager.saved == [{"userid": 5}]


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


@pytest.mark.runtime_qualification
def test_explicit_replay_replaces_deleted_rows_with_unrelated_opaque_cursor(tmp_path: Path):
    pl = pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    from datacoolie.engines.polars_engine import PolarsEngine

    engine = PolarsEngine()
    input_root, output_root = tmp_path / "input", tmp_path / "output"
    _write_polars_delta(engine, input_root / "events", pl.DataFrame(
        {"id": [2], "Modified At": [2], "cursor": ["next"], "value": ["new"]}))
    _write_polars_delta(engine, output_root / "events", pl.DataFrame(
        {"id": [100, 101, 102, 103], "window_start": [0, 2, 3, 10],
         "cursor": ["old"] * 4, "value": ["before", "old", "deleted", "after"]}))
    metadata = tmp_path / "metadata.json"
    _write_delta_metadata(metadata, input_root, output_root)
    payload = json.loads(metadata.read_text(encoding="utf-8"))
    payload["dataflows"][0]["source"]["watermark_columns"] = ["cursor"]
    metadata.write_text(json.dumps(payload), encoding="utf-8")
    result = _run_delta_replay(metadata, tmp_path / "state", tmp_path / "logs", job_id="opaque-range")
    assert result.succeeded == 1, result.errors
    assert sorted(row["id"] for row in _delta_rows(output_root / "events")) == [2, 100, 103]


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
    engine=None,
):
    """Run one Driver replay against the local Delta source and target."""

    from datacoolie.engines.polars_engine import PolarsEngine

    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata_path), platform=platform)
    with DataCoolieDriver(
        engine=engine if engine is not None else PolarsEngine(platform=platform),
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


def _delta_log_entries(path: Path) -> tuple[str, ...]:
    return tuple(sorted(entry.name for entry in (path / "_delta_log").glob("*.json")))


def _write_polars_delta(engine, path: Path, frame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    engine.write_to_path(frame.lazy(), str(path), mode="overwrite", fmt="delta")


class _MemoryWatermarkManager(BaseWatermarkManager):
    """Small in-memory checkpoint store for a native Driver qualification."""

    def __init__(self, watermark: dict[str, object]) -> None:
        self.current = dict(watermark)
        self.saved: list[dict[str, object]] = []

    def get_watermark(self, dataflow_id: str) -> dict[str, object]:
        return dict(self.current)

    def save_watermark(
        self,
        dataflow_id: str,
        watermark: dict[str, object],
        *,
        job_id: str | None = None,
        dataflow_run_id: str | None = None,
    ) -> None:
        self.current = dict(watermark)
        self.saved.append(dict(watermark))


def test_driver_delta_replace_uses_effective_lower_for_normal_lookback(
    tmp_path: Path,
) -> None:
    """Normal ETL replaces the effective lookback window and is rerun-stable."""

    pl = pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    from datacoolie.engines.polars_engine import PolarsEngine

    day_5 = datetime(2026, 1, 5, tzinfo=timezone.utc)
    day_10 = datetime(2026, 1, 10, tzinfo=timezone.utc)
    day_12 = datetime(2026, 1, 12, tzinfo=timezone.utc)
    day_20 = datetime(2026, 1, 20, tzinfo=timezone.utc)
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
                "id": [1, 2],
                "modified_at": [day_5, day_12],
                "value": ["new-5", "new-12"],
            }
        ),
    )
    _write_polars_delta(
        engine,
        target_path,
        pl.DataFrame(
            {
                "id": [100, 101, 102],
                "modified_at": [day_5, day_12, day_20],
                "value": ["stale-5", "stale-12", "keep"],
            }
        ),
    )

    source_connection = Connection(
        name="source",
        connection_type="lakehouse",
        format="delta",
        configure={"base_path": str(input_root)},
    )
    destination_connection = Connection(
        name="destination",
        connection_type="lakehouse",
        format="delta",
        configure={"base_path": str(output_root)},
    )
    dataflow = DataFlow(
        name="replace-events",
        source=Source(
            connection=source_connection,
            table="events",
            watermark_columns=["modified_at"],
            configure={"backward_days": 7},
        ),
        destination=Destination(
            connection=destination_connection,
            table="events",
            load_type="merge_overwrite",
            configure={"replace_by_watermark": True},
        ),
    )
    manager = _MemoryWatermarkManager({"modified_at": day_10})
    platform = LocalPlatform()

    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform),
        platform=platform,
        watermark_manager=manager,
        state_base_path=str(tmp_path / "state"),
        log_base_path=str(tmp_path / "logs"),
        config=DataCoolieRunConfig(
            job_id="native-lookback-replace",
            retry_count=0,
            retry_delay=0,
        ),
    ) as driver:
        first = driver.run(dataflows=[dataflow])
        second = driver.run(dataflows=[dataflow])

    assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
    assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
    expected = [
        {"id": 1, "modified_at": day_5, "value": "new-5"},
        {"id": 2, "modified_at": day_12, "value": "new-12"},
        {"id": 102, "modified_at": day_20, "value": "keep"},
    ]
    rows = (
        engine.read_delta(str(target_path))
        .collect()
        .select("id", "modified_at", "value")
        .sort("modified_at")
        .to_dicts()
    )
    assert rows == expected
    assert manager.saved == [{"modified_at": day_12}, {"modified_at": day_12}]


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
    """A bounded source-empty replay skips without replacing its target window."""

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
    initial_delta_log = _delta_log_entries(target_path)

    first = _run_delta_replay(
        metadata_path,
        state_root,
        log_root,
        job_id="delta-empty-first",
    )
    assert (first.total, first.succeeded, first.failed, first.skipped) == (1, 0, 0, 1), first.errors
    expected = [
        {"id": 100, "window_start": 0, "value": "before"},
        {"id": 101, "window_start": 2, "value": "stale-2"},
        {"id": 102, "window_start": 8, "value": "stale-8"},
        {"id": 103, "window_start": 10, "value": "after"},
    ]
    assert _delta_rows(target_path) == expected
    assert _delta_log_entries(target_path) == initial_delta_log
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
    assert (second.total, second.succeeded, second.failed, second.skipped) == (1, 0, 0, 1), second.errors
    assert _delta_rows(target_path) == expected
    assert _delta_log_entries(target_path) == initial_delta_log
    assert not list(state_root.rglob("watermark_value.json"))


@pytest.mark.runtime_qualification
def test_driver_delta_transformed_empty_replaces_requested_window(
    tmp_path: Path,
) -> None:
    """A nonempty source filtered empty still reaches native replacement."""

    pl = pytest.importorskip("polars")
    pytest.importorskip("deltalake")
    from datacoolie.engines.polars_engine import PolarsEngine

    class ActionTracePolarsEngine(PolarsEngine):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.action_trace: list[str] = []

        def get_count_and_max_values(self, df, columns):
            self.action_trace.append("source_count")
            return super().get_count_and_max_values(df, columns)

        def count_rows(self, df):
            self.action_trace.append("pipeline_count")
            return super().count_rows(df)

        def _prepare_replace_window_input(self, df):
            self.action_trace.append("replace_input_materialize")
            return super()._prepare_replace_window_input(df)

        def is_empty(self, df):
            self.action_trace.append("writer_empty_check")
            return super().is_empty(df)

    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    source_path = input_root / "events"
    target_path = output_root / "events"
    seed_engine = PolarsEngine()
    _write_polars_delta(
        seed_engine,
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
        seed_engine,
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
    _write_delta_metadata(
        metadata_path,
        input_root,
        output_root,
        transform={
            "filter_expression": "id < 0",
            "rename_columns": {"Modified At": "Window-Start"},
        },
    )
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"
    action_engine = ActionTracePolarsEngine()

    result = _run_delta_replay(
        metadata_path,
        state_root,
        log_root,
        job_id="delta-transformed-empty",
        engine=action_engine,
    )

    assert (result.total, result.succeeded, result.failed, result.skipped) == (1, 1, 0, 0), result.errors
    assert _delta_rows(target_path) == [
        {"id": 100, "window_start": 0, "value": "before"},
        {"id": 103, "window_start": 10, "value": "after"},
    ]
    checkpoints = sorted(state_root.rglob("watermark_value.json"))
    assert len(checkpoints) == 1
    assert json.loads(checkpoints[0].read_text(encoding="utf-8")) == {"Modified At": 3}
    assert action_engine.action_trace.count("source_count") == 1
    assert action_engine.action_trace.count("pipeline_count") == 0
    assert action_engine.action_trace.count("replace_input_materialize") == 1
    assert action_engine.action_trace.count("writer_empty_check") == 1


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
