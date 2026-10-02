"""API watermark operators reaching native Delta replacement windows."""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest

from datacoolie.core.constants import DATE_FOLDER_PARTITION_KEY
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.watermark.base import WatermarkSerializer


pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]


ApiHandler = Callable[[dict[str, list[str]], dict[str, Any]], list[dict[str, Any]]]


@contextmanager
def _api_server(handler: ApiHandler) -> Iterator[tuple[str, dict[str, Any]]]:
    state: dict[str, Any] = {"queries": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            query = parse_qs(urlparse(self.path).query)
            state["queries"].append(query)
            payload = handler(query, state)
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: Any) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _write_api_metadata(
    path: Path,
    base_url: str,
    output_root: Path,
    *,
    watermark_field: str,
    range_mapping: dict[str, Any],
) -> None:
    path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "api",
                        "format": "api",
                        "configure": {"base_url": base_url},
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
                        "name": "api-window-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": [watermark_field],
                            "configure": {
                                "endpoint": "/items",
                                "backward_days": 1,
                                "range_param_mapping": range_mapping,
                            },
                        },
                        "destination": {
                            "connection_name": "destination",
                            "table": "events",
                            "load_type": "merge_overwrite",
                            "configure": {"replace_by_watermark": True},
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_folder_metadata(
    path: Path,
    input_root: Path,
    output_root: Path,
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
                        "connection_type": "lakehouse",
                        "format": "delta",
                        "configure": {"base_path": str(output_root)},
                    },
                ],
                "dataflows": [
                    {
                        "name": "folder-window-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": [],
                            "configure": {"backward_days": 1},
                        },
                        "destination": {
                            "connection_name": "destination",
                            "table": "events",
                            "load_type": "merge_overwrite",
                            "merge_keys": ["id"],
                            "configure": {"replace_by_watermark": True},
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _run_driver(
    metadata_path: Path,
    state_root: Path,
    log_root: Path,
    *,
    job_id: str,
):
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
        return driver.run(stage="bronze")


def _run_replay(
    metadata_path: Path,
    state_root: Path,
    log_root: Path,
    *,
    start: int,
    end: int,
    job_id: str,
):
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
                start=start,
                end=end,
                chunk_column="seq",
                save_watermark=True,
            ),
        )


def _delta_rows(path: Path, *columns: str, sort_by: str) -> list[dict[str, Any]]:
    return (
        PolarsEngine()
        .read_delta(str(path))
        .collect()
        .select(*columns)
        .sort(sort_by)
        .to_dicts()
    )


def _append_delta(path: Path, rows: list[dict[str, Any]]) -> None:
    """Append fixture rows with the schema produced by the first Driver write."""

    engine = PolarsEngine()
    existing = engine.read_delta(str(path)).collect()
    incoming = pl.DataFrame(rows)
    for name, dtype in existing.schema.items():
        if name not in incoming.columns:
            incoming = incoming.with_columns(pl.lit(None, dtype=dtype).alias(name))
    incoming = incoming.select(existing.columns)
    engine.write_to_path(incoming.lazy(), str(path), mode="append", fmt="delta")


def _checkpoint_values(state_root: Path) -> dict[str, Any]:
    paths = sorted(state_root.rglob("watermark_value.json"))
    assert len(paths) == 1
    return WatermarkSerializer.deserialize(paths[0].read_text(encoding="utf-8"))


def test_api_observed_max_replaces_native_delta_window_and_preserves_tail(
    tmp_path: Path,
) -> None:
    """Observed-max uses ``> lower`` and ``<= observed_max`` at Delta."""

    def respond(query: dict[str, list[str]], state: dict[str, Any]) -> list[dict[str, Any]]:
        if "seq_from" not in query:
            assert query == {}
            return [
                {"id": 1, "seq": 1, "value": "first-1"},
                {"id": 2, "seq": 2, "value": "first-2"},
            ]
        assert query == {"seq_from": ["2"]}
        state["second_request"] = query
        return [
            {"id": 2, "seq": 2, "value": "api-equality"},
            {"id": 3, "seq": 3, "value": "new-3"},
            {"id": 4, "seq": 4, "value": "new-4"},
        ]

    mapping = {
        "seq": {
            "lower": {"name": "seq_from", "operator": ">="},
            "upper": {"name": "seq_to", "operator": "<"},
            "format": "integer",
            "response_column": "seq",
            "watermark_value": "observed_max",
        }
    }
    with _api_server(respond) as (base_url, state):
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        metadata_path = tmp_path / "metadata.json"
        _write_api_metadata(
            metadata_path,
            base_url,
            output_root,
            watermark_field="seq",
            range_mapping=mapping,
        )

        first = _run_driver(
            metadata_path,
            state_root,
            tmp_path / "logs",
            job_id="api-observed-max-delta",
        )
        assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
        assert _checkpoint_values(state_root) == {"seq": 2}

        _append_delta(
            output_root / "events",
            [
                {"id": 90, "seq": 3, "value": "stale-3"},
                {"id": 91, "seq": 5, "value": "after-5"},
            ],
        )
        second = _run_driver(
            metadata_path,
            state_root,
            tmp_path / "logs",
            job_id="api-observed-max-delta",
        )

    assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
    assert state["queries"] == [{}, {"seq_from": ["2"]}]
    assert _checkpoint_values(state_root) == {"seq": 4}
    assert _delta_rows(
        output_root / "events", "id", "seq", "value", sort_by="seq"
    ) == [
        {"id": 1, "seq": 1, "value": "first-1"},
        {"id": 2, "seq": 2, "value": "first-2"},
        {"id": 3, "seq": 3, "value": "new-3"},
        {"id": 4, "seq": 4, "value": "new-4"},
        {"id": 91, "seq": 5, "value": "after-5"},
    ]


def test_api_request_end_replay_replaces_half_open_native_delta_window(
    tmp_path: Path,
) -> None:
    """Request-end replay uses ``>= lower`` and ``< requested_end`` at Delta."""

    def respond(query: dict[str, list[str]], state: dict[str, Any]) -> list[dict[str, Any]]:
        if query == {"seq_from": ["0"], "seq_to": ["3"]}:
            return [
                {"id": 1, "seq": 0, "value": "first-0"},
                {"id": 2, "seq": 1, "value": "first-1"},
                {"id": 3, "seq": 2, "value": "first-2"},
            ]
        assert query == {"seq_from": ["3"], "seq_to": ["6"]}
        state["second_bounds"] = query
        return [
            {"id": 20, "seq": 3, "value": "new-3"},
            {"id": 21, "seq": 4, "value": "new-4"},
            {"id": 22, "seq": 6, "value": "api-upper"},
        ]

    mapping = {
        "seq": {
            "lower": {"name": "seq_from", "operator": ">="},
            "upper": {"name": "seq_to", "operator": "<"},
            "format": "integer",
            "response_column": "seq",
            "watermark_value": "request_end",
        }
    }
    with _api_server(respond) as (base_url, state):
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        metadata_path = tmp_path / "metadata.json"
        _write_api_metadata(
            metadata_path,
            base_url,
            output_root,
            watermark_field="seq",
            range_mapping=mapping,
        )

        first = _run_replay(
            metadata_path,
            state_root,
            tmp_path / "logs",
            start=0,
            end=3,
            job_id="api-request-end-delta",
        )
        assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
        assert _checkpoint_values(state_root) == {"seq": 3}

        _append_delta(
            output_root / "events",
            [
                {"id": 90, "seq": 3, "value": "stale-3"},
                {"id": 91, "seq": 4, "value": "stale-4"},
                {"id": 92, "seq": 6, "value": "after-6"},
            ],
        )
        second = _run_replay(
            metadata_path,
            state_root,
            tmp_path / "logs",
            start=3,
            end=6,
            job_id="api-request-end-delta",
        )

    assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
    assert state["queries"] == [
        {"seq_from": ["0"], "seq_to": ["3"]},
        {"seq_from": ["3"], "seq_to": ["6"]},
    ]
    assert _checkpoint_values(state_root) == {"seq": 6}
    assert _delta_rows(
        output_root / "events", "id", "seq", "value", sort_by="seq"
    ) == [
        {"id": 1, "seq": 0, "value": "first-0"},
        {"id": 2, "seq": 1, "value": "first-1"},
        {"id": 3, "seq": 2, "value": "first-2"},
        {"id": 20, "seq": 3, "value": "new-3"},
        {"id": 21, "seq": 4, "value": "new-4"},
        {"id": 92, "seq": 6, "value": "after-6"},
    ]


def test_folder_only_metadata_does_not_create_native_delta_delete_scope(
    tmp_path: Path,
) -> None:
    """A discovery cursor must not be treated as a destination row bound."""

    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    state_root = tmp_path / "state"
    metadata_path = tmp_path / "metadata.json"
    _write_folder_metadata(metadata_path, input_root, output_root)

    first_dir = input_root / "events" / "2026" / "01" / "01"
    first_dir.mkdir(parents=True)
    pl.DataFrame({"id": [1], "value": ["first"]}).write_parquet(
        first_dir / "first.parquet"
    )

    first = _run_driver(
        metadata_path,
        state_root,
        tmp_path / "logs",
        job_id="folder-only-delta",
    )
    assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
    assert _checkpoint_values(state_root) == {
        DATE_FOLDER_PARTITION_KEY: "2026-01-01T00:00:00+00:00"
    }

    later_dir = input_root / "events" / "2026" / "02" / "01"
    later_dir.mkdir(parents=True)
    pl.DataFrame({"id": [2], "value": ["later"]}).write_parquet(
        later_dir / "later.parquet"
    )

    before_rows = _delta_rows(output_root / "events", "id", "value", sort_by="id")
    before_checkpoint = _checkpoint_values(state_root)
    second = _run_driver(
        metadata_path,
        state_root,
        tmp_path / "logs",
        job_id="folder-only-delta",
    )
    # The current product failure occurs before the writer. Keep this guard
    # ahead of the status assertion so a future regression cannot delete rows
    # while still reporting the expected folder-only contract failure.
    if second.failed:
        assert _delta_rows(output_root / "events", "id", "value", sort_by="id") == before_rows
        assert _checkpoint_values(state_root) == before_checkpoint
    assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
    assert _delta_rows(
        output_root / "events", "id", "value", sort_by="id"
    ) == [
        {"id": 1, "value": "first"},
        {"id": 2, "value": "later"},
    ]
    assert _checkpoint_values(state_root) == {
        DATE_FOLDER_PARTITION_KEY: "2026-02-01T00:00:00+00:00"
    }
