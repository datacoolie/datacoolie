"""Persisted API watermark windows with deterministic request bounds."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import polars as pl
import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.core.models.run_config import ReplayConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.watermark.base import WatermarkSerializer


pytestmark = pytest.mark.integration


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


def _run_driver_with_start_operator(
    metadata_path: Path,
    state_root: Path,
    log_root: Path,
    *,
    job_id: str,
    start_operator: str,
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
        prepared = driver._prepare_execution_dataflow(dataflow)
        return driver._run_single_pipeline(
            prepared,
            watermark_start_operator=start_operator,
        )


def _run_replay(
    metadata_path: Path,
    state_root: Path,
    log_root: Path,
    replay: ReplayConfig,
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
        return driver.run_replay(provider.get_dataflows(stage="bronze"), replay)


def _write_metadata(
    path: Path,
    base_url: str,
    output_root: Path,
    *,
    watermark_columns: list[str] | None = None,
    source_config: dict | None = None,
) -> None:
    configure = {
        "endpoint": "/items",
        "watermark_param_mapping": {"updated_at": "updated_since"},
        "watermark_to_param": "updated_before",
        "watermark_param_format": "iso",
    }
    if source_config:
        configure.update(source_config)
        if "range_param_mapping" in source_config:
            for legacy_key in (
                "watermark_param_mapping",
                "watermark_to_param",
                "watermark_param_format",
                "watermark_param_location",
            ):
                configure.pop(legacy_key, None)
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
                        "connection_type": "file",
                        "format": "parquet",
                        "configure": {"base_path": str(output_root)},
                    },
                ],
                "dataflows": [
                    {
                        "name": "windowed-api-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": watermark_columns or ["updated_at"],
                            "configure": configure,
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


def _read_ids(output_root: Path) -> list[int]:
    return sorted(
        row["id"]
        for path in sorted((output_root / "events").glob("*.parquet"))
        for row in pl.read_parquet(path).select("id").to_dicts()
    )


def _read_checkpoint(state_root: Path) -> str:
    paths = sorted(state_root.rglob("watermark_value.json"))
    assert len(paths) == 1
    return paths[0].read_text(encoding="utf-8")


def _read_checkpoint_values(state_root: Path) -> dict:
    return WatermarkSerializer.deserialize(_read_checkpoint(state_root))


def test_api_request_window_and_checkpoint_share_bound_across_restart(tmp_path):
    state = {"queries": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            query = parse_qs(urlparse(self.path).query)
            state["queries"].append(query)
            if "updated_since" in query:
                payload = [
                    {"id": 2, "updated_at": "2026-09-27T10:01:00+00:00"}
                ]
            else:
                payload = [
                    {"id": 1, "updated_at": "2026-09-27T09:59:00+00:00"}
                ]
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        log_root = tmp_path / "logs"
        metadata_path = tmp_path / "metadata.json"
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        _write_metadata(metadata_path, base_url, output_root)

        first_bound = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
        with patch("datacoolie.sources.api_reader.datetime") as clock:
            clock.now.return_value = first_bound
            first = _run_driver(
                metadata_path,
                state_root,
                log_root,
                job_id="api-window-recovery",
            )

        assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
        assert _read_ids(output_root) == [1]
        first_checkpoint = _read_checkpoint(state_root)
        assert first_bound.isoformat() in first_checkpoint
        assert state["queries"][0]["updated_before"] == [first_bound.isoformat()]
        assert "updated_since" not in state["queries"][0]

        second_bound = datetime(2026, 9, 27, 10, 2, tzinfo=timezone.utc)
        with patch("datacoolie.sources.api_reader.datetime") as clock:
            clock.now.return_value = second_bound
            second = _run_driver(
                metadata_path,
                state_root,
                log_root,
                job_id="api-window-recovery",
            )

        assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
        assert _read_ids(output_root) == [1, 2]
        assert state["queries"][1]["updated_since"] == [first_bound.isoformat()]
        assert state["queries"][1]["updated_before"] == [second_bound.isoformat()]
        second_checkpoint = _read_checkpoint(state_root)
        assert second_checkpoint != first_checkpoint
        assert second_bound.isoformat() in second_checkpoint
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize(
    ("start_operator", "expected_ids"),
    [
        (None, [1, 2, 3]),
        (">=", [1, 2, 2, 3]),
    ],
    ids=["default-exclusive", "explicit-inclusive"],
)
def test_api_observed_max_equality_restart_respects_lower_operator(
    tmp_path,
    start_operator,
    expected_ids,
):
    state = {"queries": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            query = parse_qs(urlparse(self.path).query)
            state["queries"].append(query)
            if "seq_from" in query:
                payload = [
                    {"id": 2, "seq": 2},
                    {"id": 3, "seq": 3},
                ]
            else:
                payload = [{"id": 1, "seq": 1}, {"id": 2, "seq": 2}]
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        log_root = tmp_path / "logs"
        metadata_path = tmp_path / "metadata.json"
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        _write_metadata(
            metadata_path,
            base_url,
            output_root,
            watermark_columns=["seq"],
            source_config={
                "range_param_mapping": {
                    "seq": {
                        "lower": {"name": "seq_from", "operator": ">="},
                        "upper": {"name": "seq_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "seq",
                        "watermark_value": "observed_max",
                    }
                }
            },
        )

        first = _run_driver(
            metadata_path,
            state_root,
            log_root,
            job_id="api-observed-max-recovery",
        )
        assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
        assert _read_ids(output_root) == [1, 2]
        assert _read_checkpoint_values(state_root) == {"seq": 2}

        if start_operator is None:
            second = _run_driver(
                metadata_path,
                state_root,
                log_root,
                job_id="api-observed-max-recovery",
            )
            assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
        else:
            second = _run_driver_with_start_operator(
                metadata_path,
                state_root,
                log_root,
                job_id="api-observed-max-recovery",
                start_operator=start_operator,
            )
            assert second.status == "succeeded", second.message
        assert _read_ids(output_root) == expected_ids
        assert state["queries"] == [{}, {"seq_from": ["2"]}]
        assert _read_checkpoint_values(state_root) == {"seq": 3}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_api_request_end_equality_is_inclusive_and_saves_covered_end(tmp_path):
    covered_start = datetime(2026, 9, 27, tzinfo=timezone.utc)
    covered_end = datetime(2026, 9, 28, tzinfo=timezone.utc)
    state = {"mode": "replay", "queries": [], "second_upper": None}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            query = parse_qs(urlparse(self.path).query)
            state["queries"].append(query)
            if state["mode"] == "replay":
                payload = [
                    {
                        "id": 1,
                        "covered_at": (covered_end - timedelta(hours=1)).isoformat(),
                    }
                ]
            else:
                lower = datetime.fromisoformat(query["covered_from"][0])
                upper = datetime.fromisoformat(query["covered_to"][0])
                state["second_upper"] = upper
                payload = [
                    {"id": 2, "covered_at": lower.isoformat()},
                    {"id": 3, "covered_at": (lower + timedelta(hours=1)).isoformat()},
                ]
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        log_root = tmp_path / "logs"
        metadata_path = tmp_path / "metadata.json"
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        _write_metadata(
            metadata_path,
            base_url,
            output_root,
            watermark_columns=["covered_at"],
            source_config={
                "range_param_mapping": {
                    "covered_at": {
                        "lower": {"name": "covered_from", "operator": ">="},
                        "upper": {"name": "covered_to", "operator": "<"},
                        "format": "iso",
                        "response_column": "covered_at",
                        "watermark_value": "request_end",
                    }
                }
            },
        )

        first = _run_replay(
            metadata_path,
            state_root,
            log_root,
            ReplayConfig(
                start=covered_start,
                end=covered_end,
                save_watermark=True,
                chunk_column="covered_at",
            ),
            job_id="api-request-end-recovery",
        )
        assert (first.total, first.succeeded, first.failed) == (1, 1, 0), first.errors
        assert _read_ids(output_root) == [1]
        assert _read_checkpoint_values(state_root) == {"covered_at": covered_end}
        assert state["queries"][0] == {
            "covered_from": [covered_start.isoformat()],
            "covered_to": [covered_end.isoformat()],
        }

        state["mode"] = "normal"
        second = _run_driver(
            metadata_path,
            state_root,
            log_root,
            job_id="api-request-end-recovery",
        )
        assert (second.total, second.succeeded, second.failed) == (1, 1, 0), second.errors
        assert _read_ids(output_root) == [1, 2, 3]

        second_query = state["queries"][1]
        assert second_query["covered_from"] == [covered_end.isoformat()]
        assert "covered_to" in second_query
        assert state["second_upper"] is not None
        row_max = covered_end + timedelta(hours=1)
        assert state["second_upper"] > row_max
        assert _read_checkpoint_values(state_root) == {
            "covered_at": state["second_upper"]
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("location", ["params", "body"])
@pytest.mark.parametrize(
    "mapping_order",
    [("created_at", "updated_at"), ("updated_at", "created_at")],
    ids=["created-first", "updated-first"],
)
def test_api_independent_mapped_replay_column_does_not_persist_selection(
    tmp_path,
    location,
    mapping_order,
):
    state = {"requests": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            query = parse_qs(urlparse(self.path).query)
            state["requests"].append({"method": "GET", "params": query})
            self._send_rows()

        def do_POST(self):  # noqa: N802 - stdlib handler API
            size = int(self.headers.get("Content-Length", "0"))
            request_body = json.loads(self.rfile.read(size) or b"{}")
            state["requests"].append({"method": "POST", "body": request_body})
            self._send_rows()

        def _send_rows(self):
            payload = [
                {"id": 9, "created_at": 9, "updated_at": 90},
                {"id": 10, "created_at": 10, "updated_at": 100},
                {"id": 19, "created_at": 19, "updated_at": 190},
                {"id": 20, "created_at": 20, "updated_at": 200},
            ]
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        log_root = tmp_path / "logs"
        metadata_path = tmp_path / "metadata.json"
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        binding = {
            "created_at": {
                "lower": {
                    "location": location,
                    "name": "created_from",
                    "operator": ">=",
                },
                "upper": {
                    "location": location,
                    "name": "created_to",
                    "operator": "<",
                },
                "format": "integer",
                "response_column": "created_at",
                "watermark_value": "observed_max",
            },
            "updated_at": {
                "lower": {
                    "location": location,
                    "name": "updated_from",
                    "operator": ">=",
                },
                "upper": {
                    "location": location,
                    "name": "updated_to",
                    "operator": "<",
                },
                "format": "integer",
                "response_column": "updated_at",
                "watermark_value": "observed_max",
            },
        }
        _write_metadata(
            metadata_path,
            base_url,
            output_root,
            watermark_columns=["updated_at"],
            source_config={
                "method": "POST" if location == "body" else "GET",
                "body": {"kind": "event"} if location == "body" else {},
                "range_param_mapping": {
                    field: binding[field] for field in mapping_order
                },
            },
        )

        result = _run_replay(
            metadata_path,
            state_root,
            log_root,
            ReplayConfig(
                start=10,
                end=20,
                save_watermark=True,
                chunk_column="created_at",
            ),
            job_id="api-independent-replay",
        )
        assert (result.total, result.succeeded, result.failed) == (1, 1, 0), result.errors
        assert _read_ids(output_root) == [10, 19]
        if location == "params":
            assert state["requests"] == [
                {
                    "method": "GET",
                    "params": {"created_from": ["10"], "created_to": ["20"]},
                }
            ]
        else:
            assert state["requests"] == [
                {
                    "method": "POST",
                    "body": {
                        "kind": "event",
                        "created_from": 10,
                        "created_to": 20,
                    },
                }
            ]
        assert _read_checkpoint_values(state_root) == {"updated_at": 190}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_api_residual_empty_replay_preserves_seeded_target_and_state(tmp_path):
    state = {"mode": "seed", "queries": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            query = parse_qs(urlparse(self.path).query)
            state["queries"].append(query)
            if state["mode"] == "seed":
                payload = [{"id": 1, "created_at": 12, "updated_at": 100}]
            else:
                payload = [
                    {"id": 2, "created_at": 9, "updated_at": 90},
                    {"id": 3, "created_at": 20, "updated_at": 200},
                ]
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        log_root = tmp_path / "logs"
        metadata_path = tmp_path / "metadata.json"
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        _write_metadata(
            metadata_path,
            base_url,
            output_root,
            watermark_columns=["updated_at"],
            source_config={
                "range_param_mapping": {
                    "created_at": {
                        "lower": {"name": "created_from", "operator": ">="},
                        "upper": {"name": "created_to", "operator": "<"},
                        "format": "integer",
                        "response_column": "created_at",
                        "watermark_value": "observed_max",
                    }
                }
            },
        )

        seeded = _run_driver(
            metadata_path,
            state_root,
            log_root,
            job_id="api-residual-empty",
        )
        assert (seeded.total, seeded.succeeded, seeded.failed) == (1, 1, 0), seeded.errors
        assert _read_ids(output_root) == [1]
        checkpoint_before = _read_checkpoint(state_root)

        state["mode"] = "residual-empty"
        replayed = _run_replay(
            metadata_path,
            state_root,
            log_root,
            ReplayConfig(
                start=10,
                end=20,
                save_watermark=True,
                chunk_column="created_at",
            ),
            job_id="api-residual-empty",
        )
        assert (replayed.total, replayed.succeeded, replayed.failed) == (1, 0, 0)
        assert replayed.skipped == 1
        assert _read_ids(output_root) == [1]
        assert _read_checkpoint(state_root) == checkpoint_before
        assert state["queries"][1] == {
            "created_from": ["10"],
            "created_to": ["20"],
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
