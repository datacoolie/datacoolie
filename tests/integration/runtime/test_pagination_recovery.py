"""Driver-level pagination completeness and restart behavior."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.watermark.base import WatermarkSerializer


pytestmark = pytest.mark.integration


def _write_metadata(
    path: Path,
    base_url: str,
    output_root: Path,
    *,
    max_pages: int,
    source_config: dict | None = None,
) -> None:
    configure = {
        "endpoint": "/items",
        "pagination_type": "next_link",
        "data_path": "data",
        "next_link_path": "paging.next",
        "max_pages": max_pages,
    }
    if source_config:
        configure.update(source_config)
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
                        "name": "paginated-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": ["updated_at"],
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


def _run_driver(metadata_path: Path, state_root: Path, log_root: Path):
    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata_path), platform=platform)
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform),
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(state_root),
        log_base_path=str(log_root),
        config=DataCoolieRunConfig(
            job_id="pagination-recovery",
            retry_count=0,
            retry_delay=0,
        ),
    ) as driver:
        return driver.run(stage="bronze")


def _run_replay(
    metadata_path: Path,
    state_root: Path,
    log_root: Path,
    replay: ReplayConfig,
):
    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata_path), platform=platform)
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform),
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(state_root),
        log_base_path=str(log_root),
        config=DataCoolieRunConfig(
            job_id="pagination-recovery",
            retry_count=0,
            retry_delay=0,
        ),
    ) as driver:
        return driver.run_replay(provider.get_dataflows(stage="bronze"), replay)


def _read_ids(output_root: Path) -> list[int]:
    table_root = output_root / "events"
    if not table_root.exists():
        return []
    return sorted(
        row["id"]
        for path in sorted(table_root.rglob("*.parquet"))
        for row in pl.read_parquet(path).select("id").to_dicts()
    )


def _watermark_paths(state_root: Path) -> list[Path]:
    return sorted(state_root.rglob("watermark_value.json"))


def _read_watermark(state_root: Path) -> dict:
    paths = _watermark_paths(state_root)
    assert len(paths) == 1
    return WatermarkSerializer.deserialize(paths[0].read_text(encoding="utf-8"))


def test_pagination_cap_failure_does_not_commit_partial_page(tmp_path):
    state = {"requests": []}
    base_url_holder = {"value": ""}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            state["requests"].append(self.path)
            page = parse_qs(urlparse(self.path).query).get("page", ["1"])[0]
            if page == "2":
                payload = {
                    "data": [{"id": 2, "updated_at": "2026-09-27T10:01:00+00:00"}],
                    "paging": {"next": None},
                }
            else:
                payload = {
                    "data": [{"id": 1, "updated_at": "2026-09-27T10:00:00+00:00"}],
                    "paging": {"next": f"{base_url_holder['value']}/items?page=2"},
                }
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    base_url_holder["value"] = f"http://127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        log_root = tmp_path / "logs"
        metadata_path = tmp_path / "metadata.json"
        _write_metadata(
            metadata_path,
            base_url_holder["value"],
            output_root,
            max_pages=1,
        )

        failed = _run_driver(metadata_path, state_root, log_root)
        assert (failed.total, failed.succeeded, failed.failed) == (1, 0, 1)
        assert _read_ids(output_root) == []
        assert _watermark_paths(state_root) == []
        assert len(state["requests"]) == 1

        _write_metadata(
            metadata_path,
            base_url_holder["value"],
            output_root,
            max_pages=3,
        )
        state["requests"].clear()
        recovered = _run_driver(metadata_path, state_root, log_root)
        assert (recovered.total, recovered.succeeded, recovered.failed) == (1, 1, 0), recovered.errors
        assert _read_ids(output_root) == [1, 2]
        assert len(_watermark_paths(state_root)) == 1
        assert len(state["requests"]) == 2
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize(
    "failure_mode",
    ["page2-error", "cap"],
    ids=["page2-error", "page-cap"],
)
def test_pagination_page2_failure_preserves_seeded_target_and_state(
    tmp_path,
    failure_mode,
):
    state = {"mode": "seed", "requests": []}
    base_url_holder = {"value": ""}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            state["requests"].append(self.path)
            page = parse_qs(urlparse(self.path).query).get("page", ["1"])[0]
            if page == "2":
                if state["mode"] == "page2-error":
                    self.send_response(503)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                payload = {
                    "data": [{"id": 3, "updated_at": "2026-09-27T10:02:00+00:00"}],
                    "paging": {"next": None},
                }
            elif state["mode"] == "seed":
                payload = {
                    "data": [{"id": 1, "updated_at": "2026-09-27T10:00:00+00:00"}],
                    "paging": {"next": None},
                }
            else:
                payload = {
                    "data": [{"id": 2, "updated_at": "2026-09-27T10:01:00+00:00"}],
                    "paging": {"next": f"{base_url_holder['value']}/items?page=2"},
                }
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    base_url_holder["value"] = f"http://127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        log_root = tmp_path / "logs"
        metadata_path = tmp_path / "metadata.json"
        _write_metadata(
            metadata_path,
            base_url_holder["value"],
            output_root,
            max_pages=3,
        )

        seeded = _run_driver(metadata_path, state_root, log_root)
        assert (seeded.total, seeded.succeeded, seeded.failed) == (1, 1, 0), seeded.errors
        assert _read_ids(output_root) == [1]
        checkpoint_before = _watermark_paths(state_root)[0].read_text(encoding="utf-8")

        state["mode"] = failure_mode
        _write_metadata(
            metadata_path,
            base_url_holder["value"],
            output_root,
            max_pages=1 if failure_mode == "cap" else 3,
        )
        state["requests"].clear()
        failed = _run_driver(metadata_path, state_root, log_root)
        assert (failed.total, failed.succeeded, failed.failed) == (1, 0, 1)
        assert _read_ids(output_root) == [1]
        assert _watermark_paths(state_root)[0].read_text(encoding="utf-8") == checkpoint_before
        assert [urlparse(path).query for path in state["requests"]] == (
            [""] if failure_mode == "cap" else ["", "page=2"]
        )

        state["mode"] = "recovered"
        _write_metadata(
            metadata_path,
            base_url_holder["value"],
            output_root,
            max_pages=3,
        )
        state["requests"].clear()
        recovered = _run_driver(metadata_path, state_root, log_root)
        assert (recovered.total, recovered.succeeded, recovered.failed) == (1, 1, 0), recovered.errors
        assert _read_ids(output_root) == [1, 2, 3]
        assert _watermark_paths(state_root)[0].read_text(encoding="utf-8") != checkpoint_before
        assert [urlparse(path).query for path in state["requests"]] == ["", "page=2"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_pagination_bounded_range_filters_later_page_equalities_and_saves_observation(
    tmp_path,
):
    """Apply [start, end) to every page and persist the filtered row maximum."""

    lower = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
    upper = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
    later_page_max = datetime(2026, 9, 27, 11, 30, tzinfo=timezone.utc)
    state = {"requests": []}
    base_url_holder = {"value": ""}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            state["requests"].append(self.path)
            page = parse_qs(urlparse(self.path).query).get("page", ["1"])[0]
            if page == "2":
                # Both endpoint equalities arrive only on the later page.  The
                # upper equality must be removed by the source residual filter.
                payload = {
                    "data": [
                        {"id": 2, "updated_at": lower.isoformat()},
                        {"id": 3, "updated_at": upper.isoformat()},
                        {"id": 4, "updated_at": later_page_max.isoformat()},
                    ],
                    "paging": {"next": None},
                }
            else:
                payload = {
                    "data": [
                        {
                            "id": 1,
                            "updated_at": datetime(
                                2026, 9, 27, 10, 30, tzinfo=timezone.utc
                            ).isoformat(),
                        }
                    ],
                    "paging": {
                        "next": f"{base_url_holder['value']}/items?page=2"
                    },
                }
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    base_url_holder["value"] = f"http://127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        output_root = tmp_path / "output"
        state_root = tmp_path / "state"
        log_root = tmp_path / "logs"
        metadata_path = tmp_path / "metadata.json"
        _write_metadata(
            metadata_path,
            base_url_holder["value"],
            output_root,
            max_pages=3,
            source_config={
                "range_param_mapping": {
                    "updated_at": {
                        "lower": {"name": "updated_from", "operator": ">="},
                        "upper": {"name": "updated_to", "operator": "<"},
                        "format": "iso",
                        "response_column": "updated_at",
                        "watermark_value": "observed_max",
                    }
                }
            },
        )

        result = _run_replay(
            metadata_path,
            state_root,
            log_root,
            ReplayConfig(
                start=lower,
                end=upper,
                save_watermark=True,
                chunk_column="updated_at",
            ),
        )

        assert (result.total, result.succeeded, result.failed) == (1, 1, 0), result.errors
        output_paths = sorted((output_root / "events").rglob("*.parquet"))
        assert output_paths
        rows = (
            pl.concat([pl.read_parquet(path) for path in output_paths])
            .select(["id", "updated_at"])
            .sort("id")
            .to_dicts()
        )
        assert rows == [
            {"id": 1, "updated_at": "2026-09-27T10:30:00+00:00"},
            {"id": 2, "updated_at": lower.isoformat()},
            {"id": 4, "updated_at": later_page_max.isoformat()},
        ]
        assert _read_watermark(state_root) == {"updated_at": later_page_max.isoformat()}
        assert len(state["requests"]) == 2
        assert parse_qs(urlparse(state["requests"][0]).query) == {
            "updated_from": [lower.isoformat()],
            "updated_to": [upper.isoformat()],
        }
        assert parse_qs(urlparse(state["requests"][1]).query) == {"page": ["2"]}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
