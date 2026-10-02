"""Driver-level recovery checks for source boundary failures."""

from __future__ import annotations

import dataclasses
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest

from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.base import FileInfo
from datacoolie.platforms.local_platform import LocalPlatform


pytestmark = pytest.mark.integration


class _MissingMtimePlatform(LocalPlatform):
    """Local platform fixture that can reproduce an unavailable file mtime."""

    def __init__(self, *, missing_mtime: bool = False) -> None:
        super().__init__()
        self.missing_mtime = missing_mtime

    def list_files(
        self,
        path: str,
        *,
        recursive: bool = False,
        extension: str | None = None,
    ) -> list[FileInfo]:
        infos = super().list_files(path, recursive=recursive, extension=extension)
        if not self.missing_mtime:
            return infos
        return [dataclasses.replace(info, modification_time=None) for info in infos]


def _run_driver(
    metadata_path: Path,
    platform: LocalPlatform,
    state_root: Path,
    log_root: Path,
    *,
    job_id: str,
):
    provider = FileProvider(
        config_path=str(metadata_path),
        platform=platform,
    )
    with DataCoolieDriver(
        engine=PolarsEngine(platform=platform),
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(state_root),
        log_base_path=str(log_root),
        config=DataCoolieRunConfig(job_id=job_id, retry_count=0, retry_delay=0),
    ) as driver:
        return driver.run(stage="bronze")


def _parquet_ids(root: Path, table: str) -> list[int]:
    table_root = root / table
    if not table_root.exists():
        return []
    return sorted(
        row["id"]
        for path in sorted(table_root.rglob("*.parquet"))
        for row in pl.read_parquet(path).select("id").to_dicts()
    )


def _watermark_snapshot(state_root: Path) -> tuple[Path, str] | None:
    paths = sorted(state_root.rglob("watermark_value.json"))
    if not paths:
        return None
    path = paths[0]
    return path, path.read_text(encoding="utf-8")


def _write_file_metadata(
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
                        "name": "events",
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


def test_missing_file_mtime_keeps_target_and_checkpoint_until_recovery(tmp_path):
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    state_root = tmp_path / "state"
    log_root = tmp_path / "logs"
    metadata_path = tmp_path / "metadata.json"
    input_table = input_root / "events"
    input_table.mkdir(parents=True)
    _write_file_metadata(metadata_path, input_root, output_root)

    first = input_table / "first.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(first)
    os.utime(first, (1_700_000_000, 1_700_000_000))

    initial = _run_driver(
        metadata_path,
        _MissingMtimePlatform(),
        state_root,
        log_root,
        job_id="file-mtime-recovery",
    )
    assert (initial.total, initial.succeeded, initial.failed) == (1, 1, 0), initial.errors
    assert _parquet_ids(output_root, "events") == [1]
    checkpoint_before = _watermark_snapshot(state_root)
    assert checkpoint_before is not None

    second = input_table / "second.parquet"
    pl.DataFrame({"id": [2]}).write_parquet(second)
    os.utime(second, (1_700_003_600, 1_700_003_600))

    failed = _run_driver(
        metadata_path,
        _MissingMtimePlatform(missing_mtime=True),
        state_root,
        log_root,
        job_id="file-mtime-recovery",
    )
    assert (failed.total, failed.succeeded, failed.failed) == (1, 0, 1)
    assert _parquet_ids(output_root, "events") == [1]
    assert _watermark_snapshot(state_root) == checkpoint_before

    recovered = _run_driver(
        metadata_path,
        _MissingMtimePlatform(),
        state_root,
        log_root,
        job_id="file-mtime-recovery",
    )
    assert (recovered.total, recovered.succeeded, recovered.failed) == (1, 1, 0), recovered.errors
    assert _parquet_ids(output_root, "events") == [1, 2]
    checkpoint_after = _watermark_snapshot(state_root)
    assert checkpoint_after is not None
    assert checkpoint_after[0] == checkpoint_before[0]
    assert checkpoint_after[1] != checkpoint_before[1]


def _write_api_metadata(path: Path, base_url: str, output_root: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "api",
                        "format": "api",
                        "configure": {
                            "base_url": base_url,
                            "auth_type": "bearer",
                            "auth_token": "synthetic-secret",
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
                        "name": "api-events",
                        "stage": "bronze",
                        "source": {
                            "connection_name": "source",
                            "table": "events",
                            "watermark_columns": ["updated_at"],
                            "configure": {
                                "endpoint": "/items",
                                "pagination_type": "next_link",
                                "data_path": "data",
                                "next_link_path": "paging.next",
                                "max_pages": 3,
                            },
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


def test_api_continuation_failure_keeps_target_and_checkpoint_until_recovery(tmp_path):
    server_state = {"mode": "foreign", "requests": []}
    base_url_holder = {"value": ""}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            server_state["requests"].append(
                {
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                }
            )
            parsed = urlparse(self.path)
            if parsed.path != "/items":
                self.send_error(404)
                return
            page = parse_qs(parsed.query).get("page", ["1"])[0]
            if page == "2":
                payload = {
                    "data": [{"id": 2, "updated_at": "2026-01-02T00:00:00Z"}],
                    "paging": {"next": None},
                }
            else:
                if server_state["mode"] == "foreign":
                    next_link = (
                        "http://127.0.0.1:1/items?page=2&token=synthetic-secret"
                    )
                else:
                    next_link = f"{base_url_holder['value']}/items?page=2"
                payload = {
                    "data": [{"id": 1, "updated_at": "2026-01-01T00:00:00Z"}],
                    "paging": {"next": next_link},
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
        _write_api_metadata(metadata_path, base_url_holder["value"], output_root)

        failed = _run_driver(
            metadata_path,
            LocalPlatform(),
            state_root,
            log_root,
            job_id="api-boundary-recovery",
        )
        assert (failed.total, failed.succeeded, failed.failed) == (1, 0, 1)
        assert _parquet_ids(output_root, "events") == []
        assert _watermark_snapshot(state_root) is None
        assert len(server_state["requests"]) == 1
        assert server_state["requests"][0]["authorization"] == "Bearer synthetic-secret"

        server_state["mode"] = "same-origin"
        server_state["requests"].clear()
        recovered = _run_driver(
            metadata_path,
            LocalPlatform(),
            state_root,
            log_root,
            job_id="api-boundary-recovery",
        )
        assert (recovered.total, recovered.succeeded, recovered.failed) == (1, 1, 0), recovered.errors
        assert _parquet_ids(output_root, "events") == [1, 2]
        checkpoint = _watermark_snapshot(state_root)
        assert checkpoint is not None
        assert len(server_state["requests"]) == 2
        assert all(
            request["authorization"] == "Bearer synthetic-secret"
            for request in server_state["requests"]
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
