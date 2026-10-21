"""Tests for the PostgreSQL metadata API response contract."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import pytest


def _load_server_module():
    script = Path(__file__).parents[3] / "usecase-sim" / "docker" / "pg_api_metadata_server.py"
    spec = importlib.util.spec_from_file_location("pg_api_metadata_server_for_test", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_json_server_module():
    script = Path(__file__).parents[3] / "usecase-sim" / "metadata" / "api" / "api_metadata_server.py"
    spec = importlib.util.spec_from_file_location("api_metadata_server_for_test", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_postgres_api_dataflow_response_preserves_source_filter_expression() -> None:
    server = _load_server_module()
    row = SimpleNamespace(
        dataflow_id="df-1",
        workspace_id="ws-1",
        name="orders",
        description=None,
        stage="bronze",
        group_number=None,
        execution_order=None,
        processing_mode="batch",
        is_active=True,
        configure="{}",
        transform="{}",
        source_schema="sales",
        source_table="orders",
        source_query=None,
        source_python_function=None,
        source_filter_expression="status = 'open'",
        source_watermark_columns="[]",
        source_configure="{}",
        destination_schema=None,
        destination_table="orders",
        destination_load_type="append",
        destination_merge_keys="[]",
        destination_configure="{}",
    )

    payload = server._row_to_dataflow(row, {"connection_id": "src"}, {"connection_id": "dst"})

    assert payload["source"]["filter_expression"] == "status = 'open'"
    assert "source_filter_expression" in server._DF_SELECT


def test_json_api_server_keeps_source_filter_expression(tmp_path: Path) -> None:
    server = _load_json_server_module()
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(
        '{"connections": [{"name": "src", "connection_type": "file", "format": "parquet"}], '
        '"dataflows": [{"name": "orders", "source": {"connection_name": "src", '
        '"table": "orders", "filter_expression": "status = \'open\'"}, '
        '"destination": {"connection_name": "src", "table": "orders", "load_type": "append"}}]}',
        encoding="utf-8",
    )

    loaded = server._load_metadata(str(metadata_path))

    assert loaded["dataflows"][0]["source"]["filter_expression"] == "status = 'open'"


def test_json_api_reads_groups_and_shared_sections_without_disk_aggregate(tmp_path):
    server = _load_json_server_module()
    group = tmp_path / "group.json"
    shared = tmp_path / "connections.json"
    group.write_text(json.dumps({"dataflows": [{"name": "orders",
        "source": {"connection_name": "src", "table": "orders"},
        "destination": {"connection_name": "src", "table": "target", "load_type": "append"}}]}))
    shared.write_text(json.dumps({"connections": [{"name": "src",
        "connection_type": "file", "format": "parquet"}]}))
    before = {path: path.read_bytes() for path in tmp_path.iterdir()}
    metadata = server._load_metadata([str(group)], connections_path=str(shared))
    assert metadata["dataflows"][0]["source"]["connection"]["name"] == "src"
    assert {path: path.read_bytes() for path in tmp_path.iterdir()} == before
    with pytest.raises(ValueError, match="Duplicate"):
        server._load_metadata([str(group), str(group)], connections_path=str(shared))
