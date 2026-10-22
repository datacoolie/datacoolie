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


def _load_setup_module():
    script = Path(__file__).parents[3] / "usecase-sim" / "scripts" / "bootstrap" / "setup_metadata.py"
    spec = importlib.util.spec_from_file_location("setup_metadata_for_pg_api_test", script)
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


def test_json_api_binds_default_workspace_and_rejects_cross_workspace_state(tmp_path):
    server = _load_json_server_module()
    from datacoolie.utils.identity import name_to_uuid

    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps({
        "connections": [
            {"name": "src", "connection_type": "file", "format": "parquet"},
            {"name": "dst", "connection_type": "file", "format": "parquet"},
        ],
        "dataflows": [{
            "name": "orders",
            "source": {"connection_name": "src", "table": "orders"},
            "destination": {"connection_name": "dst", "table": "orders", "load_type": "append"},
        }],
    }))

    with pytest.raises(ValueError, match="workspace_id is required"):
        server.create_app(str(metadata_path), watermark_dir=tmp_path / "unbound")

    app = server.create_app(
        str(metadata_path), watermark_dir=tmp_path / "watermarks", workspace_id="ws-a"
    )
    client = app.test_client()
    connection_id = name_to_uuid("src", "ws-a")
    dataflow_id = name_to_uuid("orders", "ws-a")
    assert client.get("/workspaces/ws-a/connections").get_json()["pagination"]["total"] == 2
    assert client.get(f"/workspaces/ws-a/connections/{connection_id}").status_code == 200
    assert client.get(f"/workspaces/ws-b/connections/{connection_id}").status_code == 404
    assert client.get("/workspaces/ws-b/dataflows").get_json()["pagination"]["total"] == 0
    assert client.put(
        f"/workspaces/ws-a/watermarks/{dataflow_id}",
        json={"current_value": "one"},
    ).status_code == 200
    assert client.get(
        f"/workspaces/ws-b/watermarks/{dataflow_id}"
    ).get_json() == {"current_value": None}
    assert client.put(
        f"/workspaces/ws-b/watermarks/{dataflow_id}",
        json={"current_value": "wrong"},
    ).status_code == 404
    assert client.get(
        f"/workspaces/ws-a/watermarks/{dataflow_id}"
    ).get_json()["current_value"] == "one"


def test_pg_api_scopes_sqlite_routes_and_watermarks(tmp_path):
    server = _load_server_module()
    setup = _load_setup_module()
    from datacoolie.utils.identity import name_to_uuid

    db_path = tmp_path / "metadata.db"
    url = "sqlite:///" + str(db_path).replace("\\", "/")
    document = {
        "connections": [
            {"name": "src", "connection_type": "file", "format": "parquet"},
            {"name": "dst", "connection_type": "file", "format": "parquet"},
        ],
        "dataflows": [{
            "name": "orders",
            "source": {"connection_name": "src", "table": "orders"},
            "destination": {"connection_name": "dst", "table": "orders", "load_type": "append"},
        }],
        "schema_hints": [{
            "connection_name": "src", "table_name": "orders",
            "hints": [{"column_name": "id", "data_type": "BIGINT"}],
        }],
    }
    setup.seed_db(url, document, "ws-a", truncate=False)
    setup.seed_db(url, document, "ws-b", truncate=False)

    app = server.create_app(url)
    client = app.test_client()
    src_a = name_to_uuid("src", "ws-a")
    flow_a = name_to_uuid("orders", "ws-a")
    flow_b = name_to_uuid("orders", "ws-b")
    assert client.get("/workspaces/ws-a/connections").get_json()["pagination"]["total"] == 2
    assert client.get("/workspaces/ws-b/dataflows").get_json()["pagination"]["total"] == 1
    assert client.get(f"/workspaces/ws-b/dataflows/{flow_a}").status_code == 404
    hints = client.get(
        f"/workspaces/ws-a/schema-hints?connection_id={src_a}"
    ).get_json()["data"]
    assert len(hints) == 1 and hints[0]["connection_id"] == src_a
    assert client.put(
        f"/workspaces/ws-a/watermarks/{flow_a}",
        json={"current_value": "a"},
    ).status_code == 200
    assert client.get(
        f"/workspaces/ws-b/watermarks/{flow_b}"
    ).get_json()["current_value"] is None
    assert client.put(
        f"/workspaces/ws-b/watermarks/{flow_a}",
        json={"current_value": "cross"},
    ).status_code == 404
    assert client.get(
        f"/workspaces/ws-a/watermarks/{flow_a}"
    ).get_json()["current_value"] == "a"


def test_json_api_uses_explicit_endpoint_ids_for_same_name_scopes(tmp_path):
    server = _load_json_server_module()
    metadata_path = tmp_path / "scoped.json"
    metadata_path.write_text(json.dumps({
        "connections": [
            {"name": "shared", "connection_id": "src-a", "workspace_id": "ws-a",
             "connection_type": "file", "format": "parquet"},
            {"name": "shared", "connection_id": "src-b", "workspace_id": "ws-b",
             "connection_type": "file", "format": "parquet"},
        ],
        "dataflows": [
            {"name": "flow", "dataflow_id": "flow-a", "workspace_id": "ws-a",
             "source": {"connection_id": "src-a", "table": "a"},
             "destination": {"connection_id": "src-a", "table": "a", "load_type": "append"}},
            {"name": "flow", "dataflow_id": "flow-b", "workspace_id": "ws-b",
             "source": {"connection_id": "src-b", "table": "b"},
             "destination": {"connection_id": "src-b", "table": "b", "load_type": "append"}},
        ],
    }))
    app = server.create_app(str(metadata_path), watermark_dir=tmp_path / "watermarks")
    client = app.test_client()
    response = client.get("/workspaces/ws-b/dataflows/flow-b")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["source"]["connection"]["connection_id"] == "src-b"
    assert client.get("/workspaces/ws-a/dataflows/flow-b").status_code == 404
