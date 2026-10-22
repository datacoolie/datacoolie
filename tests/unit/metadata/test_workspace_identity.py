"""Identity generation and composition through actual document/provider boundaries."""

from __future__ import annotations

import copy
import json

import pytest

from datacoolie.core.exceptions import ConfigurationError, MetadataError, WatermarkError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.source import Source
from datacoolie.core.models.destination import Destination
from datacoolie.metadata.api_provider import APIProvider
from datacoolie.metadata.documents.mapping import build_connections, build_dataflows, build_grouped_schema_hints
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.utils.identity import name_to_uuid, normalize_document_workspace


@pytest.mark.parametrize("workspace,name,expected", [
    (None, "orders", "7172410a-6350-5482-b4a1-c584648f28f7"),
    ("ws-a", "orders", "fcceb473-347c-51ca-97c9-3f66ffa78ff4"),
    ("ws-b", "orders", "18051ca6-2b24-539f-a47b-d1a628596f32"),
    (" ws-a ", " đơn hàng ", "47903909-075e-5594-b53f-df9874def877"),
])
def test_literal_identity_vectors(workspace, name, expected):
    assert name_to_uuid(name, workspace) == expected
    assert Connection(name=name, workspace_id=workspace).connection_id == expected


def _document():
    return {
        "connections": [{"name": " endpoint ", "workspace_id": ws} for ws in ("ws-a", "ws-b")],
        "dataflows": [{
            "name": "orders", "workspace_id": ws,
            "source": {"connection_name": " endpoint "},
            "destination": {"connection_name": " endpoint ", "table": "orders"},
        } for ws in ("ws-a", "ws-b")],
    }


@pytest.mark.parametrize("enable_cache", [False, True])
def test_mixed_workspace_resolution_and_watermark_isolation(tmp_path, enable_cache):
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps(_document()), encoding="utf-8")
    provider = FileProvider(str(path), LocalPlatform(), enable_cache=enable_cache,
                            watermark_base_path=str(tmp_path / "state"))
    flows = provider.get_dataflows()
    assert len({flow.dataflow_id for flow in flows}) == 2
    for flow in flows:
        assert flow.source.connection.workspace_id == flow.workspace_id
        assert flow.source.connection.connection_id == name_to_uuid(" endpoint ", flow.workspace_id)
    with pytest.raises(MetadataError, match="ambiguous"):
        provider.get_connection_by_name(" endpoint ")
    assert provider.get_connection_by_id(flows[0].source.connection.connection_id).workspace_id == "ws-a"
    provider.update_watermark(flows[0].dataflow_id, '{"value": 1}')
    assert provider.get_watermark(flows[0].dataflow_id) == '{"value": 1}'
    assert provider.get_watermark(flows[1].dataflow_id) is None
    with pytest.raises(WatermarkError, match="not owned"):
        provider.update_watermark("foreign-flow", '{"value": 2}')


def test_single_scope_inherits_before_generation_and_keeps_explicit_id():
    document = _document()
    document["connections"] = [{"name": " endpoint ", "connection_id": "authored", "workspace_id": "ws-a"}]
    document["dataflows"] = [document["dataflows"][0]]
    del document["dataflows"][0]["workspace_id"]
    original = copy.deepcopy(document)
    normalized = normalize_document_workspace(document)
    connections = build_connections(normalized["connections"])
    flow = build_dataflows(normalized["dataflows"], connections)[0]
    assert document == original
    assert flow.dataflow_id == "fcceb473-347c-51ca-97c9-3f66ffa78ff4"
    assert flow.source.connection.connection_id == "authored"
    assert flow.workspace_id == "ws-a"


def test_mixed_scope_omission_and_cross_scope_inline_fail():
    document = _document()
    del document["dataflows"][0]["workspace_id"]
    with pytest.raises(ValueError, match="every entity"):
        normalize_document_workspace(document)
    with pytest.raises(ValueError, match="mismatch"):
        normalize_document_workspace({"dataflows": [{"name": "flow", "workspace_id": "ws-a",
            "source": {"connection": {"name": "inline", "workspace_id": "ws-b"}}}]})
    with pytest.raises(ConfigurationError, match="workspace_id"):
        DataFlow(name="flow", source=Source(connection=Connection(name="a", workspace_id="ws-a")),
                 destination=Destination(connection=Connection(name="b"), table="t"))


def test_explicit_id_collision_and_hint_id_selection():
    rows = [{"name": "endpoint", "workspace_id": ws, "connection_id": "same"} for ws in ("ws-a", "ws-b")]
    with pytest.raises(MetadataError, match="Duplicate connection_id"):
        build_connections(rows)
    connections = build_connections(_document()["connections"])
    group = {"connection_name": " endpoint ", "table_name": "t", "hints": [{"column_name": "id", "data_type": "INT"}]}
    with pytest.raises(MetadataError, match="ambiguous"):
        build_grouped_schema_hints([group], connections)
    group["connection_id"] = connections[1].connection_id
    assert next(iter(build_grouped_schema_hints([group], connections)))[0] == connections[1].connection_id


def test_api_provider_scope_precedes_generation_and_rejects_foreign_payload():
    provider = APIProvider(base_url="https://example.invalid", api_key="unused", workspace_id="ws-a")
    try:
        assert provider._scoped_connection({"name": "orders", "connection_type": "file", "format": "parquet"}).connection_id == "fcceb473-347c-51ca-97c9-3f66ffa78ff4"
        with pytest.raises(MetadataError, match="provider workspace"):
            provider._scoped_connection({"name": "orders", "workspace_id": "ws-b"})
    finally:
        provider.close()


def test_standalone_explicit_id_watermark_service(tmp_path):
    provider = FileProvider(platform=LocalPlatform(), watermark_base_path=str(tmp_path))
    provider.update_watermark("explicit-id", '{"value": 1}')
    assert provider.get_watermark("explicit-id") == '{"value": 1}'
    assert not provider._initialized


def test_direct_flow_scope_inheritance_does_not_mutate_shared_connection():
    connection = Connection(name="orders")
    source = Source(connection=connection)
    destination = Destination(connection=connection, table="t")
    flows = [DataFlow(name="flow", workspace_id=ws, source=source, destination=destination) for ws in ("ws-a", "ws-b")]
    assert connection.workspace_id is None
    assert connection.connection_id == "7172410a-6350-5482-b4a1-c584648f28f7"
    assert source.connection is connection and destination.connection is connection
    assert [flow.source.connection.connection_id for flow in flows] == ["fcceb473-347c-51ca-97c9-3f66ffa78ff4", "18051ca6-2b24-539f-a47b-d1a628596f32"]


def test_hint_names_are_not_reinterpreted_as_ids():
    connections = build_connections([
        {"workspace_id": "ws-a", "name": "a", "connection_id": "b"},
        {"workspace_id": "ws-b", "name": "b", "connection_id": "c"},
    ])
    group = {"connection_name": "b", "table_name": "t", "hints": [{"column_name": "id", "data_type": "INT"}]}
    assert next(iter(build_grouped_schema_hints([group], connections)))[0] == "c"
    group["connection_id"] = "b"
    assert next(iter(build_grouped_schema_hints([group], connections)))[0] == "b"
    group["connection_id"] = "a"
    with pytest.raises(MetadataError, match="unknown connection"):
        build_grouped_schema_hints([group], connections)
