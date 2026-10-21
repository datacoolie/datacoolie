"""Focused tests for simulator metadata direct documents and in-memory workflow selection."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SIM_ROOT = PRODUCT_ROOT / "usecase-sim"


@pytest.mark.parametrize("platform,filename", [
    ("aws", "aws_glue_use_cases.json"),
    ("databricks", "databricks_use_cases.json"),
    ("fabric", "fabric_use_cases.json"),
])
def test_platform_smoke_is_self_contained_and_reads_its_own_written_targets(platform, filename):
    directory = SIM_ROOT / "platforms" / platform
    assert list(directory.rglob("*.json")) == [directory / filename]
    metadata = json.loads((directory / filename).read_text())
    assert "shared_metadata" not in metadata and "metadata_documents" not in metadata
    connections = {c["name"]: c for c in metadata["connections"]}
    produced = set()
    for flow in sorted(metadata["dataflows"], key=lambda f: f["execution_order"]):
        source, destination = flow["source"], flow["destination"]
        assert source["connection_name"] in connections
        assert destination["connection_name"] in connections
        source_connection = connections[source["connection_name"]]
        if source_connection.get("format") in {"delta", "iceberg"}:
            assert (source_connection["format"], source["table"]) in produced
        else:
            assert source_connection["format"] == "csv"
        destination_connection = connections[destination["connection_name"]]
        assert destination["table"].startswith("platform_smoke_")
        produced.add((destination_connection["format"], destination["table"]))


@pytest.fixture(scope="module")
def composition():
    sys.path.insert(0, str(SIM_ROOT))
    from runner import metadata_documents
    return metadata_documents


def _write_json(root: Path, relative: str, value: object) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")
    return path


def test_workflow_preserves_manifest_order_and_deduplicates_shared_profile(composition):
    root = SIM_ROOT
    workflow = composition.load_metadata(
        "scenarios/workflows/local_use_cases.json", root=root
    )
    manifest = json.loads(
        (root / "scenarios/workflows/local_use_cases.json").read_text()
    )
    assert [flow["name"] for flow in workflow["dataflows"]] == manifest["flow_order"]
    assert len(workflow["connections"]) == len({c["name"] for c in workflow["connections"]})
    assert len(workflow["schema_hints"]) == 3
    assert "metadata_documents" not in workflow
    assert "flow_order" not in workflow


def test_exact_files_merge_in_memory_and_explicit_sections_replace(composition, tmp_path):
    from runner.metadata_documents import load_metadata_files
    one = _write_json(tmp_path, "one.json", {"connections": [{"name": "old"}],
        "dataflows": [{"name": "one"}]})
    two = _write_json(tmp_path, "two.json", {"dataflows": [{"name": "two"}]})
    shared = _write_json(tmp_path, "connections.json", {"connections": [{"name": "shared"}]})
    hints = _write_json(tmp_path, "schema_hints.json", {"schema_hints": []})
    before = {path: path.read_bytes() for path in tmp_path.iterdir()}
    document = load_metadata_files([one, two], connections_path=shared,
        schema_hints_path=hints, root=tmp_path)
    assert document == {"connections": [{"name": "shared"}],
        "dataflows": [{"name": "one"}, {"name": "two"}], "schema_hints": []}
    document["dataflows"][0]["name"] = "changed"
    assert {path: path.read_bytes() for path in tmp_path.iterdir()} == before
    assert load_metadata_files([one, two], root=tmp_path)["dataflows"][0]["name"] == "one"


@pytest.mark.parametrize("section,rows", [
    ("connections", [{"name": "shared"}, {"name": "shared"}]),
    ("dataflows", [{"name": "one"}, {"name": "one"}]),
    ("dataflows", [{"name": "one", "dataflow_id": "same"},
                   {"name": "two", "dataflow_id": "same"}]),
])
def test_exact_files_reject_duplicate_identity_before_export_or_seed(composition, tmp_path, section, rows):
    from runner.metadata_documents import load_metadata_files
    one = _write_json(tmp_path, "one.json", {section: rows[:1]})
    two = _write_json(tmp_path, "two.json", {section: rows[1:]})
    with pytest.raises(ValueError, match="Duplicate"):
        load_metadata_files([one, two], root=tmp_path)


@pytest.mark.parametrize("section,id_field", [("connections", "connection_id"),
                                              ("dataflows", "dataflow_id")])
def test_exact_files_reject_explicit_id_colliding_with_derived_id(composition, tmp_path, section, id_field):
    from runner.metadata_documents import load_metadata_files
    from datacoolie.utils.identity import name_to_uuid
    one = _write_json(tmp_path, "one.json", {section: [{"name": "one"}]})
    two = _write_json(tmp_path, "two.json", {section: [{"name": "two", id_field: name_to_uuid("one")}]})
    with pytest.raises(ValueError, match=f"Duplicate {id_field}"):
        load_metadata_files([one, two], root=tmp_path)


def test_workflow_rejects_duplicate_flows_and_unresolved_connections(composition, tmp_path):
    _write_json(tmp_path, "one.json", {"dataflows": [{"name": "flow", "source": {"connection_name": "source"}, "destination": {"connection_name": "dest"}}]})
    _write_json(tmp_path, "two.json", {"dataflows": [{"name": "flow"}]})
    _write_json(tmp_path, "connections.json", {"connections": [{"name": "source"}, {"name": "dest"}]})
    manifest = {"metadata_documents": ["one.json", "two.json"], "flow_order": ["flow"], "connections_path": "connections.json"}
    _write_json(tmp_path, "workflow.json", manifest)
    with pytest.raises(ValueError, match="Duplicate"):
        composition.load_metadata("workflow.json", root=tmp_path)
    manifest.update(metadata_documents=["one.json"], connection_order=["source"])
    _write_json(tmp_path, "workflow.json", manifest)
    with pytest.raises(ValueError, match="cannot resolve"):
        composition.load_metadata("workflow.json", root=tmp_path)


def test_manifest_rejects_nested_or_escaping_references(composition, tmp_path):
    manifest = {"metadata_documents": ["workflow.json"], "flow_order": []}
    _write_json(tmp_path, "workflow.json", manifest)
    with pytest.raises(ValueError, match="plain group"):
        composition.load_metadata("workflow.json", root=tmp_path)
    manifest["metadata_documents"] = ["../outside.json"]
    _write_json(tmp_path, "workflow.json", manifest)
    with pytest.raises(ValueError, match="escapes"):
        composition.load_metadata("workflow.json", root=tmp_path)


def test_plain_invalid_document_stays_unchanged_and_obsolete_group_is_rejected(composition, tmp_path):
    source = _write_json(tmp_path, "startup.json", {"dataflows": "invalid"})
    before = source.read_bytes()
    assert composition.load_metadata(source) == {"dataflows": "invalid"}
    assert source.read_bytes() == before
    group = _write_json(tmp_path, "old.json", {"shared_metadata": "connections.json", "dataflows": []})
    with pytest.raises(ValueError, match="obsolete shared_metadata"):
        composition.load_metadata(group)


def test_external_plain_document_loads_in_memory_without_runtime_files(composition, tmp_path):
    source = _write_json(tmp_path, "outside/source.json", {"dataflows": []})
    assert composition.load_metadata(source, root=tmp_path / "sim") == {"dataflows": []}
    assert not (tmp_path / "sim").exists()


@pytest.mark.parametrize("mode", ["plain", "exact", "workflow"])
def test_metadata_reads_own_nested_rows_and_observe_source_changes(composition, tmp_path, mode):
    flow = {"name": "flow", "source": {"connection_name": "shared"},
            "destination": {"connection_name": "shared"},
            "transform": {"select_columns": ["id", "value"]}}
    connection = {"name": "shared", "configure": {"headers": {"x-test": "original"}}}
    hint = {"connection_name": "shared", "table_name": "events",
            "hints": [{"column_name": "id", "data_type": "INT"}]}
    group = _write_json(tmp_path, "group.json", {"dataflows": [flow]})
    shared = _write_json(tmp_path, "connections.json", {"connections": [connection]})
    hints = _write_json(tmp_path, "schema_hints.json", {"schema_hints": [hint]})
    if mode == "plain":
        group = _write_json(tmp_path, "plain.json", {"dataflows": [flow],
            "connections": [connection], "schema_hints": [hint]})
        def read():
            return composition.load_metadata(group)
    elif mode == "exact":
        def read():
            return composition.load_metadata_files(group,
                connections_path=shared, schema_hints_path=hints, root=tmp_path)
    else:
        manifest = _write_json(tmp_path, "workflow.json", {
            "metadata_documents": ["group.json"], "flow_order": ["flow"],
            "connections_path": "connections.json", "schema_hints_path": "schema_hints.json",
            "connection_order": ["shared"]})
        def read():
            return composition.load_metadata(manifest, root=tmp_path)
    before = {path: path.read_bytes() for path in tmp_path.iterdir()}
    first, second = read(), read()
    first["dataflows"][0]["transform"]["select_columns"].append("changed")
    first["connections"][0]["configure"]["headers"]["x-test"] = "changed"
    first["schema_hints"][0]["hints"][0]["data_type"] = "changed"
    assert second["dataflows"][0]["transform"]["select_columns"] == ["id", "value"]
    assert second["connections"][0]["configure"]["headers"]["x-test"] == "original"
    assert second["schema_hints"][0]["hints"][0]["data_type"] == "INT"
    assert {path: path.read_bytes() for path in before} == before
    updated = json.loads(group.read_text())
    updated["dataflows"][0]["transform"]["select_columns"] = ["new_column"]
    group.write_text(json.dumps(updated), encoding="utf-8")
    assert read()["dataflows"][0]["transform"]["select_columns"] == ["new_column"]
