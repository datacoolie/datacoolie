"""Focused tests for simulator metadata composition and materialization."""

from __future__ import annotations

import hashlib
import importlib.util
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
    spec = importlib.util.spec_from_file_location(
        "usecase_sim_metadata_composition_test",
        SIM_ROOT / "runner" / "metadata_composition.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_json(root: Path, relative: str, value: object) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")
    return path


def _group(name: str, shared: str, flow_name: str) -> dict:
    return {
        "shared_metadata": shared,
        "dataflows": [{"name": flow_name, "stage": name}],
    }


def test_workflow_preserves_manifest_order_and_deduplicates_shared_profile(composition):
    root = SIM_ROOT
    workflow = composition.load_metadata(
        "metadata/file/workflows/local_use_cases.json", root=root
    )
    manifest = json.loads(
        (root / "metadata/file/workflows/local_use_cases.json").read_text()
    )
    assert [flow["name"] for flow in workflow["dataflows"]] == manifest["flow_order"]
    assert len(workflow["connections"]) == len({c["name"] for c in workflow["connections"]})
    assert len(workflow["schema_hints"]) == 3
    assert "metadata_documents" not in workflow
    assert "flow_order" not in workflow


def test_workflow_rejects_conflicting_shared_sections_and_duplicate_flows(composition, tmp_path):
    _write_json(
        tmp_path,
        "metadata/file/shared/one.json",
        {"connections": [{"name": "same", "connection_type": "file"}], "schema_hints": []},
    )
    _write_json(
        tmp_path,
        "metadata/file/shared/two.json",
        {"connections": [{"name": "same", "connection_type": "api"}], "schema_hints": []},
    )
    _write_json(tmp_path, "metadata/file/one.json", _group("one", "metadata/file/shared/one.json", "flow"))
    _write_json(tmp_path, "metadata/file/two.json", _group("two", "metadata/file/shared/two.json", "other"))
    _write_json(
        tmp_path,
        "metadata/file/workflow.json",
        {
            "metadata_documents": ["metadata/file/one.json", "metadata/file/two.json"],
            "flow_order": ["flow", "other"],
        },
    )
    with pytest.raises(composition.MetadataCompositionError, match="connection 'same'"):
        composition.load_metadata("metadata/file/workflow.json", root=tmp_path)

    _write_json(tmp_path, "metadata/file/two.json", _group("two", "metadata/file/shared/one.json", "flow"))
    with pytest.raises(composition.MetadataCompositionError, match="Duplicate dataflow"):
        composition.load_metadata("metadata/file/workflow.json", root=tmp_path)


@pytest.mark.parametrize(
    "documents",
    [
        {"metadata_documents": ["../outside.json"], "flow_order": []},
        {"metadata_documents": ["metadata/file/cycle.json"], "flow_order": []},
    ],
)
def test_composition_rejects_escaping_and_cycles(composition, tmp_path, documents):
    if documents["metadata_documents"][0].startswith("../"):
        _write_json(tmp_path, "metadata/file/workflow.json", documents)
        with pytest.raises(composition.MetadataCompositionError, match="escapes simulator root"):
            composition.load_metadata("metadata/file/workflow.json", root=tmp_path)
        return

    _write_json(
        tmp_path,
        "metadata/file/cycle.json",
        {"shared_metadata": "metadata/file/cycle.json", "dataflows": []},
    )
    with pytest.raises(composition.MetadataCompositionError, match="cycle"):
        composition.load_metadata("metadata/file/cycle.json", root=tmp_path)


def test_materialization_is_content_hashed_and_plain_invalid_json_passes_through(composition, tmp_path):
    _write_json(tmp_path, "metadata/file/shared/profile.json", {"connections": [], "schema_hints": []})
    source = _write_json(
        tmp_path,
        "metadata/file/group.json",
        _group("one", "metadata/file/shared/profile.json", "flow"),
    )
    materialized = Path(composition.materialize_metadata_path(source, root=tmp_path))
    first_bytes = materialized.read_bytes()
    assert materialized.parent == tmp_path / ".runtime/data/metadata"
    assert materialized.stem.endswith(hashlib.sha256(first_bytes).hexdigest())

    _write_json(tmp_path, "metadata/file/group.json", _group("changed", "metadata/file/shared/profile.json", "flow"))
    second = Path(composition.materialize_metadata_path(source, root=tmp_path))
    assert second != materialized
    assert materialized.read_bytes() == first_bytes

    malformed = _write_json(
        tmp_path,
        "metadata/file/startup_failure.json",
        {"connections": [], "dataflows": "this must be a list"},
    )
    assert composition.load_metadata(malformed, root=tmp_path)["dataflows"] == "this must be a list"
    assert composition.materialize_metadata_path(malformed, root=tmp_path) == str(malformed)
    for name, raw in (("array.json", ["plain"]), ("scalar.json", "plain"), ("invalid.json", "{")):
        path = tmp_path / "metadata/file" / name
        path.write_text(raw if isinstance(raw, str) else json.dumps(raw), encoding="utf-8")
        assert composition.materialize_metadata_path(path, root=tmp_path) == str(path)


def test_materialization_passthrough_for_uri_and_non_json(composition):
    assert composition.materialize_metadata_path("s3://bucket/metadata.json") == "s3://bucket/metadata.json"
    assert composition.materialize_metadata_path("metadata/file/local.yaml") == "metadata/file/local.yaml"


def test_external_plain_json_keeps_caller_path_and_can_be_written_to_runtime(composition, tmp_path, monkeypatch):
    source = tmp_path / "docs" / "connections.json"
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps({"connections": [], "dataflows": []}), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert composition.materialize_metadata_path("docs/connections.json", root=tmp_path / "sim") == "docs/connections.json"
    compiled = composition.write_composed_metadata(source, root=tmp_path / "sim")
    assert compiled == tmp_path / "sim/.runtime/data/metadata/connections.json"
    assert json.loads(compiled.read_text())["dataflows"] == []
