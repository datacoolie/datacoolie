"""Contract tests for exact-file scenario bindings and inline providers."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
RUNNER_DIR = PRODUCT_ROOT / "usecase-sim" / "runner"


def _load_bindings():
    sys.path.insert(0, str(PRODUCT_ROOT / "usecase-sim"))
    try:
        from runner.metadata_bindings import (
            BoundFileProvider,
            MemoryFileProvider,
            MetadataBindingError,
            bind_document,
        )
    finally:
        sys.path.pop(0)
    return BoundFileProvider, MemoryFileProvider, MetadataBindingError, bind_document


def _document() -> dict:
    return {
        "connections": [
            {
                "name": "source",
                "connection_id": "source-id",
                "connection_type": "file",
                "format": "csv",
                "configure": {"base_path": "__ROOT__"},
            },
            {
                "name": "destination",
                "connection_id": "destination-id",
                "connection_type": "file",
                "format": "csv",
                "configure": {"base_path": "__ROOT__"},
            },
        ],
        "dataflows": [
            {
                "name": "flow",
                "dataflow_id": "flow-id",
                "stage": "read",
                "source": {"connection_name": "source", "table": "input"},
                "destination": {"connection_name": "destination", "table": "output"},
                "description": "authored",
            }
        ],
        "schema_hints": [],
    }


def test_bindings_apply_patch_delete_and_tokens_without_rewriting_keys():
    _, _, _, bind_document = _load_bindings()
    document = _document()
    bound = bind_document(
        document,
        {
            "tokens": {"__ROOT__": "/tmp/case"},
            "dataflows": {
                "flow": {
                    "patch": {"description": "patched", "source.table": "bound"},
                    "delete": ["destination.table"],
                }
            },
        },
        dataflows=["flow"],
    )

    assert bound["connections"][0]["configure"]["base_path"] == "/tmp/case"
    assert bound["dataflows"][0]["description"] == "patched"
    assert bound["dataflows"][0]["source"]["table"] == "bound"
    assert "destination" in bound["dataflows"][0]
    assert "table" not in bound["dataflows"][0]["destination"]
    assert "__ROOT__" in document["connections"][0]["configure"]["base_path"]


@pytest.mark.parametrize(
    "binding",
    [
        {"tokens": {"flow": "other"}},
        {"dataflows": {"flow": {"patch": {"stage": "write"}}}},
        {"dataflows": {"flow": {"replace": {"name": "other", "dataflow_id": "flow-id", "stage": "read"}}}},
    ],
)
def test_bindings_cannot_change_name_stage_or_id(binding):
    _, _, error, bind_document = _load_bindings()
    with pytest.raises(error, match="identity"):
        bind_document(_document(), binding)


def test_replacement_is_complete_and_preserves_identity_only():
    _, _, _, bind_document = _load_bindings()
    result = bind_document(
        _document(),
        {
            "dataflows": {
                "flow": {
                    "replace": {
                        "name": "flow",
                        "dataflow_id": "flow-id",
                        "stage": "read",
                        "source": {"connection_name": "source"},
                        "destination": {"connection_name": "destination"},
                    }
                }
            }
        },
    )
    assert "description" not in result["dataflows"][0]


def test_exact_overlay_cannot_make_a_replaced_away_target_valid(tmp_path):
    _, _, error, bind_document = _load_bindings()
    group = tmp_path / "group.json"
    connections = tmp_path / "connections.json"
    group.write_text(json.dumps(_document()), encoding="utf-8")
    connections.write_text(
        json.dumps({"connections": [_document()["connections"][1]]}), encoding="utf-8"
    )
    with pytest.raises(error, match="Unknown connection binding target"):
        bind_document(
            str(group),
            {"connections": {"source": {"patch": {"description": "ignored"}}}},
            connections_path=str(connections),
        )


def test_memory_provider_snapshots_document_and_uses_generated_coordinate():
    _, memory_provider, _, _ = _load_bindings()
    from datacoolie.platforms.local_platform import LocalPlatform

    document = _document()
    provider = memory_provider(document=document, platform=LocalPlatform())
    document["dataflows"][0]["name"] = "mutated-after-construction"
    provider.initialize()
    assert provider._config_paths == [memory_provider.MEMORY_CONFIG_PATH]
    assert provider.get_dataflows()[0].name == "flow"
    provider.close()


def test_bound_provider_full_validates_then_filters_in_selector_order():
    bound_provider, _, _, _ = _load_bindings()
    from datacoolie.platforms.local_platform import LocalPlatform

    base = PRODUCT_ROOT / "usecase-sim" / "metadata" / "file"
    provider = bound_provider(
        config_path=[str(base / "read.json")],
        connections_path=str(base / "connections.json"),
        schema_hints_path=str(base / "schema_hints.json"),
        platform=LocalPlatform(),
        dataflows=["api_request__max_pages"],
    )
    provider.initialize()
    selected = provider.get_dataflows(stage="api_request__max_pages")
    assert [flow.name for flow in selected] == ["api_request__max_pages"]
    provider.close()


@pytest.mark.parametrize("tokens", [{}, {"__ROOT__": "/bound"}])
def test_bound_copy_never_shares_mutable_rows_with_input_or_patch(tokens):
    _, _, _, bind_document = _load_bindings()
    document = _document()
    patch = {"connections": {"source": {"patch": {"configure": {"headers": {"values": ["original"]}}}}},
             "tokens": tokens}
    original = json.loads(json.dumps(document))
    bound = bind_document(document, patch)
    bound["connections"][0]["configure"]["headers"]["values"].append("edited")
    bound["dataflows"][0]["source"]["table"] = "edited"
    assert document == original
    assert patch["connections"]["source"]["patch"]["configure"]["headers"]["values"] == ["original"]


def test_bindings_keep_workspace_identity_and_reject_ambiguous_name_operations():
    _, _, error, bind_document = _load_bindings()
    document = _document()
    document["connections"].append({
        "name": "source",
        "connection_id": "other-source-id",
        "workspace_id": "ws-b",
        "connection_type": "file",
        "format": "csv",
    })
    document["connections"][0]["workspace_id"] = "ws-a"
    document["connections"][1]["workspace_id"] = "ws-a"
    document["dataflows"][0]["workspace_id"] = "ws-a"

    bound = bind_document(document)
    assert {row["connection_id"] for row in bound["connections"] if row["name"] == "source"} == {
        "source-id", "other-source-id"
    }

    with pytest.raises(error, match="Ambiguous connection binding target"):
        bind_document(document, {"connections": {"source": {"patch": {"description": "x"}}}})

    with pytest.raises(error, match="identity"):
        bind_document(document, {"connections": {"destination": {
            "patch": {"workspace_id": "ws-b"}
        }}})
