from __future__ import annotations

import json
from pathlib import Path
import pytest

from datacoolie.project.documents import load_snapshot
from datacoolie.project.overlays import resolve_environment
from datacoolie.project.errors import ProjectValidationError
from datacoolie.project.build.publisher import _preserve_documents


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_project_snapshot_and_environment_overlay_are_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "metadata"
    _write(root / "connections.json", {
        "connections": [
            {"name": "source", "configure": {"host": "base", "port": 1}},
            {"name": "destination", "configure": {"base_path": "base"}},
        ]
    })
    _write(root / "dataflows" / "bronze.json", {
        "dataflows": [{
            "name": "orders",
            "stage": "bronze",
            "source": {"connection_name": "source"},
            "destination": {"connection_name": "destination", "table": "orders"},
        }]
    })
    _write(root / "environments" / "test.json", {
        "connections": [{"name": "source", "configure": {"host": "test"}}],
        "dataflows": [{"name": "orders", "source": {"filter_expression": "active = 1"}}],
    })
    snapshot = load_snapshot(root)
    resolved, overlay_path = resolve_environment(snapshot, "test")
    assert overlay_path == root / "environments" / "test.json"
    assert resolved["connections"][0]["configure"] == {"host": "test", "port": 1}
    assert resolved["dataflows"][0]["stage"] == "bronze"
    assert resolved["dataflows"][0]["source"]["filter_expression"] == "active = 1"


def test_snapshot_ignores_environment_files_until_selected(tmp_path: Path) -> None:
    root = tmp_path / "metadata"
    _write(root / "connections.json", {"connections": []})
    _write(root / "environments" / "dev.json", {"connections": [{"name": "ignored"}]})
    snapshot = load_snapshot(root)
    assert snapshot.merged()["connections"] == []


def test_workspace_overlay_and_provenance_preserve_shard_ownership(tmp_path):
    root = tmp_path / "metadata"
    for workspace in ("ws-a", "ws-b"):
        _write(root / f"{workspace}.json", {"connections": [{
            "name": "shared", "workspace_id": workspace,
            "connection_id": f"id-{workspace}", "configure": {"host": workspace},
        }]})
    _write(root / "environments" / "test.json", {"connections": [{
        "name": "shared", "workspace_id": "ws-b", "configure": {"host": "updated"},
    }]})
    snapshot = load_snapshot(root)
    resolved, _ = resolve_environment(snapshot, "test")
    assert [row["configure"]["host"] for row in resolved["connections"]] == ["ws-a", "updated"]
    output = tmp_path / "output"
    _preserve_documents(snapshot, resolved, output_root=output, fmt="preserve")
    assert json.loads((output / "ws-a.json").read_text())["connections"][0]["connection_id"] == "id-ws-a"
    assert json.loads((output / "ws-b.json").read_text())["connections"][0]["configure"]["host"] == "updated"


@pytest.mark.parametrize("overlay", [
    {"connections": [{"name": "shared", "configure": {"host": "leaked"}}]},
    {"connections": [{"connection_id": "id-ws-a", "workspace_id": "ws-b"}]},
    {"patches": [{"match": {"type": "connections", "where": {"name": "shared"}}, "patch": {"configure": {"host": "leaked"}}}]},
    {"patches": [{"match": {"type": "connections", "where": {"connection_id": "id-ws-a"}}, "patch": {"workspace_id": "ws-b"}}]},
])
def test_overlay_rejects_ambiguous_name_or_workspace_mutation(tmp_path, overlay):
    root = tmp_path / "metadata"
    _write(root / "common.json", {"connections": [{"name": "shared", "workspace_id": ws, "connection_id": f"id-{ws}"} for ws in ("ws-a", "ws-b")]})
    _write(root / "environments" / "test.json", overlay)
    with pytest.raises(ProjectValidationError, match="[Aa]mbiguous|workspace|identity"):
        resolve_environment(load_snapshot(root), "test")


def test_id_only_overlay_keeps_unique_name_and_identity(tmp_path):
    root = tmp_path / "metadata"
    _write(root / "common.json", {"connections": [{"name": "shared", "connection_id": "explicit", "workspace_id": "ws-a"}]})
    _write(root / "environments" / "test.json", {"connections": [{"connection_id": "explicit", "configure": {"host": "updated"}}]})
    resolved, _ = resolve_environment(load_snapshot(root), "test")
    assert resolved["connections"] == [{"name": "shared", "connection_id": "explicit", "workspace_id": "ws-a", "configure": {"host": "updated"}}]


@pytest.mark.parametrize("section,id_field", [("connections", "connection_id"), ("dataflows", "dataflow_id")])
@pytest.mark.parametrize("absent_id", [None, "", "  "])
def test_overlay_unusable_id_inherits_canonical_explicit_id(tmp_path, section, id_field, absent_id):
    root = tmp_path / "metadata"
    _write(root / "common.json", {section: [{"name": "shared", id_field: "explicit", "workspace_id": "ws-a"}]})
    _write(root / "environments" / "test.json", {section: [{"name": "shared", id_field: absent_id, "configure": {"host": "updated"}}]})
    resolved, _ = resolve_environment(load_snapshot(root), "test")
    assert len(resolved[section]) == 1
    assert resolved[section][0][id_field] == "explicit"
    assert resolved[section][0]["configure"] == {"host": "updated"}


