from __future__ import annotations

import json
from pathlib import Path

from datacoolie.project.documents import load_snapshot
from datacoolie.project.overlays import resolve_environment


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


