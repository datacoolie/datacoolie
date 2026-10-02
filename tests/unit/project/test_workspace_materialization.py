from __future__ import annotations

import json
from pathlib import Path

import pytest

from datacoolie.project.build import build_project
from datacoolie.project.config import load_project_config
from datacoolie.project.scaffold import init_project
from datacoolie.project.validation.artifacts import validate_artifact


def _project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr("datacoolie.project.scaffold._latest_agents", lambda **_: "# agents\n")
    project = tmp_path / "example"
    init_project(project, config_seed={
        "schema_version": 1,
        "project": {"name": "example"},
        "components": {
            "metadata": {"path": "metadata"},
            "sql": [{"path": "sql/orders"}, {"path": "sql/reporting"}],
            "functions": [
                {"path": "functions/loaders", "packaging": "zip"},
                {"path": "functions/helpers", "packaging": "copy"},
            ],
        },
        "environments": {
            "dev": {"platform": "local"},
            "qa": {"platform": "local"},
        },
    })
    metadata = project / "metadata"
    (metadata / "connections.json").write_text(json.dumps({"connections": [
        {"name": "source", "connection_type": "file", "format": "csv"},
        {"name": "destination", "connection_type": "file", "format": "parquet"},
    ]}), encoding="utf-8")
    (metadata / "dataflows" / "orders.json").write_text(json.dumps({"dataflows": [{
        "name": "orders",
        "stage": "bronze",
        "source": {"connection_name": "source", "query": "sql/orders/orders.sql"},
        "destination": {"connection_name": "destination", "table": "orders"},
    }]}), encoding="utf-8")
    (project / "sql" / "orders" / "orders.sql").write_text("select 1\n", encoding="utf-8")
    (project / "sql" / "reporting" / "report.sql").write_text("select 2\n", encoding="utf-8")
    (project / "functions" / "loaders" / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "functions" / "loaders" / "source.py").write_text("VALUE = 2\n", encoding="utf-8")
    (project / "functions" / "helpers" / "helper.py").write_text("VALUE = 3\n", encoding="utf-8")
    for env in ("dev", "qa"):
        (project / "runners" / env / "run_local.py").write_text(f"# {env}\n", encoding="utf-8")
    return project


def test_build_all_environments_creates_exact_current_projection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = _project(tmp_path, monkeypatch)
    result = build_project(load_project_config(project))
    build = project / ".builds" / "artifacts" / result["build_id"]
    current = project / ".builds" / "current"
    assert (build / "manifest.json").is_file()
    assert (current / "manifest.json").read_bytes() == (build / "manifest.json").read_bytes()
    assert not (build / "build.json").exists()
    assert not (build / "SHA256SUMS").exists()
    assert set(path.relative_to(build).as_posix() for path in build.rglob("*")) == set(
        path.relative_to(current).as_posix() for path in current.rglob("*")
    )
    manifest = json.loads((build / "manifest.json").read_text(encoding="utf-8"))
    assert set(manifest["environments"]) == {"dev", "qa"}
    assert all("deployment_path" not in entry for entry in manifest["environments"].values())
    assert validate_artifact(current).ok
    comparison = validate_artifact(current).details["current_comparison"]
    assert comparison["ok"] is True


def test_current_comparison_catches_tampering(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = _project(tmp_path, monkeypatch)
    result = build_project(load_project_config(project))
    current_file = project / ".builds" / "current" / "dev" / "runners" / "run_local.py"
    current_file.write_text("tampered\n", encoding="utf-8")
    report = validate_artifact(project / ".builds" / "current")
    assert report.ok is False
    assert any(item.code == "current.hash_mismatch" for item in report.errors)
    assert result["build_id"] in report.details["current_comparison"]["build_path"]


def test_extracted_environment_is_limited_scope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = _project(tmp_path, monkeypatch)
    result = build_project(load_project_config(project))
    report = validate_artifact(project / ".builds" / "artifacts" / result["build_id"] / "qa")
    assert report.details["limited_scope"] is True
    assert report.details["current_comparison"]["performed"] is False

