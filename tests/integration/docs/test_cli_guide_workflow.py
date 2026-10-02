"""Execute the public CLI walkthrough against the canonical project archive."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

from docs.scripts._examples import is_excluded


ROOT = Path(__file__).resolve().parents[3]
PROJECT_SOURCE = ROOT / "docs" / "examples" / "files" / "projects" / "artifact"

pytestmark = pytest.mark.integration


def _python_environment() -> dict[str, str]:
    environment = dict(os.environ)
    source_path = str(ROOT / "src")
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        source_path if not existing else source_path + os.pathsep + existing
    )
    return environment


def _archive_project(source: Path, destination: Path) -> None:
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(source.rglob("*")):
            if not path.is_file() or is_excluded(
                path, source, project_roots=(source,)
            ):
                continue
            name = Path(source.name, path.relative_to(source)).as_posix()
            archive.writestr(name, path.read_bytes())


def _run_cli(project: Path, *arguments: str) -> tuple[subprocess.CompletedProcess[str], dict]:
    completed = subprocess.run(
        [sys.executable, "-m", "datacoolie", *arguments],
        cwd=project.parent,
        env=_python_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"CLI did not return JSON: stdout={completed.stdout!r} "
            f"stderr={completed.stderr!r}"
        ) from exc
    return completed, payload


def test_downloaded_cli_walkthrough_handles_spaces_and_artifact_scopes(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "artifact.zip"
    _archive_project(PROJECT_SOURCE, archive)
    working = tmp_path / "cli tutorial with spaces"
    working.mkdir()
    with zipfile.ZipFile(archive) as payload:
        payload.extractall(working)
    project = working / PROJECT_SOURCE.name
    authored_config = (project / "datacoolie.yml").read_bytes()

    result, payload = _run_cli(
        project,
        "--format",
        "json",
        "validate",
        "--project-dir",
        str(project),
    )
    assert result.returncode == 0
    assert payload["ok"] is True
    assert payload["data"]["scope"] == "project"

    result, payload = _run_cli(
        project,
        "--format",
        "json",
        "build",
        "--project-dir",
        str(project),
        "--dry-run",
    )
    assert result.returncode == 0
    assert payload["ok"] is True
    assert payload["data"]["status"] == "dry_run"
    assert not (project / ".builds").exists()
    assert (project / "datacoolie.yml").read_bytes() == authored_config

    result, payload = _run_cli(
        project,
        "--format",
        "json",
        "build",
        "--project-dir",
        str(project),
    )
    assert result.returncode == 0
    assert payload["ok"] is True
    assert payload["data"]["status"] in {"created", "reused"}
    build_path = Path(payload["data"]["build_path"])
    current_path = Path(payload["data"]["current_path"])
    assert build_path.is_dir()
    assert current_path.is_dir()
    assert (project / "datacoolie.yml").read_bytes() == authored_config

    result, payload = _run_cli(
        project,
        "--format",
        "json",
        "inspect",
        "artifact",
        "--project-dir",
        str(project),
    )
    assert result.returncode == 0
    assert payload["ok"] is True
    assert payload["data"]["artifact_type"] == "datacoolie_build"
    assert payload["data"]["limited_scope"] is False

    result, payload = _run_cli(
        project,
        "--format",
        "json",
        "validate",
        "--artifact-path",
        str(current_path),
    )
    assert result.returncode == 0
    assert payload["ok"] is True
    comparison = payload["data"]["details"]["current_comparison"]
    assert comparison["performed"] is True
    assert comparison["ok"] is True

    result, payload = _run_cli(
        project,
        "--format",
        "json",
        "validate",
        "--artifact-path",
        str(build_path),
    )
    assert result.returncode == 0
    assert payload["ok"] is True
    assert payload["data"]["details"]["limited_scope"] is False
    assert payload["data"]["details"]["current_comparison"]["performed"] is False

    environment_path = current_path / "dev"
    result, payload = _run_cli(
        project,
        "--format",
        "json",
        "validate",
        "--artifact-path",
        str(environment_path),
    )
    assert result.returncode == 0
    assert payload["ok"] is True
    assert payload["data"]["details"]["limited_scope"] is True

    query = next(current_path.rglob("orders.sql"))
    query.write_text(query.read_text(encoding="utf-8") + "\n-- changed\n", encoding="utf-8")
    result, payload = _run_cli(
        project,
        "--format",
        "json",
        "validate",
        "--artifact-path",
        str(current_path),
    )
    assert result.returncode == 1
    assert payload["ok"] is False
