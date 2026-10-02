"""Exercise the public project CLI and its framework loading seam."""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from datacoolie import __version__
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.preparation.query import resolve_query
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.project.validation.artifacts import validate_artifact


pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "cli" / "projects" / "artifact_project"


def _run_cli(*arguments: str) -> dict[str, object]:
    result = subprocess.run(
        [sys.executable, "-m", "datacoolie", "--format", "json", *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert isinstance(payload, dict), result.stdout
    assert payload["schema_version"] == 1, result.stdout
    assert payload["datacoolie_version"] == __version__, result.stdout
    assert payload["ok"] is True, result.stdout
    assert isinstance(payload["data"], dict), result.stdout
    return payload["data"]


def test_cli_builds_all_environments_and_fileprovider_can_prepare_query(
    tmp_path: Path,
) -> None:
    project = tmp_path / "artifact-project"
    shutil.copytree(FIXTURE_ROOT, project)

    validation = _run_cli("validate", "--project-dir", str(project))
    assert validation["scope"] == "project"
    assert validation["errors"] == []

    build = _run_cli("build", "--project-dir", str(project))
    build_id = build["build_id"]
    build_root = Path(str(build["build_path"]))
    current_root = Path(str(build["current_path"]))
    assert build["environments"] == ["dev", "prod"]
    assert isinstance(build_id, str) and build_id
    assert build_root.is_dir()
    assert current_root.is_dir()
    current_manifest = json.loads((current_root / "manifest.json").read_text(encoding="utf-8"))
    assert current_manifest["build_id"] == build_id
    assert current_manifest["datacoolie_version"] == __version__
    assert not (current_root / "build.json").exists()
    assert not (current_root / "SHA256SUMS").exists()

    for environment in ("dev", "prod"):
        environment_root = build_root / environment
        manifest = json.loads((environment_root / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["datacoolie_version"] == __version__
        assert (environment_root / "metadata" / "metadata.json").is_file()
        assert (environment_root / "queries" / "orders.sql").is_file()
        report = validate_artifact(environment_root)
        assert report.ok, report.errors

    # The CLI integration boundary stops at a valid, loadable artifact.  The
    # framework seam confirms that the built metadata remains declarative while
    # query content is resolved only for execution.
    environment_root = build_root / "dev"
    platform = LocalPlatform()
    provider = FileProvider(
        platform=platform,
        metadata_base_path=str(environment_root / "metadata"),
    )
    try:
        provider.initialize()
        dataflows = provider.get_dataflows(stage="project_query")
        assert len(dataflows) == 1
        declared_query = dataflows[0].source.query
        assert declared_query == "artifact:/queries/orders.sql"
        execution_query = resolve_query(
            declared_query,
            platform,
            artifact_base_path=str(environment_root),
        )
        assert execution_query is not None
        assert execution_query.lstrip().lower().startswith("select")
    finally:
        provider.close()


def test_cli_delivers_runner_files_to_matching_environment(
    tmp_path: Path,
) -> None:
    project = tmp_path / "runner-project"
    shutil.copytree(FIXTURE_ROOT, project)
    (project / "runners" / "dev").mkdir(parents=True)
    (project / "runners" / "prod").mkdir(parents=True)
    (project / "runners" / "dev" / "run.py").write_bytes(b"dev-runner\x00")
    (project / "runners" / "prod" / "run.py").write_bytes(b"prod-runner\xff")

    build = _run_cli("build", "--project-dir", str(project))
    root = Path(str(build["build_path"]))
    assert (root / "dev" / "runners" / "run.py").read_bytes() == b"dev-runner\x00"
    assert (root / "prod" / "runners" / "run.py").read_bytes() == b"prod-runner\xff"
    manifests = {
        environment: json.loads((root / environment / "manifest.json").read_text(encoding="utf-8"))
        for environment in ("dev", "prod")
    }
    assert manifests["dev"]["components"]["runners"]["files"] == ["runners/run.py"]
    assert manifests["prod"]["components"]["runners"]["files"] == ["runners/run.py"]
    assert validate_artifact(root).ok
