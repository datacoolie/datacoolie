from __future__ import annotations

import json
from pathlib import Path
import zipfile

import pytest

from datacoolie.project.build import build_project
from datacoolie.project.build.functions import package_functions, plan_function_packaging
from datacoolie.project.build.planning import create_build_plan
from datacoolie.project.build.publisher import verify_build
from datacoolie.project.config import load_project_config, project_config_from_mapping
from datacoolie.project.errors import ProjectConfigError, ProjectError
from datacoolie.project.runners import discover_runner_layout
from datacoolie.project.scaffold import init_project
from datacoolie.project.validation.artifacts import validate_artifact
from datacoolie.project.validation.project import validate_project


def _empty_project(monkeypatch: pytest.MonkeyPatch, root: Path) -> Path:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# agents\n",
    )
    init_project(root, environments=["dev", "prod"])
    (root / "metadata" / "dataflows" / "empty.json").write_text(
        json.dumps({"dataflows": []}),
        encoding="utf-8",
    )
    return root


def test_init_creates_runner_directory_for_each_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = _empty_project(monkeypatch, tmp_path / "project")
    assert (project / "runners" / "dev" / ".gitkeep").is_file()
    assert (project / "runners" / "prod" / ".gitkeep").is_file()


def test_build_copies_runner_bytes_and_records_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = _empty_project(monkeypatch, tmp_path / "project")
    (project / "runners" / "dev" / "nested").mkdir()
    (project / "runners" / "dev" / "nested" / "run.py").write_bytes(b"\x00runner\xff")
    (project / "runners" / "dev" / "nested" / "ignored.pyc").write_bytes(b"ignored")
    (project / "runners" / "dev" / "__pycache__").mkdir()
    (project / "runners" / "dev" / "__pycache__" / "run.pyc").write_bytes(b"ignored")
    built = build_project(load_project_config(project))
    environment = Path(built["build_path"]) / "dev"
    assert (environment / "runners" / "nested" / "run.py").read_bytes() == b"\x00runner\xff"
    assert not (environment / "runners" / "nested" / "ignored.pyc").exists()
    manifest = json.loads((environment / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["components"]["runners"] == {
        "path": "runners",
        "files": ["runners/nested/run.py"],
    }


def test_runner_tampering_breaks_artifact_integrity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = _empty_project(monkeypatch, tmp_path / "project")
    (project / "runners" / "dev" / "run.py").write_text("print(1)\n", encoding="utf-8")
    built = build_project(load_project_config(project))
    artifact = Path(built["build_path"])
    (artifact / "dev" / "runners" / "run.py").write_text("tampered\n", encoding="utf-8")
    assert not validate_project(load_project_config(project)).errors
    assert not validate_artifact(artifact).ok
    with pytest.raises(ProjectError):
        verify_build(artifact)


def test_runner_bytes_change_build_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = _empty_project(monkeypatch, tmp_path / "project")
    runner = project / "runners" / "dev" / "run.py"
    runner.write_text("print(1)\n", encoding="utf-8")
    first = build_project(load_project_config(project))
    runner.write_text("print(2)\n", encoding="utf-8")
    second = build_project(load_project_config(project))
    assert first["build_id"] != second["build_id"]


def test_pycache_noise_is_ignored_by_copy_fingerprint_and_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = _empty_project(monkeypatch, tmp_path / "project")
    functions = project / "functions"
    (functions / "main.py").write_text("VALUE = 1\n", encoding="utf-8")
    config = load_project_config(project)
    before = create_build_plan(config).input_digest

    cache = functions / "__pycache__"
    cache.mkdir()
    (cache / "main.cpython-311.pyc").write_bytes(b"cache")
    (functions / "main.pyc").write_bytes(b"cache")
    after = create_build_plan(config).input_digest
    assert after == before

    built = build_project(config)
    artifact = Path(built["build_path"])
    assert not list(artifact.rglob("*.pyc"))
    assert not list(artifact.rglob("__pycache__"))


def test_zip_packaging_is_deterministic_and_ignores_cache_noise(tmp_path: Path) -> None:
    root = tmp_path / "functions"
    root.mkdir()
    (root / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "main.py").write_text("VALUE = 2\n", encoding="utf-8")
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "main.pyc").write_bytes(b"cache")
    (root / "main.pyc").write_bytes(b"cache")
    plan = plan_function_packaging(root, "zip")

    first = tmp_path / "first"
    second = tmp_path / "second"
    package_functions(root, "zip", first, plan=plan)
    package_functions(root, "zip", second, plan=plan)
    first_bytes = (first / "functions.zip").read_bytes()
    second_bytes = (second / "functions.zip").read_bytes()
    assert first_bytes == second_bytes
    with zipfile.ZipFile(first / "functions.zip") as archive:
        assert archive.namelist() == ["functions/", "functions/__init__.py", "functions/main.py"]


def test_wheel_backend_inputs_are_fingerprinted_but_cache_noise_is_not(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "datacoolie.project.build.functions.importlib.util.find_spec",
        lambda name: object(),
    )
    project = _empty_project(monkeypatch, tmp_path / "project")
    functions = project / "functions"
    (functions / "main.py").write_text("VALUE = 1\n", encoding="utf-8")
    backend = functions / "pyproject.toml"
    backend.write_text(
        """[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "test-functions"
version = "0.1.0"
""",
        encoding="utf-8",
    )
    config = load_project_config(project)
    before = create_build_plan(config).input_digest
    (functions / "__pycache__").mkdir()
    (functions / "__pycache__" / "main.pyc").write_bytes(b"cache")
    assert create_build_plan(config).input_digest == before
    backend.write_text(backend.read_text(encoding="utf-8").replace("0.1.0", "0.2.0"), encoding="utf-8")
    assert create_build_plan(config).input_digest != before


def test_symlink_named_as_cache_is_rejected_before_ignoring(
    tmp_path: Path,
) -> None:
    root = tmp_path / "functions"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (root / "__pycache__").symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    with pytest.raises(ProjectError, match="symlink"):
        plan_function_packaging(root, "copy")


def test_runner_layout_rejects_unassigned_or_case_mismatched_entries(tmp_path: Path) -> None:
    project = tmp_path / "project"
    runners = project / "runners"
    runners.mkdir(parents=True)
    (runners / "run.py").write_text("x", encoding="utf-8")
    (runners / "DEV").mkdir()
    layout = discover_runner_layout(project, ["dev"])
    assert len(layout.problems) == 2
    assert any("not assigned" in problem for problem in layout.problems)
    assert any("exactly match" in problem for problem in layout.problems)


def test_runner_validation_is_in_resources_scope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = _empty_project(monkeypatch, tmp_path / "project")
    (project / "runners" / "qa").mkdir()
    report = validate_project(load_project_config(project), only={"resources"})
    assert not report.ok
    assert any(item.code == "runner.invalid" for item in report.errors)


def test_component_roots_cannot_overlap_reserved_runners_directory() -> None:
    with pytest.raises(ProjectConfigError, match="reserved for environment runners"):
        project_config_from_mapping(
            {
                "project": {"name": "invalid"},
                "components": {"metadata": {"path": "runners/metadata"}},
                "environments": {"dev": {}},
            }
        )


def test_runner_build_fails_on_invalid_layout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = _empty_project(monkeypatch, tmp_path / "project")
    (project / "runners" / "unknown").mkdir()
    with pytest.raises(ProjectError, match="Invalid runners layout"):
        build_project(load_project_config(project))
