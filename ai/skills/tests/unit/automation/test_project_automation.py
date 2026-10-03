from __future__ import annotations

import ast
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import render_automation


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    project.mkdir()
    (project / "datacoolie.yml").write_text(
        "schema_version: 1\nproject:\n  name: example\ncomponents:\n  metadata:\n    path: metadata\nenvironments:\n  dev:\n    platform: local\n",
        encoding="utf-8",
    )
    (project / "metadata").mkdir()
    return project


def test_renderer_creates_only_direct_cli_wrappers(tmp_path: Path) -> None:
    project = _project(tmp_path)
    output = render_automation.render(project)
    assert output == project / "automation"
    assert (output / "build.py").is_file()
    assert (output / "validate.py").is_file()
    assert (output / "README.md").is_file()
    assert not (output / "datacoolie_build").exists()
    for path in (output / "build.py", output / "validate.py"):
        ast.parse(path.read_text(encoding="utf-8"))

    with pytest.raises(ValueError, match="already exists"):
        render_automation.render(project)
    assert render_automation.render(project, force=True) == output


def test_renderer_requires_datacoolie_contract(tmp_path: Path) -> None:
    project = tmp_path / "missing-contract"
    project.mkdir()
    with pytest.raises(ValueError, match="Project contract"):
        render_automation.render(project)


def test_renderer_default_is_quiet_and_explicit_layout_is_deprecated(tmp_path: Path, capsys) -> None:
    project = _project(tmp_path)
    render_automation.render(project)
    assert capsys.readouterr().err == ""

    with pytest.warns(DeprecationWarning, match="metadata_layout is deprecated"):
        render_automation.render(project, force=True, metadata_layout="split")


def test_renderer_cli_accepts_legacy_layout_and_surfaces_warning(tmp_path: Path, monkeypatch) -> None:
    project = _project(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["render_automation.py", "--workspace", str(project), "--metadata-layout", "preserve"],
    )
    with pytest.warns(DeprecationWarning, match="metadata_layout is deprecated"):
        assert render_automation.main() == 0


@pytest.mark.parametrize("validate_code,build_code", [(0, 0), (7, 0), (0, 9)])
def test_build_wrapper_forwards_arguments_and_stops_on_validation_failure(
    tmp_path, monkeypatch, validate_code, build_code,
):
    project = _project(tmp_path)
    wrapper = render_automation.render(project) / "build.py"
    calls = []

    def run(command, *, check):
        assert check is False
        calls.append(command)
        return SimpleNamespace(returncode=validate_code if command[1] == "validate" else build_code)

    monkeypatch.setattr("subprocess.run", run)
    monkeypatch.setattr("shutil.which", lambda name: "stub-dc" if name == "dc" else None)
    monkeypatch.setattr(sys, "argv", [str(wrapper), "--output-dir", "custom output"])
    module = runpy.run_path(str(wrapper))
    assert module["main"]() == (validate_code or build_code)
    expected = [["stub-dc", "validate", "--project-dir", str(project), "--format", "json"]]
    if validate_code == 0:
        expected.append(["stub-dc", "build", "--project-dir", str(project), "--format", "json",
                         "--output-dir", "custom output"])
    assert calls == expected


def test_validate_wrapper_preserves_exit_code_and_arguments(tmp_path, monkeypatch):
    project = _project(tmp_path)
    wrapper = render_automation.render(project) / "validate.py"
    calls = []

    def run(command, *, check):
        assert check is False
        calls.append(command)
        return SimpleNamespace(returncode=13)

    monkeypatch.setattr("subprocess.run", run)
    monkeypatch.setattr("shutil.which", lambda name: "stub-dc" if name == "dc" else None)
    monkeypatch.setattr(sys, "argv", [str(wrapper), "--strict"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(wrapper), run_name="__main__")
    assert exc.value.code == 13
    assert calls == [["stub-dc", "validate", "--project-dir", str(project), "--format", "json", "--strict"]]


@pytest.mark.parametrize("available", ["dc", "datacoolie", None])
def test_wrapper_resolves_installed_alias_or_module(tmp_path, monkeypatch, available):
    wrapper = render_automation.render(_project(tmp_path)) / "build.py"
    monkeypatch.setattr("shutil.which", lambda name: f"stub-{name}" if name == available else None)
    expected = [f"stub-{available}"] if available else [sys.executable, "-m", "datacoolie"]
    assert runpy.run_path(str(wrapper))["_cli"]() == expected
