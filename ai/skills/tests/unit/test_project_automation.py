from __future__ import annotations

import ast
from pathlib import Path

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
        content = path.read_text(encoding="utf-8")
        assert "materialize" not in content
        assert "datacoolie_build" not in content
        assert ("-m" in content and "datacoolie" in content) or "shutil.which" in content

    with pytest.raises(ValueError, match="already exists"):
        render_automation.render(project)
    assert render_automation.render(project, force=True) == output


def test_renderer_requires_datacoolie_contract(tmp_path: Path) -> None:
    project = tmp_path / "missing-contract"
    project.mkdir()
    with pytest.raises(ValueError, match="Project contract"):
        render_automation.render(project)
