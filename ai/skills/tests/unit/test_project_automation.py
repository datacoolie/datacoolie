"""Tests for optional project-owned build automation rendering."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import render_automation


def test_render_automation_is_conditional_and_self_contained(tmp_path: Path) -> None:
    workspace = tmp_path / "project_dcws"
    workspace.mkdir()
    (workspace / "config.yaml").write_text("schema_version: 1\n", encoding="utf-8")

    output = render_automation.render(workspace)

    assert output == workspace / "automation"
    assert (output / "build.py").is_file()
    assert (output / "AUTOMATION-MANIFEST.json").is_file()
    assert (output / "datacoolie_build/scripts/materialize.py").is_file()
    assert (output / "datacoolie_build/scripts/merge.py").is_file()
    assert (output / "datacoolie_build/scripts/inspect_capabilities.py").is_file()
    assert (output / "datacoolie_build/scripts/validate_build.py").is_file()
    assert (output / "datacoolie_build/scripts/requirements.txt").is_file()
    assert (output / "datacoolie_build/schemas/0.1.0/metadata.schema.json").is_file()
    assert (
        output / "datacoolie_build/schemas/build-verification-receipt.schema.json"
    ).is_file()
    assert (output / "datacoolie_build/schemas/current-build.schema.json").is_file()
    assert not (
        output / "datacoolie_build/schemas/design-approval.schema.json"
    ).exists()
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in output.rglob("*.py")
    )
    assert "datacoolie-init" not in text
    assert "datacoolie-metadata" not in text
    manifest = (output / "AUTOMATION-MANIFEST.json").read_text(encoding="utf-8")
    assert "datacoolie_build/scripts/requirements.txt" in manifest
    assert "datacoolie-design" not in manifest
    compile((output / "build.py").read_text(encoding="utf-8"), "build.py", "exec")

    with pytest.raises(ValueError, match="already exists"):
        render_automation.render(workspace)

    assert render_automation.render(workspace, force=True) == output


def test_build_automation_has_no_sibling_skill_dependency() -> None:
    skill_root = Path(render_automation.__file__).resolve().parent.parent
    maintained = [
        skill_root / "scripts" / "materialize.py",
        skill_root / "scripts" / "render_automation.py",
    ]
    for path in maintained:
        assert "datacoolie-design" not in path.read_text(encoding="utf-8")


def test_isolated_build_skill_renders_approved_workspace(tmp_path: Path) -> None:
    source = Path(render_automation.__file__).resolve().parent.parent
    isolated = tmp_path / "datacoolie-build"
    shutil.copytree(source, isolated, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    workspace = tmp_path / "project_dcws"
    (workspace / "architecture").mkdir(parents=True)
    (workspace / "architecture/current.md").write_text("# Approved architecture\n", encoding="utf-8")
    (workspace / "config.yaml").write_text("schema_version: 1\n", encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(isolated / "scripts/render_automation.py"),
            "--workspace",
            str(workspace),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert (workspace / "automation/build.py").is_file()
    assert not (
        workspace / "automation/datacoolie_build/schemas/design-approval.schema.json"
    ).exists()
