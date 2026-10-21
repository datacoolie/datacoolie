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


def test_renderer_default_is_quiet_and_explicit_layout_is_deprecated(
    tmp_path: Path, capsys
) -> None:
    project = _project(tmp_path)
    render_automation.render(project)
    assert capsys.readouterr().err == ""

    with pytest.warns(DeprecationWarning, match="metadata_layout is deprecated"):
        render_automation.render(project, force=True, metadata_layout="split")


def test_renderer_cli_accepts_legacy_layout_and_surfaces_warning(
    tmp_path: Path, monkeypatch
) -> None:
    project = _project(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "render_automation.py",
            "--workspace",
            str(project),
            "--metadata-layout",
            "preserve",
        ],
    )
    with pytest.warns(DeprecationWarning, match="metadata_layout is deprecated"):
        assert render_automation.main() == 0


def test_wrappers_use_current_python_module_with_different_path(
    tmp_path: Path, monkeypatch
) -> None:
    project = _project(tmp_path)
    output = render_automation.render(project)
    monkeypatch.setenv("PATH", str(tmp_path / "other-cli"))

    for name in ("build.py", "validate.py"):
        module = runpy.run_path(str(output / name))
        assert module["_cli"]() == [sys.executable, "-m", "datacoolie"]


def test_generated_wrappers_keep_cli_and_project_boundaries(tmp_path: Path) -> None:
    output = render_automation.render(_project(tmp_path))
    for name in ("build.py", "validate.py"):
        source = (output / name).read_text(encoding="utf-8")
        ast.parse(source)
        assert '[sys.executable, "-m", "datacoolie"]' in source
        assert "shutil" not in source
        assert "which(" not in source
        assert "os.environ" not in source
        assert "datacoolie.yml" not in source
        assert "manifest" not in source


@pytest.mark.parametrize(
    "validate_code,validate_stdout,expected_code",
    [
        (0, '{"ok": false}\n', 1),
        (0, "not-json\n", 1),
        (7, '{"ok": true}\n', 7),
    ],
)
def test_build_wrapper_requires_successful_validation(
    tmp_path,
    monkeypatch,
    capsys,
    validate_code,
    validate_stdout,
    expected_code,
):
    project = _project(tmp_path)
    wrapper = render_automation.render(project) / "build.py"
    calls = []

    def run(command, *, check, capture_output, text):
        assert check is False
        assert capture_output is True
        assert text is True
        calls.append(command)
        return SimpleNamespace(
            returncode=validate_code,
            stdout=validate_stdout,
            stderr="validation stderr\n",
        )

    monkeypatch.setattr("subprocess.run", run)
    monkeypatch.setattr(sys, "argv", [str(wrapper), "--output-dir", "custom output"])
    module = runpy.run_path(str(wrapper))
    assert module["main"]() == expected_code
    assert calls == [
        [
            sys.executable,
            "-m",
            "datacoolie",
            "validate",
            "--project-dir",
            str(project),
            "--format",
            "json",
        ]
    ]
    captured = capsys.readouterr()
    assert captured.out == validate_stdout
    assert captured.err == "validation stderr\n"


@pytest.mark.parametrize(
    "build_code,build_stdout,expected_code",
    [
        (0, '{"ok": false}\n', 1),
        (0, "malformed\n", 1),
        (13, '{"ok": true}\n', 13),
    ],
)
def test_build_wrapper_requires_successful_build(
    tmp_path,
    monkeypatch,
    capsys,
    build_code,
    build_stdout,
    expected_code,
):
    project = _project(tmp_path)
    wrapper = render_automation.render(project) / "build.py"
    calls = []
    responses = [
        SimpleNamespace(
            returncode=0, stdout='{"ok": true}\n', stderr="validation stderr\n"
        ),
        SimpleNamespace(
            returncode=build_code, stdout=build_stdout, stderr="build stderr\n"
        ),
    ]

    def run(command, *, check, capture_output, text):
        assert check is False
        assert capture_output is True
        assert text is True
        calls.append(command)
        return responses[len(calls) - 1]

    monkeypatch.setattr("subprocess.run", run)
    monkeypatch.setattr(sys, "argv", [str(wrapper), "--output-dir", "custom output"])
    module = runpy.run_path(str(wrapper))
    assert module["main"]() == expected_code
    assert calls == [
        [
            sys.executable,
            "-m",
            "datacoolie",
            "validate",
            "--project-dir",
            str(project),
            "--format",
            "json",
        ],
        [
            sys.executable,
            "-m",
            "datacoolie",
            "build",
            "--project-dir",
            str(project),
            "--format",
            "json",
            "--output-dir",
            "custom output",
        ],
    ]
    captured = capsys.readouterr()
    assert captured.out == '{"ok": true}\n' + build_stdout
    assert captured.err == "validation stderr\nbuild stderr\n"


@pytest.mark.parametrize(
    "returncode,stdout,expected_code",
    [
        (0, '{"ok": true}\n', 0),
        (0, '{"ok": false}\n', 1),
        (0, "malformed\n", 1),
        (13, '{"ok": true}\n', 13),
    ],
)
def test_validate_wrapper_requires_successful_json_and_preserves_arguments(
    tmp_path,
    monkeypatch,
    capsys,
    returncode,
    stdout,
    expected_code,
):
    project = _project(tmp_path)
    wrapper = render_automation.render(project) / "validate.py"
    calls = []

    def run(command, *, check, capture_output, text):
        assert check is False
        assert capture_output is True
        assert text is True
        calls.append(command)
        return SimpleNamespace(
            returncode=returncode, stdout=stdout, stderr="validate stderr\n"
        )

    monkeypatch.setattr("subprocess.run", run)
    monkeypatch.setattr(sys, "argv", [str(wrapper), "--strict"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(wrapper), run_name="__main__")
    assert exc.value.code == expected_code
    assert calls == [
        [
            sys.executable,
            "-m",
            "datacoolie",
            "validate",
            "--project-dir",
            str(project),
            "--format",
            "json",
            "--strict",
        ]
    ]
    captured = capsys.readouterr()
    assert captured.out == stdout
    assert captured.err == "validate stderr\n"
