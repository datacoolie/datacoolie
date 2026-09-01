"""Focused safety and import-contract tests for Python-function artifacts."""

from __future__ import annotations

import zipfile
import subprocess
from pathlib import Path

import pytest

import validate_functions


def _zip(path: Path, members: dict[str, str]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return path


def test_zip_identity_requires_one_project_package(tmp_path: Path) -> None:
    artifact = _zip(
        tmp_path / "project.zip",
        {
            "project_a/__init__.py": "",
            "project_b/__init__.py": "",
        },
    )

    with pytest.raises(ValueError, match="exactly one top-level import package"):
        validate_functions.inspect_artifact(artifact)


def test_zip_identity_rejects_content_outside_project_package(tmp_path: Path) -> None:
    artifact = _zip(
        tmp_path / "project.zip",
        {
            "project_package/__init__.py": "",
            "sitecustomize.py": "raise RuntimeError('must never load')",
        },
    )

    with pytest.raises(ValueError, match="only its declared top-level import package"):
        validate_functions.inspect_artifact(artifact)


@pytest.mark.parametrize("unsafe_name", ["../escape.py", "C:/escape.py", "package/native.so"])
def test_zip_identity_rejects_unsafe_or_compiled_members(
    tmp_path: Path, unsafe_name: str
) -> None:
    artifact = _zip(
        tmp_path / "project.zip",
        {
            "project_package/__init__.py": "",
            unsafe_name: "x",
        },
    )

    with pytest.raises(ValueError, match="unsafe member|pure-Python"):
        validate_functions.inspect_artifact(artifact)


def test_import_validation_rejects_positional_only_framework_arguments(tmp_path: Path) -> None:
    artifact = _zip(
        tmp_path / "project.zip",
        {
            "project_package/__init__.py": "",
            "project_package/sources.py": (
                "def load(engine, source, watermark_start, watermark_end, /): return None\n"
            ),
        },
    )
    identity = validate_functions.inspect_artifact(artifact)

    with pytest.raises(subprocess.CalledProcessError):
        validate_functions.validate_imports(
            artifact,
            identity,
            ["project_package.sources.load"],
        )


def test_import_validation_accepts_framework_kwargs(tmp_path: Path) -> None:
    artifact = _zip(
        tmp_path / "project.zip",
        {
            "project_package/__init__.py": "",
            "project_package/sources.py": "def load(**kwargs): return None\n",
        },
    )
    identity = validate_functions.inspect_artifact(artifact)

    validate_functions.validate_imports(
        artifact,
        identity,
        ["project_package.sources.load"],
    )
