"""Failure semantics for the metadata-skill integration runner."""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture(scope="module")
def metadata_runner() -> ModuleType:
    runner_path = Path(__file__).resolve().parents[1] / "run_metadata.py"
    spec = importlib.util.spec_from_file_location("run_metadata", runner_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_missing_required_fixture_fails(
    metadata_runner: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        metadata_runner,
        "VALIDATION_CHECKS",
        (("required-check", tmp_path / "missing.json"),),
    )

    assert metadata_runner.run() == 1


def test_validator_failure_is_propagated(
    metadata_runner: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    metadata_file = tmp_path / "metadata.json"
    metadata_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        metadata_runner,
        "VALIDATION_CHECKS",
        (("required-check", metadata_file),),
    )
    monkeypatch.setattr(
        metadata_runner.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 1, "", "invalid"),
    )

    assert metadata_runner.run() == 1


def test_all_required_checks_pass(
    metadata_runner: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    files = (tmp_path / "one.json", tmp_path / "two.json")
    for metadata_file in files:
        metadata_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        metadata_runner,
        "VALIDATION_CHECKS",
        tuple((metadata_file.stem, metadata_file) for metadata_file in files),
    )
    monkeypatch.setattr(
        metadata_runner.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "valid", ""),
    )

    assert metadata_runner.run() == 0
