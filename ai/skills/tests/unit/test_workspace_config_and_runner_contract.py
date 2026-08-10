"""Executable checks for workspace config and ordered runner stage groups."""

from __future__ import annotations

import ast
import argparse
import importlib.util
import sys
import types
from pathlib import Path
from pathlib import PurePosixPath
from urllib.parse import urlparse

import pytest
import yaml


AI_DIR = Path(__file__).resolve().parents[3]
VALIDATOR_PATH = AI_DIR / "skills" / "datacoolie-build" / "scripts" / "validate_config.py"
LOCAL_RUNNER_PATH = (
    AI_DIR / "skills" / "datacoolie-build" / "templates" / "runners" / "run_local_polars.py.example"
)


class _PlatformRegistry:
    def list_plugins(self) -> list[str]:
        return ["local", "cloud_a"]


def _load_validator(monkeypatch: pytest.MonkeyPatch):
    fake_datacoolie = types.ModuleType("datacoolie")
    fake_datacoolie.platform_registry = _PlatformRegistry()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "datacoolie", fake_datacoolie)
    spec = importlib.util.spec_from_file_location("workspace_config_validator", VALIDATOR_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_config(path: Path, config: dict) -> None:
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def test_workspace_config_accepts_environment_platform_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validator = _load_validator(monkeypatch)
    path = tmp_path / "config.yaml"
    _write_config(
        path,
        {
            "schema_version": 1,
            "project": {"name": "example", "workspace_name": "example_dcws"},
            "environments": {"dev": {"platform": "local"}},
        },
    )

    result = validator.validate_config(
        path, expected_environment="dev", expected_platform="local"
    )

    assert result["environments"]["dev"]["platform"] == "local"


@pytest.mark.parametrize("extra_key", ["engine", "stage", "metadata_path", "watermark_path"])
def test_workspace_config_rejects_runtime_environment_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra_key: str
) -> None:
    validator = _load_validator(monkeypatch)
    path = tmp_path / "config.yaml"
    _write_config(
        path,
        {
            "schema_version": 1,
            "project": {"name": "example", "workspace_name": "example_dcws"},
            "environments": {"dev": {"platform": "local", extra_key: "invalid"}},
        },
    )

    with pytest.raises(ValueError, match="Additional properties"):
        validator.validate_config(path)


def test_workspace_config_rejects_runner_platform_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validator = _load_validator(monkeypatch)
    path = tmp_path / "config.yaml"
    _write_config(
        path,
        {
            "schema_version": 1,
            "project": {"name": "example", "workspace_name": "example_dcws"},
            "environments": {"test": {"platform": "cloud_a"}},
        },
    )

    with pytest.raises(ValueError, match="not 'local'"):
        validator.validate_config(
            path, expected_environment="test", expected_platform="local"
        )


def test_workspace_config_rejects_unregistered_platform(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validator = _load_validator(monkeypatch)
    path = tmp_path / "config.yaml"
    _write_config(
        path,
        {
            "schema_version": 1,
            "project": {"name": "example", "workspace_name": "example_dcws"},
            "environments": {"test": {"platform": "missing_platform"}},
        },
    )

    with pytest.raises(ValueError, match="Unsupported environment platform"):
        validator.validate_config(path)


def test_workspace_config_checks_only_selected_environment_platforms(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validator = _load_validator(monkeypatch)
    path = tmp_path / "config.yaml"
    _write_config(
        path,
        {
            "schema_version": 1,
            "project": {"name": "example", "workspace_name": "example_dcws"},
            "environments": {
                "dev": {"platform": "local"},
                "prod": {"platform": "not_installed_here"},
            },
        },
    )

    result = validator.validate_config(path, selected_environments=["dev"])
    assert result["environments"]["prod"]["platform"] == "not_installed_here"

    with pytest.raises(ValueError, match="prod=not_installed_here"):
        validator.validate_config(path)


def _load_normalizer():
    tree = ast.parse(LOCAL_RUNNER_PATH.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "normalize_stage_plan"
    )
    module = ast.Module(body=[function], type_ignores=[])
    namespace: dict[str, object] = {}
    exec(compile(module, str(LOCAL_RUNNER_PATH), "exec"), namespace)
    return namespace["normalize_stage_plan"]


def _load_non_empty_stage():
    tree = ast.parse(LOCAL_RUNNER_PATH.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "non_empty_stage"
    )
    namespace = {"argparse": argparse}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(LOCAL_RUNNER_PATH), "exec"), namespace)
    return namespace["non_empty_stage"]


def _load_persistent_path_validator():
    tree = ast.parse(LOCAL_RUNNER_PATH.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "require_persistent_path"
    )
    module = ast.Module(body=[function], type_ignores=[])
    namespace = {
        "argparse": argparse,
        "PurePosixPath": PurePosixPath,
        "urlparse": urlparse,
    }
    exec(compile(module, str(LOCAL_RUNNER_PATH), "exec"), namespace)
    return namespace["require_persistent_path"]


def test_stage_cli_preserves_parallel_groups_and_sequential_order() -> None:
    normalize = _load_normalizer()
    stage_plan = normalize(
        [["stage1,stage2"], ["stage3"], ["stage4", "stage5"]]
    )

    assert stage_plan == ["stage1,stage2", "stage3", ["stage4", "stage5"]]

    calls: list[str | list[str] | None] = []
    for stage_group in stage_plan or [None]:
        calls.append(stage_group)
    assert calls == ["stage1,stage2", "stage3", ["stage4", "stage5"]]


def test_empty_stage_plan_runs_all_once() -> None:
    normalize = _load_normalizer()
    calls = [stage_group for stage_group in normalize(None) or [None]]
    assert calls == [None]


def test_blank_stage_is_rejected_at_cli_boundary() -> None:
    validate = _load_non_empty_stage()
    assert validate("stage1,stage2") == "stage1,stage2"
    with pytest.raises(argparse.ArgumentTypeError, match="must not be blank"):
        validate("   ")


def test_failed_stage_group_is_a_sequential_barrier() -> None:
    stage_plan = ["stage1", "stage2", "stage3"]
    calls: list[str] = []

    for stage_group in stage_plan:
        calls.append(stage_group)
        failed = stage_group == "stage2"
        if failed:
            break

    assert calls == ["stage1", "stage2"]


def test_mutable_state_paths_reject_build_directory() -> None:
    validate = _load_persistent_path_validator()

    assert validate("logs/dev") == "logs/dev"
    with pytest.raises(argparse.ArgumentTypeError, match=r"outside \.builds"):
        validate(".builds/build-id/dev/watermarks")
