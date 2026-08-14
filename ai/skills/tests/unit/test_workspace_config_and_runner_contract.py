"""Executable checks for workspace config and single-stage runner passthrough."""

from __future__ import annotations

import ast
import importlib.util
import sys
import types
from pathlib import Path

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


def test_stage_cli_is_one_unmodified_framework_argument() -> None:
    content = LOCAL_RUNNER_PATH.read_text(encoding="utf-8")
    assert 'parser.add_argument("--stage")' in content
    assert "result = driver.run(stage=args.stage)" in content
    assert content.count("driver.run(") == 1
    for stale in (
        "normalize_stage_plan",
        "stage_groups",
        'action="append"',
        'nargs="+"',
        "non_empty_stage",
        "require_persistent_path",
    ):
        assert stale not in content


def test_runner_passes_paths_directly_to_framework() -> None:
    content = LOCAL_RUNNER_PATH.read_text(encoding="utf-8")
    assert "watermark_base_path=args.watermark_base_path" in content
    assert "base_log_path=args.base_log_path" in content
    assert 'parser.add_argument("--watermark-base-path", required=True)' in content
    assert 'parser.add_argument("--base-log-path", required=True)' in content
