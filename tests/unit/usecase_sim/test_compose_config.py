"""An isolated stack override reaches both startup paths before any mutation."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


SIM = Path(__file__).resolve().parents[3] / "usecase-sim"


@pytest.fixture
def modules(monkeypatch):
    monkeypatch.syspath_prepend(str(SIM))
    monkeypatch.syspath_prepend(str(SIM / "scripts"))
    monkeypatch.delenv("DATACOOLIE_COMPOSE_OVERRIDE", raising=False)
    from runner import compose_config, run_scenario
    spec = importlib.util.spec_from_file_location(
        "compose_override_platform", SIM / "scripts/bootstrap/setup_platform.py",
    )
    platform = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(platform)
    monkeypatch.setattr(platform.shutil, "which", lambda _: "docker")
    return compose_config, run_scenario, platform


def test_default_commands_remain_unchanged(modules, monkeypatch):
    config, runner, platform = modules
    assert config.compose_file_arguments("base.yml", {}) == ["-f", "base.yml"]
    assert config.compose_startup_arguments({}) == []
    calls = Mock(return_value=SimpleNamespace(stdout="false"))
    monkeypatch.setattr(runner.subprocess, "run", calls)
    runner._ensure_service_running("mock-api")
    assert calls.call_args.args[0] == [
        "docker", "compose", "-f", runner.DOCKER_COMPOSE_FILE,
        "up", "-d", "--wait", "mock-api",
    ]
    call = Mock(return_value=0)
    monkeypatch.setattr(platform.subprocess, "call", call)
    assert platform._compose("up", "-d", "mock-api") == 0
    assert call.call_args.args[0] == [
        "docker", "compose", "-f", str(platform.COMPOSE_FILE), "up", "-d", "mock-api",
    ]


def test_override_and_pinned_startup_reach_both_consumers(modules, monkeypatch, tmp_path):
    _, runner, platform = modules
    override = tmp_path / "compose.override.json"
    override.write_text('{"services": {}}', encoding="utf-8")
    monkeypatch.setenv("DATACOOLIE_COMPOSE_OVERRIDE", str(override))
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "benchmark_1")
    calls = Mock(return_value=SimpleNamespace(stdout="false"))
    monkeypatch.setattr(runner.subprocess, "run", calls)
    runner._ensure_service_running("mock-api")
    assert calls.call_args.args[0] == [
        "docker", "compose", "-f", runner.DOCKER_COMPOSE_FILE,
        "-f", str(override.resolve()), "up", "-d", "--wait",
        "--no-build", "--pull", "never", "mock-api",
    ]
    call = Mock(return_value=0)
    monkeypatch.setattr(platform.subprocess, "call", call)
    platform._compose("up", "-d", "mock-api")
    assert call.call_args.args[0] == [
        "docker", "compose", "-f", str(platform.COMPOSE_FILE),
        "-f", str(override.resolve()), "up", "--no-build", "--pull", "never", "-d", "mock-api",
    ]
    platform._compose("down")
    assert call.call_args.args[0][-1] == "down"
    assert "--no-build" not in call.call_args.args[0]


@pytest.mark.parametrize("value,project", [
    ("", "benchmark"), (" ", "benchmark"), ("relative.json", "benchmark"),
    ("missing", "benchmark"), ("existing", ""), ("existing", "UPPER"),
    ("existing", "-bad"), ("existing", "has space"),
])
def test_bad_override_fails_before_inspect_or_compose(modules, monkeypatch, tmp_path, value, project):
    _, runner, platform = modules
    existing = tmp_path / "override.json"
    existing.write_text("{}", encoding="utf-8")
    if value == "existing":
        value = str(existing)
    elif value == "missing":
        value = str(tmp_path / "absent.json")
    monkeypatch.setenv("DATACOOLIE_COMPOSE_OVERRIDE", value)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", project)
    run = Mock(return_value=SimpleNamespace(stdout="true"))
    call = Mock()
    monkeypatch.setattr(runner.subprocess, "run", run)
    monkeypatch.setattr(platform.subprocess, "call", call)
    with pytest.raises(ValueError):
        runner._ensure_service_running("mock-api")
    with pytest.raises(ValueError):
        platform._compose("up", "-d", "mock-api")
    run.assert_not_called()
    call.assert_not_called()


def test_symlink_override_is_rejected_before_resolve(modules, tmp_path):
    config, _, _ = modules
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation unavailable on this host")
    with pytest.raises(ValueError, match="regular"):
        config.compose_file_arguments("base.yml", {
            "DATACOOLIE_COMPOSE_OVERRIDE": str(link), "COMPOSE_PROJECT_NAME": "benchmark",
        })
