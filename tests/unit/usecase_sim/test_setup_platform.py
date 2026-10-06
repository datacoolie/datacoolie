"""Bootstrap must not admit an open port while container health is pending."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest


SIM = Path(__file__).resolve().parents[3] / "usecase-sim"


@pytest.fixture
def platform_setup(monkeypatch):
    monkeypatch.syspath_prepend(str(SIM))
    monkeypatch.syspath_prepend(str(SIM / "scripts"))
    monkeypatch.chdir(SIM.parent)
    spec = importlib.util.spec_from_file_location(
        "isolated_platform_setup", SIM / "scripts/bootstrap/setup_platform.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.shutil, "which", lambda _: "docker")
    monkeypatch.setattr(module, "_ensure_mssql_database", Mock())
    return module


def test_open_oracle_port_cannot_hide_failed_healthcheck(platform_setup, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["setup_platform.py", "--services", "oracle"])
    monkeypatch.setattr(platform_setup, "port_open", lambda *args, **kwargs: True)
    ports = Mock(return_value=True)
    monkeypatch.setattr(platform_setup, "_wait_for_ports", ports)

    def docker_compose(command):
        # Docker admits a running container without --wait, but refuses the
        # not-yet-healthy Oracle container when health waiting is requested.
        return 1 if "--wait" in command else 0

    monkeypatch.setattr(platform_setup.subprocess, "call", docker_compose)
    assert platform_setup.main() == 1
    ports.assert_not_called()
    platform_setup._ensure_mssql_database.assert_not_called()


def test_healthy_subset_still_checks_exposed_ports(platform_setup, monkeypatch):
    monkeypatch.setattr(sys, "argv", [
        "setup_platform.py", "--services", "oracle", "--timeout", "42",
    ])
    compose = Mock(return_value=0)
    ports = Mock(return_value=True)
    monkeypatch.setattr(platform_setup.subprocess, "call", compose)
    monkeypatch.setattr(platform_setup, "_wait_for_ports", ports)
    assert platform_setup.main() == 0
    command = compose.call_args.args[0]
    assert command[4:] == ["up", "-d", "--wait", "--wait-timeout", "42", "oracle"]
    ports.assert_called_once_with(["oracle"], 42)


def test_healthy_container_with_unreachable_port_fails(platform_setup, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["setup_platform.py", "--services", "oracle"])
    monkeypatch.setattr(platform_setup.subprocess, "call", Mock(return_value=0))
    monkeypatch.setattr(platform_setup, "_wait_for_ports", Mock(return_value=False))
    assert platform_setup.main() == 2


def test_no_wait_keeps_explicit_readiness_opt_out(platform_setup, monkeypatch):
    monkeypatch.setattr(sys, "argv", [
        "setup_platform.py", "--services", "oracle", "--no-wait",
    ])
    compose = Mock(return_value=0)
    ports = Mock()
    monkeypatch.setattr(platform_setup.subprocess, "call", compose)
    monkeypatch.setattr(platform_setup, "_wait_for_ports", ports)
    assert platform_setup.main() == 0
    assert compose.call_args.args[0][4:] == ["up", "-d", "oracle"]
    ports.assert_not_called()


def test_shutdown_does_not_request_health_wait(platform_setup, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["setup_platform.py", "--down"])
    compose = Mock(return_value=0)
    ports = Mock()
    monkeypatch.setattr(platform_setup.subprocess, "call", compose)
    monkeypatch.setattr(platform_setup, "_wait_for_ports", ports)
    assert platform_setup.main() == 0
    assert compose.call_args.args[0][4:] == ["down"]
    ports.assert_not_called()
