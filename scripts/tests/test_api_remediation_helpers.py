"""Isolated HTTP helper execution; this does not qualify the deployed image."""

from __future__ import annotations

import importlib.util
import json
import sys
import threading
from pathlib import Path

import pytest

SIM = Path(__file__).resolve().parents[2] / "usecase-sim"
sys.path.insert(0, str(SIM / "scripts/features"))


@pytest.fixture
def server(monkeypatch):
    from werkzeug.serving import make_server

    spec = importlib.util.spec_from_file_location(
        "isolated_remediation_mock", SIM / "docker/mock_api_server.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    server = make_server("127.0.0.1", 0, module.app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv(
        "DATACOOLIE_MOCK_API_URL", f"http://127.0.0.1:{server.server_port}"
    )
    yield
    server.shutdown()
    thread.join(timeout=5)


@pytest.mark.parametrize("negative", [False, True])
def test_date_helpers_execute_real_driver_against_isolated_http(server, negative):
    from incremental.validate_api_date import validate

    receipt = validate(negative=negative, qualification_scope="isolated-http-helper-test")
    value = json.loads(receipt.read_text())
    assert value["status"] == "passed"
    if negative:
        assert value["details"]["no_http"] is True
        assert value["details"]["actual_exit_code"] == 2
    else:
        assert len(value["runs"]) == 3
        assert value["details"]["stable_rerun"] is True


def test_max_pages_helper_executes_exact_negative_against_isolated_http(server):
    from read.validate_api_max_pages import validate

    receipt = validate(qualification_scope="isolated-http-helper-test")
    value = json.loads(receipt.read_text())
    assert value["status"] == "passed"
    assert value["details"]["actual_exit_code"] == 2
    assert value["details"]["checkpoint_unchanged"] is True
