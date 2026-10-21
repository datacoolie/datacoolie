"""Bounded real Spark 3.5 container probe; never touches shared tables."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tests.support.datatype_config import require_docker_services

pytestmark = [
    pytest.mark.integration,
    pytest.mark.spark,
    pytest.mark.runtime_qualification,
    pytest.mark.xdist_group("spark"),
]
PRODUCT_ROOT = Path(__file__).resolve().parents[3]


def test_native_delta_path_contract_preserves_existing_table():
    require_docker_services(("spark",))
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-w",
            "/datacoolie",
            "datacoolie-spark",
            "python3",
            "/datacoolie/tests/support/spark_delta_path_probe_worker.py",
        ],
        cwd=PRODUCT_ROOT,
        capture_output=True,
        text=True,
        timeout=240,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    observation = json.loads(result.stdout.splitlines()[-1])
    assert observation["status"] == "passed", observation
    assert observation["spark_version"].startswith("3.5."), observation
    assert observation["relative_sql_error_class"] in {
        "DELTA_MISSING_DELTA_TABLE", "TABLE_OR_VIEW_NOT_FOUND"
    }, observation
    assert set(observation["native_failures"]) == {
        "empty_log", "corrupt", "unsupported"
    }, observation
    assert observation["permission"]["status"] in {"propagated", "skipped-root"}, observation
