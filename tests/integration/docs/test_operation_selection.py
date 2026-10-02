"""Public runner selection, status and dry-run behavior with local metadata."""

from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "docs/examples/files/projects/incremental"
RUNNER = ROOT / "docs/examples/files/runners/local/run.py"
pytestmark = pytest.mark.integration


def _snapshot_files(root: Path) -> dict[str, bytes] | None:
    """Capture a tree without allowing a subprocess to hide a side effect."""
    if not root.exists():
        return None
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.mark.parametrize(
    "case", ["dry-run", "empty-stage", "inactive", "other-shard", "read-failure", "pending"]
)
def test_public_runner_selection_preserves_output_and_state(tmp_path, case):
    repository_snapshot = _snapshot_files(PROJECT)
    repository_runtime_roots = (ROOT / "state", ROOT / "logs", ROOT / "watermarks")
    repository_runtime_snapshot = {
        root: _snapshot_files(root) for root in repository_runtime_roots
    }

    project = tmp_path / "project"
    shutil.copytree(PROJECT, project)
    metadata_file = project / "metadata/dataflows/orders_incremental.json"
    payload = json.loads(metadata_file.read_text())
    flow = payload["dataflows"][0]
    if case == "pending":
        failing = deepcopy(flow)
        failing.update(group_number=0, execution_order=0, name="orders_failing")
        failing["source"]["table"] = "nonexistent"
        waiting = deepcopy(flow)
        waiting.update(group_number=0, execution_order=1, name="orders_waiting")
        payload["dataflows"] = [failing, waiting]
    else:
        flow["group_number"] = 0
        if case == "inactive":
            flow["is_active"] = False
        if case == "read-failure":
            flow["source"]["table"] = "nonexistent"
    metadata_file.write_text(json.dumps(payload), encoding="utf-8")
    runtime = tmp_path / "state"
    command = [sys.executable, str(RUNNER), "--metadata-base-path", str(project / "metadata"),
               "--working-directory", str(project), "--state-base-path", str(runtime)]
    if case == "dry-run":
        command += ["--dry-run"]
    elif case == "empty-stage":
        command += ["--stage", "absent-stage"]
    elif case == "other-shard":
        command += ["--job-num", "2", "--job-index", "1"]
    elif case == "pending":
        command += ["--max-workers", "1"]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + environment.get("PYTHONPATH", "")
    completed = subprocess.run(command, cwd=tmp_path, env=environment, capture_output=True,
                               text=True, check=False, timeout=60)
    assert completed.returncode == (1 if case in {"read-failure", "pending"} else 0), completed.stdout + completed.stderr
    assert not list((project / "data/output").rglob("*.parquet"))
    assert not list(runtime.rglob("watermark_value.json"))
    logs = list((runtime / "logs/execution_logs/job_run_log").rglob("*.json"))
    assert len(logs) == 1
    record = json.loads(logs[0].read_text().splitlines()[0])
    # Loading filters inactive metadata and other shards before execution;
    # directly supplying an inactive flow has a separate SKIPPED contract.
    # The persisted job summary counts terminal observations only.  The
    # scheduler's admitted total/pending split is recorded by the operation
    # completion event below.
    expected_total = 1 if case == "pending" else 0 if case in {"empty-stage", "other-shard", "inactive"} else 1
    assert record["total_dataflows"] == expected_total
    assert record["total_failed"] == (1 if case in {"read-failure", "pending"} else 0)
    assert record["total_skipped"] == (1 if case == "dry-run" else 0)
    assert record["status"] == ("failed" if case in {"read-failure", "pending"} else "skipped")
    assert record["total_pending"] is None
    if case == "pending":
        system_records = [
            json.loads(line)
            for path in (runtime / "logs/system_logs").rglob("*.json")
            for line in path.read_text(encoding="utf-8").splitlines()
        ]
        operation = next(
            item for item in system_records if item.get("event_name") == "operation.finished"
        )
        assert "total=2, succeeded=0, failed=1, skipped=0, pending=1" in operation["msg"]

    # The command ran outside the checkout and received every writable root
    # explicitly.  Only the copied project and its requested runtime may be
    # touched; the repository fixture and default repository roots stay byte
    # identical.
    assert {entry.name for entry in tmp_path.iterdir()} == {"project", "state"}
    assert _snapshot_files(PROJECT) == repository_snapshot
    assert {
        root: _snapshot_files(root) for root in repository_runtime_roots
    } == repository_runtime_snapshot


@pytest.mark.parametrize("attributes", ['[]', '{"value":NaN}'])
def test_public_runner_rejects_invalid_attributes_before_execution(tmp_path, attributes):
    runtime = tmp_path / "state"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + environment.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, str(RUNNER), "--metadata-base-path", str(PROJECT / "metadata"),
         "--state-base-path", str(runtime), "--run-attributes-json", attributes],
        cwd=tmp_path, env=environment, capture_output=True, text=True, check=False, timeout=60,
    )
    assert result.returncode != 0
    assert not runtime.exists()
    assert "run_attributes" in result.stderr
