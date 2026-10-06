"""Current-session CLI rejection and byte-preservation oracles for API cases."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _stateful_support import PRODUCT_ROOT, LOG_DIR, DriverEvidence
from runner.runtime_logs import job_sessions, records, require_job_logs
from datacoolie.utils.identity import name_to_uuid
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.watermark.base import WatermarkSerializer


def snapshot(root):
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }


def seed_checkpoint(case, flow, values):
    provider = FileProvider(
        config_path=str(case.metadata),
        platform=LocalPlatform(),
        watermark_base_path=str(case.watermarks / "provider"),
    )
    try:
        provider.initialize()
        provider.update_watermark(
            name_to_uuid(flow), WatermarkSerializer.serialize(values)
        )
    finally:
        provider.close()


def assert_rejection(exit_code, rows, *, flow, session_id, error):
    if exit_code != 2 or not session_id:
        raise AssertionError(
            f"Expected child exit 2 and current session, got {exit_code}/{session_id}"
        )
    flows = [r for r in rows if r.get("_type") == "dataflow_run_log"]
    if (
        len(flows) != 1
        or flows[0].get("dataflow_id") != name_to_uuid(flow)
        or flows[0].get("log_session_id") != session_id
        or flows[0].get("status") != "failed"
    ):
        raise AssertionError(
            f"Missing unique failed expected flow in current session: {flows!r}"
        )
    attempts = flows[0].get("attempts", [])
    if not attempts or any(
        (a.get("source") or {}).get("message") != error
        or (a.get("source") or {}).get("status") != "failed"
        for a in attempts
    ):
        raise AssertionError(f"Exact source rejection mismatch: {attempts!r}")
    if any(a.get("destination") is not None for a in attempts):
        raise AssertionError("Rejected source reached the destination phase")


def run_rejection(case, *, flow, stage, error, engine="polars"):
    """A real child CLI exit 2 is necessary, but insufficient, for outer PASS."""
    job = "rejection-" + uuid4().hex
    before_sessions = job_sessions(LOG_DIR, job)
    before_output, before_state = snapshot(case.output), snapshot(case.watermarks)
    command = [
        sys.executable,
        "-u",
        str(PRODUCT_ROOT / "usecase-sim/runner/run.py"),
        "--engine",
        engine,
        "--platform",
        "local",
        "--metadata-source",
        "file",
        "--metadata-path",
        str(case.metadata),
        "--stage",
        stage,
        "--log-path",
        str(LOG_DIR),
        "--job-id",
        job,
        "--state-base-path",
        str(case.state),
        "--watermark-path",
        str(case.watermarks / "provider"),
        "--max-workers",
        "1",
    ]
    child = subprocess.run(
        command,
        cwd=PRODUCT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=150,
    )
    # Unique case-owned evidence; never overwrite historical dispatcher logs.
    (case.receipt_root / "child-console.log").write_text(
        child.stdout + child.stderr, encoding="utf-8"
    )
    sessions = job_sessions(LOG_DIR, job) - before_sessions
    if len(sessions) != 1:
        raise AssertionError(f"Expected one new child session, got {sessions!r}")
    session = next(iter(sessions))
    require_job_logs(LOG_DIR, job, session)
    rows = [row for _, row in records(LOG_DIR, job, "execution_logs", session)]
    assert_rejection(child.returncode, rows, flow=flow, session_id=session, error=error)
    if (
        snapshot(case.output) != before_output
        or snapshot(case.watermarks) != before_state
    ):
        raise AssertionError("Rejected API read changed destination/checkpoint bytes")
    evidence = DriverEvidence(
        job,
        stage,
        session,
        {"total": 1, "succeeded": 0, "failed": 1, "skipped": 0},
        tuple(rows),
    )
    return evidence, {
        "command": command,
        "actual_exit_code": child.returncode,
        "expected_exit_code": 2,
        "exact_source_error": error,
        "output_unchanged": True,
        "checkpoint_unchanged": True,
    }
