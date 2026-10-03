"""Read simulator framework records with explicit job/session correlation."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any


def records(
    root: Path, job_id: str, category: str, session_id: str | None = None
) -> Iterator[tuple[Path, dict[str, Any]]]:
    """Yield matching JSONL records from one direct framework category."""
    for path in sorted((root / category).rglob("*.json")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(value, dict) or value.get("job_id") != job_id:
                continue
            if session_id is None or value.get("log_session_id") == session_id:
                yield path, value


def job_sessions(root: Path, job_id: str) -> set[str]:
    return {
        value["log_session_id"]
        for _, value in records(root, job_id, "execution_logs/job_run_log")
        if isinstance(value.get("log_session_id"), str)
    }


def require_job_logs(root: Path, job_id: str, session_id: str | None = None) -> None:
    jobs = [
        value for _, value in records(root, job_id, "execution_logs", session_id)
        if value.get("_type") == "job_run_log"
    ]
    if len(jobs) != 1:
        raise AssertionError(f"Expected one current job snapshot for {job_id}, got {len(jobs)}")
    if not list(records(root, job_id, "system_logs", session_id)):
        raise AssertionError(f"No current system logs for {job_id}")
