"""Read simulator framework records with explicit job/session correlation."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from runner.runtime_profile import optimized
from runner.runtime_timing import phase


_STEM = re.compile(r"(job|dataflow|system)_(\d{8})_(\d{6})_(-?\d+)_(-?\d+)_(.+)\.json")
_PREFIXES = {
    "execution_logs": {"job", "dataflow"},
    "execution_logs/job_run_log": {"job"},
    "execution_logs/dataflow_run_log": {"dataflow"},
    "system_logs": {"system"},
}


def _filename_jobs(path: Path, category: str) -> set[str] | None:
    """Recognize only the current writer layout; unknown/legacy names are opaque.

    A name ending in _part_00000001 can be a snapshot job ID or a batch
    suffix. Retain both interpretations so neither job can be hidden.
    """
    from datacoolie.logging.persistence.layout import safe_filename_token

    match = _STEM.fullmatch(path.name)
    if not match or match[1] not in _PREFIXES.get(category, set()):
        return None
    number, index = int(match[4]), int(match[5])
    if (number < 1 or not 0 <= index < number or
            str(number) != match[4] or str(index) != match[5]):
        return None
    try:
        datetime.strptime(match[2] + match[3], "%Y%m%d%H%M%S")
    except ValueError:
        return None
    token = match[6]
    if safe_filename_token(unquote(token)) != token:
        return None  # Noncanonical percent encoding or an older name format.
    jobs = {token}
    if match[1] != "job":
        batch = re.fullmatch(r"(.+)_part_\d{8,}", token)
        if batch:
            jobs.add(batch[1])
    return jobs


def records(
    root: Path, job_id: str, category: str, session_id: str | None = None,
    *, full_scan: bool = False,
) -> Iterator[tuple[Path, dict[str, Any]]]:
    """Yield current job/session records, with opt-in writer-layout pruning.

    JSON identities remain authoritative. Unknown names are always inspected;
    full_scan preserves the original reader for controls and unusual layouts.
    No parsed-record cache survives a call, so later writes remain visible.
    """
    with phase("log_lookup_s"):
        paths = sorted((root / category).rglob("*.json"))
        if optimized() and not full_scan:
            from datacoolie.logging.persistence.layout import safe_filename_token

            token = safe_filename_token(job_id)
            paths = [path for path in paths
                     if (jobs := _filename_jobs(path, category)) is None or token in jobs]
    for path in paths:
        matching = []
        with phase("log_lookup_s"):
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
                    matching.append((path, value))
        yield from matching


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
