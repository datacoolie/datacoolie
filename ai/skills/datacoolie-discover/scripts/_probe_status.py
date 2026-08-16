"""Optional machine-readable status for disposable discovery probes."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from _artifact_io import atomic_write_json

PARTIAL_EXIT_CODE = 3


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_probe_status(
    path: Path | None,
    *,
    source: str,
    probe: str,
    row_count: int,
    issues: Iterable[str] = (),
) -> str:
    issue_list = [str(issue) for issue in issues]
    status = "partial" if issue_list else "complete"
    if path is None:
        return status
    payload = {
        "source": source,
        "probe": probe,
        "status": status,
        "row_count": row_count,
        "issues": issue_list,
        "finished_at": utc_now(),
    }
    atomic_write_json(path, payload)
    return status
