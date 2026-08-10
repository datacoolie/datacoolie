"""Optional machine-readable status for disposable discovery probes."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from _artifact_io import atomic_write_json
from _observation_contract import utc_observed_at

PARTIAL_EXIT_CODE = 3


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
        "finished_at": utc_observed_at(),
    }
    atomic_write_json(path, payload)
    return status
