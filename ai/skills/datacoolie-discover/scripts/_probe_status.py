"""Optional machine-readable status for disposable discovery probes."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
import hashlib

from _artifact_io import atomic_write_json

PARTIAL_EXIT_CODE = 3


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def observation_scope(
    rows: Iterable[dict[str, str]], *, kind: str, filters: dict | None = None
) -> dict:
    """Keep source-object coverage distinct from per-object schema coverage."""
    fields = (
        "source",
        "object_type",
        "catalog",
        "schema",
        "object",
        "source_operation",
    )
    return {
        "kind": kind,
        "filters": filters or {},
        "objects": sorted({tuple(row[field] for field in fields) for row in rows}),
    }


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def write_probe_status(
    path: Path | None,
    *,
    source: str,
    probe: str,
    row_count: int,
    issues: Iterable[str] = (),
    output_path: Path | None = None,
    scope: dict | None = None,
    details: dict | None = None,
) -> str:
    issue_list = [str(issue) for issue in issues]
    status = "partial" if issue_list else "complete"
    if path is None:
        return status
    if output_path is not None and path.resolve() == output_path.resolve():
        raise ValueError("Probe status output must be distinct from observation output")
    payload = {
        "source": source,
        "probe": probe,
        "status": status,
        "row_count": row_count,
        "issues": issue_list,
        "finished_at": utc_now(),
    }
    if output_path is not None:
        payload["output_sha256"] = file_sha256(output_path)
    if scope is not None:
        payload["scope"] = scope
    if details is not None:
        payload["details"] = details
    atomic_write_json(path, payload)
    return status
