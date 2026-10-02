"""Validate the small, local record produced by an upload adapter.

This intentionally validates observations of local commands only.  Artifact
integrity belongs to ``dc validate``; remote integrity and activation are not
claimed by this record.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any


_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_PHASES = ("artifact", "current")
_PHASE_STATUS = {"success", "failed", "skipped"}
_STATUS = {"success", "partial_failure", "failed"}


def validate_record(value: Any, *, require_success: bool = False) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("Upload record must be a JSON object")
    required = {
        "schema_version",
        "release_id",
        "build_id",
        "environment",
        "deployment_path",
        "source",
        "status",
        "uploads",
    }
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"Upload record is missing: {', '.join(missing)}")
    if value["schema_version"] != 1:
        raise ValueError("Unsupported upload record schema_version")
    for field in ("release_id", "build_id"):
        if not isinstance(value[field], str) or not _ID.fullmatch(value[field]):
            raise ValueError(f"Invalid {field}")
    for field in ("environment", "deployment_path", "source"):
        if not isinstance(value[field], str) or not value[field].strip():
            raise ValueError(f"{field} must be a non-empty string")
    if value["status"] not in _STATUS:
        raise ValueError("Invalid upload record status")
    uploads = value["uploads"]
    if not isinstance(uploads, dict):
        raise ValueError("uploads must be an object")
    for phase in _PHASES:
        entry = uploads.get(phase)
        if not isinstance(entry, dict):
            raise ValueError(f"uploads.{phase} must be an object")
        if entry.get("status") not in _PHASE_STATUS:
            raise ValueError(f"Invalid uploads.{phase}.status")
        files = entry.get("files")
        if not isinstance(files, int) or isinstance(files, bool) or files < 0:
            raise ValueError(f"uploads.{phase}.files must be a non-negative integer")
    artifact_status = uploads["artifact"]["status"]
    current_status = uploads["current"]["status"]
    if artifact_status == "skipped":
        raise ValueError("Artifact upload cannot be skipped")
    if artifact_status == "success" and current_status == "skipped":
        raise ValueError("Current upload cannot be skipped after artifact upload succeeds")
    if artifact_status != "success" and current_status != "skipped":
        raise ValueError("Current upload must be skipped when artifact upload fails")
    expected = (
        "success"
        if artifact_status == current_status == "success"
        else "failed"
        if artifact_status == "failed"
        else "partial_failure"
    )
    if value["status"] != expected:
        raise ValueError(
            f"status {value['status']!r} does not match upload phases; expected {expected!r}"
        )
    if require_success and value["status"] != "success":
        raise ValueError("Upload record is not successful")
    return {
        "ok": True,
        "schema_version": 1,
        "release_id": value["release_id"],
        "build_id": value["build_id"],
        "environment": value["environment"],
        "status": value["status"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record", type=Path)
    parser.add_argument("--require-success", action="store_true")
    args = parser.parse_args(argv)
    try:
        value = json.loads(args.record.read_text(encoding="utf-8"))
        result = validate_record(value, require_success=args.require_success)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
