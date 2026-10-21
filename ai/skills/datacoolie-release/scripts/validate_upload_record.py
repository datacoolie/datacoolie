"""Validate the small, local record produced by an upload adapter.

This intentionally validates observations of local commands only.  Artifact
integrity belongs to ``dc validate``; remote integrity and activation are not
claimed by this record.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


_PHASES = ("artifact", "current")
_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "upload-record.schema.json"


def _validate_schema(value: Any) -> None:
    try:
        schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Upload record schema is unavailable") from exc
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(
        validator.iter_errors(value),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        details = []
        for error in errors:
            location = ".".join(str(part) for part in error.absolute_path) or "<root>"
            details.append(f"{location}: {error.message}")
        raise ValueError("Invalid upload record:\n- " + "\n- ".join(details))


def _validate_phase_semantics(
    value: dict[str, Any], *, require_zero_skipped: bool = False
) -> None:
    uploads = value["uploads"]
    if require_zero_skipped and any(
        entry["status"] == "skipped" and entry["files"] != 0
        for entry in uploads.values()
    ):
        raise ValueError("Skipped upload phases must report zero files")
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
def _validate_v1_semantics(value: dict[str, Any]) -> None:
    _validate_phase_semantics(value)


def _validate_v2_semantics(value: dict[str, Any]) -> None:
    # ``record`` describes the adapter's persistence attempt.  The validator
    # has no record path or filesystem transaction to authenticate, so it
    # validates the stage's schema and leaves atomic-write truth to the CLI.
    preflight = value["preflight"]
    if preflight["status"] == "failed":
        if value["status"] != "failed":
            raise ValueError("A failed preflight requires a failed upload record")
        for phase in _PHASES:
            entry = value["uploads"][phase]
            if entry["status"] != "skipped" or entry["files"] != 0:
                raise ValueError("A failed preflight must skip both upload phases")
        _validate_record_stage(value["record"])
        return

    if "error" in preflight:
        raise ValueError("A passed preflight must not contain an error")
    for field in ("release_id", "build_id", "environment", "deployment_path", "source"):
        if value[field] is None:
            raise ValueError(f"{field} is required after preflight succeeds")
    _validate_phase_semantics(value, require_zero_skipped=True)
    _validate_record_stage(value["record"])


def _validate_record_stage(record: dict[str, Any]) -> None:
    status = record["status"]
    if status == "failed" and "error" not in record:
        raise ValueError("A failed record stage requires a diagnostic error")
    if status in {"written", "not_requested"} and "error" in record:
        raise ValueError(f"A {status} record stage must not contain an error")


def validate_record(value: Any, *, require_success: bool = False) -> dict[str, Any]:
    # JSON Schema is the sole owner of record shape and types.  The functions
    # below only inspect relationships between already validated fields.
    _validate_schema(value)
    version = value["schema_version"]
    if version == 1:
        _validate_v1_semantics(value)
    elif version == 2:
        _validate_v2_semantics(value)
    else:  # guarded by the schema, retained as a defensive boundary
        raise ValueError("Unsupported upload record schema_version")
    if require_success and value["status"] != "success":
        raise ValueError("Upload record is not successful")
    return {
        "ok": True,
        "schema_version": version,
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
