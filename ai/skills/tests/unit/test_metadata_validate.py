"""Behavior and CLI exit-code tests for build-owned metadata validation."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import validate as metadata_validate
from _schema_resolver import find_schemas_dir, load_schema


VALIDATE_SCRIPT = Path(metadata_validate.__file__).resolve()


def test_validate_metadata_reports_stable_error_paths() -> None:
    schema = load_schema("0.1.0", find_schemas_dir())

    errors = metadata_validate.validate_metadata(
        {"connections": "not-an-array", "dataflows": []}, schema
    )

    assert errors
    assert errors[0]["path"] == "connections"
    assert errors[0]["message"]
    assert errors[0]["schema_path"]


def test_validate_cli_distinguishes_invalid_metadata_and_missing_input(tmp_path: Path) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text(
        json.dumps({"connections": "not-an-array", "dataflows": []}), encoding="utf-8"
    )

    invalid_run = subprocess.run(
        [sys.executable, str(VALIDATE_SCRIPT), str(invalid), "--quiet"], check=False
    )
    missing_run = subprocess.run(
        [sys.executable, str(VALIDATE_SCRIPT), str(tmp_path / "missing.json"), "--quiet"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert invalid_run.returncode == 1
    assert missing_run.returncode == 2
    assert "File not found" in missing_run.stderr
