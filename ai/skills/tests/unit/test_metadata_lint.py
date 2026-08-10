"""Behavior tests for the build-owned metadata linter."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import lint as metadata_lint


LINT_SCRIPT = Path(metadata_lint.__file__).resolve()


def _metadata(*, load_type: str = "full_load") -> dict[str, object]:
    return {
        "connections": [
            {"name": "source", "is_active": True},
            {"name": "destination", "is_active": True},
        ],
        "dataflows": [
            {
                "name": "orders",
                "stage": "ingest",
                "source": {"connection_name": "source"},
                "destination": {
                    "connection_name": "destination",
                    "load_type": load_type,
                },
            }
        ],
    }


def test_lint_clean_metadata_and_incremental_failures() -> None:
    assert metadata_lint.run_lint(_metadata(), "polars", "dev") == []

    warnings = metadata_lint.run_lint(_metadata(load_type="merge_upsert"), "polars", "prod")

    assert {warning.rule for warning in warnings} == {
        "merge-keys-required",
        "watermark-for-incremental",
    }


def test_lint_cli_exit_codes_distinguish_clean_warning_and_input_error(tmp_path: Path) -> None:
    clean = tmp_path / "clean.json"
    clean.write_text(json.dumps(_metadata()), encoding="utf-8")
    warning = tmp_path / "warning.json"
    warning.write_text(json.dumps(_metadata(load_type="scd2")), encoding="utf-8")

    clean_run = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), str(clean), "--quiet"], check=False
    )
    warning_run = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), str(warning), "--quiet"], check=False
    )
    missing_run = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), str(tmp_path / "missing.json"), "--quiet"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert clean_run.returncode == 0
    assert warning_run.returncode == 1
    assert missing_run.returncode == 2
    assert "File not found" in missing_run.stderr
