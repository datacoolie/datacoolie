from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from datacoolie.project.validation.metadata import validate_metadata_document


# Keep subprocesses rooted at this checkout so the command tests exercise the
# product source tree rather than an incidental editable installation.
PRODUCT_ROOT = Path(__file__).resolve().parents[3]


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "datacoolie", "--format", "json", *args],
        cwd=PRODUCT_ROOT,
        env={**os.environ, "PYTHONPATH": str(PRODUCT_ROOT / "src")},
        check=False,
        capture_output=True,
        text=True,
    )


def test_cli_subprocess_imports_this_checkout() -> None:
    result = subprocess.run(
        [sys.executable, "-c", "import datacoolie; print(datacoolie.__file__)"],
        cwd=PRODUCT_ROOT,
        env={**os.environ, "PYTHONPATH": str(PRODUCT_ROOT / "src")},
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert Path(result.stdout.strip()).resolve().is_relative_to(PRODUCT_ROOT / "src")


def test_cli_metadata_convert_round_trips_json_to_yaml(tmp_path: Path) -> None:
    source = tmp_path / "metadata.json"
    target = tmp_path / "metadata.yml"
    source.write_text(
        json.dumps({"connections": [], "dataflows": [], "schema_hints": []}),
        encoding="utf-8",
    )
    result = _run(
        "metadata",
        "convert",
        "--input",
        str(source),
        "--output",
        str(target),
        "--to",
        "yaml",
    )
    assert result.returncode == 0, result.stderr
    assert target.is_file()
    assert "connections" in target.read_text(encoding="utf-8")


def test_cli_metadata_convert_requires_overwrite(tmp_path: Path) -> None:
    source = tmp_path / "metadata.json"
    target = tmp_path / "metadata.json"
    source.write_text(json.dumps({"connections": []}), encoding="utf-8")
    result = _run(
        "metadata",
        "convert",
        "--input",
        str(source),
        "--output",
        str(target),
    )
    assert result.returncode != 0
    assert "overwrite" in result.stdout


def test_cli_metadata_convert_rejects_missing_input(tmp_path: Path) -> None:
    result = _run(
        "metadata",
        "convert",
        "--input",
        str(tmp_path / "missing.json"),
        "--output",
        str(tmp_path / "output.json"),
    )
    assert result.returncode != 0
    assert "not found" in result.stdout


def test_validate_metadata_reports_schema_errors_before_model_construction() -> None:
    report = validate_metadata_document(
        {"connections": "not-an-array", "dataflows": []},
        source="metadata.json",
    )
    assert report.ok is False
    assert any(item.code == "metadata.schema" for item in report.errors)


def test_cli_validation_uses_framework_service_and_json_envelope(tmp_path: Path) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text(
        json.dumps({"connections": "not-an-array", "dataflows": []}),
        encoding="utf-8",
    )
    result = _run("validate", "--metadata-path", str(invalid))
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "validation.failed"


def test_cli_validation_distinguishes_missing_input(tmp_path: Path) -> None:
    result = _run(
        "validate",
        "--metadata-path",
        str(tmp_path / "missing.json"),
    )
    assert result.returncode != 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
