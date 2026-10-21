from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

from datacoolie import __version__
from datacoolie.cli.main import main
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


def _read_payload(capsys) -> dict[str, object]:
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 1
    assert payload["datacoolie_version"] == __version__
    return payload


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
    document = {"connections": [], "dataflows": [], "schema_hints": []}
    source = tmp_path / "metadata.json"
    target = tmp_path / "metadata.yml"
    source.write_text(json.dumps(document), encoding="utf-8")
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
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["data"]["status"] == "converted"
    assert payload["data"]["format"] == "yaml"
    assert Path(payload["data"]["input"]).resolve() == source.resolve()
    assert Path(payload["data"]["output"]).resolve() == target.resolve()
    assert target.is_file()
    assert yaml.safe_load(target.read_text(encoding="utf-8")) == document


def test_cli_metadata_convert_requires_overwrite(tmp_path: Path, capsys) -> None:
    source = tmp_path / "metadata.json"
    target = tmp_path / "metadata.json"
    source.write_text(json.dumps({"connections": []}), encoding="utf-8")
    before = source.read_bytes()
    result = main(
        [
            "--format",
            "json",
            "metadata",
            "convert",
            "--input",
            str(source),
            "--output",
            str(target),
        ]
    )
    payload = _read_payload(capsys)
    assert result == 1
    assert payload["ok"] is False
    assert payload["error"]["code"] == "operation.failed"
    assert "overwrite" in payload["error"]["message"]
    assert source.read_bytes() == before


def test_cli_metadata_convert_rejects_missing_input(tmp_path: Path, capsys) -> None:
    output = tmp_path / "output.json"
    result = main(
        [
            "--format",
            "json",
            "metadata",
            "convert",
            "--input",
            str(tmp_path / "missing.json"),
            "--output",
            str(output),
        ]
    )
    payload = _read_payload(capsys)
    assert result == 1
    assert payload["ok"] is False
    assert payload["error"]["code"] == "operation.failed"
    assert "not found" in payload["error"]["message"]
    assert not output.exists()


def test_validate_metadata_reports_schema_errors_before_model_construction() -> None:
    report = validate_metadata_document(
        {"connections": "not-an-array", "dataflows": []},
        source="metadata.json",
    )
    assert report.ok is False
    assert any(item.code == "metadata.schema" for item in report.errors)


def test_cli_validation_uses_framework_service_and_json_envelope(tmp_path: Path, capsys) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text(
        json.dumps({"connections": "not-an-array", "dataflows": []}),
        encoding="utf-8",
    )
    before = invalid.read_bytes()
    result = main(
        [
            "--format",
            "json",
            "validate",
            "--metadata-path",
            str(invalid),
        ]
    )
    payload = _read_payload(capsys)
    assert result == 1
    assert payload["ok"] is False
    assert payload["error"]["code"] == "validation.failed"
    assert set(payload) == {"schema_version", "datacoolie_version", "ok", "data", "error"}
    assert payload["data"] is None
    assert invalid.read_bytes() == before


def test_cli_validation_distinguishes_missing_input(tmp_path: Path, capsys) -> None:
    result = main(
        [
            "--format",
            "json",
            "validate",
            "--metadata-path",
            str(tmp_path / "missing.json"),
        ]
    )
    payload = _read_payload(capsys)
    assert result == 1
    assert payload["ok"] is False
    assert payload["error"]["code"] == "validation.failed"
    assert "does not exist" in payload["error"]["message"]
