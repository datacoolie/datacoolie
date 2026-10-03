"""Exercise the Discover scripts as subprocesses against local fixtures."""
from __future__ import annotations

import csv
import io
import subprocess
import sys
from pathlib import Path

import pytest

from _observation_contract import CSV_HEADER, validate_observations

TESTS = Path(__file__).resolve().parents[2]
SCRIPTS = TESTS.parent / "datacoolie-discover/scripts"


@pytest.mark.parametrize("filename,table,extra", [
    ("sales.parquet", "sales", []),
    ("products.csv", "products", []),
    ("delta_products", "delta", ["--format", "delta"]),
])
def test_file_schema_cli(filename, table, extra):
    output = _run("introspect_files.py", "schema", "--path",
                  str(TESTS / "fixtures/files" / filename), "--source", "fixture",
                  "--table", table, *extra)
    rows = _observations(output, "fixture")
    assert {row["object"] for row in rows} == {table}


def _run(script: str, *args: str) -> str:
    result = subprocess.run([sys.executable, str(SCRIPTS / script), *args],
                            capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode == 0, result.stderr
    return result.stdout


def _observations(output: str, source: str):
    reader = csv.DictReader(io.StringIO(output))
    assert reader.fieldnames == CSV_HEADER
    rows = validate_observations(reader)
    assert rows
    assert {row["source"] for row in rows} == {source}
    return rows


def test_structure_cli():
    output = _run("introspect_files.py", "structure", "--path",
                  str(TESTS / "fixtures/files"), "--source", "fixture")
    assert "# Folder Structure" in output
    assert "Delta table" in output


def test_openapi_cli():
    rows = _observations(_run("introspect_api.py", "--spec",
                              str(TESTS / "fixtures/api/openapi-petstore.json"),
                              "--source", "petstore"), "petstore")
    assert len(rows) >= 5
