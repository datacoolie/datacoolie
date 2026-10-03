"""Discover adapter CLI tests against the seeded source services."""
from __future__ import annotations

import csv
import io
import subprocess
import sys
from pathlib import Path

import pytest

from _observation_contract import CSV_HEADER, validate_observations

pytestmark = pytest.mark.integration
SCRIPTS = Path(__file__).resolve().parents[2] / "datacoolie-discover/scripts"


def _rows(script, args, environment, source, minimum):
    result = subprocess.run([sys.executable, str(SCRIPTS / script), *args],
                            env=environment, capture_output=True, text=True,
                            timeout=30, check=False)
    assert result.returncode == 0, result.stderr
    reader = csv.DictReader(io.StringIO(result.stdout))
    assert reader.fieldnames == CSV_HEADER
    rows = validate_observations(reader)
    assert len(rows) >= minimum
    assert {row["source"] for row in rows} == {source}
    return rows


@pytest.mark.parametrize("name", ["postgres", "mysql", "mssql"])
def test_database_catalog_cli(name, source_environment):
    _rows("introspect_db.py", ["--url-env", f"DATACOOLIE_TEST_{name.upper()}_URL",
                              "--source", name], source_environment, name, 5)


def test_iceberg_catalog_cli(source_environment):
    rows = _rows("introspect_lakehouse.py", ["--iceberg", "http://localhost:8182",
                                           "--source", "iceberg-test"],
                 source_environment, "iceberg-test", 3)
    assert {"transactions", "customers", "daily_revenue", "product_metrics"} <= {
        row["object"] for row in rows
    }
