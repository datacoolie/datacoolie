"""Artifact result oracles reject stale, incomplete and altered business data."""

import importlib.util
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _validator():
    path = Path(__file__).resolve().parents[3] / "usecase-sim/scripts/features/metadata_artifacts/validate_artifact_output.py"
    spec = importlib.util.spec_from_file_location("artifact_oracle", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("mutation", ["missing_id", "wrong_value", "duplicate", "valid"])
def test_artifact_business_rows(tmp_path, monkeypatch, mutation):
    module = _validator()
    monkeypatch.setattr(module, "DEFAULT_ROOT", tmp_path)
    rows = [
        {"order_id": 101, "amount": 10.0, "category": "alpha"},
        {"order_id": 102, "amount": 20.0, "category": "beta"},
        {"order_id": 103, "amount": 30.0, "category": "gamma"},
    ]
    if mutation == "missing_id":
        rows.pop()
    elif mutation == "wrong_value":
        rows[1]["amount"] = 200.0
    elif mutation == "duplicate":
        rows.append(rows[-1])
    output = tmp_path / "result"
    output.mkdir()
    pq.write_table(pa.Table.from_pylist(rows), output / "part.parquet")
    if mutation == "valid":
        module.validate(str(output))
    else:
        with pytest.raises(AssertionError, match="query result differs"):
            module.validate(str(output))
