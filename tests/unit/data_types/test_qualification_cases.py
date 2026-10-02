"""Validate the independently authored runtime qualification contract."""

from __future__ import annotations

import json
from pathlib import Path


FIXTURE = Path(__file__).parents[2] / "fixtures" / "data_types" / "qualification_cases.json"


def test_qualification_case_ids_and_expected_contract_are_explicit() -> None:
    cases = json.loads(FIXTURE.read_text(encoding="utf-8"))
    ids = [case["case_id"] for case in cases]

    assert cases
    assert len(ids) == len(set(ids))
    for case in cases:
        assert case["source_type"]
        assert case["type_system"]
        assert case["expected"]["logical_type"]
        assert set(case["expected"]["formats"]) == {"parquet", "delta", "iceberg"}
        assert "values" in case["expected"]
