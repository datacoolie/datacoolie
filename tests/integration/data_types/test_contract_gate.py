"""Smoke the opt-in qualification contract without opening external services."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.datatype_qualification]

FIXTURE = Path(__file__).parents[2] / "fixtures" / "data_types" / "qualification_cases.json"


def test_enabled_qualification_can_load_independent_cases() -> None:
    cases = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert {case["boundary"] for case in cases} == {
        "source_extraction",
        "weak_input_hint",
        "native_cast",
    }
