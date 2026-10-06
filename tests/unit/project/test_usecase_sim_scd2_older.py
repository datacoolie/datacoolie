"""Mutation checks for the older-effective SCD2 simulator oracle."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
FEATURES_ROOT = PRODUCT_ROOT / "usecase-sim" / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from scd2.validate_scd2_older_effective import (  # noqa: E402
    assert_older_effective_history,
)


def _expected_history() -> list[dict[str, object]]:
    effective_old = datetime(2024, 1, 1)
    effective_control = datetime(2024, 1, 15)
    effective_new = datetime(2024, 2, 1)
    return [
        {
            "id": 1,
            "value": "new-version",
            "modified_at": effective_new,
            "__valid_from": effective_new,
            "__valid_to": None,
            "__is_current": True,
        },
        {
            "id": 1,
            "value": "late-version",
            "modified_at": effective_old,
            "__valid_from": effective_old,
            "__valid_to": None,
            "__is_current": True,
        },
        {
            "id": 2,
            "value": "untouched",
            "modified_at": effective_control,
            "__valid_from": effective_control,
            "__valid_to": None,
            "__is_current": True,
        },
    ]


@pytest.mark.parametrize("mutation", ("flag", "boundary", "value", "missing_boundary"))
def test_older_effective_oracle_rejects_mutation(mutation: str) -> None:
    expected = _expected_history()
    mutated = [dict(row) for row in expected]

    if mutation == "flag":
        mutated[1]["__is_current"] = False
    elif mutation == "boundary":
        mutated[0]["__valid_to"] = datetime(2024, 1, 1)
    elif mutation == "missing_boundary":
        del mutated[0]["__valid_to"]
    else:
        mutated[1]["value"] = "wrong-value"

    with pytest.raises(AssertionError, match=f"older-effective-{mutation}"):
        assert_older_effective_history(
            mutated,
            expected,
            label=f"older-effective-{mutation}",
        )


def test_older_effective_oracle_requires_both_current_rows() -> None:
    expected = _expected_history()
    mutated = [row for index, row in enumerate(expected) if index != 1]

    with pytest.raises(AssertionError, match="older-effective-count"):
        assert_older_effective_history(
            mutated,
            expected,
            label="older-effective-count",
        )
