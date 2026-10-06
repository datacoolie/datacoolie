"""Mutation checks for the focused SQL-root precedence oracle."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
FEATURES_ROOT = PRODUCT_ROOT / "usecase-sim" / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from metadata_artifacts.validate_sql_root_precedence import (  # noqa: E402
    ARTIFACT_ROOT_ROWS,
    EXPECTED_SQL_ROWS,
    assert_sql_root_precedence,
)


def test_sql_root_oracle_accepts_exact_marker_ids_and_values() -> None:
    assert_sql_root_precedence(EXPECTED_SQL_ROWS)


@pytest.mark.parametrize("mutation", ["wrong_root", "marker_id", "value", "missing_row"])
def test_sql_root_oracle_rejects_wrong_root_or_mutated_output(mutation: str) -> None:
    if mutation == "wrong_root":
        mutated = [dict(row) for row in ARTIFACT_ROOT_ROWS]
    else:
        mutated = [dict(row) for row in EXPECTED_SQL_ROWS]
        if mutation == "marker_id":
            mutated[0]["marker_id"] = 9701
        elif mutation == "value":
            mutated[1]["amount"] = 999.0
        else:
            mutated.pop()

    with pytest.raises(AssertionError, match="sql-root-precedence"):
        assert_sql_root_precedence(mutated, label=f"sql-root-precedence-{mutation}")
