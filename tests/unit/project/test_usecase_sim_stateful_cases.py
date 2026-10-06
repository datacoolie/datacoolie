"""Mutation tests for the focused native stateful case oracles."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
FEATURES_ROOT = PRODUCT_ROOT / "usecase-sim" / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from _stateful_support import assert_exact_rows  # noqa: E402
from maintenance.validate_maintenance import assert_maintenance_operations  # noqa: E402
from scd2.validate_scd2 import assert_scd2_history  # noqa: E402


def test_exact_rows_rejects_missing_id() -> None:
    with pytest.raises(AssertionError, match="missing-id"):
        assert_exact_rows(
            [{"id": 1, "value": "one"}],
            [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
            key="id",
            label="missing-id",
        )


def test_scd2_history_rejects_wrong_version_boundary() -> None:
    expected = [
        {
            "id": 1,
            "value": "old",
            "modified_at": datetime(2024, 1, 1),
            "__valid_from": datetime(2024, 1, 1),
            "__valid_to": datetime(2024, 2, 1),
            "__is_current": False,
        }
    ]
    mutated = [dict(expected[0], __valid_to=None)]
    with pytest.raises(AssertionError, match="wrong-version"):
        assert_scd2_history(mutated, expected, label="wrong-version")


def test_maintenance_oracle_rejects_skipped_operation() -> None:
    with pytest.raises(AssertionError, match="skipped-operation"):
        assert_maintenance_operations(
            [{"operation": "compact", "status": "skipped"}],
            {"compact": "succeeded"},
            label="skipped-operation",
        )


def test_missing_target_operation_owner_accepts_explicit_skips() -> None:
    assert_maintenance_operations(
        [
            {"operation": "compact", "status": "skipped"},
            {"operation": "cleanup", "status": "skipped"},
        ],
        {"compact": "skipped", "cleanup": "skipped"},
        label="missing-target-owner",
    )
