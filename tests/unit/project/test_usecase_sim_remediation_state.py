"""Exact state-contract and import regressions without fixture preparation."""

import importlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
sys.path.insert(0, str(SIM / "scripts"))


def test_state_preparation_import_does_not_reset_or_write():
    sys.modules.pop("features.metadata_artifacts.prepare_state_contract", None)
    with (
        patch("shutil.rmtree") as reset,
        patch("polars.DataFrame.write_delta") as write,
    ):
        module = importlib.import_module(
            "features.metadata_artifacts.prepare_state_contract"
        )
        assert callable(module.main)
    reset.assert_not_called()
    write.assert_not_called()


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [(101, 10.0, "alpha")],
        [(101, 11.0, "alpha"), (102, 20.0, "beta"), (103, 30.0, "gamma")],
        [
            (101, 10.0, "alpha"),
            (102, 20.0, "beta"),
            (103, 30.0, "gamma"),
            (104, 40.0, "extra"),
        ],
    ],
)
def test_state_oracle_rejects_wrong_rows(rows):
    from features.metadata_artifacts.validate_state_paths import assert_state_values

    with pytest.raises(AssertionError, match="rows mismatch"):
        assert_state_values(rows, {"order_id": 103})


@pytest.mark.parametrize(
    "checkpoint", [{}, {"order_id": 102}, {"order_id": "103"}, {"other": 103}]
)
def test_state_oracle_rejects_wrong_checkpoint(checkpoint):
    from features.metadata_artifacts.validate_state_paths import (
        assert_state_values,
        EXPECTED_ROWS,
    )

    with pytest.raises(AssertionError, match="checkpoint mismatch"):
        assert_state_values(EXPECTED_ROWS, checkpoint)


def test_state_oracle_accepts_only_exact_values():
    from features.metadata_artifacts.validate_state_paths import (
        assert_state_values,
        EXPECTED_ROWS,
    )

    assert_state_values(EXPECTED_ROWS, {"order_id": 103})


@pytest.mark.parametrize("session", [None, "historical"])
def test_state_paths_reject_missing_or_wrong_session(tmp_path, session):
    from features.metadata_artifacts import validate_state_paths as module

    with pytest.raises(AssertionError, match="session|snapshot"):
        module.validate(
            tmp_path / "state",
            output=tmp_path / "output",
            log_root=tmp_path / "logs",
            log_session_id=session,
        )


def test_state_paths_reject_nested_log_root(tmp_path):
    from features.metadata_artifacts import validate_state_paths as module

    with pytest.raises(AssertionError, match="independent"):
        module.validate(
            tmp_path / "state",
            output=tmp_path / "output",
            log_root=tmp_path / "state/logs",
            log_session_id="current",
        )
