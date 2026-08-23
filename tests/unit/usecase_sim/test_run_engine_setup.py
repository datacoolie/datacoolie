"""Tests for the same-process engine setup hook in the unified runner."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
RUNNER_DIR = REPO_ROOT / "usecase-sim" / "runner"
RUN_PATH = RUNNER_DIR / "run.py"
sys.path.insert(0, str(RUNNER_DIR))

SPEC = importlib.util.spec_from_file_location("usecase_sim_run", RUN_PATH)
assert SPEC and SPEC.loader
run = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(run)


class _Engine:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def register_delta_tables(self, path: str, **options: Any) -> list[str]:
        self.calls.append((path, options))
        return [
            "catalog_A.database_A.shared.orders_ambiguous",
            "catalog_A.database_B.shared.orders_ambiguous",
        ]


def test_engine_setup_receives_the_active_engine() -> None:
    engine = _Engine()

    run._run_engine_setup(
        "runner.qualified_sql_setup.register_tables",
        ["--suite", "delta-ambiguity"],
        engine,
    )

    assert len(engine.calls) == 1
    assert engine.calls[0][1] == {
        "logical_prefix": ("catalog_A",),
        "recursive": True,
    }


def test_engine_setup_rejects_modules_outside_usecase_sim() -> None:
    with pytest.raises(ValueError, match="must resolve inside usecase-sim"):
        run._run_engine_setup("pathlib.Path", [], object())


def test_engine_setup_is_optional() -> None:
    run._run_engine_setup(None, [], object())
