"""Tests for the AI-skill test orchestrator and behavioral evidence gate."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


TESTS_DIR = Path(__file__).parents[1]


def _load_script(name: str):
    path = TESTS_DIR / name
    spec = importlib.util.spec_from_file_location(f"test_harness_{path.stem}", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_skill_selection_collects_only_shared_and_owned_unit_modules() -> None:
    runner = _load_script("run_all.py")

    targets = runner._unit_targets(["release"])

    assert targets == [
        "unit/test_ai_workflow_contract.py",
        "unit/test_test_harness.py",
        "unit/test_release_receipt.py",
    ]
    assert runner._unit_targets([]) == ["unit"]


def test_integration_requires_discover_and_has_explicit_child_environment() -> None:
    runner = _load_script("run_all.py")

    with pytest.raises(ValueError, match="requires the discover validator"):
        runner._validate_selection(["release"], integration=True)

    env = runner._integration_environment({"PATH": "test-path"})
    assert env["PATH"] == "test-path"
    assert set(key for key in env if key.startswith("DATACOOLIE_TEST_")) == {
        "DATACOOLIE_TEST_POSTGRES_URL",
        "DATACOOLIE_TEST_MYSQL_URL",
        "DATACOOLIE_TEST_MSSQL_URL",
    }


def test_integration_cleanup_runs_when_seed_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _load_script("run_all.py")
    stopped: list[bool] = []
    monkeypatch.setattr(sys, "argv", ["run_all.py", "discover", "--integration"])
    monkeypatch.setattr(runner, "_run", lambda *args, **kwargs: 0)
    monkeypatch.setattr(runner, "_start_integration_services", lambda: 0)
    monkeypatch.setattr(runner, "_seed_integration_services", lambda: 1)
    monkeypatch.setattr(
        runner, "_stop_integration_services", lambda: stopped.append(True) or 0
    )

    assert runner.main() == 1
    assert stopped == [True]


def test_integration_cleanup_runs_when_start_is_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _load_script("run_all.py")
    stopped: list[bool] = []
    monkeypatch.setattr(sys, "argv", ["run_all.py", "discover", "--integration"])
    monkeypatch.setattr(runner, "_run", lambda *args, **kwargs: 0)
    monkeypatch.setattr(runner, "_start_integration_services", lambda: 1)
    monkeypatch.setattr(
        runner, "_stop_integration_services", lambda: stopped.append(True) or 0
    )

    assert runner.main() == 1
    assert stopped == [True]


def test_behavioral_evidence_binds_skill_and_eval_bytes(tmp_path: Path) -> None:
    verifier = _load_script("verify_behavioral_evidence.py")
    skill = tmp_path / "datacoolie-example"
    (skill / "evals").mkdir(parents=True)
    (skill / "SKILL.md").write_text("example skill\n", encoding="utf-8")
    evals = {
        "skill_name": "datacoolie-example",
        "evals": [
            {
                "id": 1,
                "name": "safe-case",
                "prompt": "Do the safe thing.",
                "expected_output": "Fails closed.",
                "expectations": ["No mutation", "Reports evidence"],
            }
        ],
    }
    (skill / "evals/evals.json").write_text(json.dumps(evals), encoding="utf-8")
    grading = tmp_path / "grading.json"
    grading.write_text(
        json.dumps(
            {
                "expectations": [
                    {"text": "No mutation", "passed": True, "evidence": "Stopped."},
                    {"text": "Reports evidence", "passed": True, "evidence": "Reported."},
                ],
                "summary": {"passed": 2, "failed": 0, "total": 2, "pass_rate": 1.0},
            }
        ),
        encoding="utf-8",
    )
    evidence = verifier.build_evidence(skill, [grading])

    verifier.validate_evidence(skill, evidence)
    evidence["evaluated_at"] = "not-a-timestampZ"
    with pytest.raises(ValueError, match="valid UTC"):
        verifier.validate_evidence(skill, evidence)
    evidence = verifier.build_evidence(skill, [grading])
    evidence["results"][0]["expectations"][0]["passed"] = False
    with pytest.raises(ValueError, match="failed or unevidenced"):
        verifier.validate_evidence(skill, evidence)
    evidence = verifier.build_evidence(skill, [grading])
    (skill / "SKILL.md").write_text("changed skill\n", encoding="utf-8")
    with pytest.raises(ValueError, match="skill digest"):
        verifier.validate_evidence(skill, evidence)


def test_behavioral_evidence_rejects_failed_or_unbound_grading(tmp_path: Path) -> None:
    verifier = _load_script("verify_behavioral_evidence.py")
    skill = tmp_path / "datacoolie-example"
    (skill / "evals").mkdir(parents=True)
    (skill / "SKILL.md").write_text("example\n", encoding="utf-8")
    (skill / "evals/evals.json").write_text(
        json.dumps(
            {
                "skill_name": "datacoolie-example",
                "evals": [
                    {
                        "id": 1,
                        "name": "case",
                        "prompt": "Prompt",
                        "expected_output": "Expected",
                        "expectations": ["One", "Two"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    grading = tmp_path / "grading.json"
    grading.write_text(
        json.dumps(
            {
                "expectations": [
                    {"text": "One", "passed": True, "evidence": "ok"},
                    {"text": "Two", "passed": False, "evidence": "failed"},
                ],
                "summary": {"passed": 1, "failed": 1, "total": 2, "pass_rate": 0.5},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must pass every expectation"):
        verifier.build_evidence(skill, [grading])
