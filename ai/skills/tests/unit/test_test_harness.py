"""Tests for the AI-skill test orchestrator and behavioral evidence gate."""

from __future__ import annotations

import importlib.util
import hashlib
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
        "unit/test_release_upload.py",
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
        "eval_schema_version": 2,
        "case_kinds": {"decision": [1], "execution": []},
        "capability_families": {"safety": [1]},
        "evals": [
            {
                "id": 1,
                "prompt": "Do the safe thing.",
                "expected_output": "Fails closed.",
                "files": [],
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
                "eval_schema_version": 2,
                "case_kinds": {"decision": [1], "execution": []},
                "capability_families": {"safety": [1]},
                "evals": [
                    {
                        "id": 1,
                        "prompt": "Prompt",
                        "expected_output": "Expected",
                        "files": [],
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


def test_eval_catalog_requires_declared_schema_version(tmp_path: Path) -> None:
    verifier = _load_script("verify_behavioral_evidence.py")
    skill = tmp_path / "datacoolie-example"
    (skill / "evals").mkdir(parents=True)
    (skill / "SKILL.md").write_text("example\n", encoding="utf-8")
    (skill / "evals/evals.json").write_text(json.dumps({
        "skill_name": skill.name,
        "evals": [{
            "id": 1,
            "prompt": "Prompt",
            "expected_output": "Expected",
            "files": [],
            "expectations": ["One", "Two"],
        }],
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="must declare eval_schema_version 2"):
        verifier._eval_definitions(skill)


def _versioned_eval_skill(tmp_path: Path) -> tuple[object, Path, Path]:
    verifier = _load_script("verify_behavioral_evidence.py")
    repository = tmp_path / "repository"
    skill = repository / "ai/skills/datacoolie-example"
    (skill / "evals").mkdir(parents=True)
    (skill / "SKILL.md").write_text("example\n", encoding="utf-8")
    fixture = repository / "fixture.txt"
    fixture.write_text("observable\n", encoding="utf-8")
    document = {
        "skill_name": skill.name,
        "eval_schema_version": 2,
        "case_kinds": {"decision": [1], "execution": [2]},
        "capability_families": {"routing": [1], "execution-proof": [2]},
        "evals": [
            {
                "id": 1, "prompt": "Choose a route.", "expected_output": "Choose safely.",
                "files": [], "expectations": ["Names the route", "Does not claim execution"],
            },
            {
                "id": 2, "prompt": "Run the fixture check.",
                "expected_output": "Run a command and retain output evidence and exit code.",
                "files": ["../../../fixture.txt"],
                "expectations": ["Reports the command", "Reports observed exit code"],
            },
        ],
    }
    path = skill / "evals/evals.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return verifier, skill, path


def test_versioned_eval_catalog_accepts_complete_kind_and_capability_partitions(
    tmp_path: Path,
) -> None:
    verifier, skill, _ = _versioned_eval_skill(tmp_path)
    _, document, cases = verifier._eval_definitions(skill)

    assert document["eval_schema_version"] == 2
    assert len(cases) == 2


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda value: value["evals"].append(dict(value["evals"][0])), "ids must be unique"),
        (lambda value: value["case_kinds"].update({"other": []}), "exactly decision and execution"),
        (lambda value: value["capability_families"]["routing"].append(2), "exactly once"),
        (lambda value: value["evals"][0]["files"].append("../../../fixture.txt"), "Decision evals"),
        (lambda value: value["evals"][1]["files"].clear(), "Execution evals require"),
        (lambda value: value["evals"][1]["files"].__setitem__(0, "../../../missing.txt"), "does not exist"),
        (lambda value: value["evals"][1]["files"].append("../../../fixture.txt"), "paths must be unique"),
        (lambda value: value["evals"][1]["files"].__setitem__(0, "C:/fixture.txt"), "relative POSIX paths"),
        (lambda value: value["evals"].reverse(), "ascending id"),
        (lambda value: value["evals"][0].update({"kind": "decision"}), "unknown fields"),
        (lambda value: value["evals"][1].update({"expected_output": "No observable proof"}), "observable command"),
    ],
)
def test_versioned_eval_catalog_rejects_invalid_definitions(
    tmp_path: Path, mutate, message: str
) -> None:
    verifier, skill, path = _versioned_eval_skill(tmp_path)
    document = json.loads(path.read_text(encoding="utf-8"))
    mutate(document)
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        verifier._eval_definitions(skill)


@pytest.fixture
def original_gradings(tmp_path: Path):
    verifier = _load_script("verify_behavioral_evidence.py")
    skill = tmp_path / "datacoolie-example"
    (skill / "evals").mkdir(parents=True)
    (skill / "SKILL.md").write_text("example\n", encoding="utf-8")
    cases, paths = [], []
    for case_id in (1, 2):
        # Shared expectations ensure order checking also binds the actual artifact bytes.
        cases.append({
            "id": case_id, "prompt": f"Prompt {case_id}",
            "expected_output": "Expected", "files": [], "expectations": ["One", "Two"],
        })
        path = tmp_path / f"grading-{case_id}.json"
        path.write_text(json.dumps({
            "expectations": [
                {"text": text, "passed": True, "evidence": f"Observed {text}: {case_id}"}
                for text in ("One", "Two")
            ],
            "summary": {"passed": 2, "failed": 0, "total": 2},
        }), encoding="utf-8")
        paths.append(path)
    (skill / "evals/evals.json").write_text(
        json.dumps({
            "skill_name": skill.name,
            "eval_schema_version": 2,
            "case_kinds": {"decision": [1, 2], "execution": []},
            "capability_families": {"evidence": [1, 2]},
            "evals": cases,
        }), encoding="utf-8"
    )
    return verifier, skill, paths, verifier.build_evidence(skill, paths)


def test_original_gradings_accept_genuine_v1_and_legacy_calls(original_gradings) -> None:
    verifier, skill, paths, evidence = original_gradings
    evidence = json.loads(json.dumps(evidence))
    assert evidence["schema_version"] == 1
    assert set(evidence) == {
        "schema_version", "artifact_type", "skill_name", "skill_sha256", "evals_sha256",
        "evaluated_at", "results",
    }
    verifier.validate_evidence(skill, evidence)
    verifier.validate_evidence(skill, evidence, grading_paths=None)
    verifier.validate_evidence(skill, evidence, grading_paths=iter(paths))


@pytest.mark.parametrize("tamper", ["zero-hash", "forged-proof"])
def test_original_gradings_reject_forged_receipt(original_gradings, tamper: str) -> None:
    verifier, skill, paths, evidence = original_gradings
    if tamper == "zero-hash":
        evidence["results"][0]["grading_sha256"] = "0" * 64
    else:
        evidence["results"][0]["expectations"][0]["evidence"] = "Fabricated proof"
    # Legacy integrity checks cannot establish where a hash or evidence claim came from.
    verifier.validate_evidence(skill, evidence)
    with pytest.raises(ValueError, match="original grading mismatch"):
        verifier.validate_evidence(skill, evidence, grading_paths=paths)


@pytest.mark.parametrize("tamper", ["whitespace", "evidence"])
def test_original_gradings_reject_altered_artifact(original_gradings, tamper: str) -> None:
    verifier, skill, paths, evidence = original_gradings
    content = paths[0].read_text(encoding="utf-8")
    if tamper == "whitespace":
        content += "\n"
    else:
        grading = json.loads(content)
        grading["expectations"][0]["evidence"] = "Changed observation"
        content = json.dumps(grading)
    paths[0].write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match="original grading mismatch"):
        verifier.validate_evidence(skill, evidence, grading_paths=paths)


@pytest.mark.parametrize("tamper, message", [
    ("failed", "must pass every expectation"),
    ("summary", "summary does not match"),
    ("expectations", "expectations do not bind"),
    ("malformed-item", "expectations do not bind"),
    ("malformed-summary", "summary does not match"),
])
def test_original_gradings_revalidate_even_with_matching_digest(
    original_gradings, tamper: str, message: str
) -> None:
    verifier, skill, paths, evidence = original_gradings
    grading = json.loads(paths[0].read_text(encoding="utf-8"))
    if tamper == "failed":
        grading["expectations"][0]["passed"] = False
    elif tamper == "summary":
        grading["summary"]["total"] = 3
    elif tamper == "expectations":
        grading["expectations"][0]["text"] = "Unrelated expectation"
    elif tamper == "malformed-item":
        grading["expectations"][0] = None
    else:
        grading["summary"] = None
    paths[0].write_text(json.dumps(grading), encoding="utf-8")
    evidence["results"][0]["grading_sha256"] = hashlib.sha256(paths[0].read_bytes()).hexdigest()
    verifier.validate_evidence(skill, evidence)
    with pytest.raises(ValueError, match=message):
        verifier.validate_evidence(skill, evidence, grading_paths=paths)


@pytest.mark.parametrize("selection", [[], [0], [0, 1, 0], [1, 0], [0, 0]])
def test_original_gradings_reject_count_order_and_duplicate_mismatches(
    original_gradings, selection: list[int]
) -> None:
    verifier, skill, paths, evidence = original_gradings
    message = "Expected 2 grading files" if len(selection) != 2 else "original grading mismatch"
    with pytest.raises(ValueError, match=message):
        verifier.validate_evidence(skill, evidence, grading_paths=[paths[i] for i in selection])


@pytest.mark.parametrize("mode", ["legacy", "originals", "missing", "forged", "empty-flag"])
def test_behavioral_evidence_cli_modes(
    original_gradings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], mode: str,
) -> None:
    verifier, skill, paths, _ = original_gradings
    receipt = tmp_path / "evidence.json"
    monkeypatch.setattr(sys, "argv", [
        "verify_behavioral_evidence.py", "create", str(skill), str(receipt), *map(str, paths),
    ])
    assert verifier.main() == 0
    capsys.readouterr()
    if mode == "forged":
        evidence = json.loads(receipt.read_text(encoding="utf-8"))
        evidence["results"][0]["grading_sha256"] = "0" * 64
        receipt.write_text(json.dumps(evidence), encoding="utf-8")
    argv = ["verify_behavioral_evidence.py", "verify", str(skill), str(receipt)]
    if mode != "legacy":
        argv += ["--gradings"]
        if mode != "empty-flag":
            supplied = [tmp_path / "absent.json", paths[1]] if mode == "missing" else paths
            argv += list(map(str, supplied))
    monkeypatch.setattr(sys, "argv", argv)
    if mode == "empty-flag":
        with pytest.raises(SystemExit) as exc:
            verifier.main()
        assert exc.value.code == 2
    elif mode in {"missing", "forged"}:
        assert verifier.main() == 1
        output = capsys.readouterr()
        assert "ERROR:" in output.err
        assert "Verified" not in output.out
    else:
        assert verifier.main() == 0
        output = capsys.readouterr().out
        assert ("integrity-only" if mode == "legacy" else "supplied original gradings checked") in output
        assert "Neither mode authenticates execution or evaluator identity" in output
