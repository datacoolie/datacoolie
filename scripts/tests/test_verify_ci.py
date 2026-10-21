"""Focused tests for the CI-only shadow wrapper."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import scripts.verify_ci as verify_ci


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _repo_with_change(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--initial-branch", "main")
    _git(repo, "config", "user.name", "verify-ci-test")
    _git(repo, "config", "user.email", "verify-ci@example.invalid")
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "src/datacoolie/transformers").mkdir(parents=True)
    (repo / "src/datacoolie/transformers/row_filter.py").write_text("changed\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "change")
    head = _git(repo, "rev-parse", "HEAD")
    return repo, base, head


def test_pr_environment_reads_event_shas_and_tested_checkout(tmp_path: Path) -> None:
    repo, base, head = _repo_with_change(tmp_path)
    event = tmp_path / "event.json"
    event.write_text(
        json.dumps({"pull_request": {"base": {"sha": base}, "head": {"sha": head}}}),
        encoding="utf-8",
    )
    selection, scope = verify_ci.selection_for_environment(
        repo,
        environment={
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_SHA": head,
        },
    )

    assert scope == "pr"
    assert selection.selection == "narrow"
    assert selection.git is not None
    assert selection.git.actual_head == head


def test_main_and_manual_style_events_are_always_full() -> None:
    for event_name in ("push", "workflow_dispatch", ""):
        selection, scope = verify_ci.selection_for_environment(
            Path.cwd(), environment={"GITHUB_EVENT_NAME": event_name}
        )
        assert scope == "full"
        assert selection.selection == "full"
        assert selection.fallback is False


def test_direct_wrapper_help_does_not_require_package_import_setup() -> None:
    script = Path(__file__).resolve().parents[1] / "verify_ci.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=script.parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "shadow selector" in result.stdout


def test_release_gate_exit_code_is_propagated_and_receipt_finalized(
    tmp_path: Path,
    monkeypatch,
) -> None:
    receipt = tmp_path / "receipt.json"
    monkeypatch.setattr(verify_ci, "run_release_gate", lambda _repo: 17)

    result = verify_ci.main(
        ["--repo", str(tmp_path), "--receipt", str(receipt)],
        environment={"GITHUB_EVENT_NAME": "push"},
    )

    assert result == 17
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    assert payload["run_scope"] == "full"
    assert payload["release_gate"] == "scripts/verify_release.py"
    assert payload["release_gate_exit_code"] == 17


def test_run_release_gate_uses_existing_script_without_extra_flags(tmp_path: Path, monkeypatch) -> None:
    calls: list[tuple[list[str], Path]] = []

    class Completed:
        returncode = 3

    def fake_run(command, *, cwd, check):
        calls.append((command, cwd))
        assert check is False
        return Completed()

    monkeypatch.setattr(verify_ci.subprocess, "run", fake_run)
    assert verify_ci.run_release_gate(tmp_path) == 3
    assert calls == [([sys.executable, str(tmp_path / "scripts" / "verify_release.py")], tmp_path)]
