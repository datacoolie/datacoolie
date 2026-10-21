"""Focused contract tests for conservative CI test selection."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

import scripts.select_tests as select_module
from scripts.select_tests import (
    COMMON_SENTINEL,
    OPT_IN_EXCLUSIONS,
    ChangedFile,
    GitBinding,
    parse_name_status_z,
    select_changes,
    select_from_git,
    select_paths,
    write_receipt,
)


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--initial-branch", "main")
    _git(repo, "config", "user.name", "selector-test")
    _git(repo, "config", "user.email", "selector@example.invalid")
    return repo


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def test_union_includes_family_consumers_and_common_sentinel() -> None:
    selection = select_paths(
        [
            "src/datacoolie/transformers/row_filter.py",
            "src/datacoolie/platforms/local_platform.py",
        ]
    )

    assert selection.selection == "narrow"
    assert selection.families == ("platforms", "transformers")
    assert "tests/unit/transformers/" in selection.test_roots
    assert "tests/unit/platforms/" in selection.test_roots
    assert COMMON_SENTINEL in selection.test_roots
    assert selection.fallback is False


@pytest.mark.parametrize(
    ("paths", "reason"),
    [
        (["src/datacoolie/new_component.py"], "unknown changed path"),
        ([".pytest_cache/v/cache/lastfailed"], "empty effective diff"),
        ([], "empty diff"),
        (["../escape.py"], "malformed path selection"),
        (["tests/unit/transformers/test_removed.py"], "deleted test"),
    ],
)
def test_unknown_empty_and_deleted_inputs_fall_back_to_full(paths: list[str], reason: str) -> None:
    if reason == "deleted test":
        selection = select_changes(
            [ChangedFile("D", "tests/unit/transformers/test_removed.py", "tests/unit/transformers/test_removed.py")]
        )
    else:
        selection = select_paths(paths)

    assert selection.selection == "full"
    assert selection.test_roots == ("tests/",)
    assert reason in selection.reasons[0]
    assert selection.fallback is True


def test_shared_dependency_and_ci_changes_are_full() -> None:
    for path in ("pyproject.toml", "poetry.lock", ".github/workflows/ci.yml", "scripts/select_tests.py"):
        selection = select_paths([path])
        assert selection.selection == "full", path
        assert "full suite" in selection.reasons[0]


def test_cli_and_public_recipe_tests_have_project_family_owners() -> None:
    cli = select_paths(["tests/unit/cli/test_project_cli.py"])
    recipe = select_paths(["tests/integration/docs/test_public_recipe_execution.py"])

    assert cli.selection == "narrow"
    assert cli.families == ("project-cli-schema",)
    assert "tests/unit/cli/" in cli.test_roots
    assert "tests/integration/docs/" in cli.test_roots
    assert recipe.selection == "narrow"
    assert recipe.families == ("docs-examples",)


def test_nul_parser_preserves_rename_and_delete_paths() -> None:
    changes = parse_name_status_z(
        b"R100\x00src/datacoolie/engines/old.py\x00src/datacoolie/engines/new.py\x00"
        b"D\x00tests/unit/engines/test_old.py\x00"
    )

    assert changes == (
        ChangedFile("R100", "src/datacoolie/engines/old.py", "src/datacoolie/engines/new.py"),
        ChangedFile("D", "tests/unit/engines/test_old.py", "tests/unit/engines/test_old.py"),
    )
    assert changes[0].paths == (
        "src/datacoolie/engines/old.py",
        "src/datacoolie/engines/new.py",
    )


def test_malformed_nul_stream_is_rejected() -> None:
    with pytest.raises(ValueError, match="NUL terminated"):
        parse_name_status_z(b"M\x00src/datacoolie/engines/file.py")


def test_receipt_binds_git_and_reports_opt_in_exclusions(tmp_path: Path) -> None:
    path = tmp_path / "receipt.json"
    selection = select_paths(["src/datacoolie/engines/polars_engine.py"], git=GitBinding("a" * 40, "b" * 40, "c" * 40, "d" * 40, "d" * 40))
    write_receipt(path, selection, event_name="pull_request", run_scope="pr")
    receipt = json.loads(path.read_text(encoding="utf-8"))

    assert receipt["run_scope"] == "pr"
    assert receipt["git"]["tested_head"] == "d" * 40
    assert receipt["git"]["actual_head"] == "d" * 40
    assert "benchmark" in receipt["opt_in_exclusions"]
    assert set(OPT_IN_EXCLUSIONS).issubset(receipt["opt_in_exclusions"])


def test_real_diverged_history_uses_merge_tree_and_binds_tested_head(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    base = _commit(repo, "base")
    _git(repo, "checkout", "-b", "feature")
    (repo / "src/datacoolie/transformers").mkdir(parents=True)
    (repo / "src/datacoolie/transformers/new.py").write_text("feature\n", encoding="utf-8")
    feature_head = _commit(repo, "feature")
    _git(repo, "checkout", "main")
    (repo / "docs").mkdir()
    (repo / "docs/guide.md").write_text("main branch\n", encoding="utf-8")
    main_head = _commit(repo, "main change")
    _git(repo, "merge", "--no-ff", "feature", "-m", "merge feature")
    tested_head = _git(repo, "rev-parse", "HEAD")

    selection = select_from_git(
        repo,
        base_ref=base,
        head_ref=feature_head,
        tested_head_ref=tested_head,
    )

    assert selection.selection == "narrow"
    assert selection.git is not None
    assert selection.git.base == base
    assert selection.git.head == feature_head
    assert selection.git.tested_head == tested_head
    assert selection.git.actual_head == tested_head
    assert selection.git.merge_base == base
    assert "src/datacoolie/transformers/new.py" in selection.changed_paths
    assert "docs/guide.md" in selection.changed_paths
    assert main_head != base


def test_real_rename_and_deleted_test_are_conservative(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    (repo / "src/datacoolie/engines").mkdir(parents=True)
    old = repo / "src/datacoolie/engines/old.py"
    old.write_text("same\n", encoding="utf-8")
    _commit(repo, "base")
    base = _git(repo, "rev-parse", "HEAD")
    new = repo / "src/datacoolie/engines/new.py"
    old.rename(new)
    rename_head = _commit(repo, "rename")
    renamed = select_from_git(repo, base_ref=base, head_ref=rename_head, tested_head_ref=rename_head)
    assert renamed.selection == "narrow"
    assert "src/datacoolie/engines/old.py" in renamed.changed_paths
    assert "src/datacoolie/engines/new.py" in renamed.changed_paths

    test_path = repo / "tests/unit/engines/test_removed.py"
    test_path.parent.mkdir(parents=True)
    test_path.write_text("def test_removed(): pass\n", encoding="utf-8")
    before_delete = _commit(repo, "add test")
    test_path.unlink()
    delete_head = _commit(repo, "delete test")
    deleted = select_from_git(repo, base_ref=before_delete, head_ref=delete_head, tested_head_ref=delete_head)
    assert deleted.selection == "full"
    assert "deleted test" in deleted.reasons[0]


def test_invalid_ref_and_tested_head_mismatch_fall_back(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    base = _commit(repo, "base")
    (repo / "src/datacoolie/engines/engine.py").parent.mkdir(parents=True)
    (repo / "src/datacoolie/engines/engine.py").write_text("changed\n", encoding="utf-8")
    head = _commit(repo, "head")

    invalid = select_from_git(repo, base_ref="--upload-pack=bad", head_ref=head, tested_head_ref=head)
    assert invalid.selection == "full"
    assert "invalid base ref" in invalid.reasons[0]

    mismatch = select_from_git(repo, base_ref=base, head_ref=head, tested_head_ref=base)
    assert mismatch.selection == "full"
    assert "tested HEAD does not match" in mismatch.reasons[0]
    assert mismatch.git is not None
    assert mismatch.git.tested_head == base
    assert mismatch.git.actual_head == head


def test_dirty_worktree_is_not_used_as_a_narrow_diff(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    base = _commit(repo, "base")
    (repo / "src/datacoolie/engines").mkdir(parents=True)
    (repo / "src/datacoolie/engines/engine.py").write_text("committed\n", encoding="utf-8")
    head = _commit(repo, "head")
    (repo / "src/datacoolie/engines/engine.py").write_text("uncommitted\n", encoding="utf-8")

    selection = select_from_git(repo, base_ref=base, head_ref=head, tested_head_ref=head)

    assert selection.selection == "full"
    assert "worktree is dirty" in selection.reasons[0]


def test_unlaunchable_git_is_a_full_fallback(monkeypatch, tmp_path: Path) -> None:
    def no_git(*_args, **_kwargs):
        raise OSError("git executable missing")

    monkeypatch.setattr(select_module.subprocess, "run", no_git)
    selection = select_from_git(
        tmp_path,
        base_ref="a" * 40,
        head_ref="b" * 40,
        tested_head_ref="b" * 40,
    )

    assert selection.selection == "full"
    assert "Git could not be launched" in selection.reasons[0]


def test_malformed_git_diff_falls_back_after_commit_binding(tmp_path: Path, monkeypatch) -> None:
    repo = _repository(tmp_path)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    base = _commit(repo, "base")
    (repo / "src/datacoolie/engines").mkdir(parents=True)
    (repo / "src/datacoolie/engines/engine.py").write_text("changed\n", encoding="utf-8")
    head = _commit(repo, "head")

    def malformed_diff(*_args, **_kwargs):
        raise select_module.GitSelectionError("malformed Git diff")

    monkeypatch.setattr(select_module, "git_diff", malformed_diff)
    selection = select_from_git(repo, base_ref=base, head_ref=head, tested_head_ref=head)

    assert selection.selection == "full"
    assert "malformed Git diff" in selection.reasons[0]
    assert selection.git is not None
    assert selection.git.tested_head == head
