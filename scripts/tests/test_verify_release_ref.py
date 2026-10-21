"""Exercise release-ref checks against isolated real Git histories."""

from pathlib import Path
import subprocess

import pytest

from scripts.verify_release_ref import validate_release_ref


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    git(tmp_path, "init", "--initial-branch", "main")
    git(tmp_path, "config", "user.name", "release-ref-test")
    git(tmp_path, "config", "user.email", "release-ref@example.invalid")
    (tmp_path / "README.md").write_text("base\n", encoding="utf-8")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-m", "base")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    return tmp_path


@pytest.mark.parametrize("annotated", [False, True])
def test_tag_on_main_is_accepted(repo: Path, annotated: bool) -> None:
    git(repo, "tag", *(["-a", "-m", "release"] if annotated else []), "v0.2.0")
    assert validate_release_ref(repo, "v0.2.0") == git(repo, "rev-parse", "HEAD")


def test_tag_on_ancestor_of_main_is_accepted(repo: Path) -> None:
    git(repo, "tag", "v0.2.0")
    tagged = git(repo, "rev-parse", "HEAD")
    git(repo, "commit", "--allow-empty", "-m", "later main")
    git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    git(repo, "checkout", "--detach", tagged)
    assert validate_release_ref(repo, "v0.2.0") == tagged


def test_tag_on_unmerged_branch_is_rejected(repo: Path) -> None:
    git(repo, "checkout", "-b", "feature")
    git(repo, "commit", "--allow-empty", "-m", "unmerged")
    git(repo, "tag", "v0.2.0")
    with pytest.raises(ValueError, match="not an ancestor"):
        validate_release_ref(repo, "v0.2.0")


def test_tag_different_from_checkout_is_rejected(repo: Path) -> None:
    git(repo, "tag", "v0.2.0")
    git(repo, "commit", "--allow-empty", "-m", "different checkout")
    with pytest.raises(ValueError, match="checked-out commit"):
        validate_release_ref(repo, "v0.2.0")


def test_missing_main_is_rejected(repo: Path) -> None:
    git(repo, "tag", "v0.2.0")
    git(repo, "update-ref", "-d", "refs/remotes/origin/main")
    with pytest.raises(ValueError, match="Git release-ref check failed"):
        validate_release_ref(repo, "v0.2.0")


@pytest.mark.parametrize("tag", ["v0.2.0", "0.2.0", "v../invalid", "vbad space"])
def test_missing_or_invalid_tag_is_rejected(repo: Path, tag: str) -> None:
    with pytest.raises(ValueError):
        validate_release_ref(repo, tag)


def test_shell_characters_in_git_tag_are_literal(repo: Path) -> None:
    tag = "v$(touch${IFS}unexpected)"
    git(repo, "tag", tag)
    assert validate_release_ref(repo, tag) == git(repo, "rev-parse", "HEAD")
    assert not (repo / "unexpected").exists()
