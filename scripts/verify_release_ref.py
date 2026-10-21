"""Require a release tag to identify the checkout and belong to origin/main.

CI must fetch full branch and tag history before invoking this stdlib-only
check. The local build gate deliberately does not require an existing Git tag.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess


def git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=repo_root, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise ValueError(f"Git release-ref check failed: {' '.join(args)}")
    return result.stdout.strip()


def validate_release_ref(repo_root: Path, tag: str) -> str:
    """Return the checked commit; reject invalid, missing or unrelated refs."""

    if not tag.startswith("v"):
        raise ValueError("Release tag must start with v")
    tag_ref = f"refs/tags/{tag}"
    git(repo_root, "check-ref-format", tag_ref)
    head = git(repo_root, "rev-parse", "--verify", "HEAD^{commit}")
    tagged = git(repo_root, "rev-parse", "--verify", f"{tag_ref}^{{commit}}")
    if head != tagged:
        raise ValueError("Release tag does not identify the checked-out commit")
    main = git(repo_root, "rev-parse", "--verify", "refs/remotes/origin/main^{commit}")
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", head, main],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 1:
        raise ValueError("Release commit is not an ancestor of origin/main")
    if result.returncode != 0:
        raise ValueError("Could not establish release commit ancestry in origin/main")
    return head


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="Existing release tag, e.g. v0.2.0")
    args = parser.parse_args()
    try:
        commit = validate_release_ref(Path(__file__).resolve().parents[1], args.tag)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"Release ref verification failed: {exc}\n")
    print(f"Release tag {args.tag} identifies {commit} in origin/main history.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
