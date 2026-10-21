"""Select a conservative product test surface from a Git diff.

The selector is deliberately a small, declarative map.  It is used for CI
shadow receipts first; callers must keep the full release gate until every
family has been qualified.  Unknown, malformed, or incomplete evidence always
returns the full product suite.

GitHub's diff output is parsed as NUL-delimited bytes so a filename containing
whitespace, tabs, or newlines cannot change the selection.  The Git helpers
also bind the diff to the checked out commit, which matters for pull request
merge commits whose tree can contain conflict-resolution changes.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
from typing import Iterable, Sequence


SCHEMA_VERSION = 1
FULL_TEST_ROOTS = ("tests/",)
RELEASE_CONTRACT_ROOTS = ("scripts/tests/",)
COMMON_SENTINEL = "tests/integration/runtime/test_write_boundary_recovery.py"
OPT_IN_EXCLUSIONS = (
    "spark",
    "spark4_qualification",
    "cloud_integration",
    "datatype_qualification",
    "runtime_qualification",
    "benchmark",
)

# Build/cache/publication outputs do not describe a source change.  If they are
# the only paths in a diff, the selector still falls back to the full suite
# because an empty effective diff is not safe evidence for a narrow run.
IGNORED_PREFIXES = (
    ".cache/",
    ".pytest_cache/",
    ".ruff_cache/",
    ".venv/",
    "build/",
    "dist/",
    "site/",
    "docs/schema/",
    "src/datacoolie.egg-info/",
)
IGNORED_SUFFIXES = (".pyc", ".pyo")


@dataclass(frozen=True)
class ImpactRule:
    """One owner for a changed surface and its conservative consumers."""

    family: str
    prefixes: tuple[str, ...]
    test_roots: tuple[str, ...]
    reason: str
    full: bool = False


def _roots(*roots: str) -> tuple[str, ...]:
    """Return stable, duplicate-free test roots."""

    return tuple(sorted(set(roots)))


# Keep the map in this module.  A second copy in workflow YAML or docs would
# drift silently and make a receipt impossible to explain.
IMPACT_MAP: tuple[ImpactRule, ...] = (
    ImpactRule(
        "transformers",
        ("src/datacoolie/transformers/", "tests/unit/transformers/"),
        _roots(
            "tests/unit/engines/",
            "tests/unit/orchestration/execution/",
            "tests/unit/transformers/",
            "tests/integration/docs/",
            "tests/unit/docs/",
            COMMON_SENTINEL,
        ),
        "transformer implementation or tests include pipeline, engine, and docs consumers",
    ),
    ImpactRule(
        "sources",
        ("src/datacoolie/sources/", "tests/unit/sources/"),
        _roots(
            "tests/unit/engines/",
            "tests/unit/orchestration/",
            "tests/unit/platforms/",
            "tests/unit/sources/",
            "tests/integration/docs/",
            "tests/integration/runtime/test_source_boundary_recovery.py",
            COMMON_SENTINEL,
        ),
        "source implementation or tests include orchestration, engine, platform, and runtime consumers",
    ),
    ImpactRule(
        "destinations",
        ("src/datacoolie/destinations/", "tests/unit/destinations/"),
        _roots(
            "tests/unit/destinations/",
            "tests/unit/engines/",
            "tests/unit/orchestration/",
            "tests/unit/platforms/",
            "tests/integration/docs/",
            "tests/integration/runtime/test_write_boundary_recovery.py",
            COMMON_SENTINEL,
        ),
        "destination implementation or tests include orchestration, engine, platform, and write consumers",
    ),
    ImpactRule(
        "engines",
        ("src/datacoolie/engines/", "tests/unit/engines/"),
        _roots(
            "tests/unit/destinations/",
            "tests/unit/engines/",
            "tests/unit/orchestration/",
            "tests/unit/sources/",
            "tests/unit/transformers/",
            "tests/integration/cli/test_project_workflow.py",
            "tests/integration/docs/",
            "tests/unit/project/test_runners.py",
            "tests/unit/docs/",
            COMMON_SENTINEL,
        ),
        "engine implementation or tests include source, destination, transformer, orchestration, and output consumers",
    ),
    ImpactRule(
        "platforms",
        ("src/datacoolie/platforms/", "tests/unit/platforms/"),
        _roots(
            "tests/unit/destinations/",
            "tests/unit/engines/",
            "tests/unit/platforms/",
            "tests/unit/sources/",
            "tests/integration/runtime/test_source_boundary_recovery.py",
            COMMON_SENTINEL,
        ),
        "platform implementation or tests include source, engine, destination, and runtime consumers",
    ),
    ImpactRule(
        "project-cli-schema",
        (
            "src/datacoolie/cli/",
            "src/datacoolie/metadata/",
            "src/datacoolie/project/",
            "tests/integration/cli/",
            "tests/unit/cli/",
            "tests/unit/metadata/",
            "tests/unit/project/",
        ),
        _roots(
            "tests/integration/cli/",
            "tests/integration/docs/",
            "tests/unit/metadata/",
            "tests/unit/project/",
            "tests/unit/docs/",
            "tests/unit/cli/",
            "tests/unit/orchestration/",
            COMMON_SENTINEL,
        ),
        "project, CLI, metadata, or schema changes include project and public loading consumers",
    ),
    ImpactRule(
        "docs-examples",
        (
            "docs/",
            "overrides/",
            "properdocs.yml",
            "scripts/verify_docs_seo.py",
            "docs/scripts/",
            "tests/integration/docs/",
            "tests/unit/docs/",
        ),
        _roots(
            "tests/integration/cli/test_project_workflow.py",
            "tests/integration/docs/",
            "tests/unit/docs/",
            COMMON_SENTINEL,
        ),
        "documentation, examples, generators, or docs tests include docs and consumed example checks",
    ),
    ImpactRule(
        "simulator",
        (
            "usecase-sim/",
            "tests/unit/usecase_sim/",
            "tests/unit/project/test_usecase_sim_",
            "tests/integration/runtime/test_usecase_sim_",
        ),
        _roots(
            "tests/integration/runtime/",
            "tests/unit/project/",
            "tests/unit/usecase_sim/",
            COMMON_SENTINEL,
        ),
        "simulator tooling, catalog, fixtures, or tests include scenario and project consumers",
    ),
    ImpactRule(
        "shared-contract",
        (
            ".github/",
            "src/datacoolie/core/",
            "src/datacoolie/logging/",
            "src/datacoolie/orchestration/",
            "src/datacoolie/watermark/",
            "tests/conftest.py",
            "tests/fixtures/",
            "tests/support/",
            "tests/unit/core/",
            "tests/unit/data_types/",
            "tests/unit/logging/",
            "tests/unit/orchestration/",
            "tests/unit/watermark/",
            "scripts/",
            "ai/",
            "pyproject.toml",
            "poetry.lock",
            "setup.cfg",
            "tox.ini",
        ),
        FULL_TEST_ROOTS,
        "shared contract, fixture, configuration, dependency, selector, or CI changes require the full suite",
        full=True,
    ),
)


@dataclass(frozen=True)
class ChangedFile:
    """A record from ``git diff --name-status -z``."""

    status: str
    old_path: str | None = None
    new_path: str | None = None

    def __post_init__(self) -> None:
        if not self.status or not re.fullmatch(r"[A-Z][0-9]*", self.status):
            raise ValueError(f"Invalid Git change status: {self.status!r}")
        kind = self.status[0]
        if kind in {"R", "C"}:
            if not self.old_path or not self.new_path:
                raise ValueError(f"{kind} change requires old and new paths")
        elif not self.old_path and not self.new_path:
            raise ValueError("Git change requires a path")

    @property
    def paths(self) -> tuple[str, ...]:
        """Return all path identities, preserving rename/copy provenance."""

        if self.status[0] in {"R", "C"}:
            assert self.old_path is not None and self.new_path is not None
            return (self.old_path, self.new_path)
        return (self.new_path or self.old_path,)  # type: ignore[return-value]

    @property
    def deleted(self) -> bool:
        return self.status[0] == "D"


@dataclass(frozen=True)
class GitBinding:
    """Resolved commits used to produce a selection receipt."""

    base: str | None = None
    head: str | None = None
    merge_base: str | None = None
    tested_head: str | None = None
    actual_head: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "base": self.base,
            "head": self.head,
            "merge_base": self.merge_base,
            "tested_head": self.tested_head,
            "actual_head": self.actual_head,
            "diff_base": self.base,
            "diff_head": self.tested_head,
        }


@dataclass(frozen=True)
class Selection:
    """Deterministic selector output suitable for a CI receipt."""

    selection: str
    fallback: bool
    test_roots: tuple[str, ...]
    families: tuple[str, ...]
    reasons: tuple[str, ...]
    changed_paths: tuple[str, ...] = ()
    ignored_paths: tuple[str, ...] = ()
    git: GitBinding | None = None

    @property
    def is_full(self) -> bool:
        return self.selection == "full"

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "selection": self.selection,
            "fallback": self.fallback,
            "test_roots": list(self.test_roots),
            "families": list(self.families),
            "reasons": list(self.reasons),
            "changed_paths": list(self.changed_paths),
            "ignored_paths": list(self.ignored_paths),
            "release_contract_tests": list(RELEASE_CONTRACT_ROOTS),
            "opt_in_exclusions": list(OPT_IN_EXCLUSIONS),
            "git": self.git.as_dict() if self.git is not None else None,
        }


class GitSelectionError(ValueError):
    """A diff could not be safely bound to the checked out tree."""

    def __init__(self, message: str, *, binding: GitBinding | None = None) -> None:
        super().__init__(message)
        self.binding = binding


def _normalize_path(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise ValueError("Changed path must be a non-empty string")
    if "\x00" in path:
        raise ValueError("Changed path contains NUL")
    path = path.replace("\\", "/")
    if path.startswith("/") or re.match(r"^[A-Za-z]:/", path):
        raise ValueError(f"Changed path must be repository-relative: {path!r}")
    if path.startswith("./"):
        path = path[2:]
    parts = PurePosixPath(path).parts
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Changed path is not normalized: {path!r}")
    return "/".join(parts)


def _decode_path(raw: bytes) -> str:
    try:
        return _normalize_path(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ValueError("Git diff contains a non-UTF-8 path") from exc


def parse_name_status_z(data: bytes) -> tuple[ChangedFile, ...]:
    """Parse NUL-delimited ``git diff --name-status -z`` output.

    Git emits one status and one path for ordinary changes, and one status plus
    two paths for renames/copies.  A malformed stream is rejected so callers
    can use the full-suite fallback instead of guessing.
    """

    if not isinstance(data, bytes):
        raise TypeError("NUL-delimited Git output must be bytes")
    fields = data.split(b"\x00")
    if fields[-1] != b"":
        raise ValueError("Git diff output is not NUL terminated")
    fields.pop()
    changes: list[ChangedFile] = []
    index = 0
    while index < len(fields):
        try:
            status = fields[index].decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError("Git diff status is not ASCII") from exc
        if not re.fullmatch(r"[A-Z][0-9]*", status):
            raise ValueError(f"Malformed Git diff status: {status!r}")
        index += 1
        if status[0] in {"R", "C"}:
            if index + 1 >= len(fields):
                raise ValueError(f"{status} change is missing old or new path")
            old_path = _decode_path(fields[index])
            new_path = _decode_path(fields[index + 1])
            index += 2
            changes.append(ChangedFile(status, old_path, new_path))
        else:
            if index >= len(fields):
                raise ValueError(f"{status} change is missing path")
            path = _decode_path(fields[index])
            index += 1
            changes.append(ChangedFile(status, path, path))
    return tuple(changes)


def _path_matches(path: str, prefix: str) -> bool:
    return path == prefix or (prefix.endswith("/") and path.startswith(prefix)) or (
        not prefix.endswith("/") and path.startswith(prefix)
    )


def _matching_rules(path: str) -> tuple[ImpactRule, ...]:
    matches = [
        (rule, max(len(prefix) for prefix in rule.prefixes if _path_matches(path, prefix)))
        for rule in IMPACT_MAP
        if any(_path_matches(path, prefix) for prefix in rule.prefixes)
    ]
    if not matches:
        return ()
    # A specific consumer prefix (for example a usecase-sim test) owns its
    # path over a broader family prefix.  Equal-length owners remain
    # ambiguous and deliberately fall back to the full suite.
    longest = max(length for _, length in matches)
    return tuple(rule for rule, length in matches if length == longest)


def _is_ignored(path: str) -> bool:
    return path.endswith(IGNORED_SUFFIXES) or any(
        path == prefix.rstrip("/") or path.startswith(prefix) for prefix in IGNORED_PREFIXES
    )


def full_selection(
    reason: str,
    *,
    changed_paths: Iterable[str] = (),
    ignored_paths: Iterable[str] = (),
    git: GitBinding | None = None,
    fallback: bool = True,
) -> Selection:
    """Build a full-suite selection with one explicit reason."""

    return Selection(
        selection="full",
        fallback=fallback,
        test_roots=FULL_TEST_ROOTS,
        families=("full",),
        reasons=(reason,),
        changed_paths=tuple(sorted(set(changed_paths))),
        ignored_paths=tuple(sorted(set(ignored_paths))),
        git=git,
    )


def select_changes(
    changes: Iterable[ChangedFile],
    *,
    git: GitBinding | None = None,
) -> Selection:
    """Select the union of all mapped surfaces, falling back conservatively."""

    records = tuple(changes)
    if not records:
        return full_selection("empty diff; full suite is required", git=git)

    normalized_paths: list[str] = []
    ignored_paths: list[str] = []
    for change in records:
        try:
            paths = tuple(_normalize_path(path) for path in change.paths)
        except (TypeError, ValueError) as exc:
            return full_selection(f"invalid changed path; full suite is required ({exc})", git=git)
        normalized_paths.extend(paths)
        if change.deleted and any(path.startswith("tests/") for path in paths):
            return full_selection(
                f"deleted test requires full suite: {paths[0]}",
                changed_paths=normalized_paths,
                git=git,
            )

    effective_paths = []
    for path in normalized_paths:
        if _is_ignored(path):
            ignored_paths.append(path)
        else:
            effective_paths.append(path)
    if not effective_paths:
        return full_selection(
            "empty effective diff after generated/build paths were ignored; full suite is required",
            changed_paths=normalized_paths,
            ignored_paths=ignored_paths,
            git=git,
        )

    matched: dict[str, ImpactRule] = {}
    for path in sorted(set(effective_paths)):
        rules = _matching_rules(path)
        if not rules:
            return full_selection(
                f"unknown changed path requires full suite: {path}",
                changed_paths=normalized_paths,
                ignored_paths=ignored_paths,
                git=git,
            )
        if len(rules) != 1:
            families = ", ".join(rule.family for rule in rules)
            return full_selection(
                f"ambiguous impact mapping for {path} ({families}); full suite is required",
                changed_paths=normalized_paths,
                ignored_paths=ignored_paths,
                git=git,
            )
        rule = rules[0]
        matched[rule.family] = rule
        if rule.full:
            return full_selection(
                rule.reason,
                changed_paths=normalized_paths,
                ignored_paths=ignored_paths,
                git=git,
            )

    roots = _roots(*(root for rule in matched.values() for root in rule.test_roots))
    reasons = tuple(rule.reason for rule in (matched[name] for name in sorted(matched)))
    return Selection(
        selection="narrow",
        fallback=False,
        test_roots=roots,
        families=tuple(sorted(matched)),
        reasons=reasons,
        changed_paths=tuple(sorted(set(normalized_paths))),
        ignored_paths=tuple(sorted(set(ignored_paths))),
        git=git,
    )


def select_paths(paths: Iterable[str], *, git: GitBinding | None = None) -> Selection:
    """Select from repository-relative paths without Git status metadata."""

    try:
        changes = tuple(
            ChangedFile("M", _normalize_path(path), _normalize_path(path)) for path in paths
        )
    except (TypeError, ValueError) as exc:
        return full_selection(f"malformed path selection; full suite is required ({exc})", git=git)
    return select_changes(changes, git=git)


# Friendly alias for callers that naturally describe the operation as
# ``select_tests(paths)``.
def select_tests(paths: Iterable[str], *, git: GitBinding | None = None) -> Selection:
    return select_paths(paths, git=git)


def _run_git(repo_root: Path, args: Sequence[str]) -> bytes:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        raise GitSelectionError(f"Git could not be launched: {exc}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()
        raise GitSelectionError(detail or f"git {' '.join(args)} failed")
    return completed.stdout


_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
_REF_RE = re.compile(r"^(?:HEAD|refs/[A-Za-z0-9._/-]+|[A-Za-z0-9._/-]+)$")


def _validate_ref(value: str, label: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise GitSelectionError(f"invalid {label} ref")
    if value.startswith("-") or any(char.isspace() for char in value):
        raise GitSelectionError(f"invalid {label} ref")
    if not (_SHA_RE.fullmatch(value) or _REF_RE.fullmatch(value)):
        raise GitSelectionError(f"invalid {label} ref")
    return value


def resolve_commit(repo_root: Path, value: str, *, label: str = "revision") -> str:
    """Resolve a validated commit ref without invoking a shell."""

    revision = _validate_ref(value, label)
    output = _run_git(
        repo_root,
        ["rev-parse", "--verify", "--quiet", "--end-of-options", f"{revision}^{{commit}}"],
    )
    commit = output.decode("ascii", "strict").strip()
    if not _SHA_RE.fullmatch(commit):
        raise GitSelectionError(f"{label} does not resolve to a commit")
    return commit.lower()


def _is_ancestor(repo_root: Path, ancestor: str, descendant: str) -> bool:
    try:
        completed = subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, descendant],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        raise GitSelectionError(f"Git could not be launched: {exc}") from exc
    if completed.returncode not in (0, 1):
        detail = completed.stderr.decode("utf-8", "replace").strip()
        raise GitSelectionError(detail or "git ancestry check failed")
    return completed.returncode == 0


def resolve_git_context(
    repo_root: Path,
    *,
    base_ref: str,
    head_ref: str,
    tested_head_ref: str | None = None,
) -> GitBinding:
    """Resolve and validate the commits used for a PR merge-tree diff."""

    repo_root = Path(repo_root).resolve()
    actual = resolve_commit(repo_root, "HEAD", label="checked-out HEAD")
    base = resolve_commit(repo_root, base_ref, label="base")
    head = resolve_commit(repo_root, head_ref, label="head")
    tested = resolve_commit(
        repo_root,
        tested_head_ref or actual,
        label="tested HEAD",
    )
    merge_base_output = _run_git(repo_root, ["merge-base", base, head])
    merge_base = merge_base_output.decode("ascii", "strict").strip().lower()
    if not _SHA_RE.fullmatch(merge_base):
        raise GitSelectionError("merge-base did not resolve to a commit")
    binding = GitBinding(
        base=base,
        head=head,
        merge_base=merge_base,
        tested_head=tested,
        actual_head=actual,
    )
    if actual != tested:
        raise GitSelectionError("tested HEAD does not match checked-out HEAD", binding=binding)
    if not _is_ancestor(repo_root, base, tested):
        raise GitSelectionError("base is not an ancestor of tested HEAD", binding=binding)
    if not _is_ancestor(repo_root, head, tested):
        raise GitSelectionError("head is not an ancestor of tested HEAD", binding=binding)
    if not _is_ancestor(repo_root, merge_base, tested):
        raise GitSelectionError("merge-base is not an ancestor of tested HEAD", binding=binding)
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as exc:
        raise GitSelectionError(f"Git could not be launched: {exc}") from exc
    if status.returncode != 0:
        detail = status.stderr.decode("utf-8", "replace").strip()
        raise GitSelectionError(detail or "git status failed", binding=binding)
    if status.stdout:
        raise GitSelectionError("checked-out worktree is dirty", binding=binding)
    return binding


def git_diff(repo_root: Path, *, base: str, tested_head: str) -> tuple[ChangedFile, ...]:
    """Read the merge tree diff using NUL-delimited machine-readable output."""

    output = _run_git(
        Path(repo_root).resolve(),
        [
            "diff",
            "--name-status",
            "-z",
            "--find-renames",
            "--find-copies",
            base,
            tested_head,
            "--",
        ],
    )
    try:
        return parse_name_status_z(output)
    except (TypeError, ValueError) as exc:
        raise GitSelectionError(f"malformed Git diff: {exc}") from exc


def select_from_git(
    repo_root: Path,
    *,
    base_ref: str,
    head_ref: str,
    tested_head_ref: str | None = None,
) -> Selection:
    """Resolve a PR diff and return a safe selection or explicit full fallback."""

    binding: GitBinding | None = None
    try:
        binding = resolve_git_context(
            repo_root,
            base_ref=base_ref,
            head_ref=head_ref,
            tested_head_ref=tested_head_ref,
        )
        changes = git_diff(repo_root, base=binding.base, tested_head=binding.tested_head)  # type: ignore[arg-type]
    except (GitSelectionError, OSError, UnicodeError, ValueError) as exc:
        return full_selection(
            f"{exc}; full suite is required",
            git=getattr(exc, "binding", None) or binding,
        )
    return select_changes(changes, git=binding)


def write_receipt(
    path: Path,
    selection: Selection,
    *,
    event_name: str | None = None,
    run_scope: str | None = None,
    execution: str = "shadow-only",
    release_gate: str = "scripts/verify_release.py",
    release_gate_exit_code: int | None = None,
) -> None:
    """Write stable JSON for artifact upload and later qualification review."""

    payload = selection.as_dict()
    payload["event_name"] = event_name
    payload["run_scope"] = run_scope
    payload["execution"] = execution
    payload["release_gate"] = release_gate
    payload["release_gate_exit_code"] = release_gate_exit_code
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--base")
    parser.add_argument("--head")
    parser.add_argument("--tested-head")
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--event-name")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_cli_parser().parse_args(argv)
    if bool(args.base) != bool(args.head):
        selection = full_selection("base/head refs are incomplete; full suite is required")
    elif args.base and args.head:
        selection = select_from_git(
            args.repo,
            base_ref=args.base,
            head_ref=args.head,
            tested_head_ref=args.tested_head,
        )
    else:
        selection = full_selection("base/head refs are unavailable; full suite is required")
    if args.receipt:
        write_receipt(args.receipt, selection, event_name=args.event_name)
    print(json.dumps(selection.as_dict(), indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
