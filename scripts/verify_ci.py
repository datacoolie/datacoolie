"""Run the CI shadow selector and the unchanged full release verification gate.

Pull requests receive a deterministic selection receipt based on the checked
out merge tree.  The receipt is informational during qualification: this
entrypoint always invokes ``scripts/verify_release.py`` with its normal
defaults, so package, AI, documentation, release-contract, and full non-Spark
verification remain blocking.  Main pushes and manual runs record an explicit
full-run receipt.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Mapping, Sequence

# ``python scripts/verify_ci.py`` puts ``scripts/`` (rather than the product
# root) first on ``sys.path``.  CI and the direct help/regression command both
# use that supported invocation, so make the sibling ``scripts`` package
# importable before importing its modules.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.select_tests import (
    Selection,
    full_selection,
    select_from_git,
    write_receipt,
)


def _event_payload(environment: Mapping[str, str]) -> dict[str, object] | None:
    event_path = environment.get("GITHUB_EVENT_PATH")
    if not event_path:
        return None
    try:
        payload = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _pull_request_refs(
    environment: Mapping[str, str],
    payload: dict[str, object] | None,
) -> tuple[str | None, str | None]:
    """Read event-provided immutable SHAs, never interpolating ref text into a shell."""

    pull_request = payload.get("pull_request") if payload else None
    if not isinstance(pull_request, dict):
        return None, None
    base = pull_request.get("base")
    head = pull_request.get("head")
    base_sha = base.get("sha") if isinstance(base, dict) else None
    head_sha = head.get("sha") if isinstance(head, dict) else None
    if not isinstance(base_sha, str) or not isinstance(head_sha, str):
        return None, None
    # GITHUB_BASE_REF/GITHUB_HEAD_REF are retained as diagnostics only.  The
    # event payload's full SHAs are the selector inputs and are Git-validated.
    return base_sha, head_sha


def selection_for_environment(
    repo_root: Path,
    *,
    environment: Mapping[str, str] | None = None,
    base_ref: str | None = None,
    head_ref: str | None = None,
    tested_head_ref: str | None = None,
) -> tuple[Selection, str]:
    """Return the PR shadow selection or an explicit full-run decision."""

    environment = environment or os.environ
    event_name = environment.get("GITHUB_EVENT_NAME", "").strip().lower()
    if event_name != "pull_request":
        label = event_name or "local"
        return (
            full_selection(f"{label} runs always use the complete release gate", fallback=False),
            "full",
        )

    payload = _event_payload(environment)
    event_base, event_head = _pull_request_refs(environment, payload)
    # The workflow also passes the immutable event SHAs explicitly.  This
    # keeps local runner behavior deterministic if the event file is absent;
    # branch-name env vars are never used as revision arguments.
    event_base = event_base or environment.get("CI_BASE_SHA")
    event_head = event_head or environment.get("CI_HEAD_SHA")
    base_ref = base_ref or event_base
    head_ref = head_ref or event_head
    tested_head_ref = tested_head_ref or environment.get("GITHUB_SHA")
    if not base_ref or not head_ref or not tested_head_ref:
        return (
            full_selection(
                "pull request base/head/tested HEAD SHAs are unavailable; full suite is required"
            ),
            "pr",
        )
    selection = select_from_git(
        repo_root,
        base_ref=base_ref,
        head_ref=head_ref,
        tested_head_ref=tested_head_ref,
    )
    return selection, "pr"


def run_release_gate(repo_root: Path) -> int:
    """Invoke the existing release verifier with its unchanged default args."""

    command = [sys.executable, str(Path(repo_root) / "scripts" / "verify_release.py")]
    completed = subprocess.run(command, cwd=repo_root, check=False)
    return completed.returncode


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--receipt",
        type=Path,
        help="JSON receipt path; defaults to .ci-artifacts/selection-receipt.json",
    )
    parser.add_argument("--base-ref", help="Explicit validated base SHA for local PR simulation")
    parser.add_argument("--head-ref", help="Explicit validated head SHA for local PR simulation")
    parser.add_argument("--tested-head", help="Explicit checked-out SHA for local PR simulation")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> int:
    args = _parser().parse_args(argv)
    repo_root = Path(args.repo).resolve()
    environment = environment or os.environ
    selection, run_scope = selection_for_environment(
        repo_root,
        environment=environment,
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        tested_head_ref=args.tested_head,
    )
    receipt_path = args.receipt or Path(
        environment.get("CI_SELECTION_RECEIPT", ".ci-artifacts/selection-receipt.json")
    )
    if not receipt_path.is_absolute():
        receipt_path = repo_root / receipt_path

    receipt_error: OSError | None = None
    try:
        write_receipt(
            receipt_path,
            selection,
            event_name=environment.get("GITHUB_EVENT_NAME") or "local",
            run_scope=run_scope,
            execution="shadow-only",
            release_gate="scripts/verify_release.py",
        )
    except OSError as exc:
        receipt_error = exc
        print(f"CI selection receipt could not be written: {exc}", file=sys.stderr)

    # The release gate runs even if receipt storage fails.  Its return code is
    # propagated unchanged so a failed package/AI/docs/test stage remains a
    # blocking CI failure.
    gate_exit_code = run_release_gate(repo_root)
    if receipt_error is None:
        try:
            write_receipt(
                receipt_path,
                selection,
                event_name=environment.get("GITHUB_EVENT_NAME") or "local",
                run_scope=run_scope,
                execution="shadow-only",
                release_gate="scripts/verify_release.py",
                release_gate_exit_code=gate_exit_code,
            )
        except OSError as exc:
            receipt_error = exc
            print(f"CI selection receipt could not be finalized: {exc}", file=sys.stderr)

    if gate_exit_code != 0:
        return gate_exit_code if gate_exit_code > 0 else 1
    return 1 if receipt_error is not None else 0


if __name__ == "__main__":
    raise SystemExit(main())
