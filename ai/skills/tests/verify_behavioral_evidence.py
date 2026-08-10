"""Create and verify provider-neutral evidence for lifecycle-skill behavioral evals.

The script does not call a model. An external eval tool produces one grading JSON per declared
case; this script binds those successful results to the exact maintained skill and eval bytes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


IGNORED_PARTS = {"__pycache__"}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def skill_digest(skill_dir: Path) -> str:
    """Hash all maintained skill bytes in stable relative-path order."""
    digest = hashlib.sha256()
    files = sorted(
        path
        for path in skill_dir.rglob("*")
        if path.is_file()
        and not any(part in IGNORED_PARTS for part in path.parts)
        and path.suffix != ".pyc"
    )
    if not files:
        raise ValueError(f"Skill has no maintained files: {skill_dir}")
    for path in files:
        relative = path.relative_to(skill_dir).as_posix().encode("utf-8")
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _eval_definitions(skill_dir: Path) -> tuple[Path, dict[str, Any], list[dict[str, Any]]]:
    path = skill_dir / "evals" / "evals.json"
    document = _json(path)
    if document.get("skill_name") != skill_dir.name:
        raise ValueError(f"Eval skill_name does not match directory: {skill_dir.name}")
    cases = document.get("evals")
    if not isinstance(cases, list) or not cases:
        raise ValueError("Behavioral eval definitions must contain a non-empty evals array")
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("Each behavioral eval must be an object")
        if not case.get("prompt") or not case.get("expected_output"):
            raise ValueError("Each behavioral eval requires prompt and expected_output")
        expectations = case.get("expectations")
        if not isinstance(expectations, list) or len(expectations) < 2:
            raise ValueError("Each behavioral eval requires at least two expectations")
    return path, document, cases


def _validate_grading(case: dict[str, Any], path: Path) -> dict[str, Any]:
    grading = _json(path)
    expected = case["expectations"]
    actual = grading.get("expectations")
    if not isinstance(actual, list) or [item.get("text") for item in actual] != expected:
        raise ValueError(f"Grading expectations do not bind eval {case.get('name', case.get('id'))}")
    if any(item.get("passed") is not True or not item.get("evidence") for item in actual):
        raise ValueError("Behavioral grading must pass every expectation with evidence")
    summary = grading.get("summary", {})
    total = len(expected)
    if summary.get("passed") != total or summary.get("failed") != 0 or summary.get("total") != total:
        raise ValueError("Behavioral grading summary does not match passed expectations")
    return {
        "eval_id": case.get("id"),
        "eval_name": case.get("name", str(case.get("id"))),
        "grading_sha256": _sha256(path),
        "expectations": [
            {
                "text": item["text"],
                "passed": item["passed"],
                "evidence": item["evidence"],
            }
            for item in actual
        ],
        "passed": total,
        "total": total,
    }


def build_evidence(skill_dir: Path, grading_paths: Iterable[Path]) -> dict[str, Any]:
    """Build evidence only when every declared case has a successful grading artifact."""
    eval_path, _, cases = _eval_definitions(skill_dir)
    paths = list(grading_paths)
    if len(paths) != len(cases):
        raise ValueError(f"Expected {len(cases)} grading files, received {len(paths)}")
    results = [_validate_grading(case, path) for case, path in zip(cases, paths)]
    return {
        "schema_version": 1,
        "artifact_type": "behavioral_eval_evidence",
        "skill_name": skill_dir.name,
        "skill_sha256": skill_digest(skill_dir),
        "evals_sha256": _sha256(eval_path),
        "evaluated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "results": results,
    }


def validate_evidence(skill_dir: Path, evidence: dict[str, Any]) -> None:
    """Reject stale, partial, failed, or differently scoped evidence."""
    allowed = {
        "schema_version", "artifact_type", "skill_name", "skill_sha256", "evals_sha256",
        "evaluated_at", "results",
    }
    if set(evidence) != allowed:
        raise ValueError("Behavioral evidence has missing or unknown fields")
    if evidence["schema_version"] != 1 or evidence["artifact_type"] != "behavioral_eval_evidence":
        raise ValueError("Unsupported behavioral evidence contract")
    if evidence["skill_name"] != skill_dir.name:
        raise ValueError("Behavioral evidence skill_name mismatch")
    eval_path, _, cases = _eval_definitions(skill_dir)
    if evidence["skill_sha256"] != skill_digest(skill_dir):
        raise ValueError("Behavioral evidence skill digest is stale")
    if evidence["evals_sha256"] != _sha256(eval_path):
        raise ValueError("Behavioral evidence eval digest is stale")
    timestamp = str(evidence["evaluated_at"])
    try:
        parsed_timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Behavioral evidence timestamp must be valid UTC") from exc
    if (
        not timestamp.endswith("Z")
        or parsed_timestamp.tzinfo is None
        or parsed_timestamp.utcoffset() != timedelta(0)
    ):
        raise ValueError("Behavioral evidence timestamp must be UTC")
    results = evidence["results"]
    if not isinstance(results, list) or len(results) != len(cases):
        raise ValueError("Behavioral evidence must cover every declared eval")
    for case, result in zip(cases, results):
        if set(result) != {
            "eval_id", "eval_name", "grading_sha256", "expectations", "passed", "total"
        }:
            raise ValueError("Behavioral evidence result has missing or unknown fields")
        total = len(case["expectations"])
        if result.get("eval_id") != case.get("id") or result.get("eval_name") != case.get(
            "name", str(case.get("id"))
        ):
            raise ValueError("Behavioral evidence result order or identity mismatch")
        if result.get("passed") != total or result.get("total") != total:
            raise ValueError("Behavioral evidence contains a failed or partial result")
        grading_hash = result.get("grading_sha256")
        if (
            not isinstance(grading_hash, str)
            or len(grading_hash) != 64
            or any(character not in "0123456789abcdef" for character in grading_hash)
        ):
            raise ValueError("Behavioral evidence grading digest is invalid")
        expectations = result.get("expectations")
        if not isinstance(expectations, list) or [
            item.get("text") for item in expectations if isinstance(item, dict)
        ] != case["expectations"]:
            raise ValueError("Behavioral evidence expectations do not bind the declared eval")
        if any(
            not isinstance(item, dict)
            or set(item) != {"text", "passed", "evidence"}
            or item.get("passed") is not True
            or not item.get("evidence")
            for item in expectations
        ):
            raise ValueError("Behavioral evidence contains a failed or unevidenced expectation")


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create", help="Create evidence from ordered grading files.")
    create.add_argument("skill_dir", type=Path)
    create.add_argument("output", type=Path)
    create.add_argument("gradings", nargs="+", type=Path)
    verify = subparsers.add_parser("verify", help="Verify one current evidence artifact.")
    verify.add_argument("skill_dir", type=Path)
    verify.add_argument("evidence", type=Path)
    definitions = subparsers.add_parser(
        "verify-definitions", help="Validate eval-definition structure for skill directories."
    )
    definitions.add_argument("skill_dirs", nargs="+", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "create":
            if args.output.resolve().is_relative_to(args.skill_dir.resolve()):
                raise ValueError("Behavioral evidence output must be outside the skill directory")
            _write_json(args.output, build_evidence(args.skill_dir, args.gradings))
            print(f"Created behavioral evidence: {args.output}")
        elif args.command == "verify":
            validate_evidence(args.skill_dir, _json(args.evidence))
            print(f"Verified behavioral evidence: {args.evidence}")
        else:
            for skill_dir in args.skill_dirs:
                _eval_definitions(skill_dir)
                print(f"Verified behavioral eval definitions: {skill_dir.name}")
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
