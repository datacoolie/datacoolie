"""Validate complete object decisions and derive annotations plus report rows."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

from _assessment_report import render_report_table
from _artifact_io import atomic_write_json, atomic_write_text
from _observation_contract import KEY_FIELDS, canonicalize_watermark_candidate, read_observations

OBJECT_FIELDS = KEY_FIELDS[:-1]
OUTCOMES = {
    "confirmed_candidate",
    "source_native_change",
    "backward_fallback",
    "full_refresh",
    "human_decision",
}
DECISION_FIELDS = {
    "match", "outcome", "candidates", "coverage", "limitations", "fallback",
    "decision_required", "delete_evidence",
}


def _object_key(match: Mapping[str, Any]) -> tuple[str, ...]:
    if set(match) != set(OBJECT_FIELDS):
        raise ValueError("Decision match must contain exactly the object key fields")
    return tuple(str(match[field]).strip() for field in OBJECT_FIELDS)


def _required_text(item: Mapping[str, Any], field: str, index: int) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Decision {index} requires non-empty {field}")
    return value.strip()


def _load_payload(path: Path) -> list[Mapping[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "objects"}:
        raise ValueError("Assessment must contain only schema_version and objects")
    if payload["schema_version"] != 1 or not isinstance(payload["objects"], list):
        raise ValueError("Assessment requires schema_version 1 and an objects array")
    return payload["objects"]


def _validated_candidates(
    item: Mapping[str, Any],
    columns: set[str],
    index: int,
) -> list[dict[str, str]]:
    candidates = item.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError(f"Decision {index} candidates must be an array")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate_index, candidate in enumerate(candidates):
        if not isinstance(candidate, dict) or set(candidate) != {"column", "roles"}:
            raise ValueError(
                f"Decision {index} candidate {candidate_index} requires only column and roles"
            )
        column = str(candidate["column"]).strip()
        if not column or column not in columns:
            raise ValueError(f"Decision {index} references unknown candidate column: {column}")
        if column in seen:
            raise ValueError(f"Decision {index} repeats candidate column: {column}")
        seen.add(column)
        roles = str(candidate["roles"]).strip()
        if not roles or roles != canonicalize_watermark_candidate(roles):
            raise ValueError(f"Decision {index} candidate roles are empty or noncanonical")
        result.append({"column": column, "roles": roles})
    return result


def finalize(
    observations_path: Path,
    decisions_path: Path,
    annotations_output: Path,
    report_output: Path,
) -> int:
    observations = read_observations(observations_path)
    rows_by_object: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    for row in observations:
        rows_by_object[tuple(row[field] for field in OBJECT_FIELDS)].append(row)

    decisions: dict[tuple[str, ...], dict[str, Any]] = {}
    for index, item in enumerate(_load_payload(decisions_path)):
        if not isinstance(item, dict):
            raise ValueError(f"Decision {index} must be an object")
        unsupported = set(item) - DECISION_FIELDS
        missing = DECISION_FIELDS - {"delete_evidence"} - set(item)
        if unsupported or missing:
            raise ValueError(
                f"Decision {index} fields mismatch; missing={sorted(missing)}, "
                f"unsupported={sorted(unsupported)}"
            )
        match = item.get("match")
        if not isinstance(match, dict):
            raise ValueError(f"Decision {index} match must be an object")
        key = _object_key(match)
        if key not in rows_by_object:
            raise ValueError(f"Decision {index} references unknown object: {key}")
        if key in decisions:
            raise ValueError(f"Duplicate decision object: {key}")

        outcome = str(item.get("outcome", "")).strip()
        if outcome not in OUTCOMES:
            raise ValueError(f"Decision {index} has unknown outcome: {outcome}")
        candidates = _validated_candidates(
            item, {row["column"] for row in rows_by_object[key]}, index,
        )
        coverage = _required_text(item, "coverage", index)
        limitations = _required_text(item, "limitations", index)
        fallback = _required_text(item, "fallback", index)
        decision_required = _required_text(item, "decision_required", index)
        roles = {role for candidate in candidates for role in candidate["roles"].split("|")}

        if outcome == "confirmed_candidate" and not candidates:
            raise ValueError(f"Decision {index} confirmed_candidate requires candidates")
        if outcome == "backward_fallback" and "backward" not in roles:
            raise ValueError(f"Decision {index} backward_fallback requires a backward candidate")
        if "backward" in roles and outcome != "backward_fallback":
            raise ValueError(f"Decision {index} backward role requires backward_fallback outcome")
        if outcome in {"source_native_change", "full_refresh", "human_decision"} and candidates:
            raise ValueError(f"Decision {index} outcome {outcome} cannot assign column candidates")
        if outcome == "source_native_change" and coverage.lower() in {"none", "n/a", "unknown"}:
            raise ValueError(f"Decision {index} source_native_change requires observed coverage")
        if outcome == "backward_fallback" and (
            limitations.lower() in {"none", "n/a"} or fallback.lower() in {"none", "n/a"}
        ):
            raise ValueError(
                f"Decision {index} backward_fallback requires limitations and fallback behavior"
            )
        if outcome == "human_decision" and decision_required.lower() in {"none", "n/a"}:
            raise ValueError(f"Decision {index} human_decision must name the required decision")
        delete_evidence = str(item.get("delete_evidence", "")).strip()
        if "delete" in roles and delete_evidence.lower() in {"", "none", "n/a", "unknown"}:
            raise ValueError(f"Decision {index} delete role requires persistent delete evidence")

        decisions[key] = {
            "match": {field: key[position] for position, field in enumerate(OBJECT_FIELDS)},
            "outcome": outcome,
            "candidates": candidates,
            "coverage": coverage,
            "limitations": limitations,
            "fallback": fallback,
            "decision_required": decision_required,
            "delete_evidence": delete_evidence,
        }

    missing_objects = set(rows_by_object) - set(decisions)
    if missing_objects:
        raise ValueError(f"Assessment is missing object decisions: {sorted(missing_objects)}")

    annotations = []
    for key in sorted(decisions):
        decision = decisions[key]
        desired_roles = {
            candidate["column"]: candidate["roles"] for candidate in decision["candidates"]
        }
        for row in rows_by_object[key]:
            desired = desired_roles.get(row["column"], "")
            if row["watermark_candidate"] != desired:
                annotations.append({
                    "match": {**decision["match"], "column": row["column"]},
                    "set": {"watermark_candidate": desired},
                })

    atomic_write_json(annotations_output, annotations)
    atomic_write_text(
        report_output, render_report_table(decisions[key] for key in sorted(decisions)),
    )
    return len(decisions)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Finalize complete watermark assessment decisions.", allow_abbrev=False,
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--decisions", required=True, type=Path)
    parser.add_argument("--annotations-output", required=True, type=Path)
    parser.add_argument("--report-output", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        count = finalize(
            args.input, args.decisions, args.annotations_output, args.report_output,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    print(f"Finalized {count} object decision(s)")


if __name__ == "__main__":
    main()
