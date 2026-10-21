"""Merge new discovery observations or refresh explicit source boundaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from _artifact_io import atomic_write_json
from _probe_status import file_sha256
from _observation_contract import (
    CSV_HEADER,
    KEY_FIELDS,
    atomic_write_observations,
    observation_key,
    read_observations,
    validate_observations,
)


def _sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    ordinal = str(row.get("ordinal", ""))
    return tuple(str(row.get(field, "")) for field in KEY_FIELDS) + (
        int(ordinal) if ordinal else 0,
    )


def _load_bound_status(csv_path: Path, status_path: Path, rows: list[dict]) -> dict:
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Probe status must be an object")
    source, status = payload.get("source"), payload.get("status")
    count = payload.get("row_count")
    if (
        not source
        or status not in {"complete", "partial"}
        or type(count) is not int
        or count < 0
    ):
        raise ValueError(
            "Probe status requires source, complete/partial status, and row_count"
        )
    if payload.get("output_sha256") != file_sha256(csv_path):
        raise ValueError(
            "Probe status CSV SHA-256 binding is missing or does not match"
        )
    if count != len(rows) or any(row["source"] != source for row in rows):
        raise ValueError("Probe status source or row count does not match its CSV")
    scope = payload.get("scope")
    if (
        not isinstance(scope, dict)
        or scope.get("kind") not in {"source", "objects"}
        or not isinstance(scope.get("filters"), dict)
    ):
        raise ValueError("Probe status lacks explicit source/object scope")
    observed = {tuple(row[field] for field in KEY_FIELDS[:-1]) for row in rows}
    if _object_set(scope.get("objects"), source) != observed:
        raise ValueError("Probe status object scope does not match CSV objects")
    if scope["kind"] == "source" and any(scope["filters"].values()):
        raise ValueError("Filtered probe cannot claim source-wide coverage")
    return payload


def _object_set(value: Any, source: str) -> set[tuple[str, ...]]:
    if not isinstance(value, list) or any(
        not isinstance(item, (list, tuple))
        or len(item) != 6
        or any(not isinstance(part, str) for part in item)
        or item[0] != source
        for item in value
    ):
        raise ValueError("Coverage objects must contain six-field source identities")
    result = {tuple(item) for item in value}
    if len(result) != len(value):
        raise ValueError("Coverage contains duplicate object identities")
    return result


def _coverage(path: Path | None) -> dict[str, set[tuple[str, ...]]]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or not isinstance(payload.get("sources"), list)
    ):
        raise ValueError("Invalid source-boundary coverage declaration")
    result = {}
    for item in payload["sources"]:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("source"), str)
            or not item["source"].strip()
            or not isinstance(item.get("evidence"), str)
            or not item["evidence"].strip()
        ):
            raise ValueError("Coverage requires source and nonempty boundary evidence")
        if item["source"] in result:
            raise ValueError("Coverage repeats source boundary")
        result[item["source"]] = _object_set(item.get("objects"), item["source"])
    return result


def _key_payload(row: Mapping[str, Any]) -> dict[str, str]:
    return {field: str(row.get(field, "")) for field in KEY_FIELDS}


def build_diff(
    before: Iterable[Mapping[str, Any]],
    after: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    before_by_key = {observation_key(row): dict(row) for row in before}
    after_by_key = {observation_key(row): dict(row) for row in after}
    before_keys, after_keys = set(before_by_key), set(after_by_key)
    added = [
        _key_payload(after_by_key[key]) for key in sorted(after_keys - before_keys)
    ]
    removed = [
        _key_payload(before_by_key[key]) for key in sorted(before_keys - after_keys)
    ]
    changed = []
    for key in sorted(before_keys & after_keys):
        old, new = before_by_key[key], after_by_key[key]
        fields = {
            field: {"before": str(old[field]), "after": str(new[field])}
            for field in CSV_HEADER
            if str(old[field]) != str(new[field])
        }
        if fields:
            changed.append({"key": _key_payload(new), "fields": fields})
    return {
        "schema_version": 1,
        "summary": {
            "added": len(added),
            "removed": len(removed),
            "changed": len(changed),
        },
        "added": added,
        "removed": removed,
        "changed": changed,
    }


def merge(
    inputs: list[Path],
    output: Path,
    *,
    base: Path | None = None,
    replace_sources: Iterable[str] = (),
    status_inputs: Iterable[Path] = (),
    accept_partial_sources: Iterable[str] = (),
    diff_output: Path | None = None,
    probe_inputs: Iterable[tuple[Path, Path]] = (),
    coverage_input: Path | None = None,
) -> int:
    pairs = list(probe_inputs)
    status_paths = list(status_inputs)
    if pairs and (inputs or status_paths):
        raise ValueError("Do not mix --probe-input and legacy --input/--status-input")
    paths = [pair[0] for pair in pairs] if pairs else inputs
    if not paths:
        raise ValueError("At least one --input or --probe-input is required")
    if len({path.resolve() for path in paths}) != len(paths):
        raise ValueError("Duplicate observation key/input path")
    evidence_paths = [*paths, *status_paths, *(pair[1] for pair in pairs)]
    if base is not None:
        evidence_paths.append(base)
    if coverage_input is not None:
        evidence_paths.append(coverage_input)
    if output.resolve() in {path.resolve() for path in evidence_paths}:
        raise ValueError("Candidate output must be distinct from input evidence")
    if pairs and len({pair[1].resolve() for pair in pairs}) != len(pairs):
        raise ValueError("Duplicate probe status input")
    inputs_rows = [read_observations(path) for path in paths]
    replacement_rows = validate_observations(
        row for rows in inputs_rows for row in rows
    )
    replace_set = {
        str(source).strip() for source in replace_sources if str(source).strip()
    }
    accepted_partial = {
        str(source).strip() for source in accept_partial_sources if str(source).strip()
    }
    if base is None:
        if (
            replace_set
            or status_paths
            or accepted_partial
            or diff_output is not None
            or coverage_input
        ):
            raise ValueError("Refresh options require --base")
        if pairs:
            for (csv_path, status_path), rows in zip(pairs, inputs_rows):
                _load_bound_status(csv_path, status_path, rows)
        rows = replacement_rows
    else:
        if not replace_set:
            raise ValueError("Refresh mode requires at least one --replace-source")
        if accepted_partial - replace_set:
            raise ValueError("Accepted partial sources are outside replacement scope")
        if {row["source"] for row in replacement_rows} - replace_set:
            raise ValueError("Replacement inputs contain undeclared sources")
        if not pairs:
            if len(status_paths) != len(paths):
                raise ValueError(
                    "Each replacement input requires its paired probe status"
                )
            pairs = list(zip(paths, status_paths))
        if len({pair[1].resolve() for pair in pairs}) != len(pairs):
            raise ValueError("Duplicate probe status input")
        statuses = [
            _load_bound_status(csv_path, status_path, subset)
            for (csv_path, status_path), subset in zip(pairs, inputs_rows)
        ]
        if {item["source"] for item in statuses} != replace_set:
            raise ValueError(
                "Replacement sources lack probe status or include undeclared sources"
            )
        declared = _coverage(coverage_input)
        if set(declared) - replace_set:
            raise ValueError("Coverage contains undeclared sources")
        for source in sorted(replace_set):
            selected = [item for item in statuses if item["source"] == source]
            partial = any(item["status"] == "partial" for item in selected)
            if partial and source not in accepted_partial:
                raise ValueError(
                    "Partial replacement requires explicit acceptance for sources"
                )
            observed = {
                tuple(row[field] for field in KEY_FIELDS[:-1])
                for row in replacement_rows
                if row["source"] == source
            }
            full_probe = len(selected) == 1 and selected[0]["scope"]["kind"] == "source"
            if source in declared:
                expected = declared[source]
                if not observed <= expected or (
                    observed != expected
                    and not (partial and source in accepted_partial)
                ):
                    raise ValueError(
                        "Probe coverage does not cover the declared source boundary"
                    )
            elif not full_probe:
                raise ValueError(
                    "Object/filtered probes require explicit full-source coverage declaration"
                )
        base_rows = read_observations(base)
        rows = validate_observations(
            [row for row in base_rows if row["source"] not in replace_set]
            + replacement_rows
        )
        if diff_output is not None:
            if diff_output.resolve() in {
                path.resolve() for path in [output, *evidence_paths]
            }:
                raise ValueError(
                    "Diff output must be distinct from observation artifacts"
                )
            atomic_write_json(diff_output, build_diff(base_rows, rows))
    rows.sort(key=_sort_key)
    return atomic_write_observations(output, rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge new observations or refresh explicit source boundaries.",
        allow_abbrev=False,
    )
    parser.add_argument("--input", action="append", default=[], type=Path)
    parser.add_argument(
        "--probe-input",
        action="append",
        nargs=2,
        default=[],
        type=Path,
        metavar=("CSV", "STATUS"),
    )
    parser.add_argument("--coverage-input", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--base", type=Path)
    parser.add_argument("--replace-source", action="append", default=[])
    parser.add_argument("--status-input", action="append", default=[], type=Path)
    parser.add_argument("--accept-partial-source", action="append", default=[])
    parser.add_argument("--diff-output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        count = merge(
            args.input,
            args.output,
            base=args.base,
            replace_sources=args.replace_source,
            status_inputs=args.status_input,
            accept_partial_sources=args.accept_partial_source,
            diff_output=args.diff_output,
            probe_inputs=args.probe_input,
            coverage_input=args.coverage_input,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    print(f"Merged {count} observation(s) into {args.output}")


if __name__ == "__main__":
    main()
