"""Merge new discovery observations or refresh explicit source boundaries."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from _artifact_io import atomic_write_json
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


def _load_statuses(paths: Iterable[Path]) -> list[dict[str, Any]]:
    statuses = []
    seen_sources: set[str] = set()
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Probe status must be an object: {path}")
        source = str(payload.get("source", "")).strip()
        status = str(payload.get("status", "")).strip()
        row_count = payload.get("row_count")
        if (
            not source
            or status not in {"complete", "partial"}
            or not isinstance(row_count, int)
            or isinstance(row_count, bool)
            or row_count < 0
        ):
            raise ValueError(
                f"Probe status requires source, complete/partial status, and row_count: {path}"
            )
        if source in seen_sources:
            raise ValueError(f"Probe status repeats source boundary: {source}")
        seen_sources.add(source)
        statuses.append({
            "source": source, "status": status, "row_count": row_count, "path": str(path),
        })
    return statuses


def _key_payload(row: Mapping[str, Any]) -> dict[str, str]:
    return {field: str(row.get(field, "")) for field in KEY_FIELDS}


def build_diff(
    before: Iterable[Mapping[str, Any]],
    after: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    before_by_key = {observation_key(row): dict(row) for row in before}
    after_by_key = {observation_key(row): dict(row) for row in after}
    before_keys, after_keys = set(before_by_key), set(after_by_key)
    added = [_key_payload(after_by_key[key]) for key in sorted(after_keys - before_keys)]
    removed = [_key_payload(before_by_key[key]) for key in sorted(before_keys - after_keys)]
    changed = []
    for key in sorted(before_keys & after_keys):
        old, new = before_by_key[key], after_by_key[key]
        fields = {
            field: {"before": str(old[field]), "after": str(new[field])}
            for field in CSV_HEADER if str(old[field]) != str(new[field])
        }
        if fields:
            changed.append({"key": _key_payload(new), "fields": fields})
    return {
        "schema_version": 1,
        "summary": {
            "added": len(added), "removed": len(removed), "changed": len(changed),
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
) -> int:
    if not inputs:
        raise ValueError("At least one --input is required")
    replacement_rows = []
    for path in inputs:
        replacement_rows.extend(read_observations(path))

    replace_set = {str(source).strip() for source in replace_sources if str(source).strip()}
    accepted_partial = {
        str(source).strip() for source in accept_partial_sources if str(source).strip()
    }
    status_paths = list(status_inputs)
    if base is None:
        if replace_set or status_paths or accepted_partial or diff_output is not None:
            raise ValueError("Refresh options require --base")
        rows = replacement_rows
    else:
        if not replace_set:
            raise ValueError("Refresh mode requires at least one --replace-source")
        unexpected = {row["source"] for row in replacement_rows} - replace_set
        if unexpected:
            raise ValueError(f"Replacement inputs contain undeclared sources: {sorted(unexpected)}")
        statuses = _load_statuses(status_paths)
        status_sources = {item["source"] for item in statuses}
        if status_sources - replace_set:
            raise ValueError(
                f"Probe statuses contain undeclared sources: {sorted(status_sources - replace_set)}"
            )
        missing_status = replace_set - status_sources
        if missing_status:
            raise ValueError(f"Replacement sources lack probe status: {sorted(missing_status)}")
        for source in sorted(replace_set):
            observed_count = sum(row["source"] == source for row in replacement_rows)
            status_count = next(
                item["row_count"] for item in statuses if item["source"] == source
            )
            if observed_count != status_count:
                raise ValueError(
                    f"Replacement row count for {source} is {observed_count}, "
                    f"but probe status reports {status_count}"
                )
        rejected_partial = {
            item["source"] for item in statuses
            if item["status"] == "partial" and item["source"] not in accepted_partial
        }
        if rejected_partial:
            raise ValueError(
                "Partial replacement requires explicit acceptance for sources: "
                f"{sorted(rejected_partial)}"
            )
        if accepted_partial - replace_set:
            raise ValueError(
                "Accepted partial sources are outside replacement scope: "
                f"{sorted(accepted_partial - replace_set)}"
            )

        base_rows = read_observations(base)
        rows = [row for row in base_rows if row["source"] not in replace_set]
        rows.extend(replacement_rows)
        rows = validate_observations(rows)
        if diff_output is not None:
            atomic_write_json(diff_output, build_diff(base_rows, rows))

    rows.sort(key=_sort_key)
    return atomic_write_observations(output, rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge new observations or refresh explicit source boundaries.",
        allow_abbrev=False,
    )
    parser.add_argument("--input", action="append", required=True, type=Path)
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
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    print(f"Merged {count} observation(s) into {args.output}")


if __name__ == "__main__":
    main()
