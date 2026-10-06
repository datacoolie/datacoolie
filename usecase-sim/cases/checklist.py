"""Derived behavior-checklist rows for the simulator catalog audit.

The JSON catalogs remain the source of truth.  This module only normalizes
their checklist entries for reports; it does not write or enrich catalogs.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


VALID_NEXT_ACTIONS = {
    "none",
    "map_existing_owner",
    "strengthen_oracle",
    "add_focused_case",
    "qualify_profile",
    "defer",
}

CHECK_FIELDS = (
    "id",
    "acceptance",
    "preconditions",
    "case_ids",
    "assertion_owners",
    "coverage",
    "gap",
    "next_action",
)


def checklist_rows(catalog: Mapping[str, Any], group: str) -> list[dict[str, Any]]:
    """Return report rows for valid authored checks in one catalog."""

    rows: list[dict[str, Any]] = []
    behaviors = catalog.get("behaviors", [])
    if not isinstance(behaviors, list):
        return rows
    for behavior in behaviors:
        if not isinstance(behavior, Mapping):
            continue
        behavior_id = behavior.get("id")
        checks = behavior.get("checks", [])
        if not isinstance(behavior_id, str) or not isinstance(checks, list):
            continue
        for check in checks:
            if not isinstance(check, Mapping):
                continue
            check_id = check.get("id")
            if not isinstance(check_id, str) or not check_id:
                continue
            row = {
                "group": group,
                "behavior": behavior_id,
                "id": check_id,
            }
            for field in CHECK_FIELDS:
                if field in check:
                    value = check[field]
                    if isinstance(value, list):
                        value = list(value)
                    row[field] = value
            # ``check_id`` is a convenient stable alias for consumers that
            # aggregate rows without knowing the authored field names.
            row["check_id"] = check_id
            rows.append(row)
    return rows


def case_ledger_rows(catalog: Mapping[str, Any], group: str) -> list[dict[str, Any]]:
    """Return one derived qualification row for every authored case."""

    rows: list[dict[str, Any]] = []
    cases = catalog.get("cases", [])
    if not isinstance(cases, list):
        return rows
    for case in cases:
        if not isinstance(case, Mapping) or not isinstance(case.get("id"), str):
            continue
        row = {
            "group": group,
            "id": case["id"],
            "behavior": case.get("behavior"),
            "coverage": case.get("coverage"),
            "gap": case.get("gap", ""),
            "next_action": case.get("next_action"),
        }
        rows.append(row)
    return rows


def case_action_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize all case actions so scoped triage remains inspectable."""

    actions = Counter(row.get("next_action") for row in rows)
    coverage = Counter(row.get("coverage") for row in rows)
    return {
        "total": len(rows),
        "next_action": {
            str(key): value for key, value in sorted(actions.items(), key=lambda item: str(item[0]))
        },
        "coverage": {
            str(key): value for key, value in sorted(coverage.items(), key=lambda item: str(item[0]))
        },
        "non_asserted": sum(value for key, value in coverage.items() if key != "asserted"),
    }


def coverage_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize checklist coverage without changing authored values."""

    counts = Counter(
        row.get("coverage")
        for row in rows
        if isinstance(row.get("coverage"), str)
    )
    asserted = counts.get("asserted", 0)
    return {
        "total": len(rows),
        "coverage": dict(sorted(counts.items())),
        "asserted": asserted,
        "non_asserted": len(rows) - asserted,
    }


def functional_check_coverage(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize checks that qualify functional behavior.

    Performance measurement checks remain visible in the all-check summary,
    but are kept out of functional qualification totals.
    """

    return coverage_summary(
        [row for row in rows if row.get("group") != "performance"]
    )


def measurement_check_coverage(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize the performance measurement checklist only."""

    return coverage_summary(
        [row for row in rows if row.get("group") == "performance"]
    )


def gap_ledger(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return actionable residual gaps derived from non-asserted checks."""

    result: list[dict[str, Any]] = []
    for row in rows:
        if row.get("coverage") == "asserted":
            continue
        result.append(
            {
                "group": row.get("group"),
                "behavior": row.get("behavior"),
                "check": row.get("id"),
                "coverage": row.get("coverage"),
                "gap": row.get("gap", ""),
                "next_action": row.get("next_action"),
                "case_ids": list(row.get("case_ids", []))
                if isinstance(row.get("case_ids"), list)
                else row.get("case_ids"),
            }
        )
    return result


def aggregate_behavior_coverage(
    authored: object, rows: list[Mapping[str, Any]]
) -> str:
    """Compute a behavior's aggregate coverage from its checks.

    A family is asserted only when every authored check is asserted.  A
    partially checked family is represented as fixture-only for the compact
    report while the check rows retain the exact residual state.
    """

    if not rows:
        return authored if isinstance(authored, str) else "missing"
    coverages = [row.get("coverage") for row in rows]
    if all(value == "asserted" for value in coverages):
        return "asserted"
    if any(value in {"asserted", "fixture_only"} for value in coverages):
        return "fixture_only"
    if all(value == "deferred" for value in coverages):
        return "deferred"
    if all(value == "unsupported" for value in coverages):
        return "unsupported"
    return "missing"


__all__ = [
    "CHECK_FIELDS",
    "VALID_NEXT_ACTIONS",
    "aggregate_behavior_coverage",
    "case_action_summary",
    "case_ledger_rows",
    "checklist_rows",
    "coverage_summary",
    "functional_check_coverage",
    "gap_ledger",
    "measurement_check_coverage",
]
