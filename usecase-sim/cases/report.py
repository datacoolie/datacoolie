"""Review-sized report rendering for the simulator case catalog."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping


def list_groups(root: str | Path | None = None) -> list[dict[str, Any]]:
    """List groups and case counts, including groups with no runnable case."""

    from cases.catalog import audit_catalog

    report = audit_catalog(root)
    rows = []
    catalogs = report.get("catalogs", {})
    for row in report.get("groups", []):
        catalog = catalogs.get(row["group"], {})
        cases = catalog.get("cases", []) if isinstance(catalog, dict) else []
        rows.append(
            {
                **row,
                "case_ids": [
                    case.get("id")
                    for case in cases
                    if isinstance(case, dict) and case.get("id")
                ],
                "runnable": bool(row.get("dedicated_scenarios")),
            }
        )
    return rows


def compact_audit_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """Render an audit summary without duplicating full catalogs."""

    groups = []
    for row in report.get("groups", []):
        groups.append(
            {
                "group": row.get("group"),
                "behaviors": row.get("behaviors", 0),
                "cases": row.get("cases", 0),
                "coverage": row.get("coverage", {}),
                "behavior_coverage": row.get("behavior_coverage", {}),
                "execution": row.get("execution", {}),
                "dedicated_scenarios": row.get("dedicated_scenarios", []),
                "checks": row.get("checks", []),
                "check_coverage": row.get("check_coverage", {}),
                "functional_check_coverage": row.get("functional_check_coverage", {}),
                "measurement_check_coverage": row.get("measurement_check_coverage", {}),
                "gap_ledger": row.get("gap_ledger", []),
                "case_ledger": row.get("case_ledger", []),
                "case_actions": row.get("case_actions", {}),
            }
        )
    profile_cells = [
        cell for cell in report.get("profiles", []) if isinstance(cell, Mapping)
    ]
    profile_counts = Counter(cell.get("status", "not_run") for cell in profile_cells)
    profile_requirements: dict[str, dict[str, int]] = defaultdict(
        lambda: {"required": 0, "configured": 0, "missing": 0}
    )
    for cell in profile_cells:
        group = str(cell.get("group", ""))
        summary = profile_requirements[group]
        summary["required"] += 1
        if cell.get("configured_scenarios"):
            summary["configured"] += 1
        if cell.get("status") == "blocked":
            summary["missing"] += 1
    execution = {
        name: {
            "status": value.get("status", "not_run"),
            "classification": value.get("classification", "none"),
        }
        for name, value in report.get("execution", {}).items()
        if isinstance(value, Mapping)
    }
    configured_cells = sum(1 for cell in profile_cells if cell.get("configured_scenarios"))
    profile_cell_summary = {
        "required": len(profile_cells),
        "configured": configured_cells,
        "missing": sum(1 for cell in profile_cells if cell.get("status") == "blocked"),
        "statuses": dict(sorted(profile_counts.items())),
    }
    return {
        "ok": report.get("ok", False),
        "root": report.get("root"),
        "group": report.get("group"),
        "validation_scope": report.get("validation_scope", "all_catalogs"),
        "groups": groups,
        "profile_summary": dict(sorted(profile_counts.items())),
        "profile_requirements": dict(sorted(profile_requirements.items())),
        "profile_cells": [
            dict(cell)
            for cell in profile_cells
            if cell.get("status") in {"blocked", "failed", "historical"}
        ][:25],
        "profile_cell_summary": profile_cell_summary,
        "execution": execution,
        "checks": [
            dict(row)
            for row in report.get("checks", [])
            if isinstance(row, Mapping)
        ],
        "check_coverage": report.get("check_coverage", {}),
        "functional_check_coverage": report.get("functional_check_coverage", {}),
        "measurement_check_coverage": report.get("measurement_check_coverage", {}),
        "gap_ledger": [
            dict(row)
            for row in report.get("gap_ledger", [])
            if isinstance(row, Mapping)
        ],
        "case_ledger": [
            dict(row)
            for row in report.get("case_ledger", [])
            if isinstance(row, Mapping)
        ],
        "case_actions": report.get("case_actions", {}),
        "errors": report.get("errors", []),
    }


__all__ = ["compact_audit_report", "list_groups"]
