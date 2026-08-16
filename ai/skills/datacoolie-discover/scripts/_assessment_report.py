"""Render validated object decisions as a deterministic Markdown table."""
from __future__ import annotations

from typing import Any, Iterable, Mapping


def _escape(value: Any) -> str:
    return str(value).replace("\r", " ").replace("\n", " ").replace("|", "\\|").strip()


def _report_row(decision: Mapping[str, Any]) -> list[str]:
    match = decision["match"]
    qualified = ".".join(
        part for part in (match["catalog"], match["schema"], match["object"]) if part
    )
    object_name = f"{match['source']}:{match['object_type']}:{qualified}"
    if match["source_operation"]:
        object_name += f" [{match['source_operation']}]"
    candidates = ", ".join(
        f"{item['column']} ({item['roles']})" for item in decision["candidates"]
    ) or "None"
    coverage = decision["coverage"]
    if decision.get("delete_evidence"):
        coverage += f"; persistent delete evidence: {decision['delete_evidence']}"
    return [
        object_name, candidates, coverage, decision["limitations"],
        decision["fallback"], decision["decision_required"],
    ]


def render_report_table(decisions: Iterable[Mapping[str, Any]]) -> str:
    lines = [
        "| object | candidate set | coverage/type | limitations | fallback | decision required |",
        "|---|---|---|---|---|---|",
    ]
    lines.extend(
        "| " + " | ".join(_escape(value) for value in _report_row(decision)) + " |"
        for decision in decisions
    )
    return "\n".join(lines) + "\n"
