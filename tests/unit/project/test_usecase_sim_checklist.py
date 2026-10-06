"""Derived Amendment 6 checklist and gap-ledger contract tests."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_checklist_module():
    path = Path(__file__).resolve().parents[3] / "usecase-sim" / "cases" / "checklist.py"
    spec = importlib.util.spec_from_file_location("usecase_sim_checklist_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_check_rows_and_gap_ledger_are_derived_without_mutating_catalog():
    module = _load_checklist_module()
    catalog = {
        "group": "demo",
        "behaviors": [
            {
                "id": "demo.behavior",
                "coverage": "asserted",
                "checks": [
                    {
                        "id": "demo.behavior.asserted",
                        "acceptance": "the observable result is exact",
                        "preconditions": ["seed the fixture"],
                        "case_ids": ["demo.case"],
                        "assertion_owners": ["tests/test_demo.py::test_result"],
                        "coverage": "asserted",
                        "gap": "execution profile remains separately qualified",
                        "next_action": "qualify_profile",
                    },
                    {
                        "id": "demo.behavior.pending",
                        "acceptance": "the provider edge is documented",
                        "preconditions": ["select the provider profile"],
                        "case_ids": [],
                        "assertion_owners": [],
                        "coverage": "fixture_only",
                        "gap": "no provider assertion covers this edge",
                        "next_action": "add_focused_case",
                    },
                ],
            }
        ],
    }

    rows = module.checklist_rows(catalog, "demo")
    summary = module.coverage_summary(rows)
    ledger = module.gap_ledger(rows)

    assert [row["id"] for row in rows] == [
        "demo.behavior.asserted",
        "demo.behavior.pending",
    ]
    assert summary["coverage"] == {"asserted": 1, "fixture_only": 1}
    assert summary["asserted"] == 1
    assert summary["non_asserted"] == 1
    assert len(ledger) == 1
    assert ledger[0]["check"] == "demo.behavior.pending"
    assert ledger[0]["next_action"] == "add_focused_case"
    assert len(catalog["behaviors"][0]["checks"]) == 2


def test_behavior_coverage_is_asserted_only_when_every_check_is_asserted():
    module = _load_checklist_module()
    pending = [{"coverage": "asserted"}, {"coverage": "fixture_only"}]
    complete = [{"coverage": "asserted"}, {"coverage": "asserted"}]

    assert module.aggregate_behavior_coverage("asserted", pending) == "fixture_only"
    assert module.aggregate_behavior_coverage("fixture_only", complete) == "asserted"
    assert module.aggregate_behavior_coverage("missing", []) == "missing"


def test_case_ledger_keeps_every_case_action_for_scoped_triage():
    module = _load_checklist_module()
    catalog = {
        "cases": [
            {"id": "demo.asserted", "behavior": "demo.b", "coverage": "asserted", "next_action": "qualify_profile"},
            {"id": "demo.pending", "behavior": "demo.b", "coverage": "fixture_only", "gap": "needs a focused assertion", "next_action": "add_focused_case"},
        ]
    }

    rows = module.case_ledger_rows(catalog, "demo")
    summary = module.case_action_summary(rows)

    assert [row["id"] for row in rows] == ["demo.asserted", "demo.pending"]
    assert summary["total"] == 2
    assert summary["next_action"] == {"add_focused_case": 1, "qualify_profile": 1}
    assert summary["non_asserted"] == 1
