"""Tests for normalized discovery assessment and source-bound refresh."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import assess_watermarks
import enrich_observations
import finalize_watermark_assessment
import merge_observations
from _observation_contract import KEY_FIELDS, make_observation, read_observations, write_observations
from _watermark_signals import suggest_roles, tokenize_identifier


def _row(
    source: str,
    object_name: str,
    column: str,
    *,
    data_type: str = "timestamp",
    native_type: str = "datetime",
    key: str = "",
    ordinal: int = 1,
) -> dict[str, str]:
    return make_observation(
        source=source,
        object_type="table",
        schema="sales",
        object=object_name,
        column=column,
        native_type=native_type,
        data_type=data_type,
        key=key,
        ordinal=ordinal,
    )


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        write_observations(handle, rows)


@pytest.mark.parametrize("name", [
    "UpdatedAt", "updated_at", "updated-at", "lastUpdateTime", "LAST_UPDATE_TIME",
])
def test_identifier_variants_produce_the_same_update_signal(name):
    roles, reason = suggest_roles(_row("erp", "orders", name))
    assert roles == "update"
    assert "temporal" in reason


def test_identifier_tokenization_and_structural_guards():
    assert tokenize_identifier("HTTPResponseUpdatedAt") == (
        "http", "response", "updated", "at",
    )
    assert suggest_roles(_row("erp", "orders", "ROW_VERSION", data_type="binary"))[0] == (
        "change"
    )
    assert suggest_roles(_row("erp", "orders", "event_time"))[0] == ""
    identity = _row(
        "erp", "orders", "orderId", data_type="long", native_type="bigint", key="primary",
    )
    assert suggest_roles(identity)[0] == "append|auxiliary"


def test_bounded_query_reference_matches_probe_io_contract():
    skill_dir = Path(__file__).parents[2] / "datacoolie-discover"
    reference = (skill_dir / "references/evidence-queries.md").read_text(encoding="utf-8")
    skill = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
    assert "does not bind query parameters" in reference
    assert "scratch JSON `--output`" in reference
    assert ".scratch/discover/object-summary.json" in skill


def test_assessment_writes_one_compact_summary_per_object(tmp_path):
    observations = tmp_path / "observations.csv"
    shortlist = tmp_path / "shortlist.csv"
    summary = tmp_path / "summary.json"
    _write_rows(observations, [
        _row("erp", "orders", "orderId", data_type="long", key="primary"),
        _row("erp", "orders", "UpdatedAt", ordinal=2),
        _row("erp", "customers", "description", data_type="string", native_type="varchar"),
    ])

    assert assess_watermarks.assess(observations, shortlist, summary) == 3
    payload = json.loads(summary.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert [item["match"]["object"] for item in payload["objects"]] == [
        "customers", "orders",
    ]
    orders = payload["objects"][1]
    assert orders["column_count"] == 2
    assert orders["keys"] == [{"column": "orderId", "key": "primary"}]
    assert {item["column"] for item in orders["signals"]} == {"orderId", "UpdatedAt"}


def test_assessment_rejects_contradictory_object_row_estimates():
    first = _row("erp", "orders", "CreatedAt")
    second = _row("erp", "orders", "UpdatedAt", ordinal=2)
    first["row_estimate"], second["row_estimate"] = "10", "11"
    with pytest.raises(ValueError, match="contradictory row estimates"):
        assess_watermarks.build_object_summary([first, second])


def _decision(match: dict[str, str], **overrides) -> dict:
    item = {
        "match": match,
        "outcome": "human_decision",
        "candidates": [],
        "coverage": "No reliable mutation signal",
        "limitations": "Source semantics are unknown",
        "fallback": "Assess full refresh feasibility",
        "decision_required": "Owner must select a safe load pattern",
        "delete_evidence": "",
    }
    item.update(overrides)
    return item


def test_finalizer_generates_annotations_and_complete_report_table(tmp_path):
    observations = tmp_path / "observations.csv"
    rows = [
        _row("erp", "orders", "CreatedAt"),
        _row("erp", "orders", "UpdatedAt", ordinal=2),
        _row("erp", "customers", "description", data_type="string", native_type="varchar"),
    ]
    rows[2]["watermark_candidate"] = "change"
    _write_rows(observations, rows)
    order_match = {field: rows[0][field] for field in KEY_FIELDS[:-1]}
    customer_match = {field: rows[2][field] for field in KEY_FIELDS[:-1]}
    decisions = tmp_path / "decisions.json"
    decisions.write_text(json.dumps({
        "schema_version": 1,
        "objects": [
            _decision(
                order_match,
                outcome="confirmed_candidate",
                candidates=[
                    {"column": "CreatedAt", "roles": "insert"},
                    {"column": "UpdatedAt", "roles": "update"},
                ],
                coverage="Verified inserts and updates",
                limitations="Hard deletes are not observable",
                fallback="Periodic reconciliation for deletes",
                decision_required="Design must select merge behavior",
            ),
            _decision(customer_match),
        ],
    }), encoding="utf-8")
    annotations = tmp_path / "annotations.json"
    report = tmp_path / "watermark-table.md"

    assert finalize_watermark_assessment.finalize(
        observations, decisions, annotations, report,
    ) == 2
    annotation_rows = json.loads(annotations.read_text(encoding="utf-8"))
    assert [item["match"]["column"] for item in annotation_rows] == [
        "description", "CreatedAt", "UpdatedAt",
    ]
    assert annotation_rows[0]["set"]["watermark_candidate"] == ""
    enriched = tmp_path / "enriched.csv"
    assert enrich_observations.enrich(observations, annotations, enriched) == 3
    roles_by_object_column = {
        (row["object"], row["column"]): row["watermark_candidate"]
        for row in read_observations(enriched)
    }
    assert roles_by_object_column == {
        ("customers", "description"): "",
        ("orders", "CreatedAt"): "insert",
        ("orders", "UpdatedAt"): "update",
    }
    table = report.read_text(encoding="utf-8")
    assert table.count("\n| erp:table:sales.") == 2
    assert "Owner must select a safe load pattern" in table


def test_finalizer_rejects_missing_object_and_unproved_delete(tmp_path):
    observations = tmp_path / "observations.csv"
    row = _row("erp", "orders", "DeletedAt")
    _write_rows(observations, [row])
    decisions = tmp_path / "decisions.json"
    decisions.write_text(json.dumps({"schema_version": 1, "objects": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="missing object decisions"):
        finalize_watermark_assessment.finalize(
            observations, decisions, tmp_path / "a.json", tmp_path / "r.md",
        )

    match = {field: row[field] for field in KEY_FIELDS[:-1]}
    decisions.write_text(json.dumps({
        "schema_version": 1,
        "objects": [_decision(
            match,
            outcome="confirmed_candidate",
            candidates=[{"column": "DeletedAt", "roles": "delete"}],
        )],
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="persistent delete evidence"):
        finalize_watermark_assessment.finalize(
            observations, decisions, tmp_path / "a.json", tmp_path / "r.md",
        )
    payload = json.loads(decisions.read_text(encoding="utf-8"))
    payload["objects"][0]["delete_evidence"] = "Durable tombstone remains queryable"
    decisions.write_text(json.dumps(payload), encoding="utf-8")
    assert finalize_watermark_assessment.finalize(
        observations, decisions, tmp_path / "a.json", tmp_path / "r.md",
    ) == 1
    assert "Durable tombstone remains queryable" in (tmp_path / "r.md").read_text(
        encoding="utf-8",
    )


def test_refresh_replaces_only_explicit_source_and_writes_diff(tmp_path):
    base = tmp_path / "base.csv"
    replacement = tmp_path / "replacement.csv"
    output = tmp_path / "candidate.csv"
    diff = tmp_path / "diff.json"
    status = tmp_path / "erp-status.json"
    _write_rows(base, [
        _row("erp", "orders", "UpdatedAt"),
        _row("crm", "customers", "CustomerId", data_type="long", native_type="bigint"),
    ])
    changed = _row("erp", "orders", "UpdatedAt", native_type="datetime2")
    _write_rows(replacement, [changed])
    status.write_text(json.dumps({
        "source": "erp", "status": "complete", "row_count": 1,
    }), encoding="utf-8")

    assert merge_observations.merge(
        [replacement], output, base=base, replace_sources=["erp"],
        status_inputs=[status], diff_output=diff,
    ) == 2
    result = read_observations(output)
    assert next(row for row in result if row["source"] == "crm")["column"] == "CustomerId"
    payload = json.loads(diff.read_text(encoding="utf-8"))
    assert payload["summary"] == {"added": 0, "removed": 0, "changed": 1}
    assert set(payload["changed"][0]["fields"]) == {"native_type"}


def test_refresh_rejects_partial_status_without_exact_acceptance(tmp_path):
    base = tmp_path / "base.csv"
    replacement = tmp_path / "replacement.csv"
    status = tmp_path / "status.json"
    _write_rows(base, [_row("erp", "orders", "UpdatedAt")])
    _write_rows(replacement, [_row("erp", "orders", "UpdatedAt")])
    status.write_text(json.dumps({
        "source": "erp", "status": "partial", "row_count": 1,
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="explicit acceptance"):
        merge_observations.merge(
            [replacement], tmp_path / "candidate.csv", base=base,
            replace_sources=["erp"], status_inputs=[status],
        )
    assert merge_observations.merge(
        [replacement], tmp_path / "accepted.csv", base=base,
        replace_sources=["erp"], status_inputs=[status], accept_partial_sources=["erp"],
    ) == 1


def test_refresh_rejects_status_row_count_mismatch(tmp_path):
    base = tmp_path / "base.csv"
    replacement = tmp_path / "replacement.csv"
    status = tmp_path / "status.json"
    _write_rows(base, [_row("erp", "orders", "UpdatedAt")])
    _write_rows(replacement, [_row("erp", "orders", "UpdatedAt")])
    status.write_text(json.dumps({
        "source": "erp", "status": "complete", "row_count": 2,
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="probe status reports 2"):
        merge_observations.merge(
            [replacement], tmp_path / "candidate.csv", base=base,
            replace_sources=["erp"], status_inputs=[status],
        )


def test_refresh_rejects_duplicate_status_for_source_boundary(tmp_path):
    base = tmp_path / "base.csv"
    replacement = tmp_path / "replacement.csv"
    first_status = tmp_path / "status-1.json"
    second_status = tmp_path / "status-2.json"
    _write_rows(base, [_row("erp", "orders", "UpdatedAt")])
    _write_rows(replacement, [_row("erp", "orders", "UpdatedAt")])
    for status in (first_status, second_status):
        status.write_text(json.dumps({
            "source": "erp", "status": "complete", "row_count": 1,
        }), encoding="utf-8")

    with pytest.raises(ValueError, match="repeats source boundary: erp"):
        merge_observations.merge(
            [replacement], tmp_path / "candidate.csv", base=base,
            replace_sources=["erp"], status_inputs=[first_status, second_status],
        )
