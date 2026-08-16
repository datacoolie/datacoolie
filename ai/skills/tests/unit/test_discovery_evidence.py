"""Tests for the canonical discovery evidence and targeted enrichment helpers."""
from __future__ import annotations

import csv
import json

import pytest
from sqlalchemy import create_engine, text

import assess_watermarks
import enrich_observations
import merge_observations
import probe_db
from _artifact_io import atomic_write_text
from _input_safety import validate_nonsecret_locator
from _probe_status import write_probe_status
from _observation_contract import (
    CSV_HEADER,
    KEY_FIELDS,
    canonicalize_watermark_candidate,
    make_observation,
    read_observations,
    validate_observation,
    write_observations,
)


def _observation(column: str = "updated_at") -> dict[str, str]:
    return make_observation(
        source="erp",
        object_type="table",
        schema="sales",
        object="orders",
        column=column,
        native_type="datetime2",
        data_type="timestamp",
        nullable="true",
        ordinal=1,
    )


def test_shared_contract_has_exact_19_columns_and_stable_key():
    assert CSV_HEADER == [
        "source", "object_type", "catalog", "schema", "object", "source_operation",
        "column", "native_type", "data_type", "format", "precision", "scale",
        "nullable", "ordinal", "key", "reference", "row_estimate",
        "watermark_candidate", "notes",
    ]
    assert tuple(CSV_HEADER[:7]) == KEY_FIELDS


def test_watermark_roles_require_unique_canonical_tokens():
    assert canonicalize_watermark_candidate("auxiliary|append|update") == (
        "update|append|auxiliary"
    )
    row = _observation()
    row["watermark_candidate"] = "auxiliary|append"
    with pytest.raises(ValueError, match="canonical order"):
        validate_observation(row)
    row["watermark_candidate"] = "update|update"
    with pytest.raises(ValueError, match="unique"):
        validate_observation(row)
    row["watermark_candidate"] = "observed"
    with pytest.raises(ValueError, match="Unknown"):
        validate_observation(row)


def test_key_and_reference_contract_rejects_legacy_shapes():
    row = _observation()
    row["key"] = "unique:uq_orders"
    row["reference"] = "sales.customers.customer_id"
    validate_observation(row)
    row["key"] = "unique:"
    with pytest.raises(ValueError, match="key must"):
        validate_observation(row)
    row = _observation()
    row["reference"] = "→ sales.customers.customer_id"
    with pytest.raises(ValueError, match="display decoration"):
        validate_observation(row)


def test_annotation_merge_preserves_generated_order(tmp_path):
    input_path = tmp_path / "observations.csv"
    output_path = tmp_path / "enriched.csv"
    annotation_path = tmp_path / "annotations.json"
    row = _observation()
    with input_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerow(row)
    annotation_path.write_text(json.dumps([{
        "match": {field: row[field] for field in KEY_FIELDS},
        "set": {"watermark_candidate": "update"},
        "append_notes": "Confirmed by source owner",
    }]), encoding="utf-8")

    assert enrich_observations.enrich(input_path, annotation_path, output_path) == 1
    with output_path.open(newline="", encoding="utf-8") as handle:
        result = list(csv.DictReader(handle))
    assert result[0]["watermark_candidate"] == "update"
    assert "Confirmed by source owner" in result[0]["notes"]
    assert list(result[0]) == CSV_HEADER


def test_annotation_merge_rejects_unknown_key(tmp_path):
    input_path = tmp_path / "observations.csv"
    row = _observation()
    with input_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerow(row)
    unknown = {field: row[field] for field in KEY_FIELDS}
    unknown["object"] = "missing"
    annotations = tmp_path / "annotations.json"
    annotations.write_text(json.dumps([{
        "match": unknown,
        "set": {"watermark_candidate": "update"},
    }]), encoding="utf-8")

    with pytest.raises(ValueError, match="unknown observation"):
        enrich_observations.enrich(input_path, annotations, tmp_path / "output.csv")


def test_merge_is_deterministic_and_rejects_duplicate_keys(tmp_path):
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    output = tmp_path / "observations.csv"
    with first.open("w", newline="", encoding="utf-8") as handle:
        write_observations(handle, [_observation("z_column")])
    with second.open("w", newline="", encoding="utf-8") as handle:
        write_observations(handle, [_observation("a_column")])

    assert merge_observations.merge([first, second], output) == 2
    assert [row["column"] for row in read_observations(output)] == [
        "a_column", "z_column",
    ]

    with pytest.raises(ValueError, match="Duplicate observation key"):
        merge_observations.merge([first, first], output)


def test_reader_rejects_malformed_rows(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text(",".join(CSV_HEADER) + "\nonly,three,values\n", encoding="utf-8")
    with pytest.raises(ValueError, match="malformed CSV row"):
        read_observations(path)


def test_reader_rejects_any_noncanonical_header(tmp_path):
    path = tmp_path / "invalid.csv"
    path.write_text(",".join(CSV_HEADER[:-1]) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        read_observations(path)


def test_probe_status_distinguishes_complete_and_partial(tmp_path):
    status_path = tmp_path / "status.json"
    assert write_probe_status(
        status_path, source="erp", probe="database", row_count=4,
    ) == "complete"
    assert json.loads(status_path.read_text(encoding="utf-8"))["status"] == "complete"

    assert write_probe_status(
        status_path,
        source="erp",
        probe="database",
        row_count=3,
        issues=["one table was inaccessible"],
    ) == "partial"
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["status"] == "partial"
    assert payload["row_count"] == 3


def test_atomic_text_failure_preserves_existing_artifact(tmp_path, monkeypatch):
    path = tmp_path / "evidence.json"
    path.write_text("original\n", encoding="utf-8")

    def fail_replace(*_args):
        raise OSError("simulated replacement failure")

    monkeypatch.setattr("_artifact_io.os.replace", fail_replace)
    with pytest.raises(OSError, match="simulated"):
        atomic_write_text(path, "replacement\n")
    assert path.read_text(encoding="utf-8") == "original\n"


@pytest.mark.parametrize("locator", [
    "https://user:password@example.test/schema",
    "https://example.test/schema?access_token=secret",
    "s3://bucket/path?X-Amz-Signature=secret",
])
def test_secret_bearing_locators_are_rejected(locator):
    with pytest.raises(ValueError):
        validate_nonsecret_locator(locator, "source")


def test_nonsecret_locator_query_is_allowed():
    locator = "https://example.test/schema?api-version=1"
    assert validate_nonsecret_locator(locator, "source") == locator


def test_annotation_rejects_unknown_top_level_fields(tmp_path):
    annotations = tmp_path / "annotations.json"
    row = _observation()
    annotations.write_text(json.dumps([{
        "match": {field: row[field] for field in KEY_FIELDS},
        "set": {"watermark_candidate": "update"},
        "unsupported": True,
    }]), encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported fields"):
        enrich_observations.load_annotations(annotations)


def test_watermark_assessment_shortlists_roles_and_emits_no_candidate_row():
    rows = [
        _observation("created_at"),
        _observation("updated_at"),
        _observation("deleted_at"),
        make_observation(
            source="erp", object_type="table", schema="sales", object="orders",
            column="order_id", native_type="bigint", data_type="long", ordinal=4,
            key="primary",
        ),
        make_observation(
            source="erp", object_type="table", schema="sales", object="orders",
            column="customer_id", native_type="bigint", data_type="long", ordinal=5,
            reference="sales.customers.customer_id",
        ),
        _observation("transaction_date"),
        make_observation(
            source="erp", object_type="table", schema="sales", object="orders",
            column="row_version", native_type="rowversion", data_type="binary", ordinal=6,
        ),
        make_observation(
            source="erp", object_type="table", schema="sales", object="customers",
            column="description", native_type="varchar", data_type="string", ordinal=1,
        ),
    ]
    assessment = assess_watermarks.build_assessment(rows)
    roles = {row["column"]: row["suggested_roles"] for row in assessment}
    assert roles["created_at"] == "insert"
    assert roles["updated_at"] == "update"
    assert roles["deleted_at"] == "delete"
    assert roles["order_id"] == "append|auxiliary"
    assert "customer_id" not in roles
    assert roles["transaction_date"] == "backward"
    assert roles["row_version"] == "change"
    no_candidate = next(row for row in assessment if row["object"] == "customers")
    assert no_candidate["column"] == ""
    assert no_candidate["suggested_roles"] == ""
    assert "human decision" in no_candidate["reason"]


def test_watermark_assessment_writes_exact_scratch_contract(tmp_path):
    observations_path = tmp_path / "observations.csv"
    assessment_path = tmp_path / "watermark-assessment.csv"
    with observations_path.open("w", newline="", encoding="utf-8") as handle:
        write_observations(handle, [_observation("updated_at")])

    assert assess_watermarks.assess(observations_path, assessment_path) == 1
    with assessment_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert reader.fieldnames == assess_watermarks.ASSESSMENT_HEADER
    assert rows[0]["suggested_roles"] == "update"


@pytest.mark.parametrize("sql", [
    "UPDATE items SET value = 1",
    "SELECT * INTO copied_items FROM items",
    "SELECT * FROM items FOR UPDATE",
    "SELECT * FROM items; SELECT * FROM items",
    "WITH changed AS (DELETE FROM items RETURNING *) SELECT * FROM changed",
])
def test_probe_rejects_mutating_or_multiple_statements(sql):
    with pytest.raises(ValueError):
        probe_db.validate_read_only_sql(sql)


def test_probe_accepts_comments_and_mutation_words_in_literals():
    sql = "-- bounded evidence\nSELECT 'update is text' AS note"
    assert probe_db.validate_read_only_sql(sql) == sql


def test_probe_limits_rows_and_rolls_back_sqlite(tmp_path):
    database = tmp_path / "probe.db"
    engine = create_engine(f"sqlite:///{database}")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE items (id INTEGER)"))
        connection.execute(text("INSERT INTO items VALUES (1), (2), (3)"))
    engine.dispose()

    output = tmp_path / "probe.json"
    probe_db.run_probe(
        f"sqlite:///{database}", "SELECT id FROM items ORDER BY id", output, 2, 5,
    )
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["rows"] == [[1], [2]]
    assert payload["truncated"] is True
    assert payload["timeout_enforced"] is True
    assert payload["read_only_enforced"] is True
