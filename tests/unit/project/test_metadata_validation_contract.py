from __future__ import annotations

from datacoolie.project.validation.metadata import validate_metadata_document


def _metadata() -> dict:
    return {
        "connections": [
            {
                "name": "source",
                "connection_type": "file",
                "configure": {"schema_hint_type_system": "postgresql"},
            },
            {"name": "destination", "connection_type": "file"},
        ],
        "dataflows": [
            {
                "name": "orders",
                "stage": "ingest",
                "source": {"connection_name": "source"},
                "destination": {
                    "connection_name": "destination",
                    "table": "orders",
                },
            }
        ],
    }


def test_framework_validation_is_the_gate_and_contextual_advice_is_not_a_second_linter() -> (
    None
):
    report = validate_metadata_document(_metadata())
    assert report.ok is True
    assert "model-constraints" in report.checks


def test_validation_reports_missing_query_files_when_a_root_is_explicit(
    tmp_path,
) -> None:
    metadata = _metadata()
    metadata["dataflows"][0]["source"]["query"] = "sql/missing.sql"
    report = validate_metadata_document(metadata, sql_root=tmp_path / "sql")
    assert any(item.code == "query.missing" for item in report.errors)


def test_validation_resolves_source_aware_hint_parameters_offline() -> None:
    metadata = _metadata()
    metadata["dataflows"][0]["transform"] = {
        "schema_hints": [
            {"column_name": "id", "data_type": "int8"},
            {"column_name": "amount", "data_type": "numeric"},
        ],
    }
    report = validate_metadata_document(metadata, framework_version="0.2.0")
    assert not report.ok
    assert any(item.code == "metadata.datatype" for item in report.errors)
    assert report.details["datatype_hints_checked"] == 2


def test_validation_accepts_explicit_decimal_and_vendor_override() -> None:
    metadata = _metadata()
    metadata["dataflows"][0]["transform"] = {
        "schema_hints": [
            {"column_name": "id", "data_type": "int8"},
            {"column_name": "amount", "data_type": "numeric(18,2)"},
        ],
    }
    report = validate_metadata_document(metadata, framework_version="0.2.0")
    assert report.ok is True
    assert report.details["datatype_hints_checked"] == 2


def test_validation_checks_shared_schema_hints_with_source_dialect() -> None:
    metadata = _metadata()
    metadata["schema_hints"] = [
        {
            "connection_name": "source",
            "table_name": "orders",
            "hints": [
                {"column_name": "id", "data_type": "int"},
                {"column_name": "amount", "data_type": "decimal"},
            ],
        }
    ]
    report = validate_metadata_document(metadata)
    assert not report.ok
    assert report.details["grouped_datatype_hints_checked"] == 1
    assert any(
        item.path == "schema_hints[0].hints[1]"
        and item.code == "metadata.datatype"
        for item in report.errors
    )


def test_validation_counts_active_shared_schema_hints_only() -> None:
    metadata = _metadata()
    metadata["schema_hints"] = [
        {
            "connection_name": "source",
            "table_name": "orders",
            "hints": [
                {"column_name": "id", "data_type": "int", "is_active": False},
                {"column_name": "amount", "data_type": "decimal(18,2)"},
            ],
        }
    ]
    report = validate_metadata_document(metadata)
    assert report.ok is True
    assert report.details["datatype_hints_checked"] == 1
    assert report.details["grouped_datatype_hints_checked"] == 1


def test_validation_rejects_null_or_whitespace_only_dataflow_identity() -> None:
    metadata = _metadata()
    metadata["dataflows"][0]["name"] = None
    report = validate_metadata_document(metadata)
    assert not report.ok
    assert any(
        item.code == "metadata.dataflows"
        and "non-empty dataflow_id or name" in item.message
        for item in report.errors
    )

    metadata["dataflows"][0]["name"] = "   "
    report = validate_metadata_document(metadata)
    assert not report.ok
    assert any("non-empty dataflow_id or name" in item.message for item in report.errors)

    metadata["dataflows"][0]["name"] = None
    metadata["dataflows"][0]["dataflow_id"] = "   "
    report = validate_metadata_document(metadata)
    assert not report.ok
    assert any("non-empty dataflow_id or name" in item.message for item in report.errors)


def test_validation_accepts_explicit_dataflow_id_without_name() -> None:
    metadata = _metadata()
    metadata["dataflows"][0].pop("name")
    metadata["dataflows"][0]["dataflow_id"] = "df-explicit"
    report = validate_metadata_document(metadata)
    assert report.ok is True


def test_validation_rejects_inline_connection_identity_conflict() -> None:
    metadata = _metadata()
    metadata["connections"][0]["connection_id"] = "source-id"
    source_id = metadata["connections"][0]["connection_id"]
    metadata["dataflows"][0]["source"] = {
        "connection": {
            "connection_id": source_id,
            "name": "different-name",
            "connection_type": "file",
            "format": "parquet",
        }
    }
    report = validate_metadata_document(metadata)
    assert not report.ok
    assert any(
        item.code == "metadata.dataflows"
        and "conflicting name" in item.message
        for item in report.errors
    )


def test_validation_allows_distinct_explicit_ids_with_duplicate_display_names() -> None:
    metadata = _metadata()
    metadata["connections"] = [
        {
            "connection_id": "source-1",
            "name": "shared",
            "connection_type": "file",
            "format": "parquet",
        },
        {
            "connection_id": "source-2",
            "name": "shared",
            "connection_type": "file",
            "format": "parquet",
        },
    ]
    metadata["dataflows"][0]["source"] = {
        "connection": {"connection_id": "source-1", "name": "shared"},
        "table": "orders",
    }
    metadata["dataflows"][0]["destination"] = {
        "connection": {"connection_id": "source-2", "name": "shared"},
        "table": "orders_out",
    }

    report = validate_metadata_document(metadata)

    assert report.ok is True


def test_validation_rejects_ambiguous_duplicate_name_reference() -> None:
    metadata = _metadata()
    metadata["connections"] = [
        {
            "connection_id": "source-1",
            "name": "shared",
            "connection_type": "file",
            "format": "parquet",
        },
        {
            "connection_id": "source-2",
            "name": "shared",
            "connection_type": "file",
            "format": "parquet",
        },
    ]
    metadata["dataflows"][0]["source"]["connection_name"] = "shared"
    metadata["dataflows"][0]["destination"]["connection_name"] = "shared"

    report = validate_metadata_document(metadata)

    assert report.ok is False
    assert any("ambiguous" in item.message for item in report.errors)
