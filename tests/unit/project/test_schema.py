"""Contract tests for the project-owned metadata schema registry."""

from __future__ import annotations

from copy import deepcopy

import pytest

from datacoolie import __version__
from datacoolie.project.schema import (
    LATEST_SCHEMA_URL,
    PUBLIC_SCHEMA_BASE_URL,
    MetadataSchemaError,
    resolve_metadata_schema,
    validate_metadata_schema,
)
from datacoolie.project.validation.metadata import validate_metadata_document


def test_default_schema_target_is_the_installed_package_version() -> None:
    assert resolve_metadata_schema().framework_version == __version__


def test_resolver_selects_greatest_schema_not_newer_than_framework() -> None:
    selected = resolve_metadata_schema("0.2.0")

    assert selected.descriptor.version == "0.2.0"
    assert selected.framework_version == "0.2.0"
    assert selected.descriptor.public_url == (
        f"{PUBLIC_SCHEMA_BASE_URL}/0.2.0/metadata.schema.json"
    )


def test_resolver_keeps_older_frameworks_on_older_contract() -> None:
    selected = resolve_metadata_schema("0.1.5")

    assert selected.descriptor.version == "0.1.0"


def test_resolver_rejects_marker_for_a_different_resolved_schema() -> None:
    with pytest.raises(MetadataSchemaError, match=r"declares \$schema"):
        resolve_metadata_schema(
            "0.1.5",
            marker=f"{PUBLIC_SCHEMA_BASE_URL}/0.2.0/metadata.schema.json",
        )


def test_resolver_accepts_latest_marker_but_keeps_framework_version_selection() -> None:
    selected = resolve_metadata_schema("0.1.5", marker=LATEST_SCHEMA_URL)

    assert selected.descriptor.version == "0.1.0"
    assert selected.descriptor.public_url == (
        f"{PUBLIC_SCHEMA_BASE_URL}/0.1.0/metadata.schema.json"
    )


def test_validation_accepts_latest_marker_without_fetching_public_schema() -> None:
    resolved, diagnostics = validate_metadata_schema(
        {
            "$schema": LATEST_SCHEMA_URL,
            "connections": [],
            "dataflows": [],
            "schema_hints": [],
        },
        framework_version="0.2.0",
    )

    assert resolved.descriptor.version == "0.2.0"
    assert diagnostics == ()


def test_schema_validation_reports_json_pointer_and_does_not_require_resources() -> None:
    resolved, diagnostics = validate_metadata_schema(
        {
            "$schema": f"{PUBLIC_SCHEMA_BASE_URL}/0.2.0/metadata.schema.json",
            "connections": [{"name": "source"}],
            "dataflows": [
                {
                    "name": "orders",
                    "source": {"connection_name": "source"},
                    "destination": {"connection_name": "missing", "table": "orders"},
                }
            ],
            "schema_hints": [],
        },
        framework_version="0.2.0",
    )

    assert resolved.descriptor.version == "0.2.0"
    assert diagnostics == ()


def test_schema_validation_rejects_unknown_top_level_fields() -> None:
    _, diagnostics = validate_metadata_schema(
        {"connections": [], "dataflows": [], "schema_hints": [], "typo": True},
        framework_version="0.2.0",
    )

    assert diagnostics
    assert any(issue.path == "/" for issue in diagnostics)


def test_schema_validation_rejects_both_connection_reference_forms() -> None:
    _, diagnostics = validate_metadata_schema(
        {
            "connections": [{"name": "source"}],
            "dataflows": [
                {
                    "name": "orders",
                    "source": {
                        "connection_name": "source",
                        "connection": "source",
                        "table": "orders",
                    },
                    "destination": {
                        "connection_name": "source",
                        "table": "orders_out",
                    },
                }
            ],
        },
        framework_version="0.2.0",
    )

    assert diagnostics
    assert any("source" in issue.path for issue in diagnostics)


def test_project_validation_does_not_invent_sections_for_an_arbitrary_object() -> None:
    report = validate_metadata_document({"metdata": []})

    assert not report.ok
    assert report.errors[0].code == "metadata.shape"


def test_runtime_metadata_package_does_not_export_authoring_schema_service() -> None:
    import datacoolie.metadata as runtime_metadata

    assert not hasattr(runtime_metadata, "MetadataSchemaError")
    assert not hasattr(runtime_metadata, "resolve_metadata_schema")
    assert not hasattr(runtime_metadata, "validate_metadata_schema")


def _api_metadata(configure: dict) -> dict:
    return {
        "connections": [{"name": "api"}, {"name": "target"}],
        "dataflows": [{
            "name": "orders",
            "source": {"connection_name": "api", "configure": configure},
            "destination": {"connection_name": "target", "table": "orders"},
        }],
    }


@pytest.mark.parametrize("configure", [{}, {"next_link_bound_mode": "opaque"},
                                       {"next_link_bound_mode": "repeat_query_bounds"}])
def test_next_link_metadata_modes_validate_without_mutating_input(configure: dict) -> None:
    document = _api_metadata(configure)
    original = deepcopy(document)

    _, diagnostics = validate_metadata_schema(document, framework_version="0.2.0")

    assert diagnostics == ()
    assert document == original
    assert "_active_query_bounds" not in configure


@pytest.mark.parametrize("value", ["invalid", None, 0, True, {}, []])
def test_next_link_metadata_mode_rejected_at_authored_path(value: object) -> None:
    _, diagnostics = validate_metadata_schema(
        _api_metadata({"next_link_bound_mode": value}), framework_version="0.2.0",
    )

    assert diagnostics
    assert all(issue.path == "/dataflows/0/source/configure/next_link_bound_mode"
               for issue in diagnostics)


@pytest.mark.parametrize("configure", [
    {"range_param_mapping": {"modified_at": {"lower": "since", "upper": "until"}},
     "watermark_range_interval_unit": "day"},
    {"watermark_param_mapping": {"modified_at": "since"}, "watermark_to_param": "until",
     "watermark_range_interval_unit": "day", "watermark_range_start": "2026-01-01T00:00:00Z"},
])
def test_split_metadata_keeps_canonical_and_legacy_structural_forms(configure: dict) -> None:
    # Stored lower state and endpoint guarantees are runtime context, not required authoring fields.
    _, diagnostics = validate_metadata_schema(_api_metadata(configure), framework_version="0.2.0")

    assert diagnostics == ()
