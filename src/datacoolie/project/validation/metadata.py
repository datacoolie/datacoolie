"""Validation of metadata documents and their semantic model constraints."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Sequence

from datacoolie.metadata.documents.mapping import (
    build_connections,
    build_dataflows,
    build_grouped_schema_hints,
)
from datacoolie.project.errors import ProjectDependencyError
from .queries import _validate_queries
from .reports import Diagnostic, ValidationReport, _error, _warning
from .schema_hints import _validate_datatype_hints, _validate_grouped_datatype_hints

def _schema_validator():
    """Load the optional project schema service with an actionable error."""

    try:
        from datacoolie.project.schema import (
            MetadataSchemaError,
            validate_metadata_schema,
        )
    except ModuleNotFoundError as exc:
        if exc.name in {"jsonschema", "packaging"}:
            raise ProjectDependencyError(
                "Metadata JSON Schema validation requires datacoolie[cli]",
                dependency="datacoolie[cli]",
                exit_code=2,
            ) from exc
        raise
    return MetadataSchemaError, validate_metadata_schema

def validate_metadata_document(
    metadata: dict[str, Any],
    *,
    scope: str = "metadata",
    source: str | None = None,
    sql_root: Path | Sequence[Path] | None = None,
    artifact_root: Path | None = None,
    framework_version: str | None = None,
) -> ValidationReport:
    errors: list[Diagnostic] = []
    warnings: list[Diagnostic] = []
    checks = [
        "document-shape",
        "json-schema",
        "model-constraints",
        "identity-uniqueness",
        "query-references",
    ]
    if not isinstance(metadata, dict):
        _error(errors, "metadata.root", "Metadata document must be an object", source)
        return ValidationReport(scope, errors, warnings, checks, {})

    # Do not manufacture a valid empty document from an arbitrary object.  An
    # initialized project has explicit section wrappers (possibly empty),
    # while a typo such as ``{"metdata": [...]}`` must reach JSON Schema and
    # be reported as an authored-contract error.
    if not any(
        section in metadata for section in ("connections", "dataflows", "schema_hints")
    ):
        _error(
            errors,
            "metadata.shape",
            "Metadata document must contain at least one section: connections, dataflows, or schema_hints",
            source,
        )
        return ValidationReport(
            scope,
            errors,
            warnings,
            checks,
            {
                "source": source,
                "framework_version": framework_version,
                "schema_version": None,
                "schema_url": None,
                "metadata_loaded": False,
                "not_checked": [
                    "json-schema",
                    "model-constraints",
                    "identity-uniqueness",
                    "query-references",
                ],
            },
        )

    # Validation must not add default sections to a caller-owned snapshot.  The
    # normalized document is the one representation passed to both JSON Schema
    # and the semantic model builders below.
    metadata = deepcopy(metadata)
    for section in ("connections", "dataflows", "schema_hints"):
        metadata.setdefault(section, [])

    resolved_schema = None
    MetadataSchemaError, validate_metadata_schema = _schema_validator()
    try:
        resolved_schema, schema_diagnostics = validate_metadata_schema(
            metadata,
            framework_version=framework_version,
            source=source,
        )
    except MetadataSchemaError as exc:
        _error(errors, "metadata.schema_resolution", str(exc), source)
        return ValidationReport(
            scope,
            errors,
            warnings,
            checks,
            {
                "source": source,
                "framework_version": framework_version,
                "schema_version": None,
                "schema_url": None,
                "metadata_loaded": False,
                "not_checked": [
                    "model-constraints",
                    "identity-uniqueness",
                    "query-references",
                ],
            },
        )
    for issue in schema_diagnostics:
        _error(errors, "metadata.schema", issue.message, issue.path)
    if schema_diagnostics:
        return ValidationReport(
            scope,
            errors,
            warnings,
            checks,
            {
                "source": source,
                "framework_version": resolved_schema.framework_version,
                "schema_version": resolved_schema.descriptor.version,
                "schema_url": resolved_schema.descriptor.public_url,
                "metadata_loaded": False,
                "not_checked": [
                    "model-constraints",
                    "identity-uniqueness",
                    "query-references",
                ],
            },
        )

    connections = []
    dataflows = []
    try:
        connections = build_connections(metadata["connections"])
    except Exception as exc:
        _error(errors, "metadata.connections", str(exc), source)
    try:
        dataflows = build_dataflows(metadata["dataflows"], connections)
    except Exception as exc:
        _error(errors, "metadata.dataflows", str(exc), source)
    grouped_datatype_hints_checked = 0
    try:
        build_grouped_schema_hints(metadata["schema_hints"], connections)
        grouped_datatype_hints_checked = _validate_grouped_datatype_hints(
            metadata["schema_hints"], connections, errors
        )
    except Exception as exc:
        _error(errors, "metadata.schema_hints", str(exc), source)
    datatype_hints_checked = _validate_datatype_hints(dataflows, errors)
    query_checked = _validate_queries(
        metadata,
        errors,
        warnings,
        sql_root=sql_root,
        artifact_root=artifact_root,
    )
    if not metadata["dataflows"]:
        _warning(
            warnings, "metadata.empty_dataflows", "No dataflows are defined", source
        )
    details = {
        "source": source,
        "framework_version": resolved_schema.framework_version,
        "schema_version": resolved_schema.descriptor.version,
        "schema_url": resolved_schema.descriptor.public_url,
        "metadata_loaded": not errors,
        "connections": len(metadata["connections"]),
        "dataflows": len(metadata["dataflows"]),
        "schema_hints": len(metadata["schema_hints"]),
        "datatype_hints_checked": (
            datatype_hints_checked + grouped_datatype_hints_checked
        ),
        "grouped_datatype_hints_checked": grouped_datatype_hints_checked,
        "query_files_checked": query_checked,
    }
    return ValidationReport(scope, errors, warnings, checks, details)


__all__ = ["validate_metadata_document"]
