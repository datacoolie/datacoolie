from __future__ import annotations

from datacoolie.project.manifest import (
    ENVIRONMENT_ARTIFACT_TYPE,
    validate_manifest,
)


def _environment_manifest(**components: object) -> dict[str, object]:
    return {
        "schema_version": 1,
        "artifact_type": ENVIRONMENT_ARTIFACT_TYPE,
        "build_id": "build-1",
        "environment": "dev",
        "components": components,
    }


def test_environment_manifest_requires_canonical_component_shapes() -> None:
    valid = _environment_manifest(
        metadata={"path": "metadata", "layout": "single", "format": "json"},
        sql=[],
        functions=[],
    )
    assert validate_manifest(valid, expected_artifact_type=ENVIRONMENT_ARTIFACT_TYPE) is None

    malformed = _environment_manifest(
        metadata="metadata",
        sql={"path": "sql"},
        functions=[],
    )
    error = validate_manifest(malformed, expected_artifact_type=ENVIRONMENT_ARTIFACT_TYPE)
    assert error is not None
    assert "metadata" in error or "sql" in error


def test_manifest_validation_is_tooling_only_and_keeps_relative_components() -> None:
    payload = _environment_manifest(
        metadata={"path": "config/metadata", "layout": "single", "format": "json"},
        sql=[{"path": "queries"}],
        functions=[],
    )
    assert validate_manifest(payload, expected_artifact_type=ENVIRONMENT_ARTIFACT_TYPE) is None


def test_environment_manifest_validates_runner_descriptor() -> None:
    valid = _environment_manifest(
        metadata={"path": "metadata", "layout": "single", "format": "json"},
        sql=[],
        functions=[],
        runners={"path": "runners", "files": ["runners/run.py"]},
    )
    assert validate_manifest(valid, expected_artifact_type=ENVIRONMENT_ARTIFACT_TYPE) is None
    malformed = _environment_manifest(
        metadata={"path": "metadata", "layout": "single", "format": "json"},
        sql=[],
        functions=[],
        runners={"path": "other", "files": []},
    )
    assert "runners.path" in (validate_manifest(
        malformed,
        expected_artifact_type=ENVIRONMENT_ARTIFACT_TYPE,
    ) or "")
