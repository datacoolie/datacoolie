from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


SKILL_DIR = Path(__file__).resolve().parents[3] / "datacoolie-release"
SCRIPT = SKILL_DIR / "scripts" / "validate_upload_record.py"


def _module():
    spec = importlib.util.spec_from_file_location("validate_upload_record", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _record(**overrides):
    value = {
        "schema_version": 1,
        "release_id": "release-1",
        "build_id": "260915-120000-abcdef123456",
        "environment": "dev",
        "deployment_path": "target/with spaces",
        "source": ".builds/artifacts/260915-120000-abcdef123456/dev",
        "status": "success",
        "uploads": {
            "artifact": {"status": "success", "files": 3},
            "current": {"status": "success", "files": 3},
        },
    }
    value.update(overrides)
    return value


def test_upload_record_schema_and_template_are_valid() -> None:
    schema = json.loads((SKILL_DIR / "schemas/upload-record.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    template = json.loads((SKILL_DIR / "templates/upload-record.json.example").read_text())
    assert not list(Draft202012Validator(schema).iter_errors(template))
    assert _module().validate_record(template)["ok"] is True


def test_upload_record_requires_artifact_before_current() -> None:
    module = _module()
    failed = _record(
        status="failed",
        uploads={
            "artifact": {"status": "failed", "files": 0, "error": "copy failed"},
            "current": {"status": "skipped", "files": 0},
        },
    )
    assert module.validate_record(failed)["status"] == "failed"
    with pytest.raises(ValueError, match="skipped"):
        module.validate_record(_record(
            status="failed",
            uploads={
                "artifact": {"status": "failed", "files": 0},
                "current": {"status": "failed", "files": 0},
            },
        ))


def test_current_failure_is_partial_and_success_gate_is_strict() -> None:
    module = _module()
    partial = _record(
        status="partial_failure",
        uploads={
            "artifact": {"status": "success", "files": 3},
            "current": {"status": "failed", "files": 0, "error": "denied"},
        },
    )
    assert module.validate_record(partial)["status"] == "partial_failure"
    with pytest.raises(ValueError, match="not successful"):
        module.validate_record(partial, require_success=True)


def test_upload_record_rejects_skipped_phase() -> None:
    module = _module()
    with pytest.raises(ValueError, match="cannot be skipped"):
        module.validate_record(_record(
            status="partial_failure",
            uploads={
                "artifact": {"status": "success", "files": 3},
                "current": {"status": "skipped", "files": 0},
            },
        ))
    with pytest.raises(ValueError, match="cannot be skipped"):
        module.validate_record(_record(
            status="partial_failure",
            uploads={
                "artifact": {"status": "skipped", "files": 0},
                "current": {"status": "skipped", "files": 0},
            },
        ))


@pytest.mark.parametrize("field", ["deployment_path", "source", "environment"])
def test_upload_record_rejects_empty_required_strings(field: str) -> None:
    value = _record(**{field: ""})
    with pytest.raises(ValueError, match=field):
        _module().validate_record(value)


def _v2_record(**overrides):
    value = {
        "schema_version": 2,
        "release_id": "release-1",
        "build_id": "build-1",
        "environment": "dev",
        "deployment_path": "target/dev",
        "source": "retained/dev",
        "preflight": {"status": "passed"},
        "status": "success",
        "uploads": {
            "artifact": {"status": "success", "files": 2},
            "current": {"status": "success", "files": 2},
        },
        "record": {"status": "not_requested"},
    }
    value.update(overrides)
    return value


def test_upload_record_schema_owns_extra_keys_and_error_types() -> None:
    module = _module()
    extra = _record(activation="claimed")
    with pytest.raises(ValueError, match="Invalid upload record"):
        module.validate_record(extra)
    typed_error = _record(
        status="failed",
        uploads={
            "artifact": {"status": "failed", "files": 0, "error": {"message": "copy failed"}},
            "current": {"status": "skipped", "files": 0},
        },
    )
    with pytest.raises(ValueError, match="Invalid upload record"):
        module.validate_record(typed_error)


def test_valid_v1_error_strings_keep_the_legacy_schema_range() -> None:
    module = _module()
    for error in ("", "x" * 600):
        value = _record(
            status="failed",
            uploads={
                "artifact": {"status": "failed", "files": 0, "error": error},
                "current": {"status": "skipped", "files": 0},
            },
        )
        assert module.validate_record(value)["schema_version"] == 1


def test_v2_preflight_failure_skips_both_phases_and_allows_null_identities() -> None:
    module = _module()
    failed = _v2_record(
        release_id=None,
        build_id=None,
        environment=None,
        deployment_path=None,
        source=None,
        preflight={"status": "failed", "error": "source is unavailable"},
        status="failed",
        uploads={
            "artifact": {"status": "skipped", "files": 0},
            "current": {"status": "skipped", "files": 0},
        },
    )
    assert module.validate_record(failed)["schema_version"] == 2


def test_v2_requires_identities_and_existing_phase_semantics_after_preflight() -> None:
    module = _module()
    with pytest.raises(ValueError, match="required after preflight"):
        module.validate_record(_v2_record(build_id=None))

    with pytest.raises(ValueError, match="cannot be skipped"):
        module.validate_record(
            _v2_record(
                status="partial_failure",
                uploads={
                    "artifact": {"status": "success", "files": 2},
                    "current": {"status": "skipped", "files": 0},
                },
            )
        )

    with pytest.raises(ValueError, match="zero files"):
        module.validate_record(
            _v2_record(
                status="partial_failure",
                uploads={
                    "artifact": {"status": "failed", "files": 1, "error": "copy failed"},
                    "current": {"status": "skipped", "files": 2},
                },
            )
        )


def test_v2_record_stage_requires_consistent_diagnostics() -> None:
    module = _module()
    with pytest.raises(ValueError, match="requires a diagnostic"):
        module.validate_record(_v2_record(record={"status": "failed"}))
    with pytest.raises(ValueError, match="must not contain an error"):
        module.validate_record(
            _v2_record(record={"status": "written", "error": "record never written"})
        )
    with pytest.raises(ValueError, match="must not contain an error"):
        module.validate_record(
            _v2_record(record={"status": "not_requested", "error": "unexpected"})
        )
