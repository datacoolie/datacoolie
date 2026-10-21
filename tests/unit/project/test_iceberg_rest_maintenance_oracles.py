"""Mutation checks for the REST Iceberg maintenance validator boundaries."""

from __future__ import annotations

import copy
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


def _load_oracle() -> ModuleType:
    product_root = Path(__file__).resolve().parents[3]
    script = (
        product_root
        / "usecase-sim"
        / "scripts"
        / "features"
        / "maintenance"
        / "validate_iceberg_rest_maintenance.py"
    )
    spec = importlib.util.spec_from_file_location(
        "iceberg_rest_maintenance_validator", script
    )
    if spec is None or spec.loader is None:
        raise AssertionError(f"Cannot load maintenance validator from {script}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ORACLE = _load_oracle()

_TARGET = ["warehouse", "orders"]
_SENTINEL_UUID = "sentinel-uuid-1"
_SENTINEL_METADATA = '{"table-uuid":"sentinel-uuid-1","current-snapshot-id":3}'
_SENTINEL_ROWS = [{"id": 1, "value": "sentinel"}]
_SCHEMA = {
    "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "id"}]}],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
}


def _capture() -> dict[str, Any]:
    return {
        "target": list(_TARGET),
        "sentinel_uuid": _SENTINEL_UUID,
        "sentinel": {
            "uuid": _SENTINEL_UUID,
            "metadata": _SENTINEL_METADATA,
            "rows": copy.deepcopy(_SENTINEL_ROWS),
            "schema_signature": copy.deepcopy(_SCHEMA),
        },
        "state": {
            "metadata": '{"table-uuid":"target-uuid-1","current-snapshot-id":8}',
            "snapshot_ids": ["snapshot-7", "snapshot-8"],
            "refs": {"main": "snapshot-8"},
            "rows": [{"id": 10, "value": "target"}],
            "schema_signature": copy.deepcopy(_SCHEMA),
        },
    }


def _assert_capture_pair(
    effect: dict[str, Any],
    no_work: dict[str, Any] | None = None,
    final: dict[str, Any] | None = None,
) -> None:
    ORACLE._assert_capture_pair(
        effect,
        copy.deepcopy(effect) if no_work is None else no_work,
        copy.deepcopy(effect["state"]) if final is None else final,
        expected_target=_TARGET,
        expected_sentinel_uuid=_SENTINEL_UUID,
        expected_sentinel_metadata=_SENTINEL_METADATA,
        expected_sentinel_rows=_SENTINEL_ROWS,
        expected_sentinel_schema=_SCHEMA,
    )


def test_capture_pair_accepts_identical_native_metadata() -> None:
    """The effect, no-work, and final observations may share native metadata."""

    _assert_capture_pair(_capture())


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        pytest.param("metadata", "captured metadata", id="metadata-only-change"),
        pytest.param("target_uuid", "bound to", id="target-uuid-change"),
        pytest.param("sentinel_metadata", "sentinel metadata", id="sentinel-metadata-change"),
    ],
)
def test_capture_pair_rejects_continuity_mutants(
    mutation: str, message: str
) -> None:
    effect = _capture()
    no_work = copy.deepcopy(effect)
    final = copy.deepcopy(effect["state"])
    if mutation == "metadata":
        no_work["state"]["metadata"] = '{"table-uuid":"target-uuid-2"}'
    elif mutation == "target_uuid":
        effect["sentinel_uuid"] = "different-sentinel-uuid"
    else:
        no_work["sentinel"]["metadata"] = '{"table-uuid":"changed-sentinel"}'

    with pytest.raises(AssertionError, match=message):
        _assert_capture_pair(effect, no_work, final)


def _state() -> dict[str, Any]:
    return {
        "snapshot_ids": [
            "snapshot-current",
            "snapshot-tag",
            "snapshot-branch",
            "snapshot-expired",
        ],
        "refs": {
            "main": "snapshot-current",
            "tagged-release": "snapshot-tag",
            "branch-release": "snapshot-branch",
        },
        "rows": [{"id": 10, "value": "unchanged"}],
        "schema_signature": copy.deepcopy(_SCHEMA),
    }


def test_state_continuity_accepts_unchanged_rows_refs_and_schema() -> None:
    before = _state()
    ORACLE._assert_state_continuity(
        before, copy.deepcopy(before), context="maintenance test"
    )


@pytest.mark.parametrize("key", ["rows", "refs", "schema_signature"])
def test_state_continuity_rejects_single_boundary_mutants(key: str) -> None:
    before = _state()
    after = copy.deepcopy(before)
    if key == "rows":
        after["rows"][0]["value"] = "mutated"
    elif key == "refs":
        after["refs"]["tagged-release"] = "snapshot-other"
    else:
        after["schema_signature"]["current-schema-id"] = 1

    with pytest.raises(AssertionError) as error:
        ORACLE._assert_state_continuity(before, after, context="maintenance test")
    assert key in str(error.value) or key == "schema_signature"


def _spark_expiration_evidence(*, files_removed: int) -> Any:
    action = {
        "action": "expire_snapshots",
        "status": "succeeded",
        "metrics": {"files_removed": files_removed},
        "delete_callback_success_count": files_removed,
    }
    return ORACLE.DriverEvidence(
        job_id="job-1",
        stage="cleanup",
        session_id="session-1",
        result={"failed": 0, "succeeded": 1},
        records=(
            {
                "operation_details": [
                    {
                        "operation": "cleanup",
                        "status": "succeeded",
                        "actions": [action],
                    }
                ]
            },
        ),
    )


def test_spark_expiration_accepts_effect_for_current_tag_and_branch_refs() -> None:
    before = _state()
    after = copy.deepcopy(before)
    after["snapshot_ids"].remove("snapshot-expired")

    ORACLE._assert_expiration(
        _spark_expiration_evidence(files_removed=3),
        before,
        after,
        require_effect=True,
        engine_name="spark",
    )
