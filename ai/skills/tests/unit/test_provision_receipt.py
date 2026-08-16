"""Tests for exact, approval-bound provision evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

import validate_provision


SKILL_DIR = Path(__file__).parent.parent.parent / "datacoolie-provision"


def _artifact(path: Path, workspace: Path) -> dict[str, str]:
    return {
        "path": path.relative_to(workspace).as_posix(),
        "sha256": validate_provision.sha256_file(path),
    }


def _write_receipt(
    tmp_path: Path,
    *,
    operation: str = "plan",
    status: str = "succeeded",
    destructive: bool = False,
) -> tuple[Path, Path, dict]:
    workspace = tmp_path / "project_dcws"
    requirements_path = workspace / "architecture" / "current.md"
    requirements_path.parent.mkdir(parents=True)
    requirements_path.write_text("# Approved requirements\n", encoding="utf-8")

    evidence = workspace / "provision" / "evidence" / "qa"
    plan_path = evidence / "plans" / "plan-1.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_text('{"actions":[]}', encoding="utf-8")
    plan = _artifact(plan_path, workspace)
    authorizations = []
    if operation == "apply":
        authorizations.append({
            "scope": "apply",
            "reference": "explicit approval in current session",
            "environment": "qa",
            "plan_sha256": plan["sha256"],
        })
    if destructive:
        authorizations.append({
            "scope": "destructive",
            "reference": "separate destructive approval in current session",
            "environment": "qa",
            "plan_sha256": plan["sha256"],
        })

    action_status = "planned" if operation == "plan" else "succeeded"
    state_status = "unchanged" if operation == "plan" else "updated"
    check_name = "plan-validation" if operation == "plan" else "resource-observation"
    receipt = {
        "schema_version": 1,
        "artifact_type": "provision_receipt",
        "receipt_id": "receipt-1",
        "operation": operation,
        "status": status,
        "environment": "qa",
        "platform": "target-platform",
        "requirements": _artifact(requirements_path, workspace),
        "plan": plan,
        "authorizations": authorizations,
        "tool_versions": {"provisioner": "1.0.0"},
        "state": {"backend_reference": "configured-backend", "status": state_status},
        "actions": [{
            "resource": "resource.example",
            "action": "replace" if destructive else "create",
            "status": action_status,
            "data_bearing": destructive,
            "evidence": "observed result",
        }],
        "resource_outputs": [{
            "name": "resource_id",
            "value": "resource-123",
            "sensitive": False,
        }],
        "verification": [{
            "name": check_name,
            "status": "passed",
            "evidence": "observable evidence",
        }],
        "started_at": "2026-08-10T00:00:00Z",
        "finished_at": "2026-08-10T00:00:01Z",
        "unresolved_issues": [],
    }
    receipt_path = evidence / "receipts" / "receipt-1.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    return workspace, receipt_path, receipt


def test_provision_receipt_schema_is_valid() -> None:
    schema = json.loads(
        (SKILL_DIR / "schemas/provision-receipt.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)


def test_plan_and_apply_receipts_validate_exact_artifacts(tmp_path: Path) -> None:
    workspace, receipt_path, _ = _write_receipt(tmp_path)
    assert validate_provision.validate_receipt(workspace, receipt_path)["operation"] == "plan"

    workspace, receipt_path, _ = _write_receipt(tmp_path / "apply", operation="apply")
    receipt = validate_provision.validate_receipt(
        workspace, receipt_path, require_apply_success=True
    )
    assert receipt["status"] == "succeeded"


def test_changed_plan_invalidates_authorization(tmp_path: Path) -> None:
    workspace, receipt_path, receipt = _write_receipt(tmp_path, operation="apply")
    receipt["authorizations"][0]["plan_sha256"] = "2" * 64
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="Authorization plan SHA-256"):
        validate_provision.validate_receipt(workspace, receipt_path)


def test_data_bearing_change_requires_destructive_authorization(tmp_path: Path) -> None:
    workspace, receipt_path, receipt = _write_receipt(
        tmp_path, operation="apply", destructive=True
    )
    receipt["authorizations"] = [receipt["authorizations"][0]]
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="destructive authorization"):
        validate_provision.validate_receipt(workspace, receipt_path)


def test_failed_partial_apply_is_evidence_but_not_success(tmp_path: Path) -> None:
    workspace, receipt_path, receipt = _write_receipt(tmp_path, operation="apply")
    receipt["status"] = "failed"
    receipt["state"]["status"] = "partial"
    receipt["actions"][0]["status"] = "failed"
    receipt["verification"][0]["status"] = "failed"
    receipt["unresolved_issues"] = ["partial apply requires reconciliation"]
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assert validate_provision.validate_receipt(workspace, receipt_path)["status"] == "failed"
    with pytest.raises(ValueError, match="successful provision apply receipt"):
        validate_provision.validate_receipt(
            workspace, receipt_path, require_apply_success=True
        )


def test_sensitive_output_name_is_rejected(tmp_path: Path) -> None:
    workspace, receipt_path, receipt = _write_receipt(tmp_path)
    receipt["resource_outputs"][0]["name"] = "database_password"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="Sensitive-looking resource output"):
        validate_provision.validate_receipt(workspace, receipt_path)


def test_latest_or_glob_plan_selection_is_rejected(tmp_path: Path) -> None:
    workspace, receipt_path, receipt = _write_receipt(tmp_path)
    old_plan = workspace / receipt["plan"]["path"]
    latest_plan = old_plan.with_name("latest.json")
    latest_plan.write_bytes(old_plan.read_bytes())
    receipt["plan"] = _artifact(latest_plan, workspace)
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="latest and globs are forbidden"):
        validate_provision.validate_receipt(workspace, receipt_path)
