"""Tests for the hash-bound design approval contract."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import design_approval
import materialize as build_tool


SKILL_DIR = Path(__file__).parent.parent.parent / "datacoolie-design"


def _workspace(tmp_path: Path) -> tuple[Path, Path]:
    workspace = tmp_path / "project_dcws"
    architecture = workspace / "architecture" / "current.md"
    architecture.parent.mkdir(parents=True)
    architecture.write_text(
        "---\nartifact_type: architecture\n---\n# Architecture\n",
        encoding="utf-8",
    )
    return workspace, architecture


def test_design_approval_schema_is_valid():
    schema = json.loads(
        (SKILL_DIR / "schemas" / "design-approval.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)


def test_record_and_verify_approval(tmp_path):
    workspace, architecture = _workspace(tmp_path)
    receipt = design_approval.record_approval(
        workspace=workspace,
        architecture=architecture,
        approved_by="workspace owner",
        approval_reference="explicit approval in current session",
        approved_scope="stage graph and transition contracts",
        approved_at="2026-08-10T05:00:00Z",
    )
    digest = design_approval.sha256_file(architecture)
    assert receipt == workspace / ".approvals" / "design" / f"{digest}.json"
    assert design_approval.verify_approval(
        workspace=workspace, architecture=architecture,
    ) == digest
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    schema = json.loads(
        (SKILL_DIR / "schemas" / "design-approval.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)
    assert payload["architecture_path"] == "architecture/current.md"
    assert payload["decision"] == "approved"


def test_architecture_change_invalidates_receipt(tmp_path):
    workspace, architecture = _workspace(tmp_path)
    receipt = design_approval.record_approval(
        workspace=workspace,
        architecture=architecture,
        approved_by="owner",
        approval_reference="current session",
        approved_scope="material design",
    )
    architecture.write_text(
        "---\nartifact_type: architecture\n---\n# Architecture\nchanged\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="current architecture hash"):
        design_approval.verify_approval(
            workspace=workspace, architecture=architecture, receipt=receipt,
        )


def test_receipt_rejects_naive_timestamp_and_empty_evidence(tmp_path):
    workspace, architecture = _workspace(tmp_path)
    with pytest.raises(ValueError, match="violates schema at approved_at"):
        design_approval.record_approval(
            workspace=workspace,
            architecture=architecture,
            approved_by="owner",
            approval_reference="current session",
            approved_scope="material design",
            approved_at="2026-08-10T05:00:00",
        )
    with pytest.raises(ValueError, match="violates schema at approved_at"):
        design_approval.record_approval(
            workspace=workspace,
            architecture=architecture,
            approved_by="owner",
            approval_reference="current session",
            approved_scope="material design",
            approved_at="2026-08-10T12:00:00+07:00",
        )
    with pytest.raises(ValueError, match="violates schema at approved_by"):
        design_approval.record_approval(
            workspace=workspace,
            architecture=architecture,
            approved_by="",
            approval_reference="current session",
            approved_scope="material design",
        )


def test_cli_record_requires_explicit_confirmation(tmp_path):
    workspace, architecture = _workspace(tmp_path)
    with pytest.raises(SystemExit, match="requires --confirmed"):
        design_approval.main([
            "record",
            "--workspace", str(workspace),
            "--architecture", str(architecture),
            "--approved-by", "owner",
            "--approval-reference", "current session",
            "--approved-scope", "material design",
        ])


def test_build_consumer_verifies_design_receipt(tmp_path):
    workspace, architecture = _workspace(tmp_path)
    design_approval.record_approval(
        workspace=workspace,
        architecture=architecture,
        approved_by="owner",
        approval_reference="current session",
        approved_scope="material design",
    )
    binding = build_tool._validate_design_approval(workspace)
    assert binding is not None
    assert binding["architecture_sha256"] == design_approval.sha256_file(architecture)
    assert binding["approval_receipt"].endswith(".json")

    architecture.write_text(
        "---\nartifact_type: architecture\n---\n# Changed architecture\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="receipt does not exist"):
        build_tool._validate_design_approval(workspace)


def test_architecture_cannot_self_declare_approval_bypass(tmp_path):
    workspace, architecture = _workspace(tmp_path)
    architecture.write_text(
        "---\napproval_required: false\n---\n# Compatible refinement\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="receipt does not exist"):
        build_tool._validate_design_approval(workspace)


def test_build_reads_windows_line_endings_in_architecture(tmp_path):
    workspace, architecture = _workspace(tmp_path)
    architecture.write_bytes(
        b"---\r\nartifact_type: architecture\r\n---\r\n# Architecture\r\n"
    )
    design_approval.record_approval(
        workspace=workspace,
        architecture=architecture,
        approved_by="owner",
        approval_reference="current session",
        approved_scope="material design",
    )
    binding = build_tool._validate_design_approval(workspace)
    assert binding is not None
    assert binding["architecture_sha256"] == design_approval.sha256_file(architecture)


def test_build_consumer_rejects_symlinked_approval_receipt(tmp_path, monkeypatch):
    workspace, architecture = _workspace(tmp_path)
    receipt = design_approval.record_approval(
        workspace=workspace,
        architecture=architecture,
        approved_by="owner",
        approval_reference="current session",
        approved_scope="material design",
    )
    original = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == receipt or original(path),
    )
    with pytest.raises(ValueError, match="must not be a symlink"):
        build_tool._validate_design_approval(workspace)


@pytest.mark.parametrize("receipt_name", ["latest.json", "copied.json"])
def test_verify_rejects_non_hash_receipt_names(tmp_path, receipt_name):
    workspace, architecture = _workspace(tmp_path)
    receipt = design_approval.record_approval(
        workspace=workspace,
        architecture=architecture,
        approved_by="owner",
        approval_reference="current session",
        approved_scope="material design",
    )
    alias = receipt.with_name(receipt_name)
    alias.write_bytes(receipt.read_bytes())
    with pytest.raises(ValueError, match="path must match"):
        design_approval.verify_approval(
            workspace=workspace,
            architecture=architecture,
            receipt=alias,
        )


def test_receipt_schema_rejects_unknown_fields(tmp_path):
    workspace, architecture = _workspace(tmp_path)
    receipt = design_approval.record_approval(
        workspace=workspace,
        architecture=architecture,
        approved_by="owner",
        approval_reference="current session",
        approved_scope="material design",
    )
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["approval_required"] = False
    with pytest.raises(ValueError, match="violates schema"):
        design_approval.validate_receipt(payload, architecture)
