#!/usr/bin/env python3
"""Validate one explicit release receipt and all referenced workspace artifacts."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from _artifact_validation import (
    canonical_digest,
    load_object,
    reject_moving_selector,
    resolve_build_selector,
    resolve_artifact,
    sha256_file as sha256_file,
    validate_build_binding,
    validate_provision_binding,
)


def deployment_marker_sha256(receipt: dict[str, Any]) -> str:
    """Hash the immutable identity that target current must report."""
    return canonical_digest({
        "schema_version": 1,
        "build_id": receipt["build_id"],
        "release_id": receipt["release_id"],
        "environment": receipt["environment"],
        "runner": receipt["runner"],
        "runner_deployment_kind": receipt["target"]["runner_deployment_kind"],
        "runner_runtime_kind": receipt["target"]["runner_runtime_kind"],
        "metadata": receipt["metadata"],
        "functions_artifact": receipt["functions_artifact"],
    })


def deployment_intent_sha256(receipt: dict[str, Any]) -> str:
    """Hash only immutable pre-mutation release intent, never observed results."""
    target = receipt["target"]
    intent = {
        "schema_version": receipt["schema_version"],
        "action": receipt["action"],
        "build_id": receipt["build_id"],
        "environment": receipt["environment"],
        "platform": receipt["platform"],
        "target": {
            "identity": target["identity"],
            "activation_mechanism": target["activation_mechanism"],
            "runner_deployment_kind": target["runner_deployment_kind"],
            "runner_runtime_kind": target["runner_runtime_kind"],
            "protection": target["protection"],
            "candidate_reference": target["candidate_reference"],
            "current_reference": target["current_reference"],
        },
        "deployment_marker_sha256": receipt["deployment_marker"]["sha256"],
        "manifest": receipt["manifest"],
        "build_receipt": receipt["build_receipt"],
        "runner": receipt["runner"],
        "metadata": receipt["metadata"],
        "functions_artifact": receipt["functions_artifact"],
        "functions_attachment": receipt["functions_attachment"],
        "provision_receipt": receipt["provision_receipt"],
        "provision_requirements": receipt["provision_requirements"],
        "source_release": receipt["source_release"],
        "previous_active_release_id": receipt["previous_active_release_id"],
        "runtime_paths": receipt["runtime_paths"],
        "qualification_scope": receipt["qualification_scope"],
        "runtime_state": receipt["runtime_state"],
        "transfers": sorted(
            [
                {
                    "source": item["source"],
                    "target_reference": item["target_reference"],
                }
                for item in receipt["transfers"]
            ],
            key=lambda item: (item["target_reference"], item["source"]["path"]),
        ),
    }
    return canonical_digest(intent)


def _validate_schema(receipt: dict[str, Any]) -> None:
    schema_path = (
        Path(__file__).resolve().parent.parent / "schemas" / "release-receipt.schema.json"
    )
    schema = load_object(schema_path, "Release receipt schema")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(
        validator.iter_errors(receipt),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        details = []
        for error in errors:
            location = ".".join(str(part) for part in error.absolute_path) or "<root>"
            details.append(f"{location}: {error.message}")
        raise ValueError("Invalid release receipt:\n- " + "\n- ".join(details))


def _validate_semantics(receipt: dict[str, Any], allowed_sources: dict[str, str]) -> None:
    authorization = receipt["authorization"]
    for field in ("action", "environment", "build_id"):
        if authorization[field] != receipt[field]:
            raise ValueError(f"Authorization {field} does not match the release")
    expected_intent = deployment_intent_sha256(receipt)
    if authorization["deployment_intent_sha256"] != expected_intent:
        raise ValueError("Authorization deployment intent does not match the release")
    if (
        receipt["target"]["protection"] == "production"
        and authorization["source"] != "current_session"
    ):
        raise ValueError("Production release requires current-session authorization")
    if (
        receipt["runtime_state"]["action"] in {"migrate", "reset", "replay"}
        and authorization["source"] != "current_session"
    ):
        raise ValueError(
            "Runtime-state migrate, reset, or replay requires current-session authorization"
        )

    target = receipt["target"]
    if target["candidate_reference"] == target["current_reference"]:
        raise ValueError("Candidate and target current references must be distinct")
    reject_moving_selector(target["candidate_reference"], "Candidate reference")
    candidate_reference = target["candidate_reference"]
    current_reference = target["current_reference"]
    if receipt["build_id"] in candidate_reference and receipt["release_id"] not in candidate_reference:
        raise ValueError("Candidate reference must be release-addressed, not build-addressed")
    if receipt["build_id"] in current_reference or receipt["release_id"] in current_reference:
        raise ValueError("Target current reference must be stable and version-independent")
    expected_marker = deployment_marker_sha256(receipt)
    marker = receipt["deployment_marker"]
    if marker["sha256"] != expected_marker:
        raise ValueError("Deployment marker does not match the exact release slice")
    qualification_scope = receipt["qualification_scope"]
    runtime_paths = receipt["runtime_paths"]
    if qualification_scope["base_log_path"] == runtime_paths["base_log_path"]:
        raise ValueError("Qualification logs must be isolated from active runtime logs")
    if qualification_scope["watermark_base_path"] == runtime_paths["watermark_base_path"]:
        raise ValueError("Qualification watermarks must be isolated from active runtime state")

    started = datetime.fromisoformat(receipt["started_at"].replace("Z", "+00:00"))
    finished_value = receipt["finished_at"]
    finished = (
        None
        if finished_value is None
        else datetime.fromisoformat(finished_value.replace("Z", "+00:00"))
    )
    if started.tzinfo is None or (finished is not None and finished.tzinfo is None):
        raise ValueError("Release receipt timestamps require timezones")
    if finished is not None and finished < started:
        raise ValueError("Release receipt finished_at must not precede started_at")

    transfer_paths = [item["source"]["path"] for item in receipt["transfers"]]
    targets = [item["target_reference"] for item in receipt["transfers"]]
    if len(transfer_paths) != len(set(transfer_paths)) or len(targets) != len(set(targets)):
        raise ValueError("Release receipt contains duplicate transfer source or target")
    for transfer in receipt["transfers"]:
        source = transfer["source"]
        target_reference = transfer["target_reference"]
        if receipt["build_id"] in target_reference and receipt["release_id"] not in target_reference:
            raise ValueError("Transfer target must not use a build-addressed candidate")
        if allowed_sources.get(source["path"]) != source["sha256"]:
            raise ValueError("Transfer source is outside or differs from the declared release slice")
        observed = transfer["observed_sha256"]
        if observed is not None and observed != source["sha256"]:
            raise ValueError("Observed target SHA-256 does not match the source artifact")
        if transfer["status"] == "pending" and observed is not None:
            raise ValueError("Pending transfer cannot contain an observed target SHA-256")

    metadata_sources = {
        item["path"] for item in receipt["metadata"]["files"].values()
    }
    function_source = (
        None
        if receipt["functions_artifact"] is None
        else receipt["functions_artifact"]["path"]
    )
    expected_transfers = {receipt["runner"]["path"], *metadata_sources}
    if function_source is not None:
        expected_transfers.add(function_source)
        attachment_reference = receipt["functions_attachment"]["target_reference"]
        if receipt["build_id"] in attachment_reference and receipt["release_id"] not in attachment_reference:
            raise ValueError("Functions attachment must not use a build-addressed candidate")
    if set(transfer_paths) != expected_transfers:
        raise ValueError("Release transfers must cover the exact runner, metadata set, and functions artifact")

    def has_component(reference: str, component: str) -> bool:
        normalized = reference.replace("\\", "/")
        return component in [part for part in normalized.split("/") if part]

    for transfer in receipt["transfers"]:
        source_path = transfer["source"]["path"]
        if source_path in metadata_sources and not has_component(transfer["target_reference"], "metadata"):
            raise ValueError("Metadata must map to the fixed target metadata component")
        if source_path == function_source and not has_component(transfer["target_reference"], "functions"):
            raise ValueError("Functions must map to the fixed target functions component")

    status = receipt["status"]
    passed_checks = {
        check["name"] for check in receipt["verification"] if check["status"] == "passed"
    }
    future_checks = {
        "deployment-marker-integrity",
        "candidate-artifact-integrity",
        "candidate-runtime-qualification",
        "activation-preflight",
        "target-observation",
        "functions-artifact-integrity",
        "functions-attachment",
        "functions-target-import",
        "functions-target-execution",
        "functions-session-activation",
    }
    if status == "prepared" and passed_checks & future_checks:
        raise ValueError("Prepared release cannot claim post-staging verification")
    if status == "staged" and passed_checks & {
        "candidate-runtime-qualification",
        "activation-preflight",
        "target-observation",
        "functions-target-import",
        "functions-target-execution",
        "functions-session-activation",
    }:
        raise ValueError("Staged release cannot claim qualification or activation evidence")
    if status == "qualified" and passed_checks & {
        "activation-preflight", "target-observation"
    }:
        raise ValueError("Qualified release cannot claim activation or active-target evidence")
    if status in {"prepared", "staged", "qualified", "active"}:
        for required in (
            "build-preflight",
            "resource-readiness",
            "environment-isolation",
            "runtime-state-preflight",
            "shared-component-compatibility",
        ):
            if not any(
                check["name"] == required and check["status"] == "passed"
                for check in receipt["verification"]
            ):
                raise ValueError(f"Release phase {status} requires a passed {required} check")
    if status in {"staged", "qualified", "active"}:
        for required in ("candidate-artifact-integrity", "deployment-marker-integrity"):
            if required not in passed_checks:
                raise ValueError(f"Release phase {status} requires a passed {required} check")
        if marker["observed_sha256"] != marker["sha256"]:
            raise ValueError(f"Release phase {status} requires the exact deployment marker")
        if marker["observed_build_id"] != receipt["build_id"]:
            raise ValueError(f"Release phase {status} observed the wrong target build ID")
        if marker["observed_release_id"] != receipt["release_id"]:
            raise ValueError(f"Release phase {status} observed the wrong target release ID")
    if status in {"qualified", "active"}:
        qualification = next(
            (
                check
                for check in receipt["verification"]
                if check["name"] == "candidate-runtime-qualification"
                and check["status"] == "passed"
            ),
            None,
        )
        if qualification is None or qualification.get("method") not in {
            "isolated-smoke",
            "representative-run",
            "full-run",
        }:
            raise ValueError(
                f"Release phase {status} requires passed candidate runtime qualification"
            )
        function_artifact = receipt["functions_artifact"]
        attachment = receipt["functions_attachment"]
        if function_artifact is not None:
            required_function_checks = {
                "functions-artifact-integrity",
                "functions-attachment",
                "functions-target-import",
                "functions-target-execution",
            }
            if attachment["fresh_session_required"]:
                required_function_checks.add("functions-session-activation")
            missing = sorted(required_function_checks - passed_checks)
            if missing:
                raise ValueError(
                    f"Function release phase {status} requires passed checks: "
                    + ", ".join(missing)
                )
    if status == "active":
        for required in ("activation-preflight", "target-observation"):
            if required not in passed_checks:
                raise ValueError(f"Active release requires a passed {required} check")
    elif status == "failed" and not any(
        item["status"] == "failed"
        for item in [*receipt["transfers"], *receipt["verification"]]
    ) and receipt["target"]["status"] not in {"active_unhealthy", "partial", "failed"}:
        raise ValueError("Failed release requires failed, partial, or unhealthy target evidence")


def validate_receipt(
    workspace: Path,
    receipt_path: Path,
    *,
    require_success: bool = False,
    build_selector: str | None = None,
    _visited: set[Path] | None = None,
) -> dict[str, Any]:
    """Validate one explicitly selected release receipt and its exact dependencies."""
    workspace = workspace.resolve()
    if receipt_path.is_symlink():
        raise ValueError(f"Release receipt must not be a symlink: {receipt_path}")
    reject_moving_selector(str(receipt_path), "Release receipt path")
    receipt_path = receipt_path.resolve()
    visited = set() if _visited is None else _visited
    if receipt_path in visited:
        raise ValueError("Release receipt source chain contains a cycle")
    visited.add(receipt_path)
    try:
        receipt = load_object(receipt_path, "Release receipt")
        _validate_schema(receipt)
        if build_selector is not None:
            selected_build_id, _ = resolve_build_selector(workspace, build_selector)
            if selected_build_id != receipt["build_id"]:
                raise ValueError("Build selector does not match the prepared release build ID")
        if receipt_path.stem != receipt["release_id"]:
            raise ValueError("Release receipt filename must match release_id")
        expected_parent = workspace / ".releases" / receipt["environment"]
        if receipt_path.parent != expected_parent.resolve():
            raise ValueError(f"Release receipt must be stored directly under {expected_parent}")

        allowed_sources = validate_build_binding(workspace, receipt)
        validate_provision_binding(workspace, receipt)
        _validate_semantics(receipt, allowed_sources)

        source_artifact = receipt["source_release"]
        if source_artifact is not None:
            source_path = resolve_artifact(workspace, source_artifact, "Source release receipt")
            source = validate_receipt(
                workspace, source_path, require_success=True, _visited=visited
            )
            if source["build_id"] != receipt["build_id"]:
                raise ValueError("Source release build_id does not match the release")
            if receipt["action"] == "promote" and source["environment"] == receipt["environment"]:
                raise ValueError("Promotion source and target environments must differ")
            if receipt["action"] == "rollback" and source["environment"] != receipt["environment"]:
                raise ValueError("Rollback source release must belong to the target environment")
            if receipt["action"] == "rollback" and receipt["previous_active_release_id"] == source["release_id"]:
                raise ValueError("Rollback candidate cannot equal the previous active release")

        if require_success and receipt["status"] != "active":
            raise ValueError("An active release receipt is required")
        return receipt
    finally:
        visited.remove(receipt_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--require-success", action="store_true")
    parser.add_argument(
        "--build-selector",
        help="Optional explicit build ID or current selector; resolved once and matched to receipt",
    )
    args = parser.parse_args(argv)
    try:
        receipt = validate_receipt(
            args.workspace,
            args.receipt,
            require_success=args.require_success,
            build_selector=args.build_selector,
        )
        print(f"OK: verified {receipt['action']} release {receipt['release_id']}")
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
