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
    resolve_artifact,
    sha256_file as sha256_file,
    validate_build_binding,
    validate_provision_binding,
)


def deployment_intent_sha256(receipt: dict[str, Any]) -> str:
    """Hash only immutable pre-mutation release intent, never observed results."""
    target = receipt["target"]
    intent = {
        "schema_version": 1,
        "action": receipt["action"],
        "build_id": receipt["build_id"],
        "environment": receipt["environment"],
        "platform": receipt["platform"],
        "target": {
            "identity": target["identity"],
            "activation_mechanism": target["activation_mechanism"],
            "protection": target["protection"],
            "release_reference": target["release_reference"],
        },
        "manifest": receipt["manifest"],
        "build_receipt": receipt["build_receipt"],
        "runner": receipt["runner"],
        "metadata": receipt["metadata"],
        "functions": sorted(receipt["functions"], key=lambda item: item["path"]),
        "provision_receipt": receipt["provision_receipt"],
        "provision_requirements": receipt["provision_requirements"],
        "source_release": receipt["source_release"],
        "previous_active_release_id": receipt["previous_active_release_id"],
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
    mutation_attempted = bool(receipt["transfers"]) or receipt["target"]["status"] in {
        "staged", "active", "partial"
    }
    if authorization is None:
        if receipt["status"] == "succeeded" or mutation_attempted:
            raise ValueError("Target mutation or successful release requires authorization")
    else:
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

    started = datetime.fromisoformat(receipt["started_at"].replace("Z", "+00:00"))
    finished = datetime.fromisoformat(receipt["finished_at"].replace("Z", "+00:00"))
    if started.tzinfo is None or finished.tzinfo is None:
        raise ValueError("Release receipt timestamps require timezones")
    if finished < started:
        raise ValueError("Release receipt finished_at must not precede started_at")

    transfer_paths = [item["source"]["path"] for item in receipt["transfers"]]
    targets = [item["target_reference"] for item in receipt["transfers"]]
    if len(transfer_paths) != len(set(transfer_paths)) or len(targets) != len(set(targets)):
        raise ValueError("Release receipt contains duplicate transfer source or target")
    for transfer in receipt["transfers"]:
        source = transfer["source"]
        if allowed_sources.get(source["path"]) != source["sha256"]:
            raise ValueError("Transfer source is outside or differs from the declared release slice")
        observed = transfer["observed_sha256"]
        if observed is not None and observed != source["sha256"]:
            raise ValueError("Observed target SHA-256 does not match the source artifact")

    if receipt["status"] == "succeeded":
        for required in ("build-preflight", "target-observation"):
            if not any(
                check["name"] == required and check["status"] == "passed"
                for check in receipt["verification"]
            ):
                raise ValueError(f"Successful release requires a passed {required} check")
    elif not any(
        item["status"] == "failed"
        for item in [*receipt["transfers"], *receipt["verification"]]
    ) and receipt["target"]["status"] not in {"partial", "failed"}:
        raise ValueError("Failed release requires failed or partial target evidence")


def validate_receipt(
    workspace: Path,
    receipt_path: Path,
    *,
    require_success: bool = False,
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

        if require_success and receipt["status"] != "succeeded":
            raise ValueError("A successful release receipt is required")
        return receipt
    finally:
        visited.remove(receipt_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--require-success", action="store_true")
    args = parser.parse_args(argv)
    try:
        receipt = validate_receipt(
            args.workspace, args.receipt, require_success=args.require_success
        )
        print(f"OK: verified {receipt['action']} release {receipt['release_id']}")
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
