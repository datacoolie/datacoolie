#!/usr/bin/env python3
"""Validate one explicit provision receipt and its exact input artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


SENSITIVE_NAMES = (
    "password",
    "secret",
    "token",
    "credential",
    "private_key",
    "connection_string",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must contain an object: {path}")
    return payload


def _validate_schema(receipt: dict[str, Any]) -> None:
    schema_path = (
        Path(__file__).resolve().parent.parent
        / "schemas"
        / "provision-receipt.schema.json"
    )
    schema = _load_object(schema_path, "Provision receipt schema")
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
        raise ValueError("Invalid provision receipt:\n- " + "\n- ".join(details))


def _reject_moving_selector(value: str, label: str) -> None:
    parts = PurePosixPath(value.replace("\\", "/")).parts
    if any(PurePosixPath(part).stem.lower() == "latest" for part in parts) or any(
        character in value for character in "*?[]"
    ):
        raise ValueError(f"{label} must identify one exact artifact; latest and globs are forbidden")


def _resolve_artifact(
    workspace: Path,
    artifact: dict[str, str],
    label: str,
) -> Path:
    raw_path = artifact["path"]
    path_value = PurePosixPath(raw_path.replace("\\", "/"))
    if path_value.is_absolute() or ".." in path_value.parts:
        raise ValueError(f"{label} path must be workspace-relative without traversal")
    candidate = workspace / Path(*path_value.parts)
    current = workspace
    for part in path_value.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{label} path must not contain symlinks: {current}")
    resolved = candidate.resolve()
    try:
        resolved.relative_to(workspace)
    except ValueError as exc:
        raise ValueError(f"{label} path escapes the workspace") from exc
    if not resolved.is_file():
        raise ValueError(f"{label} must be an existing regular file: {resolved}")
    if sha256_file(resolved) != artifact["sha256"]:
        raise ValueError(f"{label} SHA-256 does not match persisted bytes")
    return resolved


def _validate_authorizations(receipt: dict[str, Any]) -> None:
    authorizations = receipt["authorizations"]
    scopes = [item["scope"] for item in authorizations]
    if len(scopes) != len(set(scopes)):
        raise ValueError("Receipt contains duplicate authorization scopes")
    for item in authorizations:
        if item["environment"] != receipt["environment"]:
            raise ValueError("Authorization environment does not match receipt environment")
        if item["plan_sha256"] != receipt["plan"]["sha256"]:
            raise ValueError("Authorization plan SHA-256 does not match the persisted plan")
    if receipt["operation"] == "apply":
        if "apply" not in scopes:
            raise ValueError("Apply receipt requires exact apply authorization")
        destructive = any(
            item["data_bearing"] and item["action"] in {"replace", "delete"}
            for item in receipt["actions"]
        )
        if destructive and "destructive" not in scopes:
            raise ValueError("Data-bearing replacement or deletion requires destructive authorization")


def _validate_semantics(receipt: dict[str, Any]) -> None:
    started = datetime.fromisoformat(receipt["started_at"].replace("Z", "+00:00"))
    finished = datetime.fromisoformat(receipt["finished_at"].replace("Z", "+00:00"))
    if started.tzinfo is None or finished.tzinfo is None:
        raise ValueError("Receipt timestamps require timezones")
    if finished < started:
        raise ValueError("Receipt finished_at must not precede started_at")

    if receipt["operation"] == "plan" and any(
        item["status"] not in {"planned", "skipped"} for item in receipt["actions"]
    ):
        raise ValueError("Plan receipt actions must be planned or skipped")
    if receipt["operation"] == "apply" and receipt["status"] == "succeeded":
        if receipt["state"]["status"] in {"partial", "unknown"}:
            raise ValueError("Successful apply cannot report partial or unknown state")
        if any(item["status"] in {"planned", "failed"} for item in receipt["actions"]):
            raise ValueError("Successful apply contains an incomplete or failed action")
        if not any(
            item["name"] == "resource-observation" and item["status"] == "passed"
            for item in receipt["verification"]
        ):
            raise ValueError("Successful apply requires a passed resource-observation check")
    if receipt["status"] == "failed" and not any(
        item["status"] == "failed"
        for item in [*receipt["actions"], *receipt["verification"]]
    ):
        raise ValueError("Failed receipt requires failed action or verification evidence")

    for output in receipt["resource_outputs"]:
        normalized = output["name"].lower().replace("-", "_")
        if any(name in normalized for name in SENSITIVE_NAMES):
            raise ValueError(f"Sensitive-looking resource output is forbidden: {output['name']}")


def validate_receipt(
    workspace: Path,
    receipt_path: Path,
    *,
    require_apply_success: bool = False,
) -> dict[str, Any]:
    """Validate one explicitly selected receipt; never infer a receipt or plan."""
    if receipt_path.is_symlink():
        raise ValueError(f"Provision receipt must not be a symlink: {receipt_path}")
    _reject_moving_selector(str(receipt_path), "Receipt path")
    workspace = workspace.resolve()
    receipt = _load_object(receipt_path, "Provision receipt")
    _validate_schema(receipt)

    if receipt_path.stem != receipt["receipt_id"]:
        raise ValueError("Receipt filename must match receipt_id")
    expected_parent = (
        workspace / ".evidence" / "provision" / receipt["environment"]
    ).resolve()
    if receipt_path.resolve().parent != expected_parent:
        raise ValueError(f"Receipt must be stored directly under {expected_parent}")

    requirements_path = _resolve_artifact(
        workspace, receipt["requirements"], "Requirements artifact"
    )
    plan_path = _resolve_artifact(workspace, receipt["plan"], "Plan artifact")
    _reject_moving_selector(receipt["plan"]["path"], "Plan path")
    expected_plan_parent = expected_parent / "plans"
    if plan_path.parent != expected_plan_parent:
        raise ValueError(f"Plan artifact must be stored directly under {expected_plan_parent}")
    if plan_path == requirements_path:
        raise ValueError("Plan and requirements must be distinct artifacts")

    _validate_authorizations(receipt)
    _validate_semantics(receipt)
    if require_apply_success and (
        receipt["operation"] != "apply" or receipt["status"] != "succeeded"
    ):
        raise ValueError("A successful provision apply receipt is required")
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--require-apply-success", action="store_true")
    args = parser.parse_args(argv)
    try:
        receipt = validate_receipt(
            args.workspace,
            args.receipt,
            require_apply_success=args.require_apply_success,
        )
        print(f"OK: verified {receipt['operation']} receipt {receipt['receipt_id']}")
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
