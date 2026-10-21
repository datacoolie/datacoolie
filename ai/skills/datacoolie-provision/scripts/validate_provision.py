#!/usr/bin/env python3
"""Validate one explicit provision receipt and its exact input artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
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

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_HOST_CHECK_TERMS = ("access", "capability")


def _normalise_workspace_relative(value: str, label: str) -> str:
    """Return one safe, slash-separated workspace-relative path."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty workspace-relative path")
    posix = PurePosixPath(value.replace("\\", "/"))
    windows = PureWindowsPath(value)
    if (
        posix.is_absolute()
        or windows.is_absolute()
        or bool(windows.drive)
        or ".." in posix.parts
    ):
        raise ValueError(f"{label} must be workspace-relative without traversal")
    return posix.as_posix()


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


def _scoped_readiness_evidence(
    check: dict[str, Any],
    *,
    environment: str,
    requirements_path: str,
    requirements_sha256: str,
    expected_resource: str,
    expected_capability: str,
) -> dict[str, Any]:
    """Validate the recorded scope for one readiness check.

    The JSON is intentionally evidence supplied by the provisioner.  Parsing
    and comparing its declared scope does not authenticate the observation or
    the authorization that produced it.
    """

    evidence = check.get("evidence")
    if not isinstance(evidence, str) or not evidence.strip():
        raise ValueError(f"Readiness check {check['name']!r} requires scoped JSON evidence")
    try:
        payload = json.loads(evidence)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Readiness check {check['name']!r} evidence is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Readiness check {check['name']!r} evidence must be a JSON object")
    if payload.get("environment") != environment:
        raise ValueError(f"Readiness check {check['name']!r} evidence has the wrong environment")

    requirements = payload.get("requirements")
    if not isinstance(requirements, dict):
        raise ValueError(
            f"Readiness check {check['name']!r} evidence is missing requirements scope"
        )
    evidence_path = requirements.get("path")
    if not isinstance(evidence_path, str):
        raise ValueError(
            f"Readiness check {check['name']!r} evidence has an invalid requirements path"
        )
    if _normalise_workspace_relative(evidence_path, "Readiness requirements path") != requirements_path:
        raise ValueError(f"Readiness check {check['name']!r} evidence has the wrong requirements path")
    if requirements.get("sha256") != requirements_sha256:
        raise ValueError(
            f"Readiness check {check['name']!r} evidence has the wrong requirements SHA-256"
        )

    for field in ("resource", "capability", "observation"):
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"Readiness check {check['name']!r} evidence requires a non-empty {field}"
            )
    if payload["resource"] != expected_resource:
        raise ValueError(f"Readiness check {check['name']!r} evidence has the wrong resource")
    if payload["capability"] != expected_capability:
        raise ValueError(f"Readiness check {check['name']!r} evidence has the wrong capability")
    return payload


def _is_host_access_or_capability_check(name: str) -> bool:
    normalized = name.casefold().replace("_", "-").replace(" ", "-")
    return "host" in normalized and any(term in normalized for term in _HOST_CHECK_TERMS)


def _normalise_required_observations(value: Any) -> dict[str, tuple[str, str]]:
    """Normalize API mappings or CLI-style ``(check, resource, capability)`` triples."""

    if value is None:
        return {}
    if isinstance(value, Mapping):
        entries = value.items()
    elif isinstance(value, (list, tuple)):
        entries = []
        for item in value:
            if isinstance(item, Mapping):
                name = item.get("check", item.get("name"))
                resource = item.get("resource")
                capability = item.get("capability")
            elif isinstance(item, (list, tuple)) and len(item) == 3:
                name, resource, capability = item
            else:
                raise ValueError(
                    "Required observations must be mappings or check/resource/capability triples"
                )
            entries.append((name, (resource, capability)))
    else:
        raise ValueError(
            "Required observations must be a mapping or check/resource/capability triples"
        )

    normalized: dict[str, tuple[str, str]] = {}
    for name, scope in entries:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Required observation check names must be non-empty")
        if isinstance(scope, Mapping):
            resource = scope.get("resource")
            capability = scope.get("capability")
        elif isinstance(scope, (list, tuple)) and len(scope) == 2:
            resource, capability = scope
        else:
            raise ValueError(
                f"Required observation scope is invalid for check {name!r}"
            )
        if (
            not isinstance(resource, str)
            or not resource.strip()
            or not isinstance(capability, str)
            or not capability.strip()
        ):
            raise ValueError(
                f"Required observation scope must have resource and capability for {name!r}"
            )
        if name in normalized:
            raise ValueError(f"Duplicate required observation: {name}")
        normalized[name] = (resource, capability)
    return normalized


def _validate_readiness(
    receipt: dict[str, Any],
    *,
    environment: str,
    requirements: str,
    requirements_sha256: str,
    required_checks: list[str],
    required_observations: Any,
) -> None:
    """Apply the explicit consumer readiness contract to a validated v1 receipt."""

    if not isinstance(environment, str) or not environment.strip():
        raise ValueError("--environment is required for --require-ready")
    if not isinstance(requirements_sha256, str) or not _SHA256.fullmatch(requirements_sha256):
        raise ValueError("--requirements-sha256 must be 64 lowercase hexadecimal characters")
    requirements_path = _normalise_workspace_relative(requirements, "--requirements")
    if receipt["environment"] != environment:
        raise ValueError("Readiness receipt environment does not match the expected environment")
    if _normalise_workspace_relative(receipt["requirements"]["path"], "Receipt requirements path") != requirements_path:
        raise ValueError("Readiness receipt requirements path does not match the expected path")
    if receipt["requirements"]["sha256"] != requirements_sha256:
        raise ValueError("Readiness receipt requirements SHA-256 does not match the expected hash")

    if receipt["status"] != "succeeded":
        raise ValueError("A successful provision receipt is required for readiness")
    if receipt["operation"] == "plan" and any(
        action["action"] != "no_change" for action in receipt["actions"]
    ):
        raise ValueError("A readiness plan may contain only no_change actions")
    if receipt["operation"] not in {"plan", "apply"}:
        raise ValueError("Unsupported provision operation for readiness")
    if receipt["state"]["status"] in {"partial", "unknown"}:
        raise ValueError("A partial or unknown provision state cannot be ready")

    if len(required_checks) != len(set(required_checks)):
        raise ValueError("--required-check values must be unique")
    if any(not isinstance(name, str) or not name.strip() for name in required_checks):
        raise ValueError("--required-check values must be non-empty")
    if not any(_is_host_access_or_capability_check(name) for name in required_checks):
        raise ValueError(
            "Readiness requires at least one requested host access or capability check"
        )
    observation_scopes = _normalise_required_observations(required_observations)
    mandatory_checks = {"resource-observation", *required_checks}
    missing_scopes = sorted(mandatory_checks - set(observation_scopes))
    if missing_scopes:
        raise ValueError(
            "Readiness requires expected resource/capability scope for: "
            + ", ".join(missing_scopes)
        )
    extra_scopes = sorted(set(observation_scopes) - mandatory_checks)
    if extra_scopes:
        raise ValueError(
            "Readiness has unexpected required observation scope for: "
            + ", ".join(extra_scopes)
        )

    checks_by_name: dict[str, list[dict[str, Any]]] = {}
    for check in receipt["verification"]:
        checks_by_name.setdefault(check["name"], []).append(check)

    def require_check(name: str) -> None:
        matches = checks_by_name.get(name, [])
        if len(matches) != 1:
            raise ValueError(f"Required readiness check is missing or ambiguous: {name}")
        check = matches[0]
        if check["status"] != "passed":
            raise ValueError(f"Required readiness check did not pass: {name}")
        _scoped_readiness_evidence(
            check,
            environment=environment,
            requirements_path=requirements_path,
            requirements_sha256=requirements_sha256,
            expected_resource=observation_scopes[name][0],
            expected_capability=observation_scopes[name][1],
        )

    # A plan-validation check describes only planning.  Resource observation
    # is always required independently of the consumer's requested checks.
    require_check("resource-observation")
    for name in required_checks:
        require_check(name)


def validate_receipt(
    workspace: Path,
    receipt_path: Path,
    *,
    require_apply_success: bool = False,
    require_ready: bool = False,
    environment: str | None = None,
    requirements: str | Path | None = None,
    requirements_sha256: str | None = None,
    required_checks: list[str] | None = None,
    required_observations: Any = None,
    expected_environment: str | None = None,
    requirements_path: str | Path | None = None,
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
        workspace
        / "provision"
        / "evidence"
        / receipt["environment"]
        / "receipts"
    ).resolve()
    if receipt_path.resolve().parent != expected_parent:
        raise ValueError(f"Receipt must be stored directly under {expected_parent}")

    requirements_artifact_path = _resolve_artifact(
        workspace, receipt["requirements"], "Requirements artifact"
    )
    plan_path = _resolve_artifact(workspace, receipt["plan"], "Plan artifact")
    _reject_moving_selector(receipt["plan"]["path"], "Plan path")
    expected_plan_parent = expected_parent.parent / "plans"
    if plan_path.parent != expected_plan_parent:
        raise ValueError(f"Plan artifact must be stored directly under {expected_plan_parent}")
    if plan_path == requirements_artifact_path:
        raise ValueError("Plan and requirements must be distinct artifacts")

    _validate_authorizations(receipt)
    _validate_semantics(receipt)
    if require_apply_success and (
        receipt["operation"] != "apply" or receipt["status"] != "succeeded"
    ):
        raise ValueError("A successful provision apply receipt is required")
    if require_ready:
        if environment is None:
            environment = expected_environment
        elif expected_environment is not None and environment != expected_environment:
            raise ValueError("Conflicting readiness environment values")
        if requirements is None:
            requirements = requirements_path
        elif requirements_path is not None and str(requirements) != str(requirements_path):
            raise ValueError("Conflicting readiness requirements paths")
        if environment is None or requirements is None or requirements_sha256 is None:
            raise ValueError(
                "--require-ready requires environment, requirements, requirements SHA-256, "
                "and at least one required check"
            )
        checks = [] if required_checks is None else required_checks
        _validate_readiness(
            receipt,
            environment=environment,
            requirements=str(requirements),
            requirements_sha256=requirements_sha256,
            required_checks=checks,
            required_observations=required_observations,
        )
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--require-apply-success", action="store_true")
    parser.add_argument("--require-ready", action="store_true")
    parser.add_argument("--environment")
    parser.add_argument("--requirements")
    parser.add_argument("--requirements-sha256")
    parser.add_argument("--required-check", action="append", default=[])
    parser.add_argument(
        "--required-observation",
        nargs=3,
        action="append",
        default=[],
        metavar=("CHECK", "RESOURCE", "CAPABILITY"),
    )
    args = parser.parse_args(argv)
    try:
        receipt = validate_receipt(
            args.workspace,
            args.receipt,
            require_apply_success=args.require_apply_success,
            require_ready=args.require_ready,
            environment=args.environment,
            requirements=args.requirements,
            requirements_sha256=args.requirements_sha256,
            required_checks=args.required_check,
            required_observations=args.required_observation,
        )
        print(f"OK: verified {receipt['operation']} receipt {receipt['receipt_id']}")
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
