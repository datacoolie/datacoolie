"""Workspace artifact bindings used by the release receipt validator."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse


BUILD_ID_PATTERN = re.compile(r"^(?P<date>\d{6})-(?P<digest>[0-9a-f]{12})$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _require_artifact_shape(value: Any, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an artifact object")
    if not isinstance(value.get("path"), str) or not value["path"].strip():
        raise ValueError(f"{label} path must be non-empty")
    digest = value.get("sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError(f"{label} SHA-256 is invalid")


def load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must contain an object: {path}")
    return payload


def reject_moving_selector(value: str, label: str) -> None:
    parts = PurePosixPath(value.replace("\\", "/")).parts
    if any(PurePosixPath(part).stem.lower() == "latest" for part in parts) or any(
        character in value for character in "*?[]"
    ):
        raise ValueError(f"{label} must identify one exact artifact; latest and globs are forbidden")


def resolve_artifact(
    workspace: Path,
    artifact: dict[str, str],
    label: str,
) -> Path:
    raw_path = artifact["path"]
    reject_moving_selector(raw_path, label)
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


def _artifact_map(manifest: dict[str, Any]) -> dict[str, str]:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("Build manifest artifacts must be an array")
    result: dict[str, str] = {}
    for item in artifacts:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise ValueError("Build manifest contains an invalid artifact entry")
        path = item["path"]
        digest = item.get("sha256")
        if path in result:
            raise ValueError(f"Build manifest contains duplicate artifact path: {path}")
        if not isinstance(digest, str):
            raise ValueError(f"Build manifest artifact has no SHA-256: {path}")
        result[path] = digest
    return result


def _verify_build_directory(build_dir: Path, manifest: dict[str, Any]) -> None:
    if build_dir.is_symlink():
        raise ValueError("Build directory must not be a symlink")
    for path in build_dir.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"Build artifact must not be a symlink: {path}")
    build_id = manifest.get("build_id")
    if build_id != build_dir.name:
        raise ValueError("Build directory/name mismatch")
    if manifest.get("schema_version") != 1:
        raise ValueError("Build manifest schema_version is unsupported")
    if not isinstance(manifest.get("datacoolie_version"), str) or not manifest["datacoolie_version"].strip():
        raise ValueError("Build manifest DataCoolie version is missing")
    input_digest = manifest.get("input_digest")
    if not isinstance(input_digest, str) or re.fullmatch(r"[0-9a-f]{64}", input_digest) is None:
        raise ValueError("Build input digest is invalid")
    match = BUILD_ID_PATTERN.fullmatch(build_id or "")
    if match is None:
        raise ValueError("Build ID is invalid")
    content_digest = manifest.get("content_digest")
    if not isinstance(content_digest, str) or re.fullmatch(r"[0-9a-f]{64}", content_digest) is None:
        raise ValueError("Build content digest is invalid")
    identity = {
        "input_digest": manifest.get("input_digest"),
        "artifacts": manifest.get("artifacts"),
    }
    if canonical_digest(identity) != content_digest:
        raise ValueError("Build content digest does not match the manifest")
    if match.group("digest") != content_digest[:12]:
        raise ValueError("Build ID does not match the content digest")
    created_at = manifest.get("created_at")
    if not isinstance(created_at, str) or not created_at.endswith("Z"):
        raise ValueError("Build creation timestamp must be UTC")
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Build creation timestamp is invalid") from exc
    if match.group("date") != created.astimezone(timezone.utc).strftime("%y%m%d"):
        raise ValueError("Build ID does not match the creation date")

    checksums = build_dir / "SHA256SUMS"
    if checksums.is_symlink() or not checksums.is_file():
        raise ValueError("Build checksum file is missing or symlinked")
    expected_paths: set[str] = set()
    for line in checksums.read_text(encoding="utf-8").splitlines():
        if not line.strip() or "  " not in line:
            raise ValueError("Build checksum file contains an invalid line")
        expected, relative = line.split("  ", 1)
        relative_path = PurePosixPath(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError("Build checksum path is not canonical")
        artifact = build_dir / Path(*relative_path.parts)
        if not artifact.is_file() or sha256_file(artifact) != expected:
            raise ValueError(f"Build checksum mismatch: {relative}")
        expected_paths.add(relative_path.as_posix())
    actual_paths = {
        path.relative_to(build_dir).as_posix()
        for path in build_dir.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    }
    if actual_paths != expected_paths:
        raise ValueError("Build contains untracked or missing files")


def _require_persistent_path(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Build receipt {label} must be a non-empty string")
    parts = PurePosixPath(urlparse(value).path.replace("\\", "/")).parts
    if ".builds" in {part.lower() for part in parts}:
        raise ValueError(f"Build receipt {label} must remain outside .builds/")


def _validate_build_receipt(
    receipt: dict[str, Any],
    manifest: dict[str, Any],
    release: dict[str, Any],
    receipt_path: Path,
) -> None:
    required = {
        "schema_version",
        "artifact_type",
        "receipt_id",
        "status",
        "build_id",
        "environment",
        "platform",
        "datacoolie_version",
        "runner",
        "metadata",
        "functions",
        "operation",
        "stage_plan",
        "execution_reference",
        "base_log_path",
        "watermark_base_path",
        "checks",
        "started_at",
        "finished_at",
        "unresolved_issues",
    }
    missing = sorted(required - set(receipt))
    if missing:
        raise ValueError(f"Build receipt is missing required fields: {', '.join(missing)}")
    _require_artifact_shape(receipt["runner"], "Build receipt runner")
    _require_artifact_shape(receipt["metadata"], "Build receipt metadata")
    if not isinstance(receipt["functions"], list):
        raise ValueError("Build receipt functions must be an array")
    for item in receipt["functions"]:
        _require_artifact_shape(item, "Build receipt function")
    if receipt["operation"] not in {"run", "replay", "maintenance"}:
        raise ValueError("Build receipt operation is invalid")
    if not isinstance(receipt["stage_plan"], list):
        raise ValueError("Build receipt stage_plan must be an array")
    if not isinstance(receipt["unresolved_issues"], list):
        raise ValueError("Build receipt unresolved_issues must be an array")
    if receipt_path.stem != receipt["receipt_id"]:
        raise ValueError("Build receipt filename must match receipt_id")
    for field, expected in {
        "schema_version": 1,
        "artifact_type": "build_verification",
        "status": "succeeded",
        "build_id": release["build_id"],
        "environment": release["environment"],
        "platform": release["platform"],
        "datacoolie_version": manifest.get("datacoolie_version"),
    }.items():
        if receipt.get(field) != expected:
            raise ValueError(f"Build receipt {field} does not match the release")
    runner_name = PurePosixPath(receipt["runner"]["path"]).name
    if not runner_name.startswith(f"{receipt['operation']}_"):
        raise ValueError("Build receipt operation does not match its runner")
    _require_persistent_path(receipt["base_log_path"], "base_log_path")
    _require_persistent_path(receipt["watermark_base_path"], "watermark_base_path")
    if not isinstance(receipt["execution_reference"], str) or not receipt["execution_reference"].strip():
        raise ValueError("Build receipt execution_reference must be non-empty")
    checks = receipt["checks"]
    if not isinstance(checks, list) or any(
        not isinstance(check, dict) or check.get("status") not in {"passed", "skipped"}
        for check in checks
    ):
        raise ValueError("Successful build receipt contains an invalid or failed check")
    if not any(
        isinstance(check, dict)
        and check.get("name") == "generated-runtime-execution"
        and check.get("status") == "passed"
        for check in checks
    ):
        raise ValueError("Build receipt requires a passed generated-runtime-execution check")
    if receipt["unresolved_issues"] != []:
        raise ValueError("Successful build receipt must not contain unresolved issues")
    try:
        started = datetime.fromisoformat(receipt["started_at"].replace("Z", "+00:00"))
        finished = datetime.fromisoformat(receipt["finished_at"].replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError("Build receipt timestamps are invalid") from exc
    if started.tzinfo is None or finished.tzinfo is None or finished < started:
        raise ValueError("Build receipt timestamps are invalid or out of order")


def validate_build_binding(
    workspace: Path,
    receipt: dict[str, Any],
) -> dict[str, str]:
    build_candidate = workspace / ".builds" / receipt["build_id"]
    if build_candidate.is_symlink():
        raise ValueError("Build directory must not be a symlink")
    build_dir = build_candidate.resolve()
    manifest_path = resolve_artifact(workspace, receipt["manifest"], "Build manifest")
    if manifest_path != build_dir / "manifest.json":
        raise ValueError("Manifest must be the exact build manifest")
    manifest = load_object(manifest_path, "Build manifest")
    _verify_build_directory(build_dir, manifest)
    if manifest.get("build_id") != receipt["build_id"]:
        raise ValueError("Release build_id does not match the build manifest")
    environments = manifest.get("environments")
    if not isinstance(environments, dict) or receipt["environment"] not in environments:
        raise ValueError("Release environment is not declared by the build manifest")
    environment = environments[receipt["environment"]]
    if not isinstance(environment, dict) or environment.get("platform") != receipt["platform"]:
        raise ValueError("Release platform does not match the build environment")

    declared = _artifact_map(manifest)
    selections = [
        ("Runner", receipt["runner"]),
        ("Metadata", receipt["metadata"]),
        *(("Function", item) for item in receipt["functions"]),
    ]
    selected: dict[str, str] = {}
    relative_paths: list[str] = []
    for label, artifact in selections:
        path = resolve_artifact(workspace, artifact, f"{label} artifact")
        try:
            relative = path.relative_to(build_dir).as_posix()
        except ValueError as exc:
            raise ValueError(f"{label} artifact is outside the exact build") from exc
        canonical = f".builds/{receipt['build_id']}/{relative}"
        if artifact["path"].replace("\\", "/") != canonical:
            raise ValueError(f"{label} artifact path is not canonical for the exact build")
        if declared.get(relative) != artifact["sha256"]:
            raise ValueError(f"{label} artifact does not match the build manifest: {relative}")
        selected[artifact["path"]] = artifact["sha256"]
        relative_paths.append(relative)

    runner_path, metadata_path, *function_paths = relative_paths
    if runner_path not in environment.get("runners", []):
        raise ValueError("Release runner is not declared for the build environment")
    if metadata_path != environment.get("metadata"):
        raise ValueError("Release metadata is not declared for the build environment")
    if set(function_paths) != set(manifest.get("functions") or []):
        raise ValueError("Release functions do not match the build manifest")

    build_receipt_path = resolve_artifact(
        workspace, receipt["build_receipt"], "Build verification receipt"
    )
    expected_parent = workspace / ".evidence" / "builds" / receipt["build_id"] / receipt["environment"]
    if build_receipt_path.parent != expected_parent.resolve():
        raise ValueError(f"Build receipt must be stored directly under {expected_parent}")
    build_receipt = load_object(build_receipt_path, "Build verification receipt")
    _validate_build_receipt(build_receipt, manifest, receipt, build_receipt_path)
    if build_receipt.get("runner") != {"path": runner_path, "sha256": receipt["runner"]["sha256"]}:
        raise ValueError("Build receipt runner does not match the release slice")
    if build_receipt.get("metadata") != {"path": metadata_path, "sha256": receipt["metadata"]["sha256"]}:
        raise ValueError("Build receipt metadata does not match the release slice")
    build_functions = {
        item.get("path"): item.get("sha256") for item in build_receipt.get("functions", [])
        if isinstance(item, dict)
    }
    if build_functions != {path: declared[path] for path in function_paths}:
        raise ValueError("Build receipt functions do not match the release slice")

    selected[receipt["manifest"]["path"]] = receipt["manifest"]["sha256"]
    return selected


def validate_provision_binding(workspace: Path, receipt: dict[str, Any]) -> None:
    artifact = receipt["provision_receipt"]
    if artifact is None:
        if receipt["provision_requirements"] is not None:
            raise ValueError("Provision requirements require an exact provision receipt")
        return
    if receipt["provision_requirements"] is None:
        raise ValueError("Provision receipt requires the exact blocked requirements artifact")
    path = resolve_artifact(workspace, artifact, "Provision receipt")
    expected_parent = workspace / ".evidence" / "provision" / receipt["environment"]
    if path.parent != expected_parent.resolve():
        raise ValueError(f"Provision receipt must be stored directly under {expected_parent}")
    provision = load_object(path, "Provision receipt")
    required = {
        "schema_version",
        "artifact_type",
        "receipt_id",
        "operation",
        "status",
        "environment",
        "platform",
        "requirements",
        "plan",
        "authorizations",
        "state",
        "actions",
        "verification",
        "unresolved_issues",
    }
    missing = sorted(required - set(provision))
    if missing:
        raise ValueError(f"Provision receipt is missing required fields: {', '.join(missing)}")
    if path.stem != provision["receipt_id"]:
        raise ValueError("Provision receipt filename must match receipt_id")
    for field, expected in {
        "schema_version": 1,
        "artifact_type": "provision_receipt",
        "operation": "apply",
        "status": "succeeded",
        "environment": receipt["environment"],
        "platform": receipt["platform"],
    }.items():
        if provision.get(field) != expected:
            raise ValueError(f"Provision receipt {field} does not match the release")
    if provision["requirements"] != receipt["provision_requirements"]:
        raise ValueError("Provision receipt requirements do not match the blocked prerequisite")
    _require_artifact_shape(provision["requirements"], "Provision requirements")
    _require_artifact_shape(provision["plan"], "Provision plan")
    resolve_artifact(workspace, provision["requirements"], "Provision requirements")
    plan_path = resolve_artifact(workspace, provision["plan"], "Provision plan")
    expected_plan_parent = expected_parent / "plans"
    if plan_path.parent != expected_plan_parent.resolve():
        raise ValueError(f"Provision plan must be stored directly under {expected_plan_parent}")
    authorizations = provision["authorizations"]
    if not isinstance(authorizations, list) or not any(
        isinstance(item, dict)
        and item.get("scope") == "apply"
        and item.get("environment") == receipt["environment"]
        and item.get("plan_sha256") == provision["plan"]["sha256"]
        and isinstance(item.get("reference"), str)
        and item["reference"].strip()
        for item in authorizations
    ):
        raise ValueError("Provision receipt requires exact plan-bound apply authorization")
    state = provision["state"]
    if not isinstance(state, dict) or state.get("status") in {"partial", "unknown"}:
        raise ValueError("Successful provision receipt has incomplete target state")
    actions = provision["actions"]
    if not isinstance(actions, list) or any(
        not isinstance(item, dict) or item.get("status") in {"planned", "failed"}
        for item in actions
    ):
        raise ValueError("Successful provision receipt contains incomplete actions")
    destructive = any(
        item.get("data_bearing") is True and item.get("action") in {"replace", "delete"}
        for item in actions
    )
    if destructive and not any(
        isinstance(item, dict)
        and item.get("scope") == "destructive"
        and item.get("environment") == receipt["environment"]
        and item.get("plan_sha256") == provision["plan"]["sha256"]
        and isinstance(item.get("reference"), str)
        and item["reference"].strip()
        for item in authorizations
    ):
        raise ValueError("Provision receipt requires destructive plan authorization")
    verification = provision["verification"]
    if not isinstance(verification, list) or any(
        not isinstance(item, dict) or item.get("status") not in {"passed", "skipped"}
        for item in verification
    ):
        raise ValueError("Successful provision receipt contains invalid verification")
    if not any(
        isinstance(item, dict)
        and item.get("name") == "resource-observation"
        and item.get("status") == "passed"
        for item in verification
    ):
        raise ValueError("Provision receipt requires a passed resource-observation check")
    if provision["unresolved_issues"] != []:
        raise ValueError("Successful provision receipt must not contain unresolved issues")
