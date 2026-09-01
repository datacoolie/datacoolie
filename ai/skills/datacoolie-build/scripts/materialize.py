#!/usr/bin/env python3
"""Materialize a time-addressed, content-bound immutable DataCoolie build."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePosixPath
from typing import Any

from _schema_resolver import find_schemas_dir, load_schema, resolve_schema_version
from merge import merge_metadata, write_metadata
from validate import validate_metadata
from validate_config import validate_config
from validate_functions import inspect_artifact, referenced_functions, sha256_file, validate_metadata_files


RUNNER_SUFFIXES = (".py", ".ipynb")
RUNNER_OPERATIONS = ("run", "replay", "maintenance")
METADATA_LAYOUTS = ("single", "split-connections", "split-all")
METADATA_LAYOUT_ROLES = {
    "single": {"config_path": "metadata.json"},
    "split-connections": {
        "config_path": "dataflows.json",
        "connections_path": "connections.json",
    },
    "split-all": {
        "config_path": "dataflows.json",
        "connections_path": "connections.json",
        "schema_hints_path": "schema_hints.json",
    },
}
BUILD_ID_PATTERN = re.compile(
    r"^(?P<date>\d{6})-(?P<time>\d{6})-(?P<digest>[0-9a-f]{12})$"
)
EXCLUDED_NAMES = {".env", ".env.local"}
TOOLING_FILES = (
    "_loaders.py",
    "materialize.py",
    "merge.py",
    "validate.py",
    "validate_config.py",
    "validate_functions.py",
    "_schema_resolver.py",
    "requirements.txt",
)
def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _metadata_set_digest(files: dict[str, dict[str, str]]) -> str:
    return _canonical_digest({role: item["sha256"] for role, item in files.items()})


def _validate_design_receipt(payload: Any, digest: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Design approval receipt must contain an object")
    expected_fields = {
        "schema_version",
        "artifact_type",
        "decision",
        "architecture_path",
        "architecture_sha256",
        "approved_at",
        "approved_by",
        "approval_reference",
        "approved_scope",
    }
    missing = sorted(expected_fields - set(payload))
    unknown = sorted(set(payload) - expected_fields)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing fields: {', '.join(missing)}")
        if unknown:
            details.append(f"unknown fields: {', '.join(unknown)}")
        raise ValueError(f"Malformed design approval receipt ({'; '.join(details)})")
    constants = {
        "schema_version": 1,
        "artifact_type": "design_approval",
        "decision": "approved",
        "architecture_path": "architecture/current.md",
        "architecture_sha256": digest,
    }
    for field, expected in constants.items():
        if payload[field] != expected:
            raise ValueError(f"Design approval receipt has invalid {field}")
    approved_at = payload["approved_at"]
    if not isinstance(approved_at, str) or not approved_at.endswith("Z"):
        raise ValueError("Design approval receipt requires a UTC approved_at timestamp")
    try:
        parsed = datetime.fromisoformat(approved_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Design approval receipt has invalid approved_at") from exc
    if parsed.tzinfo is None:
        raise ValueError("Design approval receipt requires a timezone-aware approved_at")
    for field in ("approved_by", "approval_reference", "approved_scope"):
        if not isinstance(payload[field], str) or not payload[field].strip():
            raise ValueError(f"Design approval receipt requires non-empty {field}")


def _validate_design_approval(workspace: Path) -> dict[str, Any] | None:
    architecture = workspace / "architecture" / "current.md"
    if not architecture.is_file():
        return None
    digest = _sha256(architecture)
    result: dict[str, Any] = {
        "architecture_path": "architecture/current.md",
        "architecture_sha256": digest,
    }

    receipt_path = (
        workspace / ".approvals" / "design" / f"architecture-{digest[:12]}.approved.json"
    )
    if receipt_path.is_symlink():
        raise ValueError("Design approval receipt must not be a symlink")
    if not receipt_path.is_file():
        raise ValueError("Matching design approval receipt does not exist")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("Design approval receipt is not valid JSON") from exc
    _validate_design_receipt(receipt, digest)
    result["approval_receipt"] = receipt_path.relative_to(workspace).as_posix()
    return result


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _build_id(content_digest: str, created_at: datetime) -> str:
    timestamp = created_at.astimezone(timezone.utc)
    return f"{timestamp:%y%m%d-%H%M%S}-{content_digest[:12]}"


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_current_descriptor(current_dir: Path) -> dict[str, Any]:
    descriptor_path = current_dir / "build.json"
    if descriptor_path.is_symlink() or not descriptor_path.is_file():
        raise ValueError(f"Current build descriptor does not exist or is a symlink: {descriptor_path}")
    try:
        payload = json.loads(descriptor_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Current build descriptor is not valid JSON: {descriptor_path}") from exc
    expected = {"schema_version", "artifact_type", "build_id"}
    if not isinstance(payload, dict) or set(payload) != expected:
        raise ValueError("Current build descriptor fields do not match the contract")
    if payload["schema_version"] != 1 or payload["artifact_type"] != "current_build":
        raise ValueError("Current build descriptor contract is unsupported")
    build_id = payload["build_id"]
    if not isinstance(build_id, str) or BUILD_ID_PATTERN.fullmatch(build_id) is None:
        raise ValueError("Current build descriptor contains an invalid build ID")
    return payload


def verify_current_build(current_dir: Path) -> dict[str, Any]:
    """Verify runnable current bytes against their exact immutable artifact."""
    if current_dir.is_symlink():
        raise ValueError(f"Current build path must not be a symlink: {current_dir}")
    current_dir = current_dir.resolve()
    _reject_symlinks(current_dir)
    descriptor = _load_current_descriptor(current_dir)
    builds_root = current_dir.parent
    if builds_root.name != ".builds":
        raise ValueError("Current build must be stored directly under .builds/current")
    build_dir = builds_root / "artifacts" / descriptor["build_id"]
    manifest = verify_build(build_dir)
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("Build manifest artifacts must be an array")
    expected_paths: set[str] = set()
    for item in artifacts:
        if not isinstance(item, dict):
            raise ValueError("Build manifest contains an invalid artifact entry")
        relative = item.get("path")
        digest = item.get("sha256")
        if not isinstance(relative, str) or not isinstance(digest, str):
            raise ValueError("Build manifest contains an invalid artifact entry")
        path = current_dir / PurePosixPath(relative)
        try:
            path.resolve().relative_to(current_dir)
        except ValueError as exc:
            raise ValueError(f"Current artifact escapes the projection: {relative}") from exc
        if not path.is_file() or _sha256(path) != digest:
            raise ValueError(f"Current artifact does not match build {descriptor['build_id']}: {relative}")
        expected_paths.add(PurePosixPath(relative).as_posix())
    actual_paths = {
        path.relative_to(current_dir).as_posix()
        for path in current_dir.rglob("*")
        if path.is_file() and path.name != "build.json"
    }
    if actual_paths != expected_paths:
        raise ValueError("Current build contains stale, untracked, or missing runtime files")
    return manifest


def _write_current_projection(workspace: Path, build_dir: Path) -> Path:
    manifest = verify_build(build_dir)
    builds_root = workspace / ".builds"
    current_dir = builds_root / "current"
    candidate = builds_root / f".current-{uuid.uuid4().hex}"
    backup = builds_root / f".current-backup-{uuid.uuid4().hex}"
    candidate.mkdir()
    moved_previous = False
    try:
        for item in manifest["artifacts"]:
            relative = PurePosixPath(item["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"Build artifact path is unsafe: {item['path']}")
            source = build_dir / relative
            destination = candidate / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        descriptor = {
            "schema_version": 1,
            "artifact_type": "current_build",
            "build_id": manifest["build_id"],
        }
        (candidate / "build.json").write_text(
            json.dumps(descriptor, indent=2) + "\n", encoding="utf-8"
        )
        verify_current_build(candidate)
        if current_dir.exists() or current_dir.is_symlink():
            if current_dir.is_symlink() or not current_dir.is_dir():
                raise ValueError(f"Current build path must be a real directory: {current_dir}")
            current_dir.rename(backup)
            moved_previous = True
        candidate.rename(current_dir)
        try:
            verify_current_build(current_dir)
        except Exception:
            shutil.rmtree(current_dir, ignore_errors=True)
            if moved_previous:
                backup.rename(current_dir)
                moved_previous = False
            raise
        if moved_previous:
            shutil.rmtree(backup, ignore_errors=True)
            moved_previous = False
        return current_dir
    except Exception:
        if candidate.exists():
            shutil.rmtree(candidate, ignore_errors=True)
        if moved_previous and backup.exists() and not current_dir.exists():
            backup.rename(current_dir)
        raise


def _is_source_file(path: Path) -> bool:
    return (
        path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
        and path.name not in EXCLUDED_NAMES
        and not path.name.startswith(".env.")
    )


def _input_entries(
    workspace: Path,
    selected_runners: set[Path],
    environments: list[str],
) -> list[dict[str, str]]:
    metadata_dir = workspace / "metadata"
    candidates = [metadata_dir / "connections.json"]
    for optional in (metadata_dir / "schema_hints.json", metadata_dir / "dataflows.json"):
        if optional.is_file():
            candidates.append(optional)
    dataflows_dir = metadata_dir / "dataflows"
    if dataflows_dir.is_dir():
        candidates.extend(
            path for path in dataflows_dir.rglob("*.json") if _is_source_file(path)
        )
    for environment in environments:
        overlay = metadata_dir / "environments" / f"{environment}.json"
        if overlay.is_file():
            candidates.append(overlay)
    functions_dir = workspace / "functions"
    if functions_dir.is_dir():
        candidates.extend(path for path in functions_dir.rglob("*") if _is_source_file(path))
    candidates.extend(selected_runners)
    unique = sorted({path.resolve() for path in candidates})
    return [
        {"path": path.relative_to(workspace).as_posix(), "sha256": _sha256(path)}
        for path in unique
    ]


def _tooling_entries() -> list[dict[str, str]]:
    scripts_dir = Path(__file__).resolve().parent
    skill_root = scripts_dir.parent
    candidates = [scripts_dir / name for name in TOOLING_FILES]
    schemas_dir = skill_root / "schemas"
    candidates.extend(
        [
            schemas_dir / "compatibility.json",
            schemas_dir / "current-build.schema.json",
            schemas_dir / "workspace-config.schema.json",
        ]
    )
    candidates.extend(path for path in schemas_dir.rglob("metadata.schema.json"))
    return [
        {"path": path.relative_to(skill_root).as_posix(), "sha256": _sha256(path)}
        for path in sorted(candidates)
    ]


def _datacoolie_version() -> str:
    try:
        return version("datacoolie")
    except PackageNotFoundError as exc:
        raise RuntimeError("datacoolie must be installed before materialization") from exc


def _validate_runner_name(name: str, platform: str) -> None:
    if not name or Path(name).name != name or "/" in name or "\\" in name:
        raise ValueError("Runner name must be a filename without directory components")
    operations = "|".join(RUNNER_OPERATIONS)
    pattern = rf"^(?:{operations})_{re.escape(platform)}_[A-Za-z0-9][A-Za-z0-9_.-]*\.(?:py|ipynb)$"
    if re.fullmatch(pattern, name) is None or not name.endswith(RUNNER_SUFFIXES):
        raise ValueError(
            f"Runner {name!r} must match "
            f"{{run|replay|maintenance}}_{platform}_<engine>[_<provider>].py|ipynb"
        )


def _runner_identity(name: str, platform: str, engines: set[str]) -> dict[str, str | None]:
    """Return fixed runner identity and reject an unregistered engine."""
    _validate_runner_name(name, platform)
    operation, remainder = name.split("_", 1)
    tail = Path(remainder).stem[len(platform) + 1 :]
    matches = sorted(
        (engine for engine in engines if tail == engine or tail.startswith(f"{engine}_")),
        key=len,
        reverse=True,
    )
    if not matches:
        available = ", ".join(sorted(engines)) or "<none>"
        raise ValueError(f"Runner {name!r} selects an unregistered engine. Available: {available}")
    engine = matches[0]
    provider = tail[len(engine) + 1 :] if tail != engine else None
    return {"operation": operation, "engine": engine, "provider": provider}


def _select_runners(
    workspace: Path,
    environment_platforms: dict[str, str],
    requested_names: list[str] | None,
) -> dict[str, list[Path]]:
    try:
        from datacoolie import engine_registry
    except ImportError as exc:
        raise RuntimeError("datacoolie must be installed before runner selection") from exc
    registered_engines = set(engine_registry.list_plugins())
    runners_dir = workspace / "runners"
    if not runners_dir.is_dir():
        raise ValueError(f"Durable runners directory not found: {runners_dir}")
    available = {
        path.name: path
        for path in runners_dir.iterdir()
        if path.is_file() and path.suffix in RUNNER_SUFFIXES
    }
    if requested_names:
        missing = sorted(set(requested_names) - set(available))
        if missing:
            raise ValueError(f"Durable runner(s) not found: {', '.join(missing)}")
        candidate_names = list(dict.fromkeys(requested_names))
    else:
        candidate_names = sorted(available)

    selected: dict[str, list[Path]] = {}
    used: set[str] = set()
    for environment, platform in environment_platforms.items():
        paths = []
        for name in candidate_names:
            try:
                _validate_runner_name(name, platform)
            except ValueError:
                continue
            _runner_identity(name, platform, registered_engines)
            paths.append(available[name])
            used.add(name)
        if not paths:
            raise ValueError(
                f"No runner compatible with environment {environment!r} platform {platform!r}"
            )
        selected[environment] = paths
    if requested_names:
        unused = sorted(set(requested_names) - used)
        if unused:
            raise ValueError(
                "Requested runner(s) do not match any configured environment platform: "
                + ", ".join(unused)
            )
    return selected


def _validate_resolved_metadata(metadata: dict[str, Any], environment: str) -> None:
    schemas_dir = find_schemas_dir()
    schema_version = resolve_schema_version(metadata, schemas_dir)
    schema = load_schema(schema_version, schemas_dir)
    errors = validate_metadata(metadata, schema)
    if errors:
        details = "; ".join(f"{item['path']}: {item['message']}" for item in errors[:10])
        raise ValueError(f"Resolved metadata for {environment!r} is invalid: {details}")


def _deterministic_zip(functions_dir: Path, output_path: Path) -> None:
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for source in sorted(path for path in functions_dir.rglob("*") if _is_source_file(path)):
            relative = source.relative_to(functions_dir)
            info = zipfile.ZipInfo(relative.as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, source.read_bytes())


def _package_functions(
    workspace: Path,
    dist_dir: Path,
    reuse_artifact: Path | None,
) -> tuple[Path, dict[str, str | None]] | None:
    functions_dir = workspace / "functions"
    if reuse_artifact is None and not functions_dir.is_dir():
        return None
    dist_dir.mkdir(parents=True, exist_ok=True)

    if reuse_artifact is not None:
        artifact = reuse_artifact.resolve()
        if not artifact.is_file() or artifact.suffix not in {".whl", ".zip"}:
            raise ValueError("Reusable functions artifact must be an existing .whl or .zip")
        destination = dist_dir / artifact.name
        shutil.copy2(artifact, destination)
        return destination, inspect_artifact(destination)

    _reject_symlinks(functions_dir)
    if (functions_dir / "pyproject.toml").is_file():
        with tempfile.TemporaryDirectory(prefix="datacoolie-functions-") as temporary:
            temporary_path = Path(temporary)
            source_copy = temporary_path / "source"
            wheel_output = temporary_path / "wheel"
            shutil.copytree(functions_dir, source_copy)
            environment = os.environ.copy()
            environment["SOURCE_DATE_EPOCH"] = "315532800"
            environment["PYTHONHASHSEED"] = "0"
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "wheel",
                    "--no-deps",
                    "--no-build-isolation",
                    "-w",
                    str(wheel_output),
                    ".",
                ],
                cwd=source_copy,
                env=environment,
                check=True,
            )
            wheels = sorted(wheel_output.glob("*.whl"))
            if len(wheels) != 1:
                raise RuntimeError("Functions wheel build must produce exactly one artifact")
            destination = dist_dir / wheels[0].name
            shutil.copy2(wheels[0], destination)
            return destination, inspect_artifact(destination)

    destination = dist_dir / "functions.zip"
    _deterministic_zip(functions_dir, destination)
    identity = inspect_artifact(destination)
    prefix = identity["import_prefix"]
    if not isinstance(prefix, str):
        raise ValueError("Functions ZIP import prefix is invalid")
    named_destination = dist_dir / f"{prefix}.zip"
    destination.replace(named_destination)
    return named_destination, identity


def _reject_wheel_version_collision(
    builds_dir: Path,
    artifact: dict[str, Any] | None,
) -> None:
    if artifact is None or artifact.get("format") != "wheel":
        return
    for manifest_path in sorted(builds_dir.glob("*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        existing = manifest.get("functions_artifact")
        if not isinstance(existing, dict):
            continue
        same_identity = (
            existing.get("format") == "wheel"
            and existing.get("distribution") == artifact.get("distribution")
            and existing.get("version") == artifact.get("version")
        )
        if same_identity and existing.get("sha256") != artifact.get("sha256"):
            raise ValueError(
                "Functions wheel distribution/version already exists with different bytes; "
                "advance the package version"
            )


def _reject_symlinks(path: Path) -> None:
    if path.is_symlink():
        raise ValueError(f"Build path must not be a symlink: {path}")
    if path.is_dir():
        for child in path.rglob("*"):
            if child.is_symlink():
                raise ValueError(f"Build artifact must not be a symlink: {child}")


def _artifact_entries(build_dir: Path) -> list[dict[str, str]]:
    return [
        {"path": path.relative_to(build_dir).as_posix(), "sha256": _sha256(path)}
        for path in sorted(build_dir.rglob("*"))
        if path.is_file() and path.name not in {"manifest.json", "SHA256SUMS"}
    ]


def _write_metadata_projection(
    environment_dir: Path,
    staging: Path,
    resolved: dict[str, Any],
    layout: str,
) -> tuple[dict[str, Any], list[Path]]:
    if layout not in METADATA_LAYOUTS:
        raise ValueError(f"Unsupported metadata layout: {layout}")
    metadata_dir = environment_dir / "metadata"
    payloads: dict[str, dict[str, Any]]
    if layout == "single":
        payloads = {"config_path": resolved}
    elif layout == "split-connections":
        primary = {"dataflows": resolved.get("dataflows", [])}
        if "$schema" in resolved:
            primary["$schema"] = resolved["$schema"]
        if "schema_hints" in resolved:
            primary["schema_hints"] = resolved["schema_hints"]
        payloads = {
            "config_path": primary,
            "connections_path": {"connections": resolved.get("connections", [])},
        }
    else:
        payloads = {
            "config_path": {"dataflows": resolved.get("dataflows", [])},
            "connections_path": {"connections": resolved.get("connections", [])},
            "schema_hints_path": {"schema_hints": resolved.get("schema_hints", [])},
        }
        if "$schema" in resolved:
            payloads["config_path"]["$schema"] = resolved["$schema"]

    files: dict[str, dict[str, str]] = {}
    outputs: list[Path] = []
    for role, filename in METADATA_LAYOUT_ROLES[layout].items():
        output = metadata_dir / filename
        write_metadata(output, payloads[role])
        outputs.append(output)
        files[role] = {
            "path": output.relative_to(staging).as_posix(),
            "sha256": _sha256(output),
        }
    return {"layout": layout, "files": files, "sha256": _metadata_set_digest(files)}, outputs


def _validate_metadata_projection(
    build_dir: Path,
    environment_name: str,
    metadata: Any,
    declared: dict[str, str],
) -> None:
    if not isinstance(metadata, dict) or set(metadata) != {"layout", "files", "sha256"}:
        raise ValueError(f"Invalid metadata set contract for environment {environment_name}")
    layout = metadata.get("layout")
    expected_roles = METADATA_LAYOUT_ROLES.get(layout)
    files = metadata.get("files")
    if expected_roles is None or not isinstance(files, dict) or set(files) != set(expected_roles):
        raise ValueError(f"Invalid metadata layout/roles for environment {environment_name}")
    if metadata.get("sha256") != _metadata_set_digest(files):
        raise ValueError(f"Metadata set digest mismatch for environment {environment_name}")

    payloads: dict[str, dict[str, Any]] = {}
    for role, filename in expected_roles.items():
        item = files.get(role)
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            raise ValueError(f"Invalid metadata file role {role} for environment {environment_name}")
        expected_path = f"{environment_name}/metadata/{filename}"
        if item.get("path") != expected_path or declared.get(expected_path) != item.get("sha256"):
            raise ValueError(f"Metadata file role {role} is not bound to {expected_path}")
        path = build_dir / PurePosixPath(expected_path)
        if not path.is_file() or _sha256(path) != item["sha256"]:
            raise ValueError(f"Metadata file role {role} does not match generated bytes")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Metadata file role {role} must contain an object")
        payloads[role] = payload

    if layout == "single":
        allowed = {"$schema", "connections", "dataflows", "schema_hints"}
        if not {"connections", "dataflows"}.issubset(payloads["config_path"]):
            raise ValueError("Single metadata file must contain connections and dataflows")
        if set(payloads["config_path"]) - allowed:
            raise ValueError("Single metadata file contains unsupported sections")
    else:
        if set(payloads["connections_path"]) != {"connections"}:
            raise ValueError("connections.json must own only the connections section")
        primary_expected = (
            {"$schema", "dataflows", "schema_hints"}
            if layout == "split-connections"
            else {"$schema", "dataflows"}
        )
        primary_keys = set(payloads["config_path"])
        if "dataflows" not in primary_keys or primary_keys - primary_expected:
            raise ValueError("dataflows.json contains invalid or duplicate section ownership")
        if layout == "split-all" and set(payloads["schema_hints_path"]) != {"schema_hints"}:
            raise ValueError("schema_hints.json must own only the schema_hints section")


def _write_checksums(build_dir: Path) -> None:
    files = sorted(
        path for path in build_dir.rglob("*") if path.is_file() and path.name != "SHA256SUMS"
    )
    lines = [f"{_sha256(path)}  {path.relative_to(build_dir).as_posix()}" for path in files]
    (build_dir / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def verify_build(build_dir: Path) -> dict[str, Any]:
    if build_dir.is_symlink():
        raise ValueError(f"Build path must not be a symlink: {build_dir}")
    build_dir = build_dir.resolve()
    _reject_symlinks(build_dir)
    manifest_path = build_dir / "manifest.json"
    checksums_path = build_dir / "SHA256SUMS"
    if not manifest_path.is_file() or not checksums_path.is_file():
        raise ValueError(f"Incomplete build: {build_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 3:
        raise ValueError(f"Unsupported build manifest schema: {build_dir}")
    build_id = manifest.get("build_id")
    if build_id != build_dir.name:
        raise ValueError(f"Build directory/name mismatch: {build_dir}")
    match = BUILD_ID_PATTERN.fullmatch(build_id or "")
    if match is None:
        raise ValueError(f"Invalid build ID: {build_id!r}")

    content_digest = manifest.get("content_digest")
    if not isinstance(content_digest, str) or re.fullmatch(r"[0-9a-f]{64}", content_digest) is None:
        raise ValueError(f"Invalid build content digest: {build_dir}")
    identity = {
        "input_digest": manifest.get("input_digest"),
        "artifacts": manifest.get("artifacts"),
    }
    if _canonical_digest(identity) != content_digest:
        raise ValueError(f"Build content digest mismatch: {build_dir}")
    if match.group("digest") != content_digest[:12]:
        raise ValueError(f"Build ID/content digest mismatch: {build_dir}")

    created_at = manifest.get("created_at")
    if not isinstance(created_at, str) or not created_at.endswith("Z"):
        raise ValueError(f"Invalid UTC build creation timestamp: {build_dir}")
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid UTC build creation timestamp: {build_dir}") from exc
    if match.group("date") != created.astimezone(timezone.utc).strftime("%y%m%d"):
        raise ValueError(f"Build ID/creation date mismatch: {build_dir}")
    if match.group("time") != created.astimezone(timezone.utc).strftime("%H%M%S"):
        raise ValueError(f"Build ID/creation time mismatch: {build_dir}")

    declared = {
        item.get("path"): item.get("sha256")
        for item in manifest.get("artifacts", [])
        if isinstance(item, dict)
    }
    environments = manifest.get("environments")
    if not isinstance(environments, dict) or not environments:
        raise ValueError(f"Build manifest environments must be a non-empty object: {build_dir}")
    for environment_name, environment in environments.items():
        if not isinstance(environment_name, str) or not isinstance(environment, dict):
            raise ValueError(f"Invalid build environment contract: {build_dir}")
        _validate_metadata_projection(
            build_dir,
            environment_name,
            environment.get("metadata"),
            declared,
        )

    function_artifact = manifest.get("functions_artifact")
    if function_artifact is not None:
        required = {
            "format",
            "path",
            "sha256",
            "import_prefix",
            "distribution",
            "version",
        }
        if not isinstance(function_artifact, dict) or set(function_artifact) != required:
            raise ValueError(f"Invalid functions artifact contract: {build_dir}")
        if function_artifact.get("format") not in {"wheel", "zip"}:
            raise ValueError(f"Invalid functions artifact format: {build_dir}")
        function_path = function_artifact.get("path")
        if not isinstance(function_path, str) or PurePosixPath(function_path).parent.as_posix() != "functions":
            raise ValueError(f"Invalid functions artifact path: {build_dir}")
        if declared.get(function_path) != function_artifact.get("sha256"):
            raise ValueError(f"Functions artifact is not bound to the build manifest: {build_dir}")

    expected_paths: set[str] = set()
    for line in checksums_path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or "  " not in line:
            raise ValueError(f"Invalid checksum line in {checksums_path}: {line!r}")
        expected, relative = line.split("  ", 1)
        artifact = build_dir / Path(relative)
        if not artifact.is_file() or _sha256(artifact) != expected:
            raise ValueError(f"Checksum mismatch: {relative}")
        expected_paths.add(Path(relative).as_posix())
    actual_paths = {
        path.relative_to(build_dir).as_posix()
        for path in build_dir.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    }
    if actual_paths != expected_paths:
        raise ValueError(f"Build contains untracked or missing files: {build_dir}")
    return manifest


def materialize(
    *,
    workspace: Path,
    runner_names: list[str] | None = None,
    functions_artifact: Path | None = None,
    metadata_layout: str = "single",
) -> dict[str, Any]:
    workspace = workspace.resolve()
    if metadata_layout not in METADATA_LAYOUTS:
        raise ValueError(f"Unsupported metadata layout: {metadata_layout}")
    design = _validate_design_approval(workspace)
    config = validate_config(workspace / "config.yaml")
    configured = config["environments"]
    platforms = {
        environment: configured[environment]["platform"]
        for environment in configured
    }
    selected = _select_runners(workspace, platforms, runner_names)
    selected_runner_paths = {path for paths in selected.values() for path in paths}

    tooling = _tooling_entries()
    inputs = _input_entries(workspace, selected_runner_paths, list(configured))
    if functions_artifact is not None:
        artifact = functions_artifact.resolve()
        inputs.append({"path": f"external-functions/{artifact.name}", "sha256": _sha256(artifact)})
    input_contract = {
        "schema_version": 1,
        "datacoolie_version": _datacoolie_version(),
        "project": config["project"],
        "environments": platforms,
        "runners": {
            env: [path.name for path in paths] for env, paths in selected.items()
        },
        "design": design,
        "metadata_layout": metadata_layout,
        "inputs": sorted(inputs, key=lambda item: item["path"]),
        "tooling": tooling,
    }
    input_digest = _canonical_digest(input_contract)
    build_state_dir = workspace / ".builds"
    _reject_symlinks(build_state_dir)
    builds_dir = build_state_dir / "artifacts"
    builds_dir.mkdir(parents=True, exist_ok=True)
    staging = builds_dir / f".tmp-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        environment_manifest: dict[str, Any] = {}
        metadata_outputs: list[Path] = []
        metadata_dir = workspace / "metadata"
        for environment, platform in platforms.items():
            environment_dir = staging / environment
            resolved = merge_metadata(metadata_dir, environment)
            _validate_resolved_metadata(resolved, environment)
            metadata_record, outputs = _write_metadata_projection(
                environment_dir,
                staging,
                resolved,
                metadata_layout,
            )
            metadata_outputs.extend(outputs)
            runner_outputs = []
            for runner_source in selected[environment]:
                destination = environment_dir / "runners" / runner_source.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(runner_source, destination)
                runner_outputs.append(destination.relative_to(staging).as_posix())
            environment_manifest[environment] = {
                "platform": platform,
                "metadata": metadata_record,
                "runners": runner_outputs,
            }

        packaged_function = _package_functions(
            workspace,
            staging / "functions",
            functions_artifact,
        )
        configured_functions: set[str] = set()
        for metadata_output in metadata_outputs:
            configured_functions.update(
                referenced_functions(json.loads(metadata_output.read_text(encoding="utf-8")))
            )
        function_record: dict[str, Any] | None = None
        if packaged_function is None:
            if configured_functions:
                raise ValueError("Resolved metadata references Python functions but no artifact exists")
        else:
            function_path, identity = packaged_function
            validation = validate_metadata_files(function_path, metadata_outputs)
            if any(validation.get(key) != value for key, value in identity.items()):
                raise ValueError("Functions artifact identity changed during validation")
            function_record = {
                **identity,
                "path": function_path.relative_to(staging).as_posix(),
                "sha256": sha256_file(function_path),
            }
            _reject_wheel_version_collision(builds_dir, function_record)
        artifacts = _artifact_entries(staging)
        identity = {"input_digest": input_digest, "artifacts": artifacts}
        content_digest = _canonical_digest(identity)
        created_at = _utc_now()
        build_id = _build_id(content_digest, created_at)
        manifest = {
            "schema_version": 3,
            "build_id": build_id,
            "content_digest": content_digest,
            "created_at": _format_utc(created_at),
            "input_digest": input_digest,
            "datacoolie_version": input_contract["datacoolie_version"],
            "project": input_contract["project"],
            "design": input_contract["design"],
            "tooling_digest": _canonical_digest(tooling),
            "inputs": input_contract["inputs"],
            "environments": environment_manifest,
            "functions_artifact": function_record,
            "artifacts": artifacts,
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        _write_checksums(staging)
        target = builds_dir / build_id
        if target.exists():
            existing = verify_build(target)
            if existing.get("content_digest") != content_digest:
                raise RuntimeError(f"Build ID collision: {build_id}")
            shutil.rmtree(staging)
            current_dir = _write_current_projection(workspace, target)
            return {
                "build_id": build_id,
                "build_dir": target,
                "current_dir": current_dir,
                "reused": True,
            }
        staging.rename(target)
        verify_build(target)
        current_dir = _write_current_projection(workspace, target)
        return {
            "build_id": build_id,
            "build_dir": target,
            "current_dir": current_dir,
            "reused": False,
        }
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument(
        "--runner-name",
        action="append",
        dest="runner_names",
        help="Durable runner to include; repeat or omit for all compatible runners",
    )
    parser.add_argument("--functions-artifact", type=Path)
    parser.add_argument(
        "--metadata-layout",
        choices=METADATA_LAYOUTS,
        default="single",
        help="Generated FileProvider layout; defaults to one metadata.json",
    )
    args = parser.parse_args()
    try:
        result = materialize(
            workspace=args.workspace,
            runner_names=args.runner_names,
            functions_artifact=args.functions_artifact,
            metadata_layout=args.metadata_layout,
        )
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    state = "reused" if result["reused"] else "created"
    print(f"OK: {state} build {result['build_id']}")
    print(result["build_dir"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
