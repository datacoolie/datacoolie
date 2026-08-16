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
from pathlib import Path
from typing import Any

from _schema_resolver import find_schemas_dir, load_schema, resolve_schema_version
from merge import merge_metadata, write_metadata
from validate import validate_metadata
from validate_config import validate_config


RUNNER_SUFFIXES = (".py", ".ipynb")
RUNNER_OPERATIONS = ("run", "replay", "maintenance")
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


def _current_pointer_path(workspace: Path, environment: str) -> Path:
    if not environment or Path(environment).name != environment:
        raise ValueError(f"Invalid current-build environment: {environment!r}")
    return workspace / ".builds" / "current" / f"{environment}.json"


def _write_current_pointer(workspace: Path, build_dir: Path, environment: str) -> Path:
    manifest = verify_build(build_dir)
    environments = manifest.get("environments")
    if not isinstance(environments, dict) or environment not in environments:
        raise ValueError(f"Build does not contain environment {environment!r}")
    pointer = _current_pointer_path(workspace, environment)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "artifact_type": "current_build",
        "environment": environment,
        "build_id": manifest["build_id"],
    }
    fd, temporary = tempfile.mkstemp(prefix=f".{pointer.name}.", dir=pointer.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        os.replace(temporary, pointer)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return pointer


def resolve_current_build(workspace: Path, environment: str) -> Path:
    """Resolve one environment pointer to an exact verified materialized build."""
    workspace = workspace.resolve()
    builds_root = workspace / ".builds"
    current_root = builds_root / "current"
    for directory in (builds_root, current_root):
        if directory.is_symlink():
            raise ValueError(f"Current-build path must not be a symlink: {directory}")
    pointer = _current_pointer_path(workspace, environment)
    if pointer.is_symlink() or not pointer.is_file():
        raise ValueError(f"Current-build pointer does not exist or is a symlink: {pointer}")
    try:
        payload = json.loads(pointer.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Current-build pointer is not valid JSON: {pointer}") from exc
    expected = {"schema_version", "artifact_type", "environment", "build_id"}
    if not isinstance(payload, dict) or set(payload) != expected:
        raise ValueError("Current-build pointer fields do not match the contract")
    if payload["schema_version"] != 1 or payload["artifact_type"] != "current_build":
        raise ValueError("Current-build pointer contract is unsupported")
    if payload["environment"] != environment:
        raise ValueError("Current-build pointer environment does not match its filename")
    build_id = payload["build_id"]
    if not isinstance(build_id, str) or BUILD_ID_PATTERN.fullmatch(build_id) is None:
        raise ValueError("Current-build pointer contains an invalid build ID")
    build_dir = workspace / ".builds" / "artifacts" / build_id
    manifest = verify_build(build_dir)
    environments = manifest.get("environments")
    if not isinstance(environments, dict) or environment not in environments:
        raise ValueError("Current build does not contain the selected environment")
    return build_dir


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
    selected_environments: list[str],
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
    for environment in selected_environments:
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
                "Requested runner(s) do not match any selected environment platform: "
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
            relative = Path("functions") / source.relative_to(functions_dir)
            info = zipfile.ZipInfo(relative.as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, source.read_bytes())


def _package_functions(
    workspace: Path,
    dist_dir: Path,
    reuse_artifact: Path | None,
    enabled: bool,
) -> list[Path]:
    if not enabled:
        return []
    functions_dir = workspace / "functions"
    if reuse_artifact is None and not functions_dir.is_dir():
        return []
    dist_dir.mkdir(parents=True, exist_ok=True)

    if reuse_artifact is not None:
        artifact = reuse_artifact.resolve()
        if not artifact.is_file() or artifact.suffix not in {".whl", ".zip"}:
            raise ValueError("Reusable functions artifact must be an existing .whl or .zip")
        destination = dist_dir / artifact.name
        shutil.copy2(artifact, destination)
        return [destination]

    if (functions_dir / "pyproject.toml").is_file():
        with tempfile.TemporaryDirectory(prefix="datacoolie-functions-") as temporary:
            subprocess.run(
                [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", temporary, "."],
                cwd=functions_dir,
                check=True,
            )
            wheels = sorted(Path(temporary).glob("*.whl"))
            if not wheels:
                raise RuntimeError("Functions wheel build produced no artifact")
            outputs = []
            for wheel in wheels:
                destination = dist_dir / wheel.name
                shutil.copy2(wheel, destination)
                outputs.append(destination)
            return outputs

    if (functions_dir / "__init__.py").is_file():
        destination = dist_dir / "functions.zip"
        _deterministic_zip(functions_dir, destination)
        return [destination]
    return []


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
    environments: list[str] | None = None,
    runner_names: list[str] | None = None,
    functions_artifact: Path | None = None,
    package_functions: bool = True,
) -> dict[str, Any]:
    workspace = workspace.resolve()
    design = _validate_design_approval(workspace)
    config = validate_config(
        workspace / "config.yaml",
        selected_environments=environments,
    )
    configured = config["environments"]
    selected_environments = list(dict.fromkeys(environments or configured.keys()))
    unknown = sorted(set(selected_environments) - set(configured))
    if unknown:
        raise ValueError(f"Unknown environment(s): {', '.join(unknown)}")
    platforms = {
        environment: configured[environment]["platform"]
        for environment in selected_environments
    }
    selected = _select_runners(workspace, platforms, runner_names)
    selected_runner_paths = {path for paths in selected.values() for path in paths}

    tooling = _tooling_entries()
    inputs = _input_entries(workspace, selected_runner_paths, selected_environments)
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
        "package_functions": package_functions,
        "design": design,
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
        metadata_dir = workspace / "metadata"
        for environment, platform in platforms.items():
            environment_dir = staging / environment
            metadata_output = environment_dir / "metadata.json"
            resolved = merge_metadata(metadata_dir, environment)
            _validate_resolved_metadata(resolved, environment)
            write_metadata(metadata_output, resolved)
            runner_outputs = []
            for runner_source in selected[environment]:
                destination = environment_dir / "runners" / runner_source.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(runner_source, destination)
                runner_outputs.append(destination.relative_to(staging).as_posix())
            environment_manifest[environment] = {
                "platform": platform,
                "metadata": metadata_output.relative_to(staging).as_posix(),
                "runners": runner_outputs,
            }

        function_outputs = _package_functions(
            workspace,
            staging / "dist",
            functions_artifact,
            package_functions,
        )
        artifacts = _artifact_entries(staging)
        identity = {"input_digest": input_digest, "artifacts": artifacts}
        content_digest = _canonical_digest(identity)
        created_at = _utc_now()
        build_id = _build_id(content_digest, created_at)
        manifest = {
            "schema_version": 1,
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
            "functions": [path.relative_to(staging).as_posix() for path in function_outputs],
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
            for environment in selected_environments:
                _write_current_pointer(workspace, target, environment)
            return {"build_id": build_id, "build_dir": target, "reused": True}
        staging.rename(target)
        verify_build(target)
        for environment in selected_environments:
            _write_current_pointer(workspace, target, environment)
        return {"build_id": build_id, "build_dir": target, "reused": False}
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument(
        "--environment",
        action="append",
        dest="environments",
        help="Environment to include; repeat or omit for all configured environments",
    )
    parser.add_argument(
        "--runner-name",
        action="append",
        dest="runner_names",
        help="Durable runner to include; repeat or omit for all compatible runners",
    )
    parser.add_argument("--functions-artifact", type=Path)
    parser.add_argument("--no-package-functions", action="store_true")
    args = parser.parse_args()
    if args.functions_artifact and args.no_package_functions:
        parser.error("--functions-artifact and --no-package-functions are mutually exclusive")
    try:
        result = materialize(
            workspace=args.workspace,
            environments=args.environments,
            runner_names=args.runner_names,
            functions_artifact=args.functions_artifact,
            package_functions=not args.no_package_functions,
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
