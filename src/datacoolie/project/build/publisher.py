"""Immutable multi-environment artifact builder and current projection."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any
import uuid

from datacoolie import __version__

from ..config import ProjectConfig
from ..documents import (
    MetadataSnapshot,
    output_suffix,
    write_document,
)
from ..errors import ProjectError, ProjectValidationError
from ..artifacts import declared_inventory, inventory_digest, inventory_entries, file_hashes
from ..manifest import BUILD_ARTIFACT_TYPE, MANIFEST_FILENAME, validate_manifest
from ..validation.metadata import validate_metadata_document
from .functions import _resource_files, package_functions
from .planning import BuildPlan, create_build_plan
from .publication import ProjectPublicationLock, publish_current
from ..runners import copy_environment_runners


def _build_id(digest: str, *, created_at: datetime | None = None) -> str:
    """Create the timestamp/content build identity used by the builder.

    The timestamp is deliberately rounded to seconds.  A build ID may be
    produced concurrently by two callers in the same second; using the exact
    same rounded instant in the manifests keeps that collision reusable when
    their input digest is identical instead of making volatile microseconds
    look like a content collision.
    """

    instant = created_at or datetime.now(timezone.utc)
    stamp = instant.strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{digest[:12]}"


def _copy_files(source: Path, destination: Path) -> None:
    # Keep an explicitly configured (possibly empty) component visible in
    # every environment projection.  Empty directories are not checksummed,
    # but their presence makes the declared layout unambiguous to consumers.
    destination.mkdir(parents=True, exist_ok=True)
    for path in _resource_files(source):
        relative = path.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def _item_identity(section: str, item: dict[str, Any], index: int) -> str:
    if section == "connections":
        return str(item.get("name") or item.get("connection_id") or f"#{index}")
    if section == "dataflows":
        return str(item.get("name") or item.get("dataflow_id") or f"#{index}")
    connection = item.get("connection_name") or item.get("connection_id") or "?"
    schema = item.get("schema_name") or ""
    table = item.get("table_name") or "?"
    return f"{connection}|{schema}|{table}"


def _preserve_documents(
    snapshot: MetadataSnapshot,
    resolved: dict[str, Any],
    *,
    output_root: Path,
    fmt: str,
) -> list[str]:
    by_origin: dict[str, dict[str, list[dict[str, Any]]]] = {}
    used: set[tuple[str, str]] = set()
    for section in ("connections", "dataflows", "schema_hints"):
        for index, item in enumerate(resolved.get(section, [])):
            key = _item_identity(section, item, index)
            origin = snapshot.origins.get((section, key))
            if origin is None:
                continue
            by_origin.setdefault(origin, {}).setdefault(section, []).append(item)
            used.add((section, key))
    written: list[str] = []
    for source in snapshot.documents:
        payload: dict[str, Any] = {
            key: value
            for key, value in source.payload.items()
            if key not in source.sections
        }
        source_origin = by_origin.get(source.relative_path, {})
        for section in source.sections:
            payload[section] = source_origin.get(section, [])
        if not payload:
            continue
        relative = Path(source.relative_path)
        if fmt == "preserve":
            target_relative = relative
            output_format = source.format
        else:
            target_relative = relative.with_suffix(output_suffix(fmt))
            output_format = fmt
        target = output_root / target_relative
        if target.exists() or target_relative.as_posix() in written:
            raise ProjectError(f"Metadata output path collision: {target_relative.as_posix()}")
        write_document(target, payload, output_format)
        written.append(target_relative.as_posix())
    additions: dict[str, list[dict[str, Any]]] = {}
    for section in ("connections", "dataflows", "schema_hints"):
        for index, item in enumerate(resolved.get(section, [])):
            key = _item_identity(section, item, index)
            if (section, key) not in used:
                additions.setdefault(section, []).append(item)
    if additions:
        suffix = output_suffix("json" if fmt == "preserve" else fmt)
        target_relative = Path("_generated") / f"environment{suffix}"
        write_document(output_root / target_relative, additions, "json" if fmt == "preserve" else fmt)
        written.append(target_relative.as_posix())
    return written


def _write_projection(
    environment_root: Path,
    metadata_relative: str,
    snapshot: MetadataSnapshot,
    resolved: dict[str, Any],
    *,
    layout: str,
    fmt: str,
) -> dict[str, Any]:
    metadata_root = environment_root.joinpath(*metadata_relative.split("/"))
    metadata_root.mkdir(parents=True, exist_ok=True)
    if layout == "single":
        suffix = output_suffix(fmt)
        target = metadata_root / f"metadata{suffix}"
        write_document(target, resolved, fmt)
        return {"path": metadata_relative, "layout": layout, "format": fmt}
    if layout == "split":
        suffix = output_suffix(fmt)
        write_document(metadata_root / f"connections{suffix}", {"connections": resolved.get("connections", [])}, fmt)
        write_document(metadata_root / f"schema_hints{suffix}", {"schema_hints": resolved.get("schema_hints", [])}, fmt)
        # Keep project-owned top-level fields (for example ``$schema`` or a
        # documented extension) in the one split document that carries the
        # dataflows.  Splitting sections must not silently discard authored
        # metadata that the aggregate/single layouts preserve.
        dataflow_document = {
            key: value
            for key, value in resolved.items()
            if key not in {"connections", "schema_hints", "dataflows"}
        }
        dataflow_document["dataflows"] = resolved.get("dataflows", [])
        write_document(metadata_root / f"metadata{suffix}", dataflow_document, fmt)
        return {"path": metadata_relative, "layout": layout, "format": fmt}
    if layout == "preserve":
        paths = _preserve_documents(snapshot, resolved, output_root=metadata_root, fmt=fmt)
        return {
            "path": metadata_relative,
            "layout": layout,
            "format": fmt,
            "files": [
                (Path(metadata_relative) / path).as_posix()
                for path in paths
            ],
        }
    raise ProjectError(f"Unsupported metadata output layout: {layout}")


def _publish_current(project_dir: Path, build_root: Path) -> Path:
    return publish_current(project_dir, build_root, verify=verify_build)


def verify_build(build_root: Path | str) -> dict[str, Any]:
    candidate = Path(build_root).expanduser()
    if candidate.is_symlink():
        raise ProjectError(f"Build root must not be a symlink: {candidate}")
    root = candidate.resolve()
    manifest_path = root / MANIFEST_FILENAME
    if not manifest_path.is_file():
        raise ProjectError(f"Build manifest not found: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectError(f"Cannot read build manifest: {manifest_path}") from exc
    if not isinstance(manifest, dict):
        raise ProjectError("Build manifest must be an object")
    manifest_error = validate_manifest(
        manifest,
        expected_artifact_type=BUILD_ARTIFACT_TYPE,
    )
    if manifest_error:
        raise ProjectError(f"Invalid build manifest: {manifest_error}")
    expected_hashes = declared_inventory(manifest)
    actual_hashes = file_hashes(root)
    if expected_hashes != actual_hashes:
        raise ProjectError("Build manifest artifact inventory does not match the files on disk")
    if manifest.get("content_digest") != inventory_digest(actual_hashes):
        raise ProjectError("Build manifest content_digest does not match the files on disk")
    return manifest


def build_project(
    config: ProjectConfig,
    *,
    metadata_layout: str | None = None,
    metadata_format: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Build every environment and publish one immutable/current identity."""

    plan: BuildPlan = create_build_plan(
        config,
        metadata_layout=metadata_layout,
        metadata_format=metadata_format,
    )
    layout = plan.layout
    effective_format = plan.metadata_format
    runner_layout = plan.runner_layout
    snapshot = plan.snapshot
    resolved_by_env = plan.resolved_by_environment
    warnings = plan.warnings
    input_digest = plan.input_digest
    if dry_run:
        return {
            "status": "dry_run",
            "project": config.project_name,
            "environments": sorted(config.environments),
            "input_digest": input_digest,
            "warnings": warnings,
            "plan": plan.preview(),
            "not_performed": [
                "metadata-serialization-and-round-trip",
                "function-packaging",
                "assembled-artifact-verification",
                "artifact-publication",
            ],
        }
    builds_root = config.project_dir / ".builds"
    artifacts_root = builds_root / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    build_started_at = datetime.now(timezone.utc).replace(microsecond=0)
    build_id = _build_id(input_digest, created_at=build_started_at)
    build_created_at = build_started_at.isoformat().replace("+00:00", "Z")
    staging = artifacts_root / f".staging-{uuid.uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        function_staging: Path | None = None
        function_records: list[dict[str, Any]] = []
        if config.functions:
            function_staging = staging / ".functions"
            for index, entry in enumerate(config.functions):
                root_staging = function_staging / str(index)
                record = package_functions(
                    config.absolute_component_paths("functions")[index],
                    entry.packaging or "auto",
                    root_staging,
                    plan=plan.function_plans[index],
                )
                if record is not None:
                    function_records.append(
                        {
                            **record,
                            "source_path": entry.path,
                            "index": index,
                        }
                    )
        environment_records: dict[str, Any] = {}
        for environment, resolved in resolved_by_env.items():
            environment_root = staging / environment
            environment_root.mkdir(parents=True, exist_ok=True)
            metadata_record = _write_projection(
                environment_root,
                config.metadata.path,
                snapshot,
                resolved,
                layout=layout,
                fmt=effective_format if effective_format != "preserve" else "preserve",
            )
            sql_records: list[dict[str, Any]] = []
            if config.sql:
                sql_roots = config.absolute_component_paths("sql")
                for index, entry in enumerate(config.sql):
                    destination = environment_root.joinpath(*entry.path.split("/"))
                    _copy_files(sql_roots[index], destination)
                    sql_records.append({"path": entry.path})
            # Re-check explicit artifact:/ references against the exact
            # environment projection after resources have been copied.
            artifact_report = validate_metadata_document(
                resolved,
                scope=f"artifact-metadata:{environment}",
                source=str(environment_root),
                # Validate shorthand references against the exact SQL roots
                # copied into this environment.  Full artifact-relative
                # declarations are still accepted by the shared validator.
                sql_root=(
                    tuple(
                        environment_root.joinpath(*entry.path.split("/"))
                        for entry in (config.sql or ())
                    )
                    if config.sql
                    else ()
                ),
                artifact_root=environment_root,
            )
            if artifact_report.errors:
                raise ProjectValidationError(
                    f"Artifact metadata validation failed for environment {environment}",
                    details=artifact_report.to_dict(),
                )
            env_function_records: list[dict[str, Any]] = []
            if function_staging is not None and function_records:
                for record in function_records:
                    index = int(record["index"])
                    entry = config.functions[index]
                    source_stage = function_staging / str(index)
                    destination = environment_root.joinpath(*entry.path.split("/"))
                    _copy_files(source_stage, destination)
                    files = sorted(
                        path.relative_to(environment_root).as_posix()
                        for path in destination.rglob("*")
                        if path.is_file()
                    )
                    env_function_records.append(
                        {
                            "path": entry.path,
                            "packaging": record.get("packaging"),
                            "files": files,
                            "root_name": record.get("root_name"),
                            "wrapped": record.get("wrapped", False),
                        }
                    )
            runner_record = copy_environment_runners(
                runner_layout,
                environment,
                environment_root,
            )
            environment_components: dict[str, Any] = {
                "metadata": metadata_record,
                "sql": sql_records,
                "functions": env_function_records,
            }
            if runner_record is not None:
                environment_components["runners"] = runner_record
            environment_manifest = {
                "schema_version": 1,
                "artifact_type": "datacoolie_environment",
                "build_id": build_id,
                "environment": environment,
                "project": {"name": config.project_name},
                "created_at": build_created_at,
                "datacoolie_version": __version__,
                "platform": config.environments[environment].platform,
                "components": environment_components,
                "metadata_layout": layout,
                "metadata_format": effective_format,
            }
            (environment_root / MANIFEST_FILENAME).write_text(
                json.dumps(environment_manifest, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            environment_record: dict[str, Any] = {
                "platform": config.environments[environment].platform,
                "metadata": metadata_record,
                "sql": sql_records,
                "functions": env_function_records,
                "manifest": MANIFEST_FILENAME,
            }
            if runner_record is not None:
                environment_record["runners"] = runner_record
            environment_records[environment] = environment_record
        if function_staging is not None and function_staging.exists():
            shutil.rmtree(function_staging, ignore_errors=True)
        artifacts = inventory_entries(staging)
        content_digest = inventory_digest(
            {entry["path"]: entry["sha256"] for entry in artifacts}
        )
        public_function_record: Any = None
        if config.functions is not None:
            public_records = []
            for record in function_records:
                public_records.append(
                    {
                        key: value
                        for key, value in record.items()
                        if key not in {"path", "source_path", "index"}
                    }
                    | {"path": record["source_path"]}
                )
            # Keep the public descriptor shape stable for one or many roots.
            # A list avoids a scalar/singular special case for the multi-root
            # functions contract.
            public_function_record = public_records
        manifest = {
            "schema_version": 1,
            "artifact_type": "datacoolie_build",
            "build_id": build_id,
            "content_digest": content_digest,
            "created_at": build_created_at,
            "datacoolie_version": __version__,
            "project": {"name": config.project_name},
            "components": config.to_dict()["components"],
            "input_digest": input_digest,
            "metadata_layout": layout,
            "metadata_format": effective_format,
            "environments": environment_records,
            "functions_artifact": public_function_record,
            "artifacts": artifacts,
        }
        (staging / MANIFEST_FILENAME).write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        target = artifacts_root / build_id
        with ProjectPublicationLock(config.project_dir):
            if target.exists():
                existing = verify_build(target)
                if (
                    existing.get("input_digest") != input_digest
                    or existing.get("metadata_layout") != layout
                    or existing.get("metadata_format") != effective_format
                    or existing.get("content_digest") != content_digest
                ):
                    raise ProjectError(f"Build ID collision with different content: {build_id}")
                shutil.rmtree(staging, ignore_errors=True)
                current = _publish_current(config.project_dir, target)
                return {
                    "status": "reused",
                    "build_id": build_id,
                    "build_path": str(target),
                    "current_path": str(current),
                    "environments": list(environment_records),
                    "warnings": warnings,
                }
            staging.rename(target)
            try:
                verify_build(target)
            except Exception:
                shutil.rmtree(target, ignore_errors=True)
                raise
            current = _publish_current(config.project_dir, target)
        return {
            "status": "created",
            "build_id": build_id,
            "build_path": str(target),
            "current_path": str(current),
            "environments": list(environment_records),
            "warnings": warnings,
        }
    except Exception:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise


__all__ = ["build_project", "verify_build"]
