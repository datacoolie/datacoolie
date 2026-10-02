"""Validation of built and extracted DataCoolie artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from datacoolie.project.artifacts import (
    declared_inventory,
    file_hashes,
    inventory_digest,
    inventory_difference,
)
from datacoolie.project.documents import load_snapshot
from datacoolie.project.errors import ProjectDependencyError, ProjectError, ProjectValidationError
from datacoolie.project.manifest import (
    BUILD_ARTIFACT_TYPE,
    ENVIRONMENT_ARTIFACT_TYPE,
    MANIFEST_FILENAME,
    is_safe_build_id,
    validate_manifest,
)
from .metadata import validate_metadata_document
from .reports import Diagnostic, ValidationReport, _add_schema_summary, _error, _warning

def _safe_child(root: Path, relative: str) -> Path:
    unresolved = root / Path(relative)
    current = unresolved
    while current != root:
        if current.is_symlink():
            raise ProjectValidationError(
                f"Artifact path must not contain symlinks: {current}"
            )
        parent = current.parent
        if parent == current:
            break
        current = parent
    candidate = unresolved.resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as exc:
        raise ProjectValidationError(
            f"Artifact path escapes its root: {relative}"
        ) from exc
    return candidate

def _manifest_component_paths(value: Any, *, component: str) -> list[str]:
    """Extract paths from a scalar/list manifest component entry."""
    if value is None:
        return []
    entries = value if isinstance(value, list) else [value]
    paths: list[str] = []
    for index, entry in enumerate(entries):
        path = entry.get("path") if isinstance(entry, dict) else entry
        if not isinstance(path, str) or not path.strip():
            raise ProjectValidationError(
                f"Manifest {component} entry {index} must contain a non-empty path"
            )
        paths.append(path)
    return paths

def _validate_function_resources(
    root: Path,
    value: Any,
    *,
    errors: list[Diagnostic],
    source: str,
) -> None:
    """Check function component directories and manifested output files."""

    if value is None:
        return
    entries = value if isinstance(value, list) else [value]
    for index, entry in enumerate(entries):
        relative = entry.get("path") if isinstance(entry, dict) else entry
        if not isinstance(relative, str) or not relative.strip():
            # Shape errors are reported by the manifest validator; avoid a
            # second, less useful path diagnostic here.
            continue
        try:
            function_root = _safe_child(root, relative)
        except ProjectError as exc:
            _error(errors, "artifact.path", str(exc), source)
            continue
        if not function_root.is_dir():
            _error(
                errors,
                "artifact.resource",
                f"Functions component directory not found: {function_root}",
                source,
            )
            continue
        files = entry.get("files", []) if isinstance(entry, dict) else []
        if not isinstance(files, list):
            continue
        for file_index, file_relative in enumerate(files):
            if not isinstance(file_relative, str) or not file_relative.strip():
                continue
            try:
                target = _safe_child(root, file_relative)
            except ProjectValidationError as exc:
                _error(
                    errors,
                    "artifact.path",
                    str(exc),
                    f"{source}:functions[{index}].files[{file_index}]",
                )
                continue
            if not target.is_file():
                _error(
                    errors,
                    "artifact.resource",
                    f"Function output file not found: {target}",
                    source,
                )

def _validate_declared_files(
    root: Path,
    value: Any,
    *,
    component: str,
    errors: list[Diagnostic],
    source: str,
) -> None:
    """Check descriptor ``files`` entries for a non-function component."""

    if value is None:
        return
    entries = value if isinstance(value, list) else [value]
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        files = entry.get("files")
        if not isinstance(files, list):
            continue
        for file_index, file_relative in enumerate(files):
            if not isinstance(file_relative, str) or not file_relative.strip():
                continue
            try:
                target = _safe_child(root, file_relative)
            except ProjectValidationError as exc:
                _error(
                    errors,
                    "artifact.path",
                    str(exc),
                    f"{source}:{component}[{index}].files[{file_index}]",
                )
                continue
            if not target.is_file():
                _error(
                    errors,
                    "artifact.resource",
                    f"{component.title()} output file not found: {target}",
                    source,
                )

def _validate_runner_resources(
    root: Path,
    value: Any,
    *,
    errors: list[Diagnostic],
    source: str,
) -> None:
    """Check the fixed runner directory and its manifested files."""

    if value is None or not isinstance(value, dict):
        return
    relative = value.get("path")
    if not isinstance(relative, str) or not relative.strip():
        return
    try:
        runner_root = _safe_child(root, relative)
    except ProjectValidationError as exc:
        _error(errors, "artifact.path", str(exc), source)
        return
    if not runner_root.is_dir():
        _error(
            errors,
            "artifact.resource",
            f"Runners directory not found: {runner_root}",
            source,
        )
    _validate_declared_files(
        root,
        value,
        component="runners",
        errors=errors,
        source=source,
    )

def _read_environment_manifest(
    path: Path, *, environment: str, build_id: Any
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectValidationError(
            f"Cannot read environment manifest: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise ProjectValidationError(f"Environment manifest must be an object: {path}")
    manifest_error = validate_manifest(
        value, expected_artifact_type="datacoolie_environment"
    )
    if manifest_error:
        raise ProjectValidationError(
            f"Invalid environment manifest: {path}: {manifest_error}"
        )
    if value.get("environment") != environment:
        raise ProjectValidationError(
            f"Environment manifest name does not match {environment!r}: {path}"
        )
    if build_id is not None and value.get("build_id") != build_id:
        raise ProjectValidationError(
            f"Environment manifest build_id does not match root manifest: {path}"
        )
    return value

def validate_artifact(
    root: Path | str, *, environment: str | None = None
) -> ValidationReport:
    candidate = Path(root).expanduser()
    if candidate.is_symlink():
        return ValidationReport(
            "artifact",
            [
                Diagnostic(
                    "error",
                    "artifact.symlink",
                    f"Artifact root must not be a symlink: {candidate}",
                    str(candidate),
                )
            ],
            [],
            ["artifact-layout", "metadata", "manifest", "inventory"],
            {
                "artifact": str(candidate.resolve()),
                "build_id": None,
                "environments": [],
                "current_comparison": {"performed": False},
            },
        )
    artifact = candidate.resolve()
    errors: list[Diagnostic] = []
    warnings: list[Diagnostic] = []
    checks = ["artifact-layout", "metadata", "manifest", "inventory"]
    metadata_reports: list[ValidationReport] = []
    if not artifact.is_dir():
        _error(errors, "artifact.missing", f"Artifact directory not found: {artifact}")
        return ValidationReport("artifact", errors, warnings, checks, {})
    manifest_path = artifact / MANIFEST_FILENAME
    manifest: dict[str, Any] | None = None
    if manifest_path.is_file():
        try:
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                manifest = value
            else:
                _error(
                    errors,
                    "manifest.shape",
                    f"{MANIFEST_FILENAME} must be an object",
                    str(manifest_path),
                )
        except (OSError, json.JSONDecodeError) as exc:
            _error(
                errors,
                "manifest.read",
                f"Cannot read manifest: {exc}",
                str(manifest_path),
            )
    else:
        _warning(
            warnings,
            "manifest.missing",
            f"No {MANIFEST_FILENAME} found; integrity checks are limited",
            str(artifact),
        )
    # Build artifacts use the root inventory below, but standalone extracted
    # environments do not have one.  Scan those limited-scope trees as well so
    # validation never silently follows a symlink outside the supplied root.
    if manifest is None or manifest.get("artifact_type") == ENVIRONMENT_ARTIFACT_TYPE:
        try:
            for child in sorted(
                artifact.rglob("*"),
                key=lambda item: (item.as_posix().casefold(), item.as_posix()),
            ):
                if child.is_symlink():
                    _error(
                        errors,
                        "artifact.symlink",
                        f"Artifact must not contain symlinks: {child}",
                        str(child),
                    )
        except OSError as exc:
            _error(
                errors,
                "artifact.read",
                f"Cannot inspect artifact tree: {exc}",
                str(artifact),
            )
    if (
        manifest is not None
        and manifest.get("artifact_type") != ENVIRONMENT_ARTIFACT_TYPE
    ):
        manifest_error = validate_manifest(
            manifest,
            expected_artifact_type=BUILD_ARTIFACT_TYPE,
        )
        if manifest_error:
            _error(errors, "manifest.invalid", manifest_error, str(manifest_path))
    # An environment directory is a valid portable runtime artifact in its
    # own right.  It carries a manifest for component discovery but does not
    # carry the build-level checksum index or environment map.
    if (
        manifest is not None
        and manifest.get("artifact_type") == ENVIRONMENT_ARTIFACT_TYPE
    ):
        manifest_error = validate_manifest(
            manifest,
            expected_artifact_type=ENVIRONMENT_ARTIFACT_TYPE,
        )
        if manifest_error:
            _error(errors, "manifest.invalid", manifest_error, str(manifest_path))
        env_name = manifest.get("environment")
        if not isinstance(env_name, str) or not env_name:
            _error(
                errors,
                "manifest.environment",
                "Environment manifest must contain environment",
                str(manifest_path),
            )
            env_name = "standalone"
        components = manifest.get("components", {})
        if not isinstance(components, dict):
            _error(
                errors,
                "manifest.components",
                "Environment manifest components must be an object",
                str(manifest_path),
            )
            components = {}
        _validate_function_resources(
            artifact,
            components.get("functions"),
            errors=errors,
            source=str(manifest_path),
        )
        _validate_runner_resources(
            artifact,
            components.get("runners"),
            errors=errors,
            source=str(manifest_path),
        )
        _validate_declared_files(
            artifact,
            components.get("metadata"),
            component="metadata",
            errors=errors,
            source=str(manifest_path),
        )
        metadata_value = components.get("metadata")
        metadata_relative = (
            metadata_value.get("path") if isinstance(metadata_value, dict) else None
        )
        if not isinstance(metadata_relative, str) or not metadata_relative:
            _error(
                errors,
                "artifact.metadata",
                "Environment manifest has no metadata path",
                str(manifest_path),
            )
        else:
            try:
                metadata_root = _safe_child(artifact, metadata_relative)
                snapshot = load_snapshot(metadata_root)
                sql_value = components.get("sql")
                sql_roots: list[Path] = []
                for sql_relative in _manifest_component_paths(
                    sql_value, component="SQL"
                ):
                    sql_root = _safe_child(artifact, sql_relative)
                    sql_roots.append(sql_root)
                    if not sql_root.is_dir():
                        _error(
                            errors,
                            "artifact.resource",
                            f"SQL component directory not found: {sql_root}",
                            str(manifest_path),
                        )
                # Runtime artifact mode does not read component declarations
                # from the manifest.  Validate references exactly as a
                # caller using only ``artifact_base_path`` would execute:
                # the declared path is joined directly below the artifact
                # root.  The manifest-declared SQL directories are still
                # checked above as tooling inventory.
                report = validate_metadata_document(
                    snapshot.merged(),
                    scope=f"artifact:{env_name}",
                    source=str(metadata_root),
                    sql_root=tuple(sql_roots),
                    artifact_root=artifact,
                )
                metadata_reports.append(report)
                errors.extend(report.errors)
                warnings.extend(report.warnings)
            except ProjectDependencyError:
                raise
            except Exception as exc:
                _error(errors, "artifact.metadata", str(exc), str(artifact))
        details = {
            "artifact": str(artifact),
            "build_id": manifest.get("build_id"),
            "environments": [],
            "limited_scope": True,
            "current_comparison": {"performed": False},
        }
        _add_schema_summary(details, metadata_reports)
        return ValidationReport("artifact", errors, warnings, checks, details)
    environments = manifest.get("environments", {}) if manifest else {}
    if manifest is not None and not isinstance(environments, dict):
        _error(
            errors,
            "manifest.environments",
            "Manifest environments must be an object",
            str(manifest_path),
        )
        environments = {}
    if not isinstance(environments, dict):
        environments = {}
    components = manifest.get("components", {}) if manifest else {}
    if manifest is not None and not isinstance(components, dict):
        _error(
            errors,
            "manifest.components",
            "Manifest components must be an object",
            str(manifest_path),
        )
        components = {}
    if not isinstance(components, dict):
        components = {}
    selected_environments = [environment] if environment else list(environments)
    if environment and environment not in environments:
        _error(
            errors,
            "environment.unknown",
            f"Unknown artifact environment: {environment}",
            str(artifact),
        )
    for env in selected_environments:
        entry = environments.get(env)
        if entry is None:
            continue
        if not isinstance(entry, dict):
            _error(
                errors,
                "artifact.environment",
                f"Invalid environment record: {env}",
                str(artifact),
            )
            continue
        if (
            not isinstance(env, str)
            or not env
            or "/" in env
            or "\\" in env
            or env in {".", ".."}
        ):
            _error(
                errors,
                "artifact.environment",
                f"Unsafe environment name: {env!r}",
                str(artifact),
            )
            continue
        try:
            environment_root = _safe_child(artifact, env)
        except ProjectError as exc:
            _error(errors, "artifact.path", str(exc), str(artifact))
            continue
        if not environment_root.is_dir():
            _error(
                errors,
                "artifact.environment",
                f"Environment directory not found: {environment_root}",
                str(artifact),
            )
        try:
            environment_manifest = _read_environment_manifest(
                environment_root / MANIFEST_FILENAME,
                environment=env,
                build_id=manifest.get("build_id") if manifest else None,
            )
        except ProjectError as exc:
            _error(
                errors,
                "artifact.environment_manifest",
                str(exc),
                str(environment_root / MANIFEST_FILENAME),
            )
            environment_manifest = None
        env_components = (
            environment_manifest.get("components", {})
            if isinstance(environment_manifest, dict)
            else entry
        )
        if not isinstance(env_components, dict):
            _error(
                errors,
                "manifest.components",
                f"Environment {env!r} components must be an object",
                str(environment_root),
            )
            env_components = {}
        _validate_function_resources(
            environment_root,
            env_components.get("functions"),
            errors=errors,
            source=str(environment_root),
        )
        _validate_runner_resources(
            environment_root,
            env_components.get("runners"),
            errors=errors,
            source=str(environment_root),
        )
        _validate_declared_files(
            environment_root,
            env_components.get("metadata"),
            component="metadata",
            errors=errors,
            source=str(environment_root),
        )
        metadata_value = env_components.get("metadata")
        metadata_relative = (
            metadata_value.get("path") if isinstance(metadata_value, dict) else None
        )
        if metadata_relative:
            try:
                metadata_root = _safe_child(environment_root, str(metadata_relative))
            except ProjectValidationError:
                # Older root manifests recorded ``env/metadata``.  Accept it
                # only when it still resolves below this environment.
                try:
                    metadata_root = _safe_child(artifact, str(metadata_relative))
                except ProjectError as exc:
                    _error(errors, "artifact.path", str(exc), str(artifact))
                    continue
            try:
                metadata_root.relative_to(environment_root)
            except ValueError:
                _error(
                    errors,
                    "artifact.path",
                    f"Environment metadata path is outside {env!r}: {metadata_relative}",
                    str(manifest_path),
                )
                continue
            try:
                snapshot = load_snapshot(metadata_root)
                sql_roots: list[Path] = []
                try:
                    sql_paths = _manifest_component_paths(
                        env_components.get("sql"), component="SQL"
                    )
                except ProjectValidationError as exc:
                    _error(
                        errors, "manifest.components", str(exc), str(environment_root)
                    )
                    sql_paths = []
                for sql_relative in sql_paths:
                    try:
                        sql_root = _safe_child(environment_root, sql_relative)
                    except ProjectValidationError as exc:
                        _error(errors, "artifact.path", str(exc), str(environment_root))
                        continue
                    sql_roots.append(sql_root)
                    if not sql_root.is_dir():
                        _error(
                            errors,
                            "artifact.resource",
                            f"SQL component directory not found: {sql_root}",
                            str(environment_root),
                        )
                # Use the manifest only as a validation-time description of
                # the copied roots; the framework still ignores it at runtime.
                report = validate_metadata_document(
                    snapshot.merged(),
                    scope=f"artifact:{env}",
                    source=str(metadata_root),
                    sql_root=tuple(sql_roots),
                    artifact_root=environment_root,
                )
                metadata_reports.append(report)
                errors.extend(report.errors)
                warnings.extend(report.warnings)
            except ProjectDependencyError:
                raise
            except Exception as exc:
                _error(errors, "artifact.metadata", str(exc), str(metadata_root))
        else:
            _error(
                errors,
                "artifact.metadata",
                f"Environment {env!r} has no metadata path",
                str(artifact),
            )
    if manifest is not None and not environments:
        _error(
            errors,
            "artifact.environment",
            "Artifact manifest contains no environments",
            str(manifest_path),
        )
    if manifest is not None:
        try:
            expected_hashes = declared_inventory(manifest)
        except ProjectError as exc:
            _error(errors, "manifest.artifacts", str(exc), str(manifest_path))
            expected_hashes = {}
        try:
            actual_hashes = file_hashes(artifact)
        except ProjectError as exc:
            _error(errors, "artifact.inventory", str(exc), str(artifact))
            actual_hashes = {}
        missing, extra, changed = inventory_difference(expected_hashes, actual_hashes)
        for relative in missing:
            _error(
                errors,
                "artifact.missing",
                f"Manifested file is missing: {relative}",
                str(artifact / relative),
            )
        for relative in extra:
            _error(
                errors,
                "artifact.extra",
                f"File is not declared by the manifest: {relative}",
                str(artifact / relative),
            )
        for relative in changed:
            _error(
                errors,
                "artifact.hash",
                f"Manifest hash mismatch: {relative}",
                str(artifact / relative),
            )
        content_digest = manifest.get("content_digest")
        if isinstance(content_digest, str) and content_digest != inventory_digest(
            actual_hashes
        ):
            _error(
                errors,
                "artifact.content_digest",
                "Manifest content_digest does not match files on disk",
                str(manifest_path),
            )

    comparison: dict[str, Any] = {"performed": False}
    if (
        manifest is not None
        and artifact.name.casefold() == "current"
        and artifact.parent.name == ".builds"
    ):
        build_id = manifest.get("build_id")
        if not is_safe_build_id(build_id):
            _error(
                errors,
                "current.build_id",
                "Current manifest has an unsafe build_id",
                str(manifest_path),
            )
        else:
            retained = artifact.parent / "artifacts" / build_id
            comparison = {
                "performed": True,
                "current_path": str(artifact),
                "build_path": str(retained),
                "build_id": build_id,
                "ok": False,
            }
            if not retained.is_dir():
                _error(
                    errors,
                    "current.history_missing",
                    f"Retained build not found for current build_id: {retained}",
                    str(retained),
                )
            else:
                try:
                    retained_manifest_path = retained / MANIFEST_FILENAME
                    if not retained_manifest_path.is_file():
                        _error(
                            errors,
                            "current.history_manifest",
                            "Retained build manifest is missing",
                            str(retained_manifest_path),
                        )
                    elif (
                        manifest_path.read_bytes()
                        != retained_manifest_path.read_bytes()
                    ):
                        _error(
                            errors,
                            "current.manifest_mismatch",
                            "Current and retained build manifests differ",
                            str(manifest_path),
                        )
                    # The current tree has already undergone the full
                    # metadata/resource validation above.  Once its root
                    # manifest is byte-identical to the retained manifest,
                    # comparing the retained inventory here proves the
                    # retained tree without recursively validating and
                    # hashing every retained file a second time.
                    retained_hashes = file_hashes(retained)
                    missing, extra, changed = inventory_difference(
                        retained_hashes, actual_hashes
                    )
                    for relative in missing:
                        _error(
                            errors,
                            "current.missing",
                            f"Current is missing retained build file: {relative}",
                            str(artifact / relative),
                        )
                    for relative in extra:
                        _error(
                            errors,
                            "current.extra",
                            f"Current has file absent from retained build: {relative}",
                            str(artifact / relative),
                        )
                    for relative in changed:
                        _error(
                            errors,
                            "current.hash_mismatch",
                            f"Current differs from retained build: {relative}",
                            str(artifact / relative),
                        )
                    comparison["ok"] = not (missing or extra or changed or errors)
                    comparison["missing"] = missing
                    comparison["extra"] = extra
                    comparison["changed"] = changed
                except ProjectError as exc:
                    _error(errors, "current.history", str(exc), str(retained))
            comparison["ok"] = comparison.get("ok", False) and not errors
    if manifest is None:
        # A caller may validate an extracted single-environment artifact.  No
        # manifest means identity/integrity are unavailable, but the default
        # metadata directory (or a uniquely named custom metadata directory)
        # can still receive a useful format/model check.
        candidates = []
        default_metadata = artifact / "metadata"
        if default_metadata.is_dir():
            candidates.append(default_metadata)
        candidates.extend(
            path
            for path in artifact.rglob("metadata")
            if path.is_dir() and path not in candidates
        )
        if not candidates:
            candidates = [artifact]
        for metadata_root in candidates[:1]:
            try:
                snapshot = load_snapshot(metadata_root)
                report = validate_metadata_document(
                    snapshot.merged(),
                    scope="artifact:standalone",
                    source=str(metadata_root),
                    sql_root=None,
                    artifact_root=artifact,
                )
                metadata_reports.append(report)
                errors.extend(report.errors)
                warnings.extend(report.warnings)
            except ProjectDependencyError:
                raise
            except Exception as exc:
                _error(errors, "artifact.metadata", str(exc), str(metadata_root))
    details = {
        "artifact": str(artifact),
        "build_id": manifest.get("build_id") if manifest else None,
        "environments": sorted(environments),
        "limited_scope": manifest is None,
        "current_comparison": comparison,
    }
    _add_schema_summary(details, metadata_reports)
    return ValidationReport(
        "artifact",
        errors,
        warnings,
        checks,
        details,
    )


__all__ = ["validate_artifact"]
