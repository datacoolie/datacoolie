"""Compact, redacted inspection reports for projects and artifacts."""

from __future__ import annotations

from importlib.metadata import distributions
import json
from pathlib import Path
from typing import Any

from .config import ProjectConfig
from .documents import load_snapshot
from .errors import ProjectValidationError
from .overlays import resolve_environment
from .manifest import (
    BUILD_ARTIFACT_TYPE,
    ENVIRONMENT_ARTIFACT_TYPE,
    MANIFEST_FILENAME,
    validate_manifest,
)
from .runners import discover_runner_layout


_SENSITIVE_PARTS = ("password", "secret", "token", "api_key", "access_key", "private_key", "connection_string")


def _redact(value: Any, *, key: str = "") -> Any:
    if any(part in key.casefold() for part in _SENSITIVE_PARTS):
        return "<redacted>"
    if isinstance(value, dict):
        return {name: _redact(item, key=str(name)) for name, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, key=key) for item in value]
    return value


def inspect_project(config: ProjectConfig) -> dict[str, Any]:
    runner_layout = discover_runner_layout(config.project_dir, config.environments)
    result: dict[str, Any] = {
        "project": config.project_name,
        "project_dir": str(config.project_dir),
        "components": {
            name: {
                "paths": list(paths),
                "exists": all(
                    config.project_dir.joinpath(*path.split("/")).is_dir()
                    for path in paths
                ),
            }
            for name, paths in config.component_paths.items()
        },
        "environments": {
            name: {
                "platform": env.platform,
                "deployment_path": env.deployment_path,
            }
            for name, env in config.environments.items()
        },
        "runners": {
            "path": "runners",
            "exists": runner_layout.exists,
            "environments": {
                environment: {
                    "exists": environment in runner_layout.environment_directories,
                    "files": [item.relative_path for item in runner_layout.files_for(environment)],
                }
                for environment in config.environments
            },
            "problems": list(runner_layout.problems),
        },
    }
    current = config.project_dir / ".builds" / "current" / MANIFEST_FILENAME
    if current.is_file():
        try:
            result["current"] = json.loads(current.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            result["current"] = {"status": "unreadable"}
    else:
        result["current"] = None
    return result


def inspect_config(config: ProjectConfig, environment: str | None = None) -> dict[str, Any]:
    payload = _redact(config.to_dict())
    payload["config_path"] = str(config.source_path) if config.source_path else None
    payload["project_dir"] = str(config.project_dir)
    payload["resolved_components"] = {
        name: [str(path) for path in config.absolute_component_paths(name)]
        for name in config.component_paths
    }
    payload["origins"] = {
        "config": str(config.source_path) if config.source_path else "constructed",
        "components": {
            name: str(config.source_path) if config.source_path else "constructed"
            for name in config.component_paths
        },
        "environments": {
            name: str(config.source_path) if config.source_path else "constructed"
            for name in config.environments
        },
    }
    if environment is not None:
        if environment not in config.environments:
            raise ProjectValidationError(f"Unknown environment: {environment}")
        payload["environments"] = {environment: payload["environments"][environment]}
        payload["origins"]["environments"] = {
            environment: payload["origins"]["environments"][environment]
        }
    return payload


def inspect_metadata(
    config: ProjectConfig | None = None,
    *,
    metadata_path: Path | str | None = None,
    environment: str | None = None,
    section: str | None = None,
    name: str | None = None,
    stage: str | None = None,
    full: bool = False,
) -> dict[str, Any]:
    valid_sections = {"connections", "dataflows", "schema_hints"}
    if (name or stage or full) and section is None:
        raise ProjectValidationError(
            "--name, --stage, and --full require --section"
        )
    if section is not None and section not in valid_sections:
        raise ProjectValidationError(f"Unknown metadata section: {section}")
    if name and section not in {"connections", "dataflows"}:
        raise ProjectValidationError("--name is supported only for connections or dataflows")
    if stage and section != "dataflows":
        raise ProjectValidationError("--stage is supported only for dataflows")
    if metadata_path is None:
        if config is None:
            raise ProjectValidationError("metadata inspection requires a project or --metadata-path")
        metadata_path = config.absolute_component_path("metadata")
    snapshot = load_snapshot(metadata_path)
    metadata = snapshot.merged()
    overlay_path = None
    if environment is not None:
        metadata, overlay_path = resolve_environment(snapshot, environment)
    result: dict[str, Any] = {
        "metadata_path": str(Path(metadata_path).resolve()),
        "environment": environment,
        "overlay": str(overlay_path) if overlay_path else None,
        "documents": [
            {"path": item.relative_path, "format": item.format, "sections": list(item.sections)}
            for item in snapshot.documents
        ],
        "counts": {key: len(metadata.get(key, [])) for key in ("connections", "dataflows", "schema_hints")},
    }
    if section:
        values = metadata.get(section, [])
        if name or stage:
            values = [
                item for item in values
                if (not name or item.get("name") == name)
                and (not stage or item.get("stage") == stage)
            ]
        result["items"] = _redact(values) if full else [
            {key: item.get(key) for key in ("name", "dataflow_id", "stage", "connection_name", "table_name") if key in item}
            for item in values
        ]
    return result


def inspect_capabilities() -> dict[str, Any]:
    # Registries are already populated by datacoolie package import.  Reading
    # names does not instantiate an SDK or create a Driver session.
    from datacoolie import (
        __version__,
        destination_registry,
        engine_registry,
        platform_registry,
        resolver_registry,
        source_registry,
        transformer_registry,
    )
    return {
        "datacoolie_version": __version__,
        "registrations": {
            "engines": sorted(engine_registry.list_plugins()),
            "platforms": sorted(platform_registry.list_plugins()),
            "sources": sorted(source_registry.list_plugins()),
            "destinations": sorted(destination_registry.list_plugins()),
            "transformers": sorted(transformer_registry.list_plugins()),
            "resolvers": sorted(resolver_registry.list_plugins()),
        },
        "distribution_count": sum(1 for _ in distributions()),
    }


def inspect_artifact(root: Path | str) -> dict[str, Any]:
    candidate = Path(root).expanduser()
    if candidate.is_symlink():
        raise ProjectValidationError(f"Artifact root must not be a symlink: {candidate}")
    artifact = candidate.resolve()
    if not artifact.is_dir():
        raise ProjectValidationError(f"Artifact directory not found: {artifact}")
    manifest = artifact / MANIFEST_FILENAME
    if not manifest.is_file():
        # An extracted single-environment artifact can be inspected without
        # the immutable build manifest.  Make the reduced coverage explicit
        # rather than pretending it has a build identity.
        files = [
            path
            for path in artifact.rglob("*")
            if path.is_file() and not path.is_symlink()
        ]
        return {
            "artifact_path": str(artifact),
            "artifact_type": None,
            "build_id": None,
            "content_digest": None,
            "project": None,
            "environments": {},
            "functions_artifact": None,
            "artifact_count": len(files),
            "limited_scope": True,
        }
    try:
        value = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectValidationError(f"Cannot read artifact manifest: {manifest}") from exc
    if not isinstance(value, dict):
        raise ProjectValidationError("Artifact manifest must be an object")
    artifact_type = value.get("artifact_type")
    if artifact_type == ENVIRONMENT_ARTIFACT_TYPE:
        manifest_error = validate_manifest(
            value,
            expected_artifact_type=ENVIRONMENT_ARTIFACT_TYPE,
        )
        if manifest_error:
            raise ProjectValidationError(f"Invalid environment manifest: {manifest_error}")
        environment = value.get("environment")
        components = value.get("components", {})
        if not isinstance(components, dict):
            components = {}
        files = [
            path
            for path in artifact.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and path != manifest
        ]
        return {
            "artifact_path": str(artifact),
            "artifact_type": ENVIRONMENT_ARTIFACT_TYPE,
            "build_id": value.get("build_id"),
            "content_digest": value.get("content_digest"),
            "project": _redact(value.get("project")),
            "environment": environment,
            "environments": {
                str(environment): {
                    "platform": value.get("platform"),
                    "components": _redact(components),
                }
            },
            "components": _redact(components),
            "functions_artifact": _redact(components.get("functions")),
            "artifact_count": len(files),
            "limited_scope": True,
        }
    if artifact_type != BUILD_ARTIFACT_TYPE:
        raise ProjectValidationError(
            f"Unsupported artifact manifest type: {artifact_type!r}"
        )
    manifest_error = validate_manifest(value, expected_artifact_type=BUILD_ARTIFACT_TYPE)
    if manifest_error:
        raise ProjectValidationError(f"Invalid build manifest: {manifest_error}")
    return {
        "artifact_path": str(artifact),
        "artifact_type": BUILD_ARTIFACT_TYPE,
        "build_id": value.get("build_id"),
        "content_digest": value.get("content_digest"),
        "project": value.get("project"),
        "environments": _redact(value.get("environments", {})),
        "functions_artifact": _redact(value.get("functions_artifact")),
        "artifact_count": len(value.get("artifacts", [])) if isinstance(value.get("artifacts"), list) else None,
        "limited_scope": False,
    }


__all__ = ["inspect_artifact", "inspect_capabilities", "inspect_config", "inspect_metadata", "inspect_project"]
