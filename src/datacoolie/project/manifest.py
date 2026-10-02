"""Project/build manifest contracts.

Manifest files describe project build outputs for CLI, validation and
inspection tooling.  They are intentionally kept outside the framework
runtime path: a Driver resolves metadata and resources only from the roots
provided by its caller.
"""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any

from datacoolie.utils.path_utils import ensure_relative_path


MANIFEST_FILENAME = "manifest.json"
BUILD_ARTIFACT_TYPE = "datacoolie_build"
ENVIRONMENT_ARTIFACT_TYPE = "datacoolie_environment"
MANIFEST_SCHEMA_VERSION = 1
_FUNCTION_PACKAGING = frozenset({"copy", "zip", "wheel"})
_BUILD_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def is_safe_build_id(value: Any) -> bool:
    """Return whether a build identity is safe to use as a directory name."""

    return isinstance(value, str) and _BUILD_ID_RE.fullmatch(value) is not None


def validate_manifest(
    value: Mapping[str, Any],
    *,
    expected_artifact_type: str | None = None,
) -> str | None:
    """Return a concise validation error, or ``None`` for a valid manifest."""

    if value.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        return f"unsupported manifest schema_version: {value.get('schema_version')!r}"
    artifact_type = value.get("artifact_type")
    if not isinstance(artifact_type, str) or not artifact_type.strip():
        return "manifest artifact_type must be a non-empty string"
    if expected_artifact_type is not None and artifact_type != expected_artifact_type:
        return (
            f"manifest artifact_type {artifact_type!r} does not match "
            f"expected {expected_artifact_type!r}"
        )
    if not is_safe_build_id(value.get("build_id")):
        return "manifest build_id must be a safe non-empty identifier"
    if artifact_type == ENVIRONMENT_ARTIFACT_TYPE:
        environment = value.get("environment")
        if not isinstance(environment, str) or not environment.strip():
            return "environment manifest must contain a non-empty environment"
    components = value.get("components")
    if components is not None:
        if not isinstance(components, Mapping):
            return "manifest components must be an object"
        component_error = _validate_components(
            components,
            strict_environment=artifact_type == ENVIRONMENT_ARTIFACT_TYPE,
        )
        if component_error:
            return component_error
    return None


def _validate_components(
    components: Mapping[str, Any],
    *,
    strict_environment: bool = False,
) -> str | None:
    """Validate descriptor paths without checking whether files exist."""

    for component in ("metadata", "sql", "functions", "runners"):
        if component not in components or components[component] is None:
            continue
        value = components[component]
        if strict_environment and component == "metadata" and not isinstance(value, Mapping):
            return "manifest components.metadata must be an object"
        if strict_environment and component in {"sql", "functions"} and not isinstance(value, list):
            return f"manifest components.{component} must be an array"
        if strict_environment and component == "runners" and not isinstance(value, Mapping):
            return "manifest components.runners must be an object"
        entries = value if isinstance(value, list) else [value]
        if component == "metadata" and not entries:
            return "manifest components.metadata cannot be an empty list"
        prefixes: set[str] = set()
        for index, entry in enumerate(entries):
            if strict_environment and component in {"sql", "functions"} and not isinstance(entry, Mapping):
                return f"manifest components.{component}[{index}] must be an object"
            if strict_environment and component == "runners" and not isinstance(entry, Mapping):
                return "manifest components.runners must be an object"
            path = entry.get("path") if isinstance(entry, Mapping) else entry
            if not isinstance(path, str) or not path.strip():
                return f"manifest components.{component}[{index}] must contain a non-empty path"
            try:
                relative = ensure_relative_path(path)
            except ValueError:
                return f"manifest components.{component}[{index}].path is invalid: {path!r}"
            if component == "runners" and strict_environment and relative != "runners":
                return "manifest components.runners.path must be 'runners'"
            if component == "sql":
                prefix = relative.rsplit("/", 1)[-1].casefold()
                if prefix in prefixes:
                    return f"manifest components.sql has duplicate folder prefix: {prefix!r}"
                prefixes.add(prefix)
            if component == "functions" and isinstance(entry, Mapping):
                packaging = entry.get("packaging")
                if strict_environment:
                    if packaging not in _FUNCTION_PACKAGING:
                        return (
                            f"manifest components.functions[{index}].packaging must be one of "
                            f"{sorted(_FUNCTION_PACKAGING)}"
                        )
                elif packaging is not None and packaging not in _FUNCTION_PACKAGING | {"auto"}:
                    return (
                        f"manifest components.functions[{index}].packaging is unsupported: "
                        f"{packaging!r}"
                    )
                if strict_environment and "files" not in entry:
                    return f"manifest components.functions[{index}].files must be an array"
            if component == "runners" and strict_environment and "files" not in entry:
                return "manifest components.runners.files must be an array"
            if isinstance(entry, Mapping) and "files" in entry:
                files = entry["files"]
                if not isinstance(files, list):
                    return f"manifest components.{component}[{index}].files must be an array"
                component_relative = relative
                seen_files: set[str] = set()
                for file_index, file_path in enumerate(files):
                    if not isinstance(file_path, str) or not file_path.strip():
                        return (
                            f"manifest components.{component}[{index}].files[{file_index}] "
                            "must be a non-empty path"
                        )
                    try:
                        file_relative = ensure_relative_path(file_path)
                    except ValueError:
                        return (
                            f"manifest components.{component}[{index}].files[{file_index}] "
                            f"is invalid: {file_path!r}"
                        )
                    if file_relative.casefold() in seen_files:
                        return (
                            f"manifest components.{component}[{index}].files has duplicate path: "
                            f"{file_path!r}"
                        )
                    seen_files.add(file_relative.casefold())
                    if strict_environment and not (
                        file_relative == component_relative
                        or file_relative.startswith(component_relative + "/")
                    ):
                        return (
                            f"manifest components.{component}[{index}].files[{file_index}] "
                            f"must be below component path {path!r}"
                        )
    return None


__all__ = [
    "BUILD_ARTIFACT_TYPE",
    "ENVIRONMENT_ARTIFACT_TYPE",
    "MANIFEST_FILENAME",
    "MANIFEST_SCHEMA_VERSION",
    "is_safe_build_id",
    "validate_manifest",
]
