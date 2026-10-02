"""Project configuration and portable project discovery.

The project configuration is deliberately smaller than ``DataCoolieRunConfig``:
it describes authored resources and build intent, not a Driver session.  This
module performs strict, side-effect-free parsing so every CLI command can use
the same rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
from typing import Any, Mapping

from .errors import ProjectConfigError, ProjectDependencyError


CONFIG_FILENAME = "datacoolie.yml"
CONFIG_SCHEMA_VERSION = 1
_ENVIRONMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_LAYOUTS = frozenset({"single", "split", "preserve"})
_FORMATS = frozenset({"json", "yaml", "excel", "preserve"})
_PACKAGING = frozenset({"auto", "copy", "wheel", "zip"})
_COMPONENT_NAMES = frozenset({"metadata", "sql", "functions"})
_RESERVED_PROJECT_DIRS = frozenset({".builds", ".runtime", ".releases", "runners"})


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ProjectConfigError(f"{label} must be a mapping")
    return dict(value)


def _unknown(mapping: Mapping[str, Any], allowed: set[str], label: str) -> None:
    unknown = sorted(str(key) for key in set(mapping) - allowed)
    if unknown:
        raise ProjectConfigError(f"Unsupported {label} field(s): {', '.join(unknown)}")


def normalize_relative_path(value: Any, *, label: str) -> str:
    """Normalize one project-relative directory and reject traversal."""

    if not isinstance(value, str) or not value.strip():
        raise ProjectConfigError(f"{label} must be a non-empty relative path")
    raw = value.strip().replace("\\", "/")
    # Check both grammars because a Windows-configured path may be read by CI
    # on a POSIX host (and vice versa).
    if PurePosixPath(raw).is_absolute() or PureWindowsPath(raw).is_absolute():
        raise ProjectConfigError(f"{label} must be project-relative: {value!r}")
    if PureWindowsPath(raw).drive or ":" in raw.split("/")[0]:
        raise ProjectConfigError(f"{label} must not contain a drive or URI scheme")
    parts = [part for part in raw.split("/") if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        raise ProjectConfigError(f"{label} must not escape the project: {value!r}")
    return "/".join(parts)


def _normalise_env_name(value: Any, *, label: str = "environment name") -> str:
    if not isinstance(value, str) or not _ENVIRONMENT_RE.fullmatch(value):
        raise ProjectConfigError(
            f"{label} must match {_ENVIRONMENT_RE.pattern}: {value!r}"
        )
    return value


@dataclass(frozen=True)
class MetadataOutput:
    layout: str = "single"
    format: str = "json"

    @classmethod
    def from_mapping(cls, value: Any) -> "MetadataOutput":
        if value is None:
            return cls()
        data = _mapping(value, "components.metadata.output")
        _unknown(data, {"layout", "format"}, "metadata output")
        layout = data.get("layout", "single")
        fmt = data.get("format", "json")
        if not isinstance(layout, str) or layout not in _LAYOUTS:
            raise ProjectConfigError(
                f"components.metadata.output.layout must be one of {sorted(_LAYOUTS)}"
            )
        if not isinstance(fmt, str) or fmt not in _FORMATS:
            raise ProjectConfigError(
                f"components.metadata.output.format must be one of {sorted(_FORMATS)}"
            )
        if layout == "preserve" and fmt not in {"preserve", "json", "yaml", "excel"}:
            raise ProjectConfigError("Unsupported metadata preserve output format")
        if layout != "preserve" and fmt == "preserve":
            raise ProjectConfigError(
                "metadata output format 'preserve' requires layout 'preserve'"
            )
        return cls(layout=layout, format=fmt)


@dataclass(frozen=True)
class ComponentConfig:
    """One authored component entry.

    Metadata has exactly one entry.  SQL and functions are represented by a
    tuple of these entries on :class:`ProjectConfig`, allowing each functions
    root to select its own packaging mode while keeping the user-facing key
    singular as ``path``.
    """

    name: str
    path: str
    packaging: str | None = None
    output: MetadataOutput | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"path": self.path}
        if self.packaging is not None:
            result["packaging"] = self.packaging
        if self.output is not None:
            result["output"] = {
                "layout": self.output.layout,
                "format": self.output.format,
            }
        return result


@dataclass(frozen=True)
class EnvironmentConfig:
    name: str
    platform: str = "local"
    deployment_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"platform": self.platform}
        if self.deployment_path is not None:
            result["deployment_path"] = self.deployment_path
        return result


@dataclass(frozen=True)
class ProjectConfig:
    project_name: str
    environments: dict[str, EnvironmentConfig]
    metadata: ComponentConfig
    sql: tuple[ComponentConfig, ...] | None = None
    functions: tuple[ComponentConfig, ...] | None = None
    schema_version: int = CONFIG_SCHEMA_VERSION
    source_path: Path | None = field(default=None, compare=False, repr=False)

    @property
    def project_dir(self) -> Path:
        if self.source_path is None:
            raise ProjectConfigError("ProjectConfig has no source path")
        return self.source_path.parent

    @property
    def component_paths(self) -> dict[str, tuple[str, ...]]:
        """Return all configured component roots in declaration order."""

        result = {"metadata": (self.metadata.path,)}
        if self.sql is not None:
            result["sql"] = tuple(item.path for item in self.sql)
        if self.functions is not None:
            result["functions"] = tuple(item.path for item in self.functions)
        return result

    def component_entries(self, name: str) -> tuple[ComponentConfig, ...]:
        """Return the entries for one component, or raise when absent."""

        if name == "metadata":
            return (self.metadata,)
        if name == "sql" and self.sql is not None:
            return self.sql
        if name == "functions" and self.functions is not None:
            return self.functions
        raise ProjectConfigError(f"Component is not configured: {name}")

    def absolute_component_paths(self, name: str) -> tuple[Path, ...]:
        return tuple(
            self.project_dir.joinpath(*entry.path.split("/"))
            for entry in self.component_entries(name)
        )

    def absolute_component_path(self, name: str) -> Path:
        paths = self.absolute_component_paths(name)
        if len(paths) != 1:
            raise ProjectConfigError(
                f"Component {name!r} has {len(paths)} roots; use absolute_component_paths()"
            )
        return paths[0]

    def to_dict(self) -> dict[str, Any]:
        components: dict[str, Any] = {"metadata": self.metadata.to_dict()}
        for name, entries in (("sql", self.sql), ("functions", self.functions)):
            if entries is None:
                continue
            serialized = [entry.to_dict() for entry in entries]
            components[name] = serialized[0] if len(serialized) == 1 else serialized
        return {
            "schema_version": self.schema_version,
            "project": {"name": self.project_name},
            "components": components,
            "environments": {
                name: environment.to_dict()
                for name, environment in sorted(self.environments.items())
            },
        }


def _component_entry(name: str, value: Any, *, default_path: str | None = None) -> ComponentConfig:
    if value is None:
        if default_path is None:
            raise ProjectConfigError(f"components.{name} is not configured")
        data: dict[str, Any] = {}
    elif isinstance(value, str):
        data = {"path": value}
    else:
        data = _mapping(value, f"components.{name}")
    allowed = {"path"}
    if name == "functions":
        allowed.add("packaging")
    if name == "metadata":
        allowed.add("output")
    _unknown(data, allowed, f"components.{name}")
    path = normalize_relative_path(
        data.get("path", default_path), label=f"components.{name}.path"
    )
    packaging: str | None = None
    output: MetadataOutput | None = None
    if name == "functions":
        packaging = data.get("packaging", "auto")
        if not isinstance(packaging, str) or packaging not in _PACKAGING:
            raise ProjectConfigError(
                f"components.functions.packaging must be one of {sorted(_PACKAGING)}"
            )
    elif name == "metadata":
        output = MetadataOutput.from_mapping(data.get("output"))
    return ComponentConfig(name=name, path=path, packaging=packaging, output=output)


def _component_entries(
    name: str,
    value: Any,
    *,
    default_path: str | None = None,
    allow_empty: bool = True,
) -> tuple[ComponentConfig, ...] | None:
    """Parse one object or a list of per-root component entries."""

    if value is None:
        if default_path is None:
            return None
        return (_component_entry(name, None, default_path=default_path),)
    if isinstance(value, list):
        if not value and not allow_empty:
            raise ProjectConfigError(f"components.{name} must contain at least one entry")
        return tuple(
            _component_entry(name, item)
            for item in value
        )
    return (_component_entry(name, value),)


def _parse_environment(name: Any, value: Any) -> EnvironmentConfig:
    env_name = _normalise_env_name(name)
    data = _mapping(value, f"environments.{env_name}")
    _unknown(data, {"platform", "deployment_path"}, f"environments.{env_name}")
    platform = data.get("platform", "local")
    if not isinstance(platform, str) or not platform.strip():
        raise ProjectConfigError(f"environments.{env_name}.platform must be a string")
    deployment = data.get("deployment_path")
    if deployment is not None and (
        not isinstance(deployment, str) or not deployment.strip()
    ):
        raise ProjectConfigError(
            f"environments.{env_name}.deployment_path must be a non-empty string"
        )
    return EnvironmentConfig(env_name, platform.strip(), deployment.strip() if deployment else None)


def project_config_from_mapping(
    value: Any,
    *,
    source_path: Path | None = None,
) -> ProjectConfig:
    data = _mapping(value, "datacoolie.yml")
    _unknown(data, {"schema_version", "project", "components", "environments"}, "project")
    schema_version = data.get("schema_version", CONFIG_SCHEMA_VERSION)
    if not isinstance(schema_version, int) or isinstance(schema_version, bool) or schema_version != CONFIG_SCHEMA_VERSION:
        raise ProjectConfigError(
            f"Unsupported datacoolie.yml schema_version {schema_version!r}; "
            f"expected {CONFIG_SCHEMA_VERSION}"
        )
    project = _mapping(data.get("project"), "project")
    _unknown(project, {"name"}, "project")
    name = project.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ProjectConfigError("project.name must be a non-empty string")
    components_data = _mapping(data.get("components", {}), "components")
    _unknown(components_data, set(_COMPONENT_NAMES), "components")
    metadata_entries = _component_entries(
        "metadata", components_data.get("metadata"), default_path="metadata", allow_empty=False
    )
    if metadata_entries is None or len(metadata_entries) != 1:
        raise ProjectConfigError("components.metadata must define exactly one path")
    metadata = metadata_entries[0]
    sql = _component_entries("sql", components_data.get("sql")) if "sql" in components_data else None
    functions = (
        _component_entries("functions", components_data.get("functions"))
        if "functions" in components_data
        else None
    )

    roots: list[tuple[str, str]] = [("metadata", metadata.path)]
    for component_name, entries in (("sql", sql), ("functions", functions)):
        if entries is None:
            continue
        roots.extend((component_name, entry.path) for entry in entries)
    root_parts = [(name, tuple(path.casefold().split("/")), path) for name, path in roots]
    for component_name, parts, path in root_parts:
        if parts and parts[0] in _RESERVED_PROJECT_DIRS:
            if parts[0] == ".builds":
                reason = "reserved for build output"
            elif parts[0] == "runners":
                reason = "reserved for environment runners"
            else:
                reason = "reserved for mutable runtime/release state"
            raise ProjectConfigError(
                f"components.{component_name}.path {path!r} is {reason}"
            )
    for index, (left, left_parts, left_path) in enumerate(root_parts):
        for right, right_parts, right_path in root_parts[index + 1 :]:
            if left_parts[: len(right_parts)] == right_parts or right_parts[: len(left_parts)] == left_parts:
                raise ProjectConfigError(
                    f"components.{left}.path {left_path!r} and "
                    f"components.{right}.path {right_path!r} overlap"
                )
    if sql is not None:
        prefixes: dict[str, str] = {}
        for entry in sql:
            prefix = entry.path.rsplit("/", 1)[-1].casefold()
            if prefix in prefixes:
                raise ProjectConfigError(
                    "components.sql paths must have unique folder names for query resolution: "
                    f"{prefixes[prefix]!r} and {entry.path!r}"
                )
            prefixes[prefix] = entry.path
    environments_data = data.get("environments")
    if not isinstance(environments_data, Mapping) or not environments_data:
        raise ProjectConfigError("environments must contain at least one named environment")
    environments: dict[str, EnvironmentConfig] = {}
    environment_names: dict[str, str] = {}
    for env_name, env_value in environments_data.items():
        parsed = _parse_environment(env_name, env_value)
        folded_name = parsed.name.casefold()
        if folded_name in environment_names:
            raise ProjectConfigError(f"Duplicate environment: {parsed.name}")
        environment_names[folded_name] = parsed.name
        environments[parsed.name] = parsed
    return ProjectConfig(
        project_name=name.strip(),
        environments=environments,
        metadata=metadata,
        sql=sql,
        functions=functions,
        schema_version=schema_version,
        source_path=source_path.resolve() if source_path else None,
    )


def load_project_config(project_dir: Path | str) -> ProjectConfig:
    """Load the nearest project's ``datacoolie.yml`` from a directory/file."""

    candidate = Path(project_dir).expanduser()
    if candidate.is_file():
        config_path = candidate
    else:
        config_path = candidate / CONFIG_FILENAME
    if not config_path.is_file():
        raise ProjectConfigError(f"Project configuration not found: {config_path}")
    try:
        import yaml  # noqa: WPS433 - optional CLI dependency
    except ImportError as exc:
        raise ProjectDependencyError(
            "PyYAML is required for project configuration; install datacoolie[cli]",
            dependency="PyYAML",
            exit_code=2,
        ) from exc
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ProjectConfigError(f"Cannot read project configuration: {config_path}") from exc
    except Exception as exc:
        raise ProjectConfigError(f"Cannot parse project configuration: {config_path}") from exc
    return project_config_from_mapping(raw, source_path=config_path)


def discover_project(start: Path | str = ".") -> ProjectConfig:
    """Discover the nearest project marker walking from *start* upward."""

    path = Path(start).expanduser().resolve()
    if path.is_file():
        path = path.parent
    for directory in (path, *path.parents):
        marker = directory / CONFIG_FILENAME
        if marker.is_file():
            return load_project_config(marker)
    raise ProjectConfigError(
        f"No {CONFIG_FILENAME} found from {path}; pass --project-dir PATH"
    )


__all__ = [
    "CONFIG_FILENAME",
    "CONFIG_SCHEMA_VERSION",
    "ComponentConfig",
    "EnvironmentConfig",
    "MetadataOutput",
    "ProjectConfig",
    "discover_project",
    "load_project_config",
    "normalize_relative_path",
    "project_config_from_mapping",
]
