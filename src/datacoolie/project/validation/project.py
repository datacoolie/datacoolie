"""Validation of a configured DataCoolie project."""

from __future__ import annotations

from datacoolie.project.config import ProjectConfig
from datacoolie.project.documents import MetadataSnapshot, load_snapshot
from datacoolie.project.errors import ProjectDependencyError
from datacoolie.project.overlays import resolve_environment
from datacoolie.project.runners import discover_runner_layout
from .metadata import validate_metadata_document
from .reports import Diagnostic, ValidationReport, _add_schema_summary, _error, _warning

def validate_project(
    config: ProjectConfig,
    *,
    environments: list[str] | None = None,
    only: set[str] | None = None,
) -> ValidationReport:
    errors: list[Diagnostic] = []
    warnings: list[Diagnostic] = []
    scopes = ("config", "metadata", "resources")
    selected_scopes = set(scopes) if only is None else set(only)
    checks = ["project-config"]
    if "metadata" in selected_scopes:
        checks.append("metadata")
    if "resources" in selected_scopes:
        checks.append("resources")
    selected = environments or list(config.environments)
    for env in selected:
        if env not in config.environments:
            _error(
                errors,
                "environment.unknown",
                f"Unknown environment: {env}",
                "environments",
            )
    if errors:
        return ValidationReport(
            "project",
            errors,
            warnings,
            ["project-config"],
            {
                "environments": selected,
                "metadata_loaded": False,
                "not_checked": [name for name in scopes if name != "config"],
            },
        )
    if "resources" in selected_scopes:
        for name, components in config.component_paths.items():
            for component in components:
                path = config.project_dir.joinpath(*component.split("/"))
                if path.is_symlink():
                    _error(
                        errors,
                        "resource.symlink",
                        f"Configured {name} directory must not be a symlink: {path}",
                        component,
                    )
                elif not path.is_dir():
                    _error(
                        errors,
                        "resource.missing",
                        f"Configured {name} directory not found: {path}",
                        component,
                    )
                elif name in {"metadata", "sql", "functions"}:
                    try:
                        for child in path.rglob("*"):
                            if child.is_symlink():
                                _error(
                                    errors,
                                    "resource.symlink",
                                    f"Configured {name} tree must not contain symlinks: {child}",
                                    str(child),
                                )
                    except OSError as exc:
                        _error(
                            errors,
                            "resource.read",
                            f"Cannot inspect configured {name} directory: {exc}",
                            component,
                        )
        runner_layout = discover_runner_layout(config.project_dir, config.environments)
        for problem in runner_layout.problems:
            _error(errors, "runner.invalid", problem, str(runner_layout.root))
    snapshot: MetadataSnapshot | None = None
    metadata_reports: list[ValidationReport] = []
    if only is None or "metadata" in only:
        try:
            snapshot = load_snapshot(config.absolute_component_path("metadata"))
        except ProjectDependencyError:
            raise
        except Exception as exc:
            _error(
                errors,
                "metadata.load",
                str(exc),
                str(config.absolute_component_path("metadata")),
            )
        if snapshot is not None:
            for env in selected:
                try:
                    metadata, _ = resolve_environment(snapshot, env)
                except Exception as exc:
                    _error(errors, "environment.overlay", str(exc), env)
                    continue
                report = validate_metadata_document(
                    metadata,
                    scope=f"metadata:{env}",
                    source=str(snapshot.root),
                    # Authored-project validation resolves shorthand SQL
                    # references against the declared roots.  An empty tuple
                    # is intentional: it reports a missing SQL base instead
                    # of silently falling back to the project directory.
                    sql_root=(
                        config.absolute_component_paths("sql") if config.sql else ()
                    ),
                    artifact_root=config.project_dir,
                )
                metadata_reports.append(report)
                errors.extend(report.errors)
                warnings.extend(report.warnings)
    if "resources" in selected_scopes:
        metadata_root = config.absolute_component_path("metadata")
        environment_dir = metadata_root / "environments"
        if environment_dir.is_dir():
            known = set(config.environments)
            for path in environment_dir.glob("*.json"):
                if path.stem not in known:
                    _warning(
                        warnings,
                        "environment.unused_overlay",
                        f"Overlay is not declared in project environments: {path.name}",
                        str(path),
                    )
    details = {
        "project": config.project_name,
        "project_dir": str(config.project_dir),
        "environments": selected,
        "metadata_loaded": snapshot is not None,
        "not_checked": [name for name in scopes if name not in selected_scopes],
    }
    _add_schema_summary(details, metadata_reports)
    return ValidationReport(
        "project",
        errors,
        warnings,
        checks,
        details,
    )


__all__ = ["validate_project"]
