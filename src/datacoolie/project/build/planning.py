"""Side-effect-free planning shared by project build and dry-run commands."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

from datacoolie import __version__

from ..config import MetadataOutput, ProjectConfig
from ..documents import MetadataSnapshot, canonical_json, check_output_format, load_snapshot
from ..errors import ProjectError, ProjectValidationError
from ..overlays import resolve_environment
from ..runners import RunnerLayout, discover_runner_layout
from ..validation.metadata import validate_metadata_document
from .functions import FunctionPackagingPlan, _component_files, plan_function_packaging


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _files_under(root: Path) -> list[Path]:
    """Return build input files while enforcing source containment rules."""

    return _component_files(root)


def _source_inputs(
    config: ProjectConfig,
    *,
    runner_layout: RunnerLayout,
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    roots = list(config.absolute_component_paths("metadata"))
    if config.sql:
        roots.extend(config.absolute_component_paths("sql"))
    if config.functions:
        roots.extend(config.absolute_component_paths("functions"))
    for root in roots:
        for path in _files_under(root):
            result.append(
                {
                    "path": path.relative_to(config.project_dir).as_posix(),
                    "sha256": _sha256(path),
                }
            )
    runner_layout.require_valid()
    for environment in runner_layout.environments:
        for item in runner_layout.files_for(environment):
            result.append(
                {
                    "path": (Path("runners") / environment / item.relative_path).as_posix(),
                    "sha256": _sha256(item.source_path),
                }
            )
    return result


def _metadata_format(
    metadata_output: MetadataOutput,
    requested_layout: str | None,
    requested_format: str | None,
) -> tuple[str, str]:
    layout = requested_layout or metadata_output.layout
    fmt = requested_format or metadata_output.format
    if layout not in {"single", "split", "preserve"}:
        raise ProjectError("Metadata layout must be one of single, split, or preserve")
    if fmt not in {"json", "yaml", "excel", "preserve"}:
        raise ProjectError("Metadata format must be one of json, yaml, excel, or preserve")
    if layout == "preserve" and fmt == "preserve":
        return layout, fmt
    if fmt == "preserve":
        raise ProjectError("Metadata format 'preserve' requires preserve layout")
    return layout, fmt


@dataclass(frozen=True)
class BuildPlan:
    """Resolved build inputs and decisions with no staging or publication."""

    config: ProjectConfig
    snapshot: MetadataSnapshot
    runner_layout: RunnerLayout
    source_inputs: list[dict[str, str]]
    resolved_by_environment: dict[str, dict[str, Any]]
    warnings: list[dict[str, Any]]
    function_plans: tuple[FunctionPackagingPlan, ...]
    layout: str
    metadata_format: str
    input_digest: str

    def preview(self) -> dict[str, Any]:
        """Return bounded, secret-free information useful before a build."""

        component_preview: dict[str, Any] = {
            "metadata": {
                "source_path": self.config.metadata.path,
                "layout": self.layout,
                "format": self.metadata_format,
                "source_documents": [item.relative_path for item in self.snapshot.documents],
                "source_file_count": _component_file_count(self.config, "metadata"),
                "environments": {},
            }
        }
        for environment, metadata in self.resolved_by_environment.items():
            component_preview["metadata"]["environments"][environment] = {
                "output_path": (Path(environment) / self.config.metadata.path).as_posix(),
                "entity_counts": _entity_counts(metadata),
            }

        if self.config.sql:
            component_preview["sql"] = [
                {
                    "source_path": entry.path,
                    "source_file_count": len(_files_under(root)),
                    "environments": {
                        environment: {
                            "output_path": (Path(environment) / entry.path).as_posix(),
                        }
                        for environment in self.resolved_by_environment
                    },
                }
                for entry, root in zip(
                    self.config.sql,
                    self.config.absolute_component_paths("sql"),
                    strict=True,
                )
            ]

        if self.config.functions:
            component_preview["functions"] = [
                {
                    **function_plan.to_dict(),
                    "source_path": entry.path,
                    "environments": {
                        environment: {
                            "output_path": (Path(environment) / entry.path).as_posix(),
                        }
                        for environment in self.resolved_by_environment
                    },
                }
                for entry, function_plan in zip(
                    self.config.functions,
                    self.function_plans,
                    strict=True,
                )
            ]

        if self.runner_layout.exists:
            component_preview["runners"] = {
                environment: {
                    "source_path": (Path("runners") / environment).as_posix(),
                    "output_path": (Path(environment) / "runners").as_posix(),
                    "file_count": len(self.runner_layout.files_for(environment)),
                    "files": [
                        item.relative_path
                        for item in self.runner_layout.files_for(environment)
                    ],
                }
                for environment in self.runner_layout.environments
            }

        return {
            "components": component_preview,
            "build_root": str(self.config.project_dir / ".builds" / "artifacts"),
            "current_path": str(self.config.project_dir / ".builds" / "current"),
        }


def _component_file_count(config: ProjectConfig, name: str) -> int:
    return sum(
        len(_files_under(root))
        for root in config.absolute_component_paths(name)
    )


def _entity_counts(metadata: dict[str, Any]) -> dict[str, int]:
    return {
        section: len(metadata.get(section, []))
        for section in ("connections", "dataflows", "schema_hints")
        if isinstance(metadata.get(section), list)
    }


def create_build_plan(
    config: ProjectConfig,
    *,
    metadata_layout: str | None = None,
    metadata_format: str | None = None,
) -> BuildPlan:
    """Resolve and validate a build without writing output files."""

    metadata_output = config.metadata.output or MetadataOutput()
    layout, effective_format = _metadata_format(
        metadata_output,
        metadata_layout,
        metadata_format,
    )
    runner_layout = discover_runner_layout(config.project_dir, config.environments)
    runner_layout.require_valid()
    metadata_root = config.absolute_component_path("metadata")
    if metadata_root.is_symlink():
        raise ProjectError(f"Build inputs must not use symlink roots: {metadata_root}")
    snapshot = load_snapshot(metadata_root)
    if effective_format == "preserve":
        for document in snapshot.documents:
            check_output_format(document.format)
    else:
        check_output_format(effective_format)
    source_inputs = _source_inputs(config, runner_layout=runner_layout)
    resolved_by_environment: dict[str, dict[str, Any]] = {}
    warnings: list[dict[str, Any]] = []

    for environment in sorted(config.environments):
        resolved, _ = resolve_environment(snapshot, environment)
        report = validate_metadata_document(
            resolved,
            scope=f"metadata:{environment}",
            source=str(snapshot.root),
            # Authored shorthand references are qualified against the
            # configured SQL roots.  A nested root such as ``shared/sql2``
            # therefore accepts ``sql2/orders.sql`` while a full
            # artifact-relative reference remains available for artifact-only
            # runners through the validation fallback.
            sql_root=(
                config.absolute_component_paths("sql")
                if config.sql
                else ()
            ),
            artifact_root=config.project_dir,
        )
        if report.errors:
            raise ProjectValidationError(
                f"Metadata validation failed for environment {environment}",
                details=report.to_dict(),
            )
        resolved_by_environment[environment] = resolved
        warnings.extend(item.to_dict() for item in report.warnings)

    function_plans = tuple(
        plan_function_packaging(
            root,
            entry.packaging or "auto",
        )
        for entry, root in zip(
            config.functions or (),
            config.absolute_component_paths("functions") if config.functions else (),
            strict=True,
        )
    )
    input_contract = {
        "schema_version": 1,
        "tool_version": __version__,
        "project": config.to_dict(),
        "inputs": source_inputs,
        "runners": {
            "root": "runners" if runner_layout.exists else None,
            "environments": sorted(runner_layout.environment_directories),
        },
        "metadata_layout": layout,
        "metadata_format": effective_format,
    }
    input_digest = sha256(canonical_json(input_contract).encode("utf-8")).hexdigest()
    return BuildPlan(
        config=config,
        snapshot=snapshot,
        runner_layout=runner_layout,
        source_inputs=source_inputs,
        resolved_by_environment=resolved_by_environment,
        warnings=warnings,
        function_plans=function_plans,
        layout=layout,
        metadata_format=effective_format,
        input_digest=input_digest,
    )


__all__ = ["BuildPlan", "create_build_plan"]
