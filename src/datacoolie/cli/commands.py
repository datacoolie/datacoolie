"""Command implementations kept separate from parser and rendering."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import os
import uuid

from datacoolie.project.build import build_project
from datacoolie.project.config import discover_project, load_project_config
from datacoolie.project.documents import canonical_json, decode_file, encode_document, format_for_path
from datacoolie.project.errors import (
    ProjectConfigError,
    ProjectDependencyError,
    ProjectError,
    ProjectValidationError,
)
from datacoolie.project.inspection import (
    inspect_artifact,
    inspect_capabilities,
    inspect_config,
    inspect_metadata,
    inspect_project,
)
from datacoolie.project.scaffold import init_project, update_agents
from datacoolie.project.validation.artifacts import validate_artifact
from datacoolie.project.validation.metadata import validate_metadata_document
from datacoolie.project.validation.project import validate_project
from datacoolie.project.overlays import resolve_environment
from .parser import CLIUsageError


def _config(args: Any):
    return load_project_config(Path(args.project_dir)) if getattr(args, "project_dir", None) else discover_project()


def _append_not_checked(details: dict[str, Any], additional: list[str]) -> None:
    """Add CLI scope exclusions without discarding service diagnostics.

    Validation services own the checks they could not perform (for example,
    model and query checks after a schema failure).  The CLI contributes only
    exclusions caused by selecting a standalone target.  Keep the service
    order, append the CLI entries, and retain the first occurrence so a
    future service can report an overlapping exclusion safely.
    """

    existing = details.get("not_checked")
    values = list(existing) if isinstance(existing, list) else []
    for item in additional:
        if item not in values:
            values.append(item)
    details["not_checked"] = values


def execute(args: Any) -> dict[str, Any]:
    command = args.command
    if command in {"init", "metadata"} and getattr(args, "project_dir", None):
        raise CLIUsageError(f"--project-dir is not supported for {command}")
    if (
        command == "inspect"
        and getattr(args, "inspect_command", None) == "capabilities"
        and getattr(args, "project_dir", None)
    ):
        raise CLIUsageError("--project-dir is not supported for inspect capabilities")
    if command == "init":
        seed = None
        if args.config:
            seed_config = Path(args.config).expanduser().resolve()
            if not seed_config.is_file():
                raise ProjectError(f"Configuration seed not found: {seed_config}")
            try:
                import yaml
                seed = yaml.safe_load(seed_config.read_text(encoding="utf-8"))
            except ImportError as exc:
                raise ProjectDependencyError(
                    "PyYAML is required for --config; install datacoolie[cli]",
                    dependency="PyYAML",
                    exit_code=2,
                ) from exc
            except Exception as exc:
                raise ProjectConfigError(f"Cannot read configuration seed: {seed_config}") from exc
            if not isinstance(seed, dict):
                raise ProjectConfigError("Configuration seed must be a YAML mapping")
        return init_project(args.path, name=args.name, environments=args.environments, config_seed=seed)
    if command == "validate":
        # Component path overrides are meaningful only when the caller has
        # selected a standalone metadata document.  Artifact validation gets
        # its roots from the environment descriptor, and project validation
        # gets them from datacoolie.yml; silently accepting these flags in
        # either mode would make a successful report misleading.
        if args.only and (args.metadata_path or args.artifact_path):
            raise CLIUsageError(
                "--only is available only for project validation; remove the "
                "standalone --metadata-path/--artifact-path"
            )
        if (args.sql_base_path or args.artifact_base_path) and not args.metadata_path:
            raise CLIUsageError(
                "--sql-base-path and --artifact-base-path require --metadata-path "
                "for standalone metadata validation"
            )
        if args.metadata_path:
            metadata_path = Path(args.metadata_path).expanduser().resolve()
            from datacoolie.project.documents import load_snapshot
            snapshot = load_snapshot(metadata_path)
            metadata = snapshot.merged()
            config = _config(args) if getattr(args, "project_dir", None) else None
            selected_env = args.environments[0] if args.environments and len(args.environments) == 1 else None
            if args.environments and len(args.environments) > 1:
                raise CLIUsageError("Standalone metadata validation accepts at most one --env")
            if selected_env:
                if config is None:
                    raise ProjectValidationError("--env for standalone metadata validation requires --project-dir")
                if selected_env not in config.environments:
                    raise ProjectValidationError(f"Unknown environment: {selected_env}")
                metadata, _ = resolve_environment(snapshot, selected_env)
            report = validate_metadata_document(
                metadata,
                scope="metadata",
                source=str(metadata_path),
                sql_root=(
                    tuple(Path(path).expanduser().resolve() for path in args.sql_base_path)
                    if args.sql_base_path
                    else (config.absolute_component_paths("sql") if config and config.sql else None)
                ),
                artifact_root=Path(args.artifact_base_path).expanduser().resolve() if args.artifact_base_path else None,
            )
            payload = report.to_dict()
            details = payload.setdefault("details", {})
            if not isinstance(details, dict):
                details = {}
                payload["details"] = details
            details["limited_scope"] = True
            _append_not_checked(
                details,
                [
                    "project-config",
                    "resource-existence",
                    "other-environments",
                ],
            )
            return payload
        if args.artifact_path:
            selected_env = args.environments[0] if args.environments and len(args.environments) == 1 else None
            if args.environments and len(args.environments) > 1:
                raise CLIUsageError("Artifact validation accepts at most one --env")
            return validate_artifact(args.artifact_path, environment=selected_env).to_dict()
        return validate_project(
            _config(args),
            environments=args.environments,
            only=set(args.only) if args.only else None,
        ).to_dict()
    if command == "inspect":
        sub = getattr(args, "inspect_command", None)
        if sub is None:
            return inspect_project(_config(args))
        if sub == "config":
            return inspect_config(_config(args), args.env)
        if sub == "metadata":
            if (args.name or args.stage or args.full) and not args.section:
                raise CLIUsageError("--name, --stage, and --full require --section")
            if args.name and args.section not in {"connections", "dataflows"}:
                raise CLIUsageError("--name is supported only for connections or dataflows")
            if args.stage and args.section != "dataflows":
                raise CLIUsageError("--stage is supported only for dataflows")
            config = _config(args) if (args.metadata_path is None or getattr(args, "project_dir", None)) else None
            if args.env and config is None:
                raise ProjectValidationError("--env for metadata inspection requires --project-dir")
            if args.env and config is not None and args.env not in config.environments:
                raise ProjectValidationError(f"Unknown environment: {args.env}")
            return inspect_metadata(
                config,
                metadata_path=args.metadata_path,
                environment=args.env,
                section=args.section,
                name=args.name,
                stage=args.stage,
                full=args.full,
            )
        if sub == "capabilities":
            return inspect_capabilities()
        if sub == "artifact":
            target = args.artifact_path
            if target is None:
                target = str(_config(args).project_dir / ".builds" / "current")
            return inspect_artifact(target)
        raise ProjectError(f"Unknown inspect operation: {sub}")
    if command == "build":
        return build_project(
            _config(args),
            metadata_layout=args.metadata_layout,
            metadata_format=args.metadata_format,
            dry_run=args.dry_run,
        )
    if command == "metadata":
        if args.metadata_command != "convert":
            raise ProjectError(f"Unknown metadata operation: {args.metadata_command}")
        source = Path(args.input).expanduser().resolve()
        target = Path(args.output).expanduser().resolve()
        if not source.is_file():
            raise ProjectError(f"Metadata input not found: {source}")
        if target.exists() and not args.overwrite:
            raise ProjectError(f"Output exists; pass --overwrite to replace: {target}")
        document = decode_file(source)
        output_format = args.to or _format_from_output(target)
        if args.to:
            expected_suffix = {"json": ".json", "yaml": ".yml", "excel": ".xlsx"}[args.to]
            accepted_suffixes = {expected_suffix}
            if args.to == "yaml":
                accepted_suffixes.add(".yaml")
            if target.suffix.lower() not in accepted_suffixes:
                raise ProjectError(
                    f"Output extension {target.suffix!r} does not match --to {args.to!r}"
                )
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.stem}.tmp-{uuid.uuid4().hex}{target.suffix}")
        try:
            temporary.write_bytes(encode_document(document, output_format))
            # Decode the produced representation to prove semantic round-trip
            # before replacing a pre-existing output.
            decoded = decode_file(temporary)
            if canonical_json(decoded) != canonical_json(document):
                raise ProjectValidationError("Metadata conversion changed semantic content")
            os.replace(temporary, target)
        except Exception:
            try:
                temporary.unlink()
            except OSError:
                pass
            raise
        return {
            "status": "converted",
            "input": str(source),
            "output": str(target),
            "format": output_format,
        }
    if command == "agents":
        if args.agents_command != "update":
            raise ProjectError(f"Unknown agents operation: {args.agents_command}")
        return update_agents(_config(args).project_dir)
    raise ProjectError(f"Unknown command: {command}")


def _format_from_output(path: Path) -> str:
    try:
        return format_for_path(path)
    except ProjectValidationError as exc:
        raise ProjectError("Output extension is unknown; pass --to json|yaml|excel") from exc


__all__ = ["execute"]
