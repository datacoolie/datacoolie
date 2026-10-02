"""Project scaffolding and canonical AGENTS.md maintenance."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import urllib.request
import uuid
from typing import Any, Mapping

from .config import CONFIG_FILENAME, project_config_from_mapping
from .errors import ProjectConfigError, ProjectDependencyError, ProjectError


AGENTS_URL = "https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md"


def _latest_agents(*, timeout: float = 20.0) -> str:
    request = urllib.request.Request(
        AGENTS_URL,
        headers={"User-Agent": "datacoolie-cli/0.1"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except Exception as exc:
        raise ProjectError(f"Cannot download latest AGENTS.md from {AGENTS_URL}") from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProjectError("Downloaded AGENTS.md is not valid UTF-8") from exc
    if not text.strip() or len(text) > 2_000_000:
        raise ProjectError("Downloaded AGENTS.md is empty or unexpectedly large")
    return text if text.endswith("\n") else text + "\n"


def _default_config(name: str, environments: list[str] | None = None) -> dict[str, Any]:
    selected = environments or ["dev"]
    return {
        "schema_version": 1,
        "project": {"name": name},
        "components": {
            "metadata": {
                "path": "metadata",
                "output": {"layout": "single", "format": "json"},
            },
            "sql": {"path": "sql"},
            "functions": {"path": "functions", "packaging": "auto"},
        },
        "environments": {environment: {"platform": "local"} for environment in selected},
    }


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8", newline="\n")


def init_project(
    target: Path | str,
    *,
    name: str | None = None,
    environments: list[str] | None = None,
    config_seed: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create the agreed empty project structure and fetch latest guidance."""

    project_dir = Path(target).expanduser().resolve()
    created_target = False
    if project_dir.exists():
        if not project_dir.is_dir():
            raise ProjectError(f"Init target is not a directory: {project_dir}")
        if any(project_dir.iterdir()):
            raise ProjectError(f"Init target is not empty: {project_dir}")
    else:
        project_dir.mkdir(parents=True, exist_ok=False)
        created_target = True
    created: list[Path] = []
    try:
        if config_seed is not None and not isinstance(config_seed, Mapping):
            raise ProjectConfigError("Configuration seed must be a mapping")
        config_data = dict(config_seed) if config_seed is not None else _default_config(
            name or project_dir.name,
            environments,
        )
        if name is not None and config_seed is not None:
            configured_project = config_data.get("project")
            if configured_project is not None and not isinstance(configured_project, Mapping):
                raise ProjectConfigError("project in --config must be a mapping")
            configured_name = (
                configured_project.get("name")
                if isinstance(configured_project, Mapping)
                else None
            )
            if configured_name not in (None, name):
                raise ProjectConfigError("--name conflicts with project.name in --config")
            config_data["project"] = {
                **(dict(configured_project) if isinstance(configured_project, Mapping) else {}),
                "name": name,
            }
        if environments and config_seed is not None:
            configured_envs = config_data.get("environments")
            if isinstance(configured_envs, Mapping) and set(configured_envs) != set(environments):
                raise ProjectConfigError("--env conflicts with environments in --config")
        parsed = project_config_from_mapping(config_data, source_path=project_dir / CONFIG_FILENAME)
        agents_text = _latest_agents()
        config_text = _dump_yaml(parsed.to_dict())
        files: dict[Path, str] = {
            project_dir / CONFIG_FILENAME: config_text,
            project_dir / "AGENTS.md": agents_text,
            project_dir / "README.md": _readme(parsed.project_name),
            project_dir / ".gitignore": _gitignore(),
            project_dir / "metadata" / "connections.json": '{"connections": []}\n',
            project_dir / "metadata" / "schema_hints.json": '{"schema_hints": []}\n',
        }
        # Component paths are configurable in a seed.  Metadata's two marker
        # files remain convenient defaults, while non-default roots are still
        # created and validated consistently.
        metadata_root = project_dir.joinpath(*parsed.metadata.path.split("/"))
        files.pop(project_dir / "metadata" / "connections.json", None)
        files.pop(project_dir / "metadata" / "schema_hints.json", None)
        files[metadata_root / "connections.json"] = '{"connections": []}\n'
        files[metadata_root / "schema_hints.json"] = '{"schema_hints": []}\n'
        for path, text in files.items():
            if path.exists():
                raise ProjectError(f"Refusing to overwrite existing init file: {path}")
            _write_text(path, text)
            created.append(path)
        component_roots = [parsed.metadata.path]
        for entries in (parsed.sql, parsed.functions):
            if entries is not None:
                component_roots.extend(entry.path for entry in entries)
        for root in component_roots:
            component_root = project_dir.joinpath(*root.split("/"))
            component_root.mkdir(parents=True, exist_ok=True)
            marker = component_root / ".gitkeep"
            if not marker.exists():
                marker.write_text("", encoding="utf-8")
                created.append(marker)
        metadata_root.joinpath("dataflows").mkdir(parents=True, exist_ok=True)
        metadata_root.joinpath("environments").mkdir(parents=True, exist_ok=True)
        for folder in (metadata_root / "dataflows", metadata_root / "environments"):
            marker = folder / ".gitkeep"
            marker.write_text("", encoding="utf-8")
            created.append(marker)
        # Runners are project tooling, not a configurable component.  Keep one
        # empty environment directory per configured environment so the
        # build projection has an unambiguous place for future scripts.
        for environment in parsed.environments:
            runner_root = project_dir / "runners" / environment
            runner_root.mkdir(parents=True, exist_ok=True)
            marker = runner_root / ".gitkeep"
            marker.write_text("", encoding="utf-8")
            created.append(marker)
    except Exception:
        # The target was empty before init.  Remove only paths created by this
        # invocation so a failed network/filesystem operation is recoverable.
        for path in sorted(created, key=lambda item: len(item.parts), reverse=True):
            try:
                if path.is_file():
                    path.unlink()
            except OSError:
                pass
        for path in sorted({item.parent for item in created}, key=lambda item: len(item.parts), reverse=True):
            try:
                if path.is_dir() and not any(path.iterdir()):
                    path.rmdir()
            except OSError:
                pass
        if created_target:
            try:
                if project_dir.is_dir() and not any(project_dir.iterdir()):
                    project_dir.rmdir()
            except OSError:
                pass
        raise
    return {
        "project_dir": str(project_dir),
        "project": parsed.project_name,
        "environments": list(parsed.environments),
        "agents_url": AGENTS_URL,
        "created": [str(path.relative_to(project_dir).as_posix()) for path in created],
    }


def update_agents(project_dir: Path | str, *, timeout: float = 20.0) -> dict[str, Any]:
    root = Path(project_dir).expanduser().resolve()
    if not root.is_dir():
        raise ProjectError(f"Project directory not found: {root}")
    target = root / "AGENTS.md"
    if target.is_symlink():
        raise ProjectError(f"Refusing to replace symlinked guidance file: {target}")
    latest = _latest_agents(timeout=timeout)
    if target.is_file():
        try:
            current = target.read_text(encoding="utf-8")
        except OSError as exc:
            raise ProjectError(f"Cannot read {target}") from exc
        if current == latest:
            return {"status": "unchanged", "path": str(target), "source": AGENTS_URL}
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = target.with_name(f"AGENTS.md.bak-{stamp}-{uuid.uuid4().hex[:8]}")
        temporary = target.with_name(f".{target.name}.tmp-{uuid.uuid4().hex}")
        try:
            shutil.copy2(target, backup)
            temporary.write_text(latest, encoding="utf-8", newline="\n")
            os.replace(temporary, target)
        except OSError as exc:
            try:
                temporary.unlink()
            except OSError:
                pass
            try:
                backup.unlink()
            except OSError:
                pass
            raise ProjectError(f"Cannot replace {target}") from exc
        return {"status": "updated", "path": str(target), "backup": str(backup), "source": AGENTS_URL}
    temporary = root / f".{target.name}.tmp-{uuid.uuid4().hex}"
    try:
        temporary.write_text(latest, encoding="utf-8", newline="\n")
        os.replace(temporary, target)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise ProjectError(f"Cannot write {target}") from exc
    return {"status": "created", "path": str(target), "source": AGENTS_URL}


def _dump_yaml(value: Mapping[str, Any]) -> str:
    try:
        import yaml  # noqa: WPS433 - optional CLI dependency
    except ImportError as exc:
        raise ProjectDependencyError(
            "PyYAML is required for project initialization; install datacoolie[cli]",
            dependency="PyYAML",
            exit_code=2,
        ) from exc
    return yaml.safe_dump(dict(value), sort_keys=False, allow_unicode=True)


def _gitignore() -> str:
    return """# DataCoolie generated and runtime state
.builds/
.runtime/
.releases/
__pycache__/
*.py[cod]
"""


def _readme(name: str) -> str:
    return f"""# {name}\n\nThis project is managed by DataCoolie.\n\n- `dc validate` checks the authored metadata and resources.\n- `dc build` creates an immutable multi-environment artifact.\n- Put environment-specific scripts or notebooks under `runners/<env>/`; build copies them without executing them.\n- Execute the resulting dataflow with a project-owned Python script or notebook.\n"""


__all__ = ["AGENTS_URL", "init_project", "update_agents"]
