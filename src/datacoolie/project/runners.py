"""Discovery and delivery of project-owned environment runners.

Runners are project tooling rather than framework metadata.  Their contract is
small and deliberately filesystem-oriented: ``runners/<environment>/`` is
copied byte-for-byte into the matching environment artifact.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Iterable

from .errors import ProjectError


RUNNERS_DIRNAME = "runners"
_IGNORED_NAMES = frozenset({".gitkeep", "__pycache__"})
_IGNORED_SUFFIXES = frozenset({".pyc", ".pyo"})


@dataclass(frozen=True)
class RunnerFile:
    """One source runner file relative to its environment directory."""

    environment: str
    relative_path: str
    source_path: Path


@dataclass(frozen=True)
class RunnerLayout:
    """The validated (or diagnosable) runner tree for one project."""

    root: Path
    environments: tuple[str, ...]
    environment_directories: frozenset[str]
    files_by_environment: dict[str, tuple[RunnerFile, ...]]
    problems: tuple[str, ...]

    @property
    def exists(self) -> bool:
        return self.root.exists() or self.root.is_symlink()

    def files_for(self, environment: str) -> tuple[RunnerFile, ...]:
        return self.files_by_environment.get(environment, ())

    def record_for(self, environment: str) -> dict[str, object] | None:
        """Return the environment-manifest descriptor, when its folder exists."""

        if environment not in self.environment_directories:
            return None
        return {
            "path": RUNNERS_DIRNAME,
            "files": [
                (Path(RUNNERS_DIRNAME) / item.relative_path).as_posix()
                for item in self.files_for(environment)
            ],
        }

    def require_valid(self) -> "RunnerLayout":
        if self.problems:
            details = "; ".join(self.problems[:5])
            if len(self.problems) > 5:
                details += f"; and {len(self.problems) - 5} more"
            raise ProjectError(f"Invalid runners layout: {details}")
        return self


def _is_ignored(path: Path) -> bool:
    return path.name in _IGNORED_NAMES or path.suffix.casefold() in _IGNORED_SUFFIXES


def _problem(problems: list[str], message: str, path: Path) -> None:
    problems.append(f"{message}: {path}")


def discover_runner_layout(
    project_dir: Path | str,
    environments: Iterable[str],
) -> RunnerLayout:
    """Discover runners without executing or interpreting their contents."""

    root = Path(project_dir).expanduser().resolve() / RUNNERS_DIRNAME
    configured = tuple(sorted(environments, key=lambda item: (item.casefold(), item)))
    files: dict[str, list[RunnerFile]] = {environment: [] for environment in configured}
    environment_directories: set[str] = set()
    problems: list[str] = []

    if root.is_symlink():
        _problem(problems, "Runner root must not be a symlink", root)
    elif not root.exists():
        return RunnerLayout(root, configured, frozenset(), {key: () for key in files}, ())
    elif not root.is_dir():
        _problem(problems, "Runner root must be a directory", root)
    else:
        configured_folded = {environment.casefold(): environment for environment in configured}

        def walk(environment: str, directory: Path, environment_root: Path) -> None:
            try:
                children = sorted(directory.iterdir(), key=lambda item: (item.name.casefold(), item.name))
            except OSError as exc:
                _problem(problems, f"Cannot read runner directory ({exc})", directory)
                return
            for child in children:
                if child.is_symlink():
                    _problem(problems, "Runner files must not be symlinks", child)
                    continue
                if child.name in _IGNORED_NAMES:
                    continue
                if child.is_dir():
                    walk(environment, child, environment_root)
                    continue
                if not child.is_file():
                    _problem(problems, "Runner entry must be a regular file", child)
                    continue
                if _is_ignored(child):
                    continue
                files[environment].append(
                    RunnerFile(
                        environment,
                        child.relative_to(environment_root).as_posix(),
                        child,
                    )
                )

        try:
            children = sorted(root.iterdir(), key=lambda item: (item.name.casefold(), item.name))
        except OSError as exc:
            _problem(problems, f"Cannot read runner root ({exc})", root)
            children = ()
        for child in children:
            if child.is_symlink():
                _problem(problems, "Runner entries must not be symlinks", child)
                continue
            if child.name in _IGNORED_NAMES:
                continue
            if child.is_file():
                if not _is_ignored(child):
                    _problem(problems, "Runner file is not assigned to an environment", child)
                continue
            if not child.is_dir():
                _problem(problems, "Runner entry must be a regular directory", child)
                continue
            environment = child.name
            exact = environment in configured
            if not exact:
                folded = configured_folded.get(environment.casefold())
                message = (
                    f"Runner environment name must exactly match {folded!r}"
                    if folded is not None
                    else "Runner environment is not configured"
                )
                _problem(problems, message, child)
                continue
            environment_directories.add(environment)
            walk(environment, child, child)

    normalized_files = {
        environment: tuple(
            sorted(
                entries,
                key=lambda item: (item.relative_path.casefold(), item.relative_path),
            )
        )
        for environment, entries in files.items()
    }
    return RunnerLayout(
        root,
        configured,
        frozenset(environment_directories),
        normalized_files,
        tuple(problems),
    )


def copy_environment_runners(
    layout: RunnerLayout,
    environment: str,
    environment_root: Path,
) -> dict[str, object] | None:
    """Copy one environment's runner tree and return its manifest record."""

    record = layout.record_for(environment)
    if record is None:
        return None
    destination = environment_root / RUNNERS_DIRNAME
    destination.mkdir(parents=True, exist_ok=True)
    for item in layout.files_for(environment):
        if item.source_path.is_symlink() or not item.source_path.is_file():
            raise ProjectError(f"Runner source changed during build: {item.source_path}")
        target = destination / item.relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item.source_path, target)
    return record


__all__ = [
    "RUNNERS_DIRNAME",
    "RunnerFile",
    "RunnerLayout",
    "copy_environment_runners",
    "discover_runner_layout",
]
