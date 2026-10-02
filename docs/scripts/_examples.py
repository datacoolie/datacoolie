"""Shared inventory and reference helpers for the public examples docs.

The examples directory is an editable source library. This module contains
only pure path/table helpers so the page hook and generated source/project
views agree on which files are public and how their paths are interpreted.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable, Iterator, Sequence


DOCS_DIR = Path(__file__).resolve().parents[1]
FILES_DIR = DOCS_DIR / "examples" / "files"

SKIP_NAMES = frozenset({".DS_Store", "Thumbs.db", ".gitkeep"})
SKIP_SUFFIXES = frozenset({".pyc", ".pyo", ".pyd"})
SKIP_DIRS = frozenset({".git", ".runtime", ".builds", "__pycache__"})
SKIP_PROJECT_PREFIXES = (("data", "output"), ("dist",))

_SECTION_START = re.compile(
    r"^\s*<!--\s*dc-examples-section:\s*([^>]+?)\s*-->\s*$"
)
_SECTION_END = re.compile(r"^\s*<!--\s*/dc-examples-section\s*-->\s*$")
_TABLE_HEADER = "| File/Folder | Description | Links |"
_TABLE_SEPARATOR = "|---|---|---|"


@dataclass(frozen=True)
class ReferenceRow:
    """One authored row in an examples folder reference table."""

    path: str
    description: str
    links: str


def _relative_parts(path: Path, root: Path) -> tuple[str, ...]:
    return path.relative_to(root).parts


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def discover_project_roots(files_dir: Path = FILES_DIR) -> tuple[Path, ...]:
    """Find complete examples projects by their project contract.

    Looking for datacoolie.yml keeps the inventory independent from the
    display grouping. A nested contract under an already discovered project
    is treated as a project member rather than a second project.
    """

    candidates = sorted(
        {
            path.parent
            for path in files_dir.rglob("datacoolie.yml")
            if path.is_file() and not path.is_symlink()
        },
        key=lambda path: (len(path.relative_to(files_dir).parts), path.as_posix()),
    )
    roots: list[Path] = []
    for candidate in candidates:
        if any(_is_relative_to(candidate, root) for root in roots):
            continue
        roots.append(candidate)
    return tuple(sorted(roots, key=lambda path: path.relative_to(files_dir).as_posix()))


def project_root_for(
    path: Path,
    project_roots: Sequence[Path] | None = None,
) -> Path | None:
    """Return the deepest discovered project containing path."""

    roots = project_roots or discover_project_roots()
    containing = [root for root in roots if _is_relative_to(path, root)]
    if not containing:
        return None
    return max(containing, key=lambda root: len(root.parts))


def is_excluded(
    path: Path,
    library_root: Path = FILES_DIR,
    *,
    project_roots: Sequence[Path] | None = None,
) -> bool:
    """Return whether path is generated noise or an unsafe source node."""

    if path.is_symlink() or path.name in SKIP_NAMES:
        return True
    if path.suffix.lower() in SKIP_SUFFIXES:
        return True

    try:
        library_parts = _relative_parts(path, library_root)
    except ValueError:
        return True
    if any(part in SKIP_DIRS for part in library_parts):
        return True

    project_root = project_root_for(path, project_roots)
    if project_root is None:
        return False
    project_parts = _relative_parts(path, project_root)
    return any(project_parts[: len(prefix)] == prefix for prefix in SKIP_PROJECT_PREFIXES)


def visible_children(
    directory: Path,
    library_root: Path = FILES_DIR,
    *,
    project_roots: Sequence[Path] | None = None,
) -> list[Path]:
    """Return public children in directory-first deterministic order."""

    roots = project_roots or discover_project_roots(library_root)
    children = [
        path
        for path in directory.iterdir()
        if not is_excluded(path, library_root, project_roots=roots)
    ]
    return sorted(children, key=lambda path: (path.is_file(), path.name.casefold()))


def iter_public_files(
    library_root: Path = FILES_DIR,
    *,
    project_roots: Sequence[Path] | None = None,
) -> Iterator[Path]:
    """Yield public source files in stable library-relative order."""

    roots = project_roots or discover_project_roots(library_root)
    candidates = sorted(
        library_root.rglob("*"), key=lambda path: path.relative_to(library_root).as_posix()
    )
    for path in candidates:
        if path.is_file() and not is_excluded(path, library_root, project_roots=roots):
            yield path


def normalise_reference_path(value: str) -> str:
    """Normalize an authored table path, accepting already-rendered dashes."""

    path = value.strip().strip(chr(96)).strip()
    path = re.sub(r"^(?:[─-]{2,}\s*)+", "", path)
    path = path.replace("\\", "/").strip()
    if path in {"", "."}:
        return "."
    trailing_slash = path.endswith("/")
    normalized = "/".join(part for part in path.strip("/").split("/") if part)
    return normalized + ("/" if trailing_slash else "")


def reference_label(relative: str) -> str:
    """Render a section-relative path using the PBIR-style depth prefix."""

    normalized = normalise_reference_path(relative)
    if normalized == ".":
        return "."
    is_directory = normalized.endswith("/")
    parts = normalized.rstrip("/").split("/")
    prefix = "──" * (len(parts) - 1)
    label = parts[-1] + ("/" if is_directory else "")
    return f"{prefix} {label}" if prefix else label


def split_table_row(line: str) -> list[str]:
    """Split a simple examples table row while preserving cell content."""

    if not line.lstrip().startswith("|"):
        return []
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def parse_reference_sections(markdown: str) -> dict[str, tuple[ReferenceRow, ...]]:
    """Read bounded index tables keyed by their section marker."""

    sections: dict[str, list[ReferenceRow]] = {}
    current: str | None = None
    table_started = False
    table_seen = False
    for line in markdown.splitlines():
        start = _SECTION_START.match(line)
        if start:
            current = start.group(1).strip().strip("/")
            if not current:
                raise ValueError("Examples section marker must name a path")
            if current in sections:
                raise ValueError(f"Duplicate examples section: {current}")
            sections[current] = []
            table_started = False
            table_seen = False
            continue
        if _SECTION_END.match(line):
            if current is None:
                raise ValueError("Examples section end marker has no matching start")
            if not table_seen:
                raise ValueError(f"Examples section has no reference table: {current}")
            current = None
            table_started = False
            table_seen = False
            continue
        if current is None:
            continue
        if line.strip() == _TABLE_HEADER:
            table_started = True
            table_seen = True
            continue
        if table_started and line.strip() == _TABLE_SEPARATOR:
            continue
        if table_started and line.lstrip().startswith("|"):
            cells = split_table_row(line)
            if len(cells) != 3:
                raise ValueError(f"Invalid examples reference row: {line}")
            path = normalise_reference_path(cells[0])
            canonical = path.rstrip("/") or "."
            if not path or canonical in {
                row.path.rstrip("/") or "." for row in sections[current]
            }:
                raise ValueError(f"Duplicate or empty examples row in {current}: {line}")
            sections[current].append(
                ReferenceRow(path=path, description=cells[1], links=cells[2])
            )
            continue
        if table_started and line.strip() and not line.lstrip().startswith("|"):
            table_started = False
    if current is not None:
        raise ValueError(f"Unclosed examples section: {current}")
    return {key: tuple(value) for key, value in sections.items()}


def render_reference_table(rows: Iterable[ReferenceRow]) -> str:
    """Render rows with hierarchical labels for the published index."""

    materialized = tuple(rows)
    lines = [_TABLE_HEADER, _TABLE_SEPARATOR]
    lines.extend(
        f"| {chr(96)}{reference_label(row.path)}{chr(96)} | {row.description} | {row.links} |"
        for row in materialized
    )
    return "\n".join(lines)


def section_root(section: str, files_dir: Path = FILES_DIR) -> Path:
    """Resolve and validate a section marker as a path under files/."""

    relative = Path(*normalise_reference_path(section).split("/"))
    root = (files_dir / relative).resolve()
    base = files_dir.resolve()
    if root != base and not _is_relative_to(root, base):
        raise ValueError(f"Examples section escapes files root: {section}")
    if not root.is_dir():
        raise ValueError(f"Examples section directory does not exist: {section}")
    return root


def validate_reference_sections(
    sections: dict[str, tuple[ReferenceRow, ...]],
    files_dir: Path = FILES_DIR,
) -> None:
    """Ensure every authored reference row resolves to an included node."""

    roots = discover_project_roots(files_dir)
    for section, rows in sections.items():
        root = section_root(section, files_dir)
        seen: set[str] = set()
        for row in rows:
            normalized = normalise_reference_path(row.path)
            canonical = normalized.rstrip("/") or "."
            if canonical in seen:
                raise ValueError(f"Duplicate examples row in {section}: {normalized}")
            seen.add(canonical)
            target = root if canonical == "." else root / canonical
            if not target.exists() or is_excluded(target, files_dir, project_roots=roots):
                raise ValueError(f"Examples reference path does not exist: {section}/{normalized}")

        if section == "projects":
            expected_paths = {
                path.relative_to(root).as_posix()
                for path in visible_children(root, files_dir, project_roots=roots)
            }
        else:
            expected_paths = {
                path.relative_to(root).as_posix()
                for path in root.rglob("*")
                if not is_excluded(path, files_dir, project_roots=roots)
            }
        actual_paths = {path for path in seen if path != "."}
        missing = sorted(expected_paths - actual_paths)
        if missing:
            raise ValueError(
                f"Examples section omits eligible paths in {section}: {missing}"
            )
