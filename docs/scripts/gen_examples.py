"""Publish canonical example views and deterministic project archives.

The editable source remains under ``docs/examples/files``.  This generator only
creates web-facing Markdown projections and download archives during the docs
build, so a source file is never maintained a second time in the prose pages.
"""

from __future__ import annotations

from io import BytesIO
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import zipfile

import mkdocs_gen_files

try:
    from docs.scripts._examples import (
        DOCS_DIR,
        FILES_DIR,
        discover_project_roots,
        is_excluded,
        iter_public_files,
    )
except ModuleNotFoundError as error:
    if error.name != "docs":
        raise
    helper_path = Path(__file__).with_name("_examples.py")
    helper_spec = importlib.util.spec_from_file_location(
        "datacoolie_docs_examples_helpers", helper_path
    )
    if helper_spec is None or helper_spec.loader is None:
        raise ImportError(f"Cannot load examples helper: {helper_path}") from error
    helper = importlib.util.module_from_spec(helper_spec)
    sys.modules[helper_spec.name] = helper
    helper_spec.loader.exec_module(helper)
    DOCS_DIR = helper.DOCS_DIR
    FILES_DIR = helper.FILES_DIR
    discover_project_roots = helper.discover_project_roots
    is_excluded = helper.is_excluded
    iter_public_files = helper.iter_public_files
EXAMPLES_ROOT = "examples"

_LANGUAGES = {
    ".json": "json",
    ".csv": "csv",
    ".py": "python",
    ".ipynb": "json",
    ".sql": "sql",
    ".toml": "toml",
    ".yml": "yaml",
    ".yaml": "yaml",
    ".txt": "text",
}
_PROJECTED_SUFFIXES = frozenset(_LANGUAGES)


def _source_revision() -> str | None:
    """Resolve a published revision, or ``None`` for an unpublished checkout.

    A local docs build can contain uncommitted or untracked example files. In
    that case displaying the current ``HEAD`` as a remote source revision is
    misleading because the corresponding raw path may not exist yet.
    """
    try:
        status = subprocess.run(
            [
                "git",
                "status",
                "--porcelain",
                "--untracked-files=all",
                "--",
                "docs/examples/files",
            ],
            cwd=DOCS_DIR.parent,
            check=True,
            capture_output=True,
            text=True,
        )
        if status.stdout.strip():
            return None
        configured = os.environ.get("DATACOOLIE_DOCS_REVISION")
        if configured and configured.strip():
            return configured.strip()
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=DOCS_DIR.parent,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    revision = result.stdout.strip()
    return revision or None


def _cell_text(cell: dict[str, object]) -> str:
    source = cell.get("source", "")
    if isinstance(source, list):
        return "".join(str(line) for line in source)
    return str(source)


_NOTEBOOK_LANGUAGES = {
    "csharp": "csharp",
    "c#": "csharp",
    "java": "java",
    "javascript": "javascript",
    "julia": "julia",
    "ipython": "python",
    "python": "python",
    "py": "python",
    "r": "r",
    "ruby": "ruby",
    "bash": "bash",
    "sh": "bash",
    "scala": "scala",
    "shell": "bash",
    "sql": "sql",
    "typescript": "typescript",
    "powershell": "powershell",
    "ps1": "powershell",
}


def _notebook_language(document: dict[str, object]) -> str:
    """Return a safe Pygments language for a notebook's declared kernel."""
    metadata = document.get("metadata", {})
    if not isinstance(metadata, dict):
        return "text"
    language = None
    language_info = metadata.get("language_info")
    if isinstance(language_info, dict):
        language = language_info.get("name")
    kernelspec = metadata.get("kernelspec")
    if not language and isinstance(kernelspec, dict):
        language = kernelspec.get("language")
    normalized = str(language or "").strip().lower()
    return _NOTEBOOK_LANGUAGES.get(normalized, "text")


def _markdown_fence(source: str) -> str:
    """Choose a fence longer than any backtick run in a code cell."""
    longest = max((len(run) for run in re.findall(r"`+", source)), default=0)
    return "`" * max(3, longest + 1)


def _notebook_projection(path: Path) -> str:
    document = json.loads(path.read_text(encoding="utf-8"))
    cells = document.get("cells")
    if not isinstance(cells, list):
        raise ValueError(f"Notebook has no cells array: {path}")

    language = _notebook_language(document)
    sections = [
        "This page is a generated, non-executed projection of the notebook.",
        "The `raw` `.ipynb` file is the canonical notebook source;"
        " execution counts and outputs are intentionally omitted.",
    ]
    for number, cell in enumerate(cells, start=1):
        if not isinstance(cell, dict):
            continue
        cell_type = cell.get("cell_type")
        source = _cell_text(cell).rstrip()
        # Keep a stable target for agents and readers without adding synthetic
        # Markdown/Code headings that interrupt the notebook's natural flow.
        sections.append(f'<a id="notebook-cell-{number}"></a>')
        if cell_type == "markdown":
            if source:
                sections.append(source)
        elif cell_type == "code":
            fence = _markdown_fence(source)
            sections.extend([f"{fence}{language}", source, fence])
        elif cell_type == "raw":
            if source:
                fence = _markdown_fence(source)
                sections.extend([f"{fence}text", source, fence])
    return "\n\n".join(sections) + "\n"


def _source_projection(path: Path) -> str:
    if path.suffix == ".ipynb":
        return _notebook_projection(path)
    language = _LANGUAGES.get(path.suffix, "text")
    source = path.read_text(encoding="utf-8")
    fence = "````"
    return f"{fence}{language}\n{source.rstrip()}\n{fence}\n"


def _write_source_views() -> None:
    revision = _source_revision()
    project_roots = discover_project_roots(FILES_DIR)
    for path in iter_public_files(FILES_DIR, project_roots=project_roots):
        if path.suffix.lower() not in _PROJECTED_SUFFIXES:
            continue
        relative = path.relative_to(FILES_DIR).as_posix()
        projection = _source_projection(path)
        destination = f"{EXAMPLES_ROOT}/source/{relative}.md"
        # MkDocs Material uses directory URLs.  Walk from the generated source
        # page's directory back to ``examples/`` before linking the raw file.
        raw_link = f"{'../' * len(Path(relative).parts)}files/{relative}"
        title = f"Example source: {relative}"
        repository_link = (
            f" · [View repository source](https://github.com/datacoolie/datacoolie/blob/{revision}/docs/examples/files/{relative})"
            if revision
            else ""
        )
        revision_label = f"`{revision}`" if revision else "`unpublished local checkout`"
        content = (
            f"---\ntitle: {json.dumps(title)}\ndescription: {json.dumps(f'Generated source page for the canonical DataCoolie example {relative}.')}\n---\n\n"
            f"# {title}\n\n"
            f"Source revision: {revision_label}\n\n"
            "This page is the generated `source`; it shows the complete "
            "readable projection of the canonical file.\n\n"
            f"[raw]({raw_link}){repository_link}\n\n"
            f"{projection}"
        )
        with mkdocs_gen_files.open(destination, "w") as handle:
            handle.write(content)
        mkdocs_gen_files.set_edit_path(destination, f"examples/files/{relative}")


def _iter_archive_files(root: Path):
    project_roots = (root,)
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        if path.is_file() and not is_excluded(path, root, project_roots=project_roots):
            yield path


def _project_archive(root: Path) -> bytes:
    payload = BytesIO()
    with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in _iter_archive_files(root):
            relative = Path(root.name, path.relative_to(root)).as_posix()
            info = zipfile.ZipInfo(relative, date_time=(2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, path.read_bytes())
    return payload.getvalue()


def _write_project_archives() -> None:
    for root in discover_project_roots(FILES_DIR):
        destination = f"{EXAMPLES_ROOT}/downloads/{root.name}.zip"
        with mkdocs_gen_files.open(destination, "wb") as handle:
            handle.write(_project_archive(root))


_write_source_views()
_write_project_archives()
