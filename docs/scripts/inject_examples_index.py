"""ProperDocs hook for folder-oriented examples reference tables."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import re
import sys

try:
    from docs.scripts._examples import (
        FILES_DIR,
        ReferenceRow,
        discover_project_roots,
        normalise_reference_path,
        render_reference_table,
        section_root,
        split_table_row,
        validate_reference_sections,
        visible_children,
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
    FILES_DIR = helper.FILES_DIR
    ReferenceRow = helper.ReferenceRow
    discover_project_roots = helper.discover_project_roots
    normalise_reference_path = helper.normalise_reference_path
    render_reference_table = helper.render_reference_table
    section_root = helper.section_root
    split_table_row = helper.split_table_row
    validate_reference_sections = helper.validate_reference_sections
    visible_children = helper.visible_children


TREE_MARKER = re.compile(
    r"<!--\s*dc-generated-examples-tree:\s*([^>]+?)\s*-->"
)
SECTION_BLOCK = re.compile(
    r"(?P<start><!--\s*dc-examples-section:\s*([^>]+?)\s*-->)"
    r"(?P<body>.*?)"
    r"(?P<end><!--\s*/dc-examples-section\s*-->)",
    re.DOTALL,
)
TABLE_HEADER = "| File/Folder | Description | Links |"
TABLE_SEPARATOR = "|---|---|---|"


def _tree_lines(directory: Path, prefix: str, project_roots: tuple[Path, ...]) -> list[str]:
    lines: list[str] = []
    children = visible_children(directory, FILES_DIR, project_roots=project_roots)
    for number, path in enumerate(children):
        is_last = number == len(children) - 1
        branch = "└── " if is_last else "├── "
        label = f"{path.name}/" if path.is_dir() else path.name
        lines.append(f"{prefix}{branch}{label}")
        if path.is_dir():
            child_prefix = prefix + ("    " if is_last else "│   ")
            lines.extend(_tree_lines(path, child_prefix, project_roots))
    return lines


def render_examples_tree(section: str) -> str:
    """Return a deterministic Markdown tree for one canonical folder."""

    root = section_root(section, FILES_DIR)
    relative = root.relative_to(FILES_DIR).as_posix()
    project_roots = discover_project_roots(FILES_DIR)
    lines = [chr(96) * 3 + "text", f"{relative}/"]
    lines.extend(_tree_lines(root, "", project_roots))
    lines.append(chr(96) * 3)
    return "\n".join(lines)


def _render_table_in_body(body: str, rows: tuple[ReferenceRow, ...]) -> str:
    if not rows:
        return body
    lines = body.splitlines()
    for number, line in enumerate(lines):
        if line.strip() != TABLE_HEADER:
            continue
        end = number + 1
        if end < len(lines) and lines[end].strip() == TABLE_SEPARATOR:
            end += 1
        while end < len(lines) and lines[end].lstrip().startswith("|"):
            end += 1
        replacement = render_reference_table(rows).splitlines()
        lines[number:end] = replacement
        break
    else:
        raise ValueError("Examples section has rows but no reference table header")
    return "\n".join(lines)


def _render_section(match: re.Match[str]) -> str:
    section = match.group(2).strip().strip("/")
    body = match.group("body")
    sections = {
        section: tuple(_rows_from_body(body)),
    }
    validate_reference_sections(sections, FILES_DIR)
    body = TREE_MARKER.sub(
        lambda tree_match: render_examples_tree(tree_match.group(1).strip()),
        body,
    )
    body = _render_table_in_body(body, sections[section])
    if body and not body.endswith("\n"):
        body += "\n"
    return f"{match.group('start')}{body}{match.group('end')}"


def _rows_from_body(body: str) -> list[ReferenceRow]:
    lines = body.splitlines()
    rows: list[ReferenceRow] = []
    table_started = False
    for line in lines:
        if line.strip() == TABLE_HEADER:
            table_started = True
            continue
        if table_started and line.strip() == TABLE_SEPARATOR:
            continue
        if table_started and line.lstrip().startswith("|"):
            cells = split_table_row(line)
            if len(cells) != 3:
                raise ValueError(f"Invalid examples reference row: {line}")
            path = normalise_reference_path(cells[0])
            rows.append(ReferenceRow(path=path, description=cells[1], links=cells[2]))
            continue
        if table_started and line.strip() and not line.lstrip().startswith("|"):
            table_started = False
    return rows


def on_page_markdown(markdown: str, page, config, files) -> str:  # noqa: ANN001
    """Render marked folder trees/tables while keeping index Markdown readable."""

    if "<!-- dc-examples-section:" not in markdown:
        return markdown
    return SECTION_BLOCK.sub(_render_section, markdown)
