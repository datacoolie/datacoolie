"""Regression tests for generated example project and notebook projections."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re

import pytest

from docs.scripts._examples import (
    discover_project_roots,
    iter_public_files,
    parse_reference_sections,
    ReferenceRow,
    validate_reference_sections,
)


ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"
EXAMPLES = DOCS / "examples"
FILES = EXAMPLES / "files"


def _load_notebook_projection():
    """Load the pure notebook helpers without running the docs generator."""
    generator = DOCS / "scripts" / "gen_examples.py"
    namespace = {"Path": Path, "json": json, "re": re}
    # The generator has site-writing side effects, so execute only the
    # notebook helper definitions extracted from its AST.
    import ast

    source = generator.read_text(encoding="utf-8")
    parsed = ast.parse(source, filename=str(generator))
    names = {
        "_NOTEBOOK_LANGUAGES",
        "_cell_text",
        "_notebook_language",
        "_markdown_fence",
        "_notebook_projection",
    }
    nodes = [
        node
        for node in parsed.body
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id in names
                for target in node.targets
            )
        )
        or (isinstance(node, ast.FunctionDef) and node.name in names)
    ]
    module = ast.Module(body=nodes, type_ignores=[])
    exec(compile(module, str(generator), "exec"), namespace)
    return namespace["_notebook_projection"]


def _load_examples_index_hook():
    hook_path = DOCS / "scripts" / "inject_examples_index.py"
    spec = importlib.util.spec_from_file_location("inject_examples_index", hook_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_examples_reference_sections_are_complete_and_canonical() -> None:
    index = (EXAMPLES / "index.md").read_text(encoding="utf-8")
    properdocs = (ROOT / "properdocs.yml").read_text(encoding="utf-8")
    sections = parse_reference_sections(index)
    validate_reference_sections(sections, FILES)
    assert set(sections) >= {
        "projects",
        "projects/artifact",
        "projects/function",
        "projects/incremental",
        "projects/transform",
        "configuration",
        "dataflows",
        "runners",
        "operations",
        "plugins",
    }
    assert "| File/Folder | Required | Description | Links |" not in index
    assert "docs/scripts/inject_examples_index.py" in properdocs

    tree = _load_examples_index_hook().render_examples_tree("projects")
    assert tree.startswith(chr(96) * 3 + "text\nprojects/\n")
    for entry in (
        "artifact/",
        "function/",
        "incremental/",
        "transform/",
    ):
        assert entry in tree
    assert "__pycache__" not in tree


def test_inventory_excludes_runtime_noise_but_keeps_input_sources() -> None:
    roots = discover_project_roots(FILES)
    relative_files = {
        path.relative_to(FILES).as_posix()
        for path in iter_public_files(FILES, project_roots=roots)
    }
    assert "projects/artifact/data/input/orders.csv" in relative_files
    assert "projects/artifact/data/output/orders/orders.parquet" not in relative_files
    assert not any("__pycache__" in path or path.endswith(".pyc") for path in relative_files)


def test_reference_validation_rejects_incomplete_sections() -> None:
    rows = {
        "configuration": (
            ReferenceRow(
                path="logging_modes.py",
                description="one row is not a complete section",
                links="—",
            ),
        )
    }
    with pytest.raises(ValueError, match="omits eligible paths"):
        validate_reference_sections(rows, FILES)


def test_nested_project_exclusions_are_project_relative(tmp_path: Path) -> None:
    library = tmp_path / "files"
    project = library / "projects" / "nested"
    (project / "data" / "input").mkdir(parents=True)
    (project / "data" / "output").mkdir(parents=True)
    (project / "dist").mkdir()
    (project / "__pycache__").mkdir()
    (project / "datacoolie.yml").write_text("name: nested\n", encoding="utf-8")
    (project / "data" / "input" / "orders.csv").write_text("id\n1\n", encoding="utf-8")
    (project / "data" / "output" / "generated.parquet").write_bytes(b"generated")
    (project / "dist" / "package.whl").write_bytes(b"generated")
    (project / "__pycache__" / "module.pyc").write_bytes(b"generated")
    standalone = library / "standalone" / "data" / "output"
    standalone.mkdir(parents=True)
    (standalone / "documented.csv").write_text("id\n1\n", encoding="utf-8")

    roots = discover_project_roots(library)
    public = {
        path.relative_to(library).as_posix()
        for path in iter_public_files(library, project_roots=roots)
    }
    assert "projects/nested/data/input/orders.csv" in public
    assert "standalone/data/output/documented.csv" in public
    assert "projects/nested/data/output/generated.parquet" not in public
    assert "projects/nested/dist/package.whl" not in public
    assert not any("__pycache__" in path for path in public)


def test_notebook_projection_preserves_cells_without_synthetic_headings(tmp_path: Path) -> None:
    project = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"language_info": {"name": "sql"}},
        "cells": [
            {"cell_type": "markdown", "source": ["# Intro\n", "\n", "**rich**\n"]},
            {
                "cell_type": "code",
                "source": ["SELECT " + chr(96) * 3 + " FROM orders;\n"],
                "execution_count": 7,
                "outputs": [{"text": ["SENSITIVE_OUTPUT"]}],
            },
            {"cell_type": "raw", "source": ["raw " + chr(96) + "text" + chr(96) + "\n"]},
            {"cell_type": "code", "source": []},
        ],
    }
    path = tmp_path / "sample.ipynb"
    path.write_text(json.dumps(project), encoding="utf-8")

    projection = _load_notebook_projection()(path)

    assert "Markdown cell" not in projection
    assert "Code cell" not in projection
    assert projection.count('<a id="notebook-cell-') == 4
    assert "# Intro\n\n**rich**" in projection
    assert chr(96) * 4 + "sql" in projection
    assert "SELECT " + chr(96) * 3 + " FROM orders;" in projection
    assert chr(96) * 3 + "text" in projection
    assert "raw " + chr(96) + "text" + chr(96) in projection
    assert "execution_count" not in projection
    assert "SENSITIVE_OUTPUT" not in projection
