"""Contract checks for the canonical public examples library."""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

import pytest

from docs.scripts._examples import (
    discover_project_roots,
    iter_public_files,
    project_root_for,
)

ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"
EXAMPLES = DOCS / "examples"
FILES = EXAMPLES / "files"

def test_examples_browsing_pages_are_present() -> None:
    expected = {
        "index.md",
        "runners.md",
        "configuration.md",
        "dataflows.md",
        "operations.md",
    }
    assert {path.name for path in EXAMPLES.glob("*.md")} >= expected


def test_catalog_owns_all_standalone_example_sources() -> None:
    """The catalog is the source of selection metadata, not a duplicate registry."""
    catalog = (EXAMPLES / "index.md").read_text(encoding="utf-8")
    raw_targets = set(re.findall(r"\]\(files/([^)#]+)\)", catalog))
    project_roots = discover_project_roots(FILES)
    standalone = [
        path.relative_to(FILES).as_posix()
        for path in iter_public_files(FILES, project_roots=project_roots)
        if project_root_for(path, project_roots) is None
    ]
    missing = [path for path in standalone if path not in raw_targets]
    assert not missing, f"Standalone sources missing from examples catalog: {missing}"
    dangling = [path for path in raw_targets if not (FILES / path).is_file()]
    assert not dangling, f"Catalog raw targets do not exist: {dangling}"


def test_catalog_uses_one_stable_action_vocabulary() -> None:
    catalog = (EXAMPLES / "index.md").read_text(encoding="utf-8")
    for label in ("guide", "source", "raw", "project-files", "download"):
        assert label in catalog
    assert "vocabulary" in catalog.lower() and "anchor" in catalog.lower()
    assert "not a project archive" in catalog
    assert "Standalone files do not" in catalog
    assert "catalog.json" not in catalog
    assert "source-view" not in catalog
    assert "download-zip" not in catalog


def test_canonical_sources_are_real_files_without_template_tokens() -> None:
    tokens = re.compile(r"\{\{|\{metadata_path\}|\{watermark_base_path\}|\{log_base_path\}|\{project_name\}")
    for path in sorted(FILES.rglob("*")):
        if not path.is_file() or path.suffix not in {".py", ".ipynb", ".json", ".sql", ".yml"}:
            continue
        content = path.read_text(encoding="utf-8")
        assert not tokens.search(content), f"Unresolved template token in {path.relative_to(ROOT)}"


def test_python_and_json_sources_parse() -> None:
    for path in sorted(FILES.rglob("*.py")):
        compile(path.read_text(encoding="utf-8"), str(path), "exec")
    for path in sorted(FILES.rglob("*.json")):
        json.loads(path.read_text(encoding="utf-8"))
    for path in sorted(FILES.rglob("*.ipynb")):
        notebook = json.loads(path.read_text(encoding="utf-8"))
        assert notebook.get("nbformat", 0) >= 4
        assert isinstance(notebook.get("cells"), list)


def test_cloud_runner_sources_keep_host_and_path_boundaries_explicit() -> None:
    """Cloud runners must construct their adapter and avoid local path defaults."""
    aws = (FILES / "runners" / "aws" / "run_polars_s3.py").read_text(
        encoding="utf-8"
    )
    fabric = json.loads(
        (FILES / "runners" / "fabric" / "run_polars.ipynb").read_text(
            encoding="utf-8"
        )
    )
    fabric_source = "\n".join(
        "".join(cell.get("source", []))
        for cell in fabric["cells"]
        if cell.get("cell_type") == "code"
    )
    databricks = (FILES / "runners" / "databricks" / "run_polars_sdk.py").read_text(
        encoding="utf-8"
    )
    fabric_external = (
        FILES / "runners" / "fabric" / "run_polars_azure_sdk.py"
    ).read_text(encoding="utf-8")
    glue = (FILES / "runners" / "aws" / "run_glue_spark.py").read_text(
        encoding="utf-8"
    )
    databricks_notebooks = []
    for notebook_name in (
        "run_spark.ipynb",
        "replay_spark.ipynb",
        "maintenance_spark.ipynb",
    ):
        notebook = json.loads(
            (FILES / "runners" / "databricks" / notebook_name).read_text(
                encoding="utf-8"
            )
        )
        databricks_notebooks.append(
            "\n".join(
                "".join(cell.get("source", []))
                for cell in notebook["cells"]
                if cell.get("cell_type") == "code"
            )
        )

    assert "AWSPlatform" in aws
    assert "bucket" in aws and "region" in aws
    assert "s3://" in aws
    assert "json_object" in aws and "must be an object" in aws
    assert "require_s3_uri" in aws
    assert "artifact_base_path=args.artifact_base_path" in aws
    assert "sql_base_path=args.sql_base_path" in aws
    assert "FabricPlatform(runtime=\"fabric\")" in fabric_source
    assert "PolarsEngine(platform=platform)" in fabric_source
    assert "Files/" in fabric_source
    assert "require_volume_path" in databricks
    assert "/Volumes/" in databricks
    assert "require_azure_path" in fabric_external
    assert "abfss" in fabric_external and "https" in fabric_external
    assert "require_s3_path" in glue
    for source in (aws, fabric_source, databricks, fabric_external, glue):
        assert "{metadata_path}" not in source
        assert "{{ functions_import_prefix }}" not in source
    for source in databricks_notebooks:
        assert "/Volumes/" in source
        assert "dbfs:/FileStore" not in source


def test_project_download_contract_is_documented() -> None:
    index = (EXAMPLES / "index.md").read_text(encoding="utf-8")
    assert "[source]" in index and "downloads/artifact.zip" in index
    assert "downloads/transform.zip" in index
    assert "#artifact-project" in index
    assert "#function-project" in index
    assert "#incremental-project" in index
    assert "#transform-project" in index
    for project in ("artifact", "function", "incremental", "transform"):
        assert f"[project-files](#{project}-project)" in index
        assert f"[download](downloads/{project}.zip)" in index
        assert f"[source](source/projects/{project}/" in index
        assert f"[raw](files/projects/{project}/" in index


def test_generated_project_archives_remain_independent_of_project_pages() -> None:
    generator = (DOCS / "scripts" / "gen_examples.py").read_text(encoding="utf-8")
    assert "_write_project_archives" in generator
    assert "_project_archive" in generator
    assert "_write_project_tree_views" not in generator
    assert "View repository tree" not in generator
    assert "_iter_archive_files" in generator


def test_generated_source_pages_use_the_public_source_raw_vocabulary() -> None:
    generator = (DOCS / "scripts" / "gen_examples.py").read_text(encoding="utf-8")
    assert "generated `source`" in generator
    assert "The `raw` `.ipynb` file" in generator
    assert "source-view" not in generator
    assert "download-zip" not in generator


def test_example_code_viewer_is_accessible_and_no_js_safe() -> None:
    config = (ROOT / "properdocs.yml").read_text(encoding="utf-8")
    stylesheet = (DOCS / "assets" / "stylesheets" / "example-code.css").read_text(
        encoding="utf-8"
    )
    script = (DOCS / "assets" / "javascripts" / "example-code.js").read_text(
        encoding="utf-8"
    )
    assert "assets/stylesheets/example-code.css" in config
    assert "assets/javascripts/example-code.js" in config
    assert "max-height: 24rem" in stylesheet
    assert "overflow: auto" in stylesheet
    assert "@media print" in stylesheet
    assert "aria-expanded" in script and "aria-controls" in script
    assert "Expand code" in script and "Collapse code" in script
    assert "isSourceViewPage" in script
    assert "if (isSourceViewPage())" in script
    assert "document$" in script
    assert "fetch(" not in script


def test_generated_source_views_do_not_fabricate_remote_revisions() -> None:
    """A dirty local preview must not claim that uncommitted files are public."""
    generator = (DOCS / "scripts" / "gen_examples.py").read_text(encoding="utf-8")
    assert "DATACOOLIE_DOCS_REVISION" in generator
    assert '"git",\n                "status"' in generator
    assert "unpublished local checkout" in generator
    assert "if revision" in generator


def test_external_runner_guards_reject_launcher_local_paths() -> None:
    """Cloud examples fail before a local checkout can be used as input."""
    cases = (
        (FILES / "runners/aws/run_polars_s3.py", "require_s3_uri"),
        (
            FILES / "runners/fabric/run_polars_azure_sdk.py",
            "require_azure_path",
        ),
        (
            FILES / "runners/databricks/run_polars_sdk.py",
            "require_volume_path",
        ),
    )
    for path, function_name in cases:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        function = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == function_name
        )
        namespace = {"argparse": argparse, "urlsplit": urlsplit}
        code = compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec")
        exec(code, namespace)
        with pytest.raises(argparse.ArgumentTypeError):
            namespace[function_name]("metadata.json", "--metadata-path")
