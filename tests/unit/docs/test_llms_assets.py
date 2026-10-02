"""Retrieval contracts for file and archive links in standalone LLM content."""

from pathlib import Path
import re
from urllib.parse import unquote, urlsplit

import pytest

from docs.scripts._examples import discover_project_roots
from docs.scripts.gen_llms import PUBLIC_ROOT, _absolute_links, build_llms_full


DOCS = Path(__file__).resolve().parents[3] / "docs"


@pytest.mark.parametrize(
    ("source", "target", "expected"),
    [
        ("examples/index.md", "downloads/artifact.zip", "examples/downloads/artifact.zip"),
        ("examples/runners.md", "downloads/platform-smoke.zip", "examples/downloads/platform-smoke.zip"),
        ("examples/runners.md", "files/runners/local/run.py", "examples/files/runners/local/run.py"),
        (
            "guide/getting-started/quickstart-polars.md",
            "../../examples/downloads/getting-started.zip",
            "examples/downloads/getting-started.zip",
        ),
        (
            "guide/providers/database.md",
            "../../examples/files/configuration/provider_fixtures.py",
            "examples/files/configuration/provider_fixtures.py",
        ),
        (
            "examples/runners.md",
            "files/runners/databricks/run_spark.ipynb?download=1#cell",
            "examples/files/runners/databricks/run_spark.ipynb?download=1#cell",
        ),
        ("examples/runners.md", "index.md#projects", "examples/#projects"),
    ],
)
def test_standalone_links_use_authored_source_location(source, target, expected) -> None:
    assert _absolute_links(f"[resource]({target})", source) == f"[resource]({PUBLIC_ROOT}{expected})"


@pytest.mark.parametrize("target", ["#local", "https://example.com/file.zip", "mailto:help@example.com"])
def test_external_and_local_anchor_links_are_preserved(target) -> None:
    original = f"[resource]({target})"
    assert _absolute_links(original, "examples/runners.md") == original


def test_selected_examples_include_usage_and_retrievable_canonical_assets() -> None:
    content = build_llms_full()
    for page in ("configuration", "dataflows", "operations"):
        assert f"## {PUBLIC_ROOT}examples/{page}/" in content

    archives = {f"{root.name}.zip" for root in discover_project_roots()}
    targets = re.findall(r"\]\((https://datacoolie\.github\.io/datacoolie/[^)]+)\)", content)
    checked = set()
    for target in targets:
        relative = unquote(urlsplit(target).path).removeprefix("/datacoolie/")
        if "examples/" not in relative or not any(part in relative for part in ("/files/", "/downloads/")):
            continue
        if relative.startswith("examples/downloads/"):
            assert relative.removeprefix("examples/downloads/") in archives, target
        else:
            assert relative.startswith("examples/files/"), target
            assert (DOCS / relative).is_file(), target
        checked.add(relative)
    assert "examples/downloads/getting-started.zip" in checked
    assert "examples/files/runners/local/run.py" in checked
