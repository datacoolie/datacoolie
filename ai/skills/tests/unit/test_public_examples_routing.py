"""Static contract for Skills consumption of public example sources."""

from __future__ import annotations

from pathlib import Path


SKILL = Path(__file__).resolve().parents[2] / "datacoolie-build"
PRODUCT_ROOT = SKILL.parents[2]

def test_public_examples_reference_is_catalog_first_without_registry_copy() -> None:
    reference = (SKILL / "references" / "public-examples.md").read_text(
        encoding="utf-8"
    )
    assert "https://datacoolie.github.io/datacoolie/examples/" in reference
    assert "https://datacoolie.github.io/datacoolie/guide/cli/project/" in reference
    assert "`guide`" in reference and "`source`" in reference
    assert "`project-files`" in reference and "`download`" in reference
    assert "`source-view`" not in reference and "`download-zip`" not in reference
    assert "Do not invent a" in reference
    assert "Do not silently fall" in reference and "back to `main`" in reference
    assert "catalog.json" not in reference
    assert "templates/runners/" not in reference


def test_build_skill_routes_public_examples_without_network_runtime_dependency() -> None:
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "Public runner/source examples" in skill
    assert "references/public-examples.md" in skill
    assert "never become runtime dependencies" in skill
    legacy_tree = SKILL / "templates" / "runners"
    if legacy_tree.exists():
        assert not any(
            path.is_file() and path.suffix in {".py", ".ipynb", ".example"}
            for path in legacy_tree.rglob("*")
        )


def test_docs_catalog_owns_runner_inventory() -> None:
    examples_root = PRODUCT_ROOT / "docs" / "examples"
    catalog = (examples_root / "index.md").read_text(encoding="utf-8")
    assert "## Runners" in catalog
    assert "#runners" in catalog
    assert "[source]" in catalog and "raw" in catalog
    assert "source-view" not in catalog and "download-zip" not in catalog
    runner_paths = sorted(
        path.relative_to(examples_root / "files").as_posix()
        for path in (examples_root / "files" / "runners").rglob("*")
        if path.is_file() and path.suffix in {".py", ".ipynb"}
    )
    missing = [path for path in runner_paths if f"files/{path})" not in catalog]
    assert not missing, f"Runner sources missing from docs catalog: {missing}"
