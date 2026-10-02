from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from packaging.version import Version


BUILD_SKILL = Path(__file__).resolve().parents[2] / "datacoolie-build"
PRODUCT_ROOT = BUILD_SKILL.parents[2]

METADATA_DOC_ROUTES = {
    "https://datacoolie.github.io/datacoolie/guide/metadata/": "docs/guide/metadata/index.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/first-metadata-file/": "docs/guide/metadata/first-metadata-file.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/connections/": "docs/guide/metadata/connections.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/": "docs/guide/metadata/dataflows.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/": "docs/guide/metadata/source-patterns.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/transform-patterns/": "docs/guide/metadata/transform-patterns.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/destination-and-load-patterns/": "docs/guide/metadata/destination-and-load-patterns.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/data-types/": "docs/guide/metadata/data-types.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/api-advanced/": "docs/guide/metadata/api-advanced.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/watermark-window-replacement/": "docs/guide/metadata/watermark-window-replacement.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/late-arriving-files/": "docs/guide/metadata/late-arriving-files.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/stable-keys-and-protected-output/": "docs/guide/metadata/stable-keys-and-protected-output.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/merge-and-scd2/": "docs/guide/metadata/merge-and-scd2.md",
    "https://datacoolie.github.io/datacoolie/guide/metadata/validation-checklist/": "docs/guide/metadata/validation-checklist.md",
}
METADATA_SCHEMA_ANCHORS = (
    "https://datacoolie.github.io/datacoolie/reference/metadata-schema/#connection",
    "https://datacoolie.github.io/datacoolie/reference/metadata-schema/#dataflow",
    "https://datacoolie.github.io/datacoolie/reference/metadata-schema/#source",
    "https://datacoolie.github.io/datacoolie/reference/metadata-schema/#transform",
    "https://datacoolie.github.io/datacoolie/reference/metadata-schema/#destination",
    "https://datacoolie.github.io/datacoolie/reference/metadata-schema/#schema-hint",
    "https://datacoolie.github.io/datacoolie/reference/metadata-schema/#shared-schema-hint",
)


def _cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "datacoolie", "--format", "json", *args],
        cwd=PRODUCT_ROOT,
        env={**os.environ, "PYTHONPATH": str(PRODUCT_ROOT / "src")},
        check=False,
        capture_output=True,
        text=True,
    )


def test_build_skill_owns_guidance_and_cli_owns_validation() -> None:
    content = (BUILD_SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "datacoolie.yml" in content
    assert "dc validate" in content
    assert "source.query" in content
    assert "config.yaml" not in content
    assert "through its validated `build.json`" not in content
    assert "scripts/materialize.py" not in content


def test_metadata_authoring_routes_to_public_docs() -> None:
    skill = (BUILD_SKILL / "SKILL.md").read_text(encoding="utf-8")
    quick_reference = (BUILD_SKILL / "references/schema-quick-reference.md").read_text(
        encoding="utf-8"
    )
    combined = f"{skill}\n{quick_reference}"

    assert "Read the public [Metadata Guide]" in skill
    assert "it is not a second schema" in quick_reference.lower()
    assert "https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document" in combined
    assert "https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json" in combined
    assert "not fetched from the public site" in quick_reference

    for url, relative_path in METADATA_DOC_ROUTES.items():
        assert url in combined, url
        assert (PRODUCT_ROOT / relative_path).is_file(), relative_path

    for url in METADATA_SCHEMA_ANCHORS:
        assert url in quick_reference, url


def test_specialized_build_references_keep_public_routes() -> None:
    framework_boundary = (BUILD_SKILL / "references/framework-boundary.md").read_text(
        encoding="utf-8"
    )
    polars_sql = (BUILD_SKILL / "references/polars-qualified-sql.md").read_text(
        encoding="utf-8"
    )
    orchestration = (BUILD_SKILL / "references/orchestration-contract.md").read_text(
        encoding="utf-8"
    )
    operations = (BUILD_SKILL / "references/operations-contract.md").read_text(
        encoding="utf-8"
    )

    assert "https://datacoolie.github.io/datacoolie/guide/metadata/" in framework_boundary
    assert "https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/" in polars_sql
    assert "https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/" in orchestration
    assert "https://datacoolie.github.io/datacoolie/guide/operations/replay-and-backfill/" in operations
    assert "https://datacoolie.github.io/datacoolie/guide/operations/maintenance/" in operations


def test_old_duplicate_project_helpers_are_removed() -> None:
    for relative in (
        "scripts/materialize.py",
        "scripts/merge.py",
        "scripts/validate.py",
        "scripts/validate_config.py",
        "scripts/validate_build.py",
        "scripts/validate_functions.py",
        "scripts/convert.py",
        "scripts/_loaders.py",
        "scripts/_schema_resolver.py",
        "scripts/requirements.txt",
        "scripts/lint.py",
        "scripts/inspect_capabilities.py",
    ):
        assert not (BUILD_SKILL / relative).exists(), relative


def test_capabilities_are_exposed_by_installed_cli() -> None:
    result = _cli("inspect", "capabilities")
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert "registrations" in payload["data"]


def test_metadata_schema_is_project_owned_not_a_skill_copy() -> None:
    schema_root = PRODUCT_ROOT / "src" / "datacoolie" / "project" / "schemas"
    schema = max(
        schema_root.glob("*/metadata.schema.json"),
        key=lambda path: Version(path.parent.name),
    )
    assert schema.is_file()
    value = json.loads(schema.read_text(encoding="utf-8"))
    assert value["$id"].endswith(f"/schema/{schema.parent.name}/metadata.schema.json")
    assert not list((BUILD_SKILL / "schemas").rglob("metadata.schema.json"))
    assert not (BUILD_SKILL / "scripts/_schema_resolver.py").exists()
