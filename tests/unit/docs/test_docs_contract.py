"""Focused checks for the public docs contract and downloadable examples."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import importlib.util
import re


ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"


def _parser_tree(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    """Return parser nodes by command path without duplicating the CLI map."""

    result: dict[str, argparse.ArgumentParser] = {}

    def walk(node: argparse.ArgumentParser, path: tuple[str, ...]) -> None:
        result[" ".join(path)] = node
        for action in node._actions:
            if not isinstance(action, argparse._SubParsersAction):
                continue
            for name, child in action.choices.items():
                walk(child, (*path, name))

    walk(parser, ())
    return result


def _heading_block(markdown: str, heading: str) -> str:
    lines = markdown.splitlines()
    try:
        start = next(index for index, line in enumerate(lines) if line.strip() == heading)
    except StopIteration as exc:
        raise AssertionError(f"Missing documentation heading: {heading}") from exc
    level = len(heading) - len(heading.lstrip("#"))
    end = len(lines)
    for index in range(start + 1, len(lines)):
        match = re.match(r"^(#+)\s", lines[index])
        if match and len(match.group(1)) <= level:
            end = index
            break
    return "\n".join(lines[start:end])


def _doc_owner(path: str) -> tuple[str, str]:
    if not path:
        return "index", "## Shared options"
    if path == "inspect":
        return "commands", "## `dc inspect`"
    if path.startswith("inspect "):
        return "commands", f"### `{path}`"
    if path == "metadata" or path.startswith("metadata "):
        return "commands", "## `dc metadata convert`"
    if path == "agents" or path.startswith("agents "):
        return "commands", "## `dc agents update`"
    return "commands", f"## `dc {path}`"


def _load_llms_generator():
    path = DOCS / "scripts" / "gen_llms.py"
    spec = importlib.util.spec_from_file_location("datacoolie_gen_llms", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_public_landing_pages_and_runtime_reference_are_present() -> None:
    """The docs-first navigation has one landing page per public concern."""
    expected = {
        DOCS / "introduction" / "index.md",
        DOCS / "guide" / "index.md",
        DOCS / "guide" / "getting-started" / "index.md",
        DOCS / "guide" / "providers" / "index.md",
        DOCS / "guide" / "operations" / "index.md",
        DOCS / "guide" / "platforms" / "index.md",
        DOCS / "guide" / "operations" / "runtime-configuration.md",
        DOCS / "examples" / "index.md",
        DOCS / "examples" / "runners.md",
        DOCS / "examples" / "configuration.md",
        DOCS / "examples" / "dataflows.md",
        DOCS / "examples" / "operations.md",
        DOCS / "studio" / "index.md",
        DOCS / "guide" / "cli" / "project.md",
        DOCS / "guide" / "cli" / "quickstart.md",
        DOCS / "extensions" / "index.md",
        DOCS / "project" / "index.md",
        DOCS / "scripts" / "gen_runtime_configuration.py",
    }
    missing = [str(path.relative_to(ROOT)) for path in expected if not path.is_file()]
    assert not missing, f"Missing public docs contract files: {missing}"


def test_local_artifact_runner_is_syntax_valid() -> None:
    """The canonical runner template remains importable Python syntax."""
    runner = (
        DOCS
        / "examples"
        / "files"
        / "runners"
        / "local"
        / "run_artifact_minimal.py"
    )
    compile(runner.read_text(encoding="utf-8"), str(runner), "exec")


def test_homepage_uses_framework_positioning_and_explicit_scale_contract() -> None:
    """Homepage claims match the accepted multi-engine/job-scale positioning."""
    content = (DOCS / "index.md").read_text(encoding="utf-8")
    lowered = content.lower()
    assert "multi-engine" in lowered and "multi-platform" in lowered
    assert "data pipeline framework" in lowered
    assert "group_number % job_num" in lowered
    assert "deterministic" in lowered and "random" in lowered


def test_obsolete_top_level_doc_paths_are_removed() -> None:
    """The URL migration must not leave parallel old navigation trees."""
    obsolete = (
        DOCS / "getting-started",
        DOCS / "how-to",
        DOCS / "operations",
        DOCS / "concepts",
        DOCS / "extending",
        DOCS / "adr",
        DOCS / "tutorials",
    )
    leftovers = [str(path.relative_to(ROOT)) for path in obsolete if any(path.rglob("*.md"))]
    assert not leftovers, f"Obsolete public doc trees still contain pages: {leftovers}"


def test_guide_topic_structure_has_one_root_page_and_section_indexes() -> None:
    """Keep the physical guide layout topic-oriented and unambiguous."""
    guide = DOCS / "guide"
    root_pages = sorted(path.name for path in guide.iterdir() if path.is_file())
    assert root_pages == ["index.md"]

    expected_sections = {
        "getting-started",
        "metadata",
        "providers",
        "cli",
        "operations",
        "platforms",
    }
    actual_sections = {
        path.name for path in guide.iterdir() if path.is_dir()
    }
    assert actual_sections == expected_sections
    for section in expected_sections:
        assert (guide / section / "index.md").is_file(), section


def test_guide_nav_and_legacy_redirects_use_final_routes() -> None:
    """Navigation and published legacy routes must target authored final pages."""
    config = (ROOT / "properdocs.yml").read_text(encoding="utf-8")
    required_nav_paths = (
        "guide/getting-started/index.md",
        "guide/metadata/index.md",
        "guide/providers/index.md",
        "guide/cli/index.md",
        "guide/cli/quickstart.md",
        "guide/operations/index.md",
        "guide/platforms/index.md",
        "guide/getting-started/multi-stage-dataflow.md",
        "guide/operations/run-stage.md",
    )
    for path in required_nav_paths:
        assert path in config

    required_metadata_groups = (
        "          - Configure metadata:\n",
        "              - Connections: guide/metadata/connections.md\n",
        "              - Dataflows:\n",
        "                  - Overview: guide/metadata/dataflows.md\n",
        "                  - Source: guide/metadata/source-patterns.md\n",
        "                  - Transform: guide/metadata/transform-patterns.md\n",
        "                  - Destination: guide/metadata/destination-and-load-patterns.md\n",
        "              - Datatypes and schema hints: guide/metadata/data-types.md\n",
        "              - Replace a complete watermark window: guide/metadata/watermark-window-replacement.md\n",
        "              - Incremental API with pagination: guide/metadata/api-advanced.md\n",
        "              - Late-arriving and updated files: guide/metadata/late-arriving-files.md\n",
        "              - Stable keys and protected output: guide/metadata/stable-keys-and-protected-output.md\n",
        "              - SCD2 with incremental inputs: guide/metadata/merge-and-scd2.md\n",
    )
    for group in required_metadata_groups:
        assert group in config

    obsolete_metadata_groups = (
        "          - Start here:\n",
        "          - Authoring workflow:\n",
        "          - Shared configuration:\n",
        "          - Validation:\n",
    )
    for group in obsolete_metadata_groups:
        assert group not in config

    required_redirects = (
        "getting-started/first-dataflow.md: guide/getting-started/multi-stage-dataflow.md",
        "how-to/configure-file-metadata.md: guide/providers/file.md",
        "how-to/run-a-stage.md: guide/operations/run-stage.md",
        "how-to/maintenance-vacuum-optimize.md: guide/operations/maintenance.md",
    )
    for mapping in required_redirects:
        assert mapping in config


def test_metadata_guides_link_to_contextual_reference_sections() -> None:
    """Keep metadata-guide references focused on the relevant schema section."""
    expected = {
        "guide/metadata/index.md": "#metadata-document",
        "guide/metadata/connections.md": "#connection",
        "guide/metadata/dataflows.md": "#dataflow",
        "guide/metadata/source-patterns.md": "#source",
        "guide/metadata/data-types.md": "#schema-hint",
        "guide/metadata/api-advanced.md": "#source",
        "guide/metadata/transform-patterns.md": "#transform",
        "guide/metadata/watermark-window-replacement.md": "#destination",
        "guide/providers/file.md": "#metadata-document",
        "guide/providers/api.md": "#metadata-document",
    }
    for relative_path, fragment in expected.items():
        content = (DOCS / relative_path).read_text(encoding="utf-8")
        assert f"metadata-schema.md{fragment}" in content, relative_path


def test_api_guides_do_not_misstate_offset_or_range_contract() -> None:
    source = (DOCS / "guide/metadata/source-patterns.md").read_text(encoding="utf-8")
    advanced = (DOCS / "guide/metadata/api-advanced.md").read_text(encoding="utf-8")

    assert '"watermark_param_mapping": {"updated_at": "updated_since"}' in source
    assert '"offset_param": "skip"' in source
    assert '"offset_param": "page"' not in source
    assert "record offsets" in source and "page" in source
    assert "rate_limit_delay` only delays **sequential**" in source
    for token in (
        "backward_hours",
        "backward_months",
        "backward_years",
        "backward_closing_day",
        "replace_by_watermark",
    ):
        assert token in source
    assert '"watermark_param_mapping": {"updated_at": "updated_since"}' in advanced


def test_metadata_docs_use_the_generated_latest_schema_alias() -> None:
    identity = (DOCS / "guide/metadata/index.md").read_text(
        encoding="utf-8"
    )
    reference = (DOCS / "reference/index.md").read_text(encoding="utf-8")
    latest_url = "https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json"

    assert latest_url in identity
    assert "latest` schema" in reference


def test_cross_section_references_link_to_contextual_reference_sections() -> None:
    """Keep cross-section links focused on the concept being discussed."""
    expected = {
        "guide/providers/file.md": ("metadata-providers.md#file-provider",),
        "guide/providers/database.md": ("metadata-providers.md#database-provider",),
        "guide/providers/api.md": ("metadata-providers.md#api-provider",),
        "guide/metadata/source-patterns.md": (
            "secrets.md#secrets_ref-schema",
            "watermarks.md#storage-ownership-and-path-binding",
            "metadata-schema.md#dataflowssourceconfigure",
            "metadata-schema.md#destination",
        ),
        "guide/metadata/validation-checklist.md": ("secrets.md#secrets_ref-schema",),
        "guide/metadata/index.md": (
            "cli/project.md#environment-overlays",
        ),
        "guide/metadata/merge-and-scd2.md": ("metadata-schema.md#destination",),
        "guide/getting-started/multi-stage-dataflow.md": (
            "watermarks.md#serialisation-format",
            "orchestration.md#driver",
            "transformers-and-pipeline.md#the-twelve-built-ins-order-responsibility",
            "load-strategies.md#choosing-a-strategy",
            "watermarks.md#storage-ownership-and-path-binding",
        ),
        "guide/getting-started/quickstart-polars.md": ("metadata-model.md#top-level",),
        "examples/dataflows.md": ("engines.md#qualified-sql-relations-in-polars",),
        "examples/operations.md": ("logging.md#configuration",),
        "guide/operations/run-stage.md": ("orchestration.md#job-distribution",),
        "guide/operations/runtime-configuration.md": (
            "logging.md#run-attributes",
            "watermarks.md#replay-watermark-behaviour",
        ),
        "guide/operations/replay-and-backfill.md": (
            "watermarks.md#replay-watermark-behaviour",
            "orchestration.md#replay-backfill",
        ),
        "introduction/choose-framework.md": ("metadata-model.md#top-level",),
        "introduction/ai-skills.md": ("metadata-model.md#mental-model",),
        "studio/index.md": ("metadata-model.md#mental-model",),
        "reference/api/logging.md": ("runtime-configuration.md#run-configuration",),
        "reference/concepts/metadata-model.md": (
            "runtime-configuration.md#run-configuration",
            "runtime-configuration.md#replay-configuration",
            "runtime-configuration.md#logging-configuration",
        ),
    }
    for relative_path, targets in expected.items():
        content = (DOCS / relative_path).read_text(encoding="utf-8")
        for target in targets:
            assert target in content, f"{target} missing from {relative_path}"


def test_transform_patterns_have_discoverable_pipeline_order() -> None:
    """Keep transformer configuration sections visible in execution order."""
    content = (DOCS / "guide/metadata/transform-patterns.md").read_text(encoding="utf-8")
    headings = (
        "## ColumnValueTransformer",
        "## SchemaConverter",
        "## HashColumnAdder",
        "## Deduplicator",
        "## ColumnAdder",
        "## RowFilter",
        "## SCD2ColumnAdder",
        "## SystemColumnAdder",
        "## PartitionHandler",
        "## DataMasker",
        "## ColumnProjector",
        "## ColumnNameSanitizer",
    )
    positions = [content.index(heading) for heading in headings]
    assert positions == sorted(positions)
    assert "## Transform at a glance" not in content


def test_metadata_configure_and_advanced_guides_cover_cases_and_parse_snippets() -> None:
    """Keep feature owners and combined cases discoverable and copyable."""
    required = {
        "guide/metadata/index.md": (
            "$schema",
            "extensions",
            "environment-overlays",
        ),
        "guide/metadata/connections.md": (
            "oauth2_client_credentials",
            "aws_sigv4",
        ),
        "guide/metadata/api-advanced.md": (
            "watermark_param_mapping", "max_pages", "merge_upsert",
        ),
        "guide/metadata/watermark-window-replacement.md": (
            "merge_overwrite", "replace_by_watermark", "not guaranteed atomic",
        ),
        "guide/metadata/late-arriving-files.md": (
            "date_folder_partitions", "__file_modification_time", "backward_days",
        ),
        "guide/metadata/stable-keys-and-protected-output.md": (
            "hash_columns", "deduplicate_columns", "masking_rules",
        ),
        "guide/metadata/merge-and-scd2.md": (
            "scd2_effective_column", "strictly", "partition_columns",
        ),
    }
    for relative_path, tokens in required.items():
        content = (DOCS / relative_path).read_text(encoding="utf-8")
        for token in tokens:
            assert token in content, f"{token} missing from {relative_path}"
        blocks = re.findall(r"```json\n(.*?)\n```", content, flags=re.DOTALL)
        assert blocks, relative_path
        for block in blocks:
            json.loads(block)


def test_llms_full_selection_is_generated_from_canonical_pages() -> None:
    generator = _load_llms_generator()
    content = generator.build_llms_full()
    assert "https://datacoolie.github.io/datacoolie/guide/cli/project/" in content
    assert "https://datacoolie.github.io/datacoolie/guide/cli/quickstart/" in content
    assert "https://datacoolie.github.io/datacoolie/reference/" in content
    assert "datacoolie.github.io/datacoolie/guide/cli/project.md" not in content
    assert "## https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/" in content
    for route in (
        "api-advanced",
        "watermark-window-replacement",
        "late-arriving-files",
        "stable-keys-and-protected-output",
    ):
        assert (
            f"## https://datacoolie.github.io/datacoolie/guide/metadata/{route}/"
            in content
        )


def test_public_docs_define_ownership_versions_and_release_handoff() -> None:
    reference = (DOCS / "reference" / "index.md").read_text(encoding="utf-8")
    project = (DOCS / "guide" / "cli" / "project.md").read_text(encoding="utf-8")
    assert "## Ownership and version scopes" in reference
    assert "greatest bundled" in reference
    assert "runtime metadata Providers hydrate" in reference
    assert "## Release handoff (external upload workflow)" in project
    assert "There is intentionally no `dc release` command" in project
    assert project.index("<deployment_path>/artifacts/<build_id>/") < project.index(
        "<deployment_path>/current/"
    )
    assert "do not delete files that" in project


def test_cli_docs_cover_parser_options_in_their_owning_sections() -> None:
    """Keep command docs aligned with the parser without freezing prose."""
    from datacoolie.cli.parser import create_parser

    index = (DOCS / "guide" / "cli" / "index.md").read_text(encoding="utf-8")
    commands = (DOCS / "guide" / "cli" / "commands.md").read_text(encoding="utf-8")
    pages = {"index": index, "commands": commands}
    parsers = _parser_tree(create_parser())

    for path, parser in parsers.items():
        page_name, heading = _doc_owner(path)
        block = _heading_block(pages[page_name], heading)
        options = {
            option
            for action in parser._actions
            for option in action.option_strings
            if option.startswith("--") and option not in {"--help"}
        }
        for option in options:
            assert option in block, f"{option} for `dc {path}` is not documented in {heading}"


def test_cli_project_examples_parse_and_resolve(tmp_path) -> None:
    """Exercise the documented multi-root and overlay examples against services."""
    project = (DOCS / "guide" / "cli" / "project.md").read_text(encoding="utf-8")

    import yaml

    yaml_blocks = re.findall(r"```yaml\n(.*?)\n```", project, flags=re.DOTALL)
    multi_root = next(block for block in yaml_blocks if "sql/orders" in block)
    config = yaml.safe_load(multi_root)
    assert [entry["path"] for entry in config["components"]["sql"]] == [
        "sql/orders",
        "sql/shared",
    ]
    assert config["components"]["functions"][1]["packaging"] == "zip"

    overlay_block = re.search(r"```json\n(.*?)\n```", project, flags=re.DOTALL)
    assert overlay_block
    overlay = json.loads(overlay_block.group(1))
    root = tmp_path / "metadata"
    (root / "environments").mkdir(parents=True)
    (root / "environments" / "prod.json").write_text(
        json.dumps(overlay), encoding="utf-8"
    )

    from datacoolie.project.documents import MetadataSnapshot
    from datacoolie.project.overlays import resolve_environment

    snapshot = MetadataSnapshot(
        root=root,
        sections={
            "connections": [
                {
                    "name": "warehouse",
                    "connection_type": "database",
                    "format": "sql",
                    "configure": {
                        "database_type": "postgresql",
                        "host": "warehouse.example",
                        "database": "orders",
                    },
                }
            ],
            "dataflows": [
                {
                    "name": "orders",
                    "stage": "bronze2silver",
                    "source": {"connection_name": "warehouse", "query": "SELECT 1"},
                    "destination": {"connection_name": "warehouse", "table": "orders"},
                }
            ],
            "schema_hints": [],
        },
    )
    effective, path = resolve_environment(snapshot, "prod")
    assert path == root / "environments" / "prod.json"
    assert effective["connections"][0]["configure"]["database"] == "orders_prod"
    assert effective["dataflows"][0]["source"]["filter_expression"] == "is_current = true"
    assert effective["dataflows"][-1]["name"] == "new-report"

    from datacoolie.project.validation.metadata import validate_metadata_document

    report = validate_metadata_document(effective, scope="metadata")
    assert report.ok, report.to_dict()


def test_llms_full_extracts_rendered_reference_without_site_chrome(tmp_path, monkeypatch) -> None:
    generator = _load_llms_generator()
    generated = tmp_path / "reference" / "generated"
    generated.mkdir(parents=True)
    (generated / "index.html").write_text(
        "<html><nav>ignore nav</nav><main><h1>Generated contract</h1>"
        "<p>Read <a href=\"../../guide/cli/\">the CLI</a>.</p>"
        "<pre><code>dc validate</code></pre></main><footer>ignore footer</footer></html>",
        encoding="utf-8",
    )
    monkeypatch.setattr(generator, "GENERATED_HTML", ("reference/generated/index.html",))
    content = generator.build_llms_full(site_dir=tmp_path)
    assert "ignore nav" not in content and "ignore footer" not in content
    assert "# Generated contract" in content
    assert "https://datacoolie.github.io/datacoolie/guide/cli/" in content
    assert "```\ndc validate\n```" in content
