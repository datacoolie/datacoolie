"""Contract tests for the generated authored metadata reference."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

from datacoolie.project.schema import LATEST_SCHEMA_URL, resolve_metadata_schema
from jsonschema import Draft202012Validator


SCRIPTS = Path(__file__).resolve().parents[3] / "docs" / "scripts"
sys.path.insert(0, str(SCRIPTS))

from _metadata_reference import _field_anchor, render_metadata_reference, verify_latest_reference  # noqa: E402


def _render() -> str:
    resolved = resolve_metadata_schema()
    return render_metadata_reference(
        resolved.document,
        latest_url=LATEST_SCHEMA_URL,
    )


@pytest.mark.parametrize(("name", "field", "invalid", "expected"), [
    ("ValueRule", "order", -1, "value >= `0`"),
    ("ValueRule", "columns", ["x", "x"], "items must be unique"),
    ("ValueRule", "columns", [], "must contain at least 1 item(s)"),
    ("ValueRule", "pattern", "x" * 4097, "must contain at most 4096 character(s)"),
    ("Connection", "name", "", "must contain at least 1 character(s)"),
])
def test_rendered_property_constraints_match_bundled_schema(name, field, invalid, expected) -> None:
    schema = resolve_metadata_schema().document
    property_schema = schema["$defs"][name]["properties"][field]
    assert list(Draft202012Validator(property_schema).iter_errors(invalid))
    content = _render()
    prefix = {"ValueRule": "valueRule", "Connection": "connections[]"}[name]
    row = next(line for line in content.splitlines() if line.startswith(f"| `{prefix}.{field}` |"))
    assert expected in row


def test_reference_is_generated_from_the_selected_authored_schema() -> None:
    content = _render()

    assert "# Metadata reference" in content
    assert f"Authoring schema: [latest]({LATEST_SCHEMA_URL})" in content
    descriptor = resolve_metadata_schema().descriptor
    assert descriptor.public_url not in content
    assert f"`{descriptor.version}`" not in content
    for path in ("connections[]", "dataflows[]", "schema_hints[]", "extensions"):
        assert f"`{path}`" in content
    for path in (
        "connections[].name",
        "connections[].configure",
        "dataflows[].source",
        "dataflows[].destination",
        "dataflows[].transform",
        "dataflows[].source.query",
        "dataflows[].destination.load_type",
        "dataflows[].transform.schema_hints",
        "schema_hints[].hints",
    ):
        assert f"`{path}`" in content


@pytest.mark.parametrize("field", ["version", "source_url", "sha256", "url"])
def test_reference_build_requires_matching_published_latest_alias(field: str) -> None:
    resolved = resolve_metadata_schema()
    latest = {
        "version": resolved.descriptor.version,
        "source_url": resolved.descriptor.public_url,
        "sha256": resolved.descriptor.sha256,
        "url": LATEST_SCHEMA_URL,
    }
    verify_latest_reference(
        latest, version=resolved.descriptor.version,
        public_url=resolved.descriptor.public_url,
        sha256=resolved.descriptor.sha256, latest_url=LATEST_SCHEMA_URL,
    )
    with pytest.raises(ValueError, match="published latest schema disagree"):
        verify_latest_reference(
            {**latest, field: "mismatch"}, version=resolved.descriptor.version,
            public_url=resolved.descriptor.public_url, sha256=resolved.descriptor.sha256,
            latest_url=LATEST_SCHEMA_URL,
        )


def test_reference_includes_conditional_and_open_map_boundaries() -> None:
    content = _render()

    for field in (
        "base_path",
        "base_url",
        "backward.days",
        "database_type",
        "replace_by_watermark",
        "scd2_effective_column",
        "missing_column_policy",
    ):
        assert field in content
    assert "object (open map)" in content
    assert "third-party options" in content
    assert "| `connections[]` | Connection | conditional" in content
    assert "| `extensions` | object (open map) | no" in content
    assert "dataflows[].source.connection_name` | string | conditional" in content
    assert "schema_hints[].connection_name` | string | conditional" in content
    assert "### Conditional rules" in content
    assert "exactly one of the schema alternatives" in content
    assert "Metadata document" in content
    assert "### Value rule" in content


def test_reference_separates_authored_fields_from_runtime_properties() -> None:
    content = _render()

    assert "`date_backward`" in content
    assert "are not authored fields" in content
    assert "dataflows[].source.date_backward" not in content
    assert "::: datacoolie." not in content


def test_api_reference_distinguishes_schema_enums_from_runtime_only_modes() -> None:
    content = _render()

    assert "An `enum` lists the exact values accepted by the selected schema" in content
    assert "`client_secret_post` (default) and `client_secret_basic`" in content
    assert "not page numbers" in content
    assert "without it no lower bound is sent" in content
    assert "does not throttle concurrent offset pages" in content


def test_reference_renders_every_defined_authored_field_family() -> None:
    schema = resolve_metadata_schema().document
    content = _render()
    prefixes = {
        "Connection": "connections[]",
        "DataFlow": "dataflows[]",
        "Source": "dataflows[].source",
        "Destination": "dataflows[].destination",
        "Transform": "dataflows[].transform",
        "SchemaHint": "schemaHint",
        "SharedSchemaHint": "schema_hints[]",
        "PartitionColumn": "partitionColumn",
        "AdditionalColumn": "additionalColumn",
        "ValueRule": "valueRule",
        "HashColumn": "hashColumn",
        "MaskingRule": "maskingRule",
    }

    for definition, prefix in prefixes.items():
        for field_name in schema["$defs"][definition]["properties"]:
            assert f"`{prefix}.{field_name}`" in content

    for definition in (
        "ConnectionConfigureFile",
        "ConnectionConfigureLakehouse",
        "ConnectionConfigureDatabase",
        "ConnectionConfigureApi",
    ):
        configure = schema["$defs"][definition]["then"]["properties"]["configure"]
        for field_name in configure["properties"]:
            assert f"`connections[].configure.{field_name}`" in content


def test_metadata_guide_mentions_all_schema_defined_field_names() -> None:
    """A schema field cannot become completely undiscoverable in the guide."""
    schema = resolve_metadata_schema().document
    guide_root = Path(__file__).resolve().parents[3] / "docs" / "guide" / "metadata"
    guide = "\n".join(path.read_text(encoding="utf-8") for path in guide_root.glob("*.md"))
    definitions = (
        "Connection", "DataFlow", "Source", "Destination", "Transform",
        "SchemaHint", "SharedSchemaHint", "PartitionColumn", "AdditionalColumn",
        "ValueRule", "HashColumn", "MaskingRule",
    )

    field_names = set()
    for definition in definitions:
        field_names.update(schema["$defs"][definition]["properties"])
    for definition in (
        "ConnectionConfigureFile", "ConnectionConfigureLakehouse",
        "ConnectionConfigureDatabase", "ConnectionConfigureApi",
    ):
        field_names.update(
            schema["$defs"][definition]["then"]["properties"]["configure"]["properties"]
        )

    missing = [name for name in sorted(field_names) if not re.search(rf"\b{re.escape(name)}\b", guide)]
    assert not missing, f"Metadata guide does not mention authored fields: {missing}"


def test_reference_routes_each_field_family_to_its_configuration_guide() -> None:
    content = _render()

    for route in (
        "connections.md", "dataflows.md", "source-patterns.md",
        "destination-and-load-patterns.md", "transform-patterns.md", "data-types.md",
    ):
        assert f"../guide/metadata/{route}" in content


def test_reference_orders_owners_and_keeps_legacy_section_names() -> None:
    content = _render()
    headings = [line for line in content.splitlines() if line.startswith("## ")]
    assert headings[:6] == [
        "## Reading this reference", "## Metadata document", "## Connection",
        "## Dataflow", "## Source", "## Transform",
    ]
    assert headings[6] == "## Destination"
    assert content.index("### Connection settings by endpoint type") < content.index("## Dataflow")
    assert content.index("### Shared schema hint") < content.index("## Connection")
    assert content.index("### Value rule") < content.index("## Destination")
    assert content.index("### Partition column") > content.index("## Destination")


def test_reference_has_unique_direct_field_anchors_and_nested_children() -> None:
    content = _render()
    ids = re.findall(r'<span id="([^"]+)"></span>', content)
    assert len(ids) == len(set(ids)), "Field anchors must be unique across owners and endpoints"
    for target in (
        "metadata-connections", "connection-name", "connection-configure-api-auth-type",
        "connection-configure-database-auth-type", "dataflow-is-active",
        "source-configure-pagination-type", "source-configure-backward-object-days",
        "transform-configure-missing-column-policy", "destination-configure-replace-by-watermark",
        "shared-schema-hint-hints", "value-rule-on-unmapped", "partition-column-column",
    ):
        assert target in ids
    assert _field_anchor("dataflows[].source.configure.pagination_type") == "source-configure-pagination-type"


def test_reference_has_no_placeholder_meanings_or_stale_source_contract() -> None:
    content = _render()
    assert "See the schema definition for this field." not in content
    assert "on raw source columns" not in content
    assert "A query controls the read; optional `table` can label its result" in content
    assert "use columns or aliases returned by the query" in content


def test_every_defined_nested_field_has_a_direct_anchor() -> None:
    schema = resolve_metadata_schema().document
    content = _render()
    ids = set(re.findall(r'<span id="([^"]+)"></span>', content))
    prefixes = {
        "Connection": "connections[]", "DataFlow": "dataflows[]",
        "Source": "dataflows[].source", "Transform": "dataflows[].transform",
        "Destination": "dataflows[].destination", "SchemaHint": "schemaHint",
        "SharedSchemaHint": "schema_hints[]", "PartitionColumn": "partitionColumn",
        "AdditionalColumn": "additionalColumn", "ValueRule": "valueRule",
        "HashColumn": "hashColumn", "MaskingRule": "maskingRule",
    }

    def check_properties(node: dict, prefix: str, *, endpoint: str | None = None) -> None:
        for name, child in node.get("properties", {}).items():
            path = f"{prefix}.{name}" if prefix else name + ("[]" if child.get("type") == "array" else "")
            assert _field_anchor(path, endpoint=endpoint) in ids, path
            if isinstance(child, dict) and "properties" in child:
                check_properties(child, path, endpoint=endpoint)

    check_properties(schema, "")
    for definition, prefix in prefixes.items():
        check_properties(schema["$defs"][definition], prefix)
    for definition in (
        "ConnectionConfigureFile", "ConnectionConfigureLakehouse",
        "ConnectionConfigureDatabase", "ConnectionConfigureApi",
    ):
        branch = schema["$defs"][definition]
        endpoint = branch["if"]["properties"]["connection_type"]["const"]
        configure = branch["then"]["properties"]["configure"]
        check_properties(configure, "connections[].configure", endpoint=endpoint)


def test_metadata_guide_and_skill_reference_fragments_exist() -> None:
    content = _render()
    ids = set(re.findall(r'<span id="([^"]+)"></span>', content))
    headings = re.findall(r"^#{2,6} (.+)$", content, re.MULTILINE)
    ids.update(re.sub(r"[^a-z0-9-]+", "", heading.lower().replace(" ", "-")) for heading in headings)
    root = Path(__file__).resolve().parents[3]
    consumers = list((root / "docs" / "guide" / "metadata").glob("*.md"))
    consumers.extend((root / "ai" / "skills").rglob("*.md"))
    fragments = set()
    for path in consumers:
        text = path.read_text(encoding="utf-8")
        fragments.update(re.findall(r"metadata-schema(?:\.md)?/?#([a-z0-9-]+)", text))
    assert fragments
    assert fragments <= ids, sorted(fragments - ids)


def test_api_next_link_policy_is_discoverable_in_generated_reference() -> None:
    content = _render()
    field = "dataflows[].source.configure.next_link_bound_mode"
    row = next(line for line in content.splitlines() if f"`{field}`" in line)

    assert _field_anchor(field) in row
    assert 'enum: "opaque", "repeat_query_bounds"' in row
    assert 'no; default `"opaque"`' in row
