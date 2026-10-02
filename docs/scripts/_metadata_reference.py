"""Render the authored metadata reference from a resolved JSON Schema.

This module has no MkDocs side effects so the structure and coverage rules can
be tested without starting a documentation build. The versioned schema remains
the source of truth; the small notes below explain behavior that JSON Schema
cannot express on its own.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any


FIELD_NOTES: dict[str, str] = {
    "connections[].name": "Required stable label used by `source.connection_name` and `destination.connection_name`.",
    "connections[].connection_type": "Optional when `format` identifies one unambiguous built-in family. Check the supported format/type pairs before relying on derivation.",
    "connections[].configure": "Reusable endpoint defaults. Source and destination `configure` values apply at dataflow scope; option precedence is option-specific, not a promise of a universal deep merge.",
    "connections[].configure.backward": "Reusable connection look-back; a non-empty source look-back replaces this option set rather than merging into it.",
    "connections[].secrets_ref": "Names fields whose values are secret keys or environment-variable names; do not put credentials in authored metadata. See [connection secrets](../guide/metadata/connections.md#keep-credentials-out-of-metadata).",
    "connections[].is_active": "Defaults to true. False retains the connection in metadata but skips any selected dataflow that uses it as source or destination.",
    "connections[].configure.token_auth_method": "API OAuth2 client-credentials mode only. Runtime supports `client_secret_post` (default) and `client_secret_basic`; any other string currently falls back to post and should not be used.",
    "connections[].configure.watermark_to_param_timezone": "API default timezone for upper-bound parameters and pushed-down watermark advancement. Use an IANA name or ±HH:MM offset; source-level value wins.",
    "dataflows[].is_active": "Defaults to true. False excludes normal selection and blocks execution even when the dataflow is supplied directly.",
    "dataflows[].name": "Recommended human-readable identity. A usable explicit `dataflow_id` may replace it for ID-based integrations; name-based references still require one unambiguous name.",
    "dataflows[].source": "Use exactly one connection reference form: `connection_name` or `connection`. An inline connection object still needs its own `name`; it is not registered as a reusable top-level connection. A query controls the read; optional `table` can label its result. A Python function may use any Source field it receives.",
    "dataflows[].source.filter_expression": "Filter reader output before transforms. In database query mode, use columns or aliases returned by the query, not columns hidden inside it.",
    "dataflows[].destination": "Use exactly one connection reference form: `connection_name` or `connection`. An inline connection object still needs its own `name`; it is not registered as a reusable top-level connection. `table` is required even when the destination is addressed through a catalog.",
    "dataflows[].source.query": "Inline SQL is supported. A relative `.sql` path is resolved during Driver preparation using provider `sql_base_path`, Driver `sql_base_path` as a session fallback, or `artifact_base_path`; `artifact:/...` selects the artifact root explicitly. See [SQL-file query sources](../guide/metadata/source-patterns.md#read-a-sql-file).",
    "dataflows[].source.watermark_columns": "The authored column list enables incremental filtering. The effective lower bound and `date_backward` are runtime values, not authored fields.",
    "dataflows[].source.configure.method": "HTTP method string, not a DataCoolie enum. Default GET; the reader passes the uppercased value to the HTTP client. `body` is sent as JSON.",
    "dataflows[].source.configure.pagination_type": "API pagination mode: `offset`, `cursor`, or `next_link`; omit for one response. See [pagination cases](../guide/metadata/source-patterns.md#choose-the-pagination-contract).",
    "dataflows[].source.configure.next_link_bound_mode": "Next-link continuation policy: default `opaque` follows the returned URL as-is; `repeat_query_bounds` is an explicit opt-in that adds only missing active query bounds and rejects duplicate or conflicting values. It does not decode or rewrite opaque tokens. See [next-link pagination](../guide/metadata/source-patterns.md#next-link-pagination).",
    "dataflows[].source.configure.endpoint": "Endpoint path appended to the API connection's `base_url`; omit only when requesting the base URL itself.",
    "dataflows[].source.configure.data_path": "API response JSON path to a list or one object. A missing or mismatched path yields zero records, not a path-validation error.",
    "dataflows[].source.configure.offset_param": "API offset pagination sends record offsets `0`, `page_size`, `2 × page_size`, not page numbers. Rename the parameter only if the provider expects record offsets.",
    "dataflows[].source.configure.total_path": "API offset mode only. Reads the numeric total from page 0 and enables concurrent page requests; sequential `rate_limit_delay` no longer applies.",
    "dataflows[].source.configure.max_pages": "API safety cap, default 1,000 pages. If another page is required at the cap, the reader fails instead of returning a partial result; choose the budget with the provider's completeness contract.",
    "dataflows[].source.configure.rate_limit_delay": "Seconds between sequential API pages only. It does not throttle concurrent offset pages selected by `total_path`.",
    "dataflows[].source.configure.max_retries": "Retries HTTP 429 using Retry-After or exponential backoff; other HTTP errors are not retried by this setting.",
    "dataflows[].source.configure.watermark_param_mapping": "Legacy API incremental range-split map from authored watermark column names to lower-bound parameter names. Required for saved-watermark resume in that legacy mode; without it no lower bound is sent. Canonical bounded/replay bindings use `range_param_mapping` instead.",
    "dataflows[].source.configure.range_param_mapping": "Canonical API per-field lower/upper bindings for bounded reads and replay. The selected field may be outside `source.watermark_columns`; only configured watermark columns are persisted. See [API range configuration](../guide/metadata/source-patterns.md#push-down-and-split-watermark-ranges).",
    "dataflows[].source.configure.watermark_range_interval_unit": "API incremental split mode (canonical or legacy): pair it with `range_param_mapping` for canonical lower/upper bindings, or with `watermark_to_param` and `watermark_param_mapping` for legacy mapping. The first incremental run may need `watermark_range_start` when no lower watermark is saved. Explicit bounded reads and replay receive their `[start, end)` range from the source contract and do not require split settings. See [API range configuration](../guide/metadata/source-patterns.md#push-down-and-split-watermark-ranges).",
    "dataflows[].source.configure.watermark_range_start": "Fallback lower bound for API incremental split reads (canonical or legacy) when no stored lower watermark exists. Explicit bounded reads and replay receive their `[start, end)` range from the source contract; do not add this field solely to configure replay.",
    "dataflows[].source.configure.watermark_range_to_exclusive_offset": "Legacy inclusive-upper-bound adjustment for incremental split requests. It changes only the wire value sent to that legacy endpoint; it is not a replacement for canonical bounded `range_param_mapping` operators.",
    "dataflows[].source.configure.backward_days": "Source-level look-back override for watermark preparation. Connection-level look-back is used when no source override applies.",
    "dataflows[].source.configure.backward": "Structured look-back override with `days`, `months`, `hours`, `years`, or `closing_day`; use it as one authored option set.",
    "dataflows[].destination.load_type": "`merge_overwrite` can be paired with `destination.configure.replace_by_watermark: true` for bounded window replacement.",
    "dataflows[].destination.configure.replace_by_watermark": "Only meaningful with `load_type: merge_overwrite`; it switches the delete scope from matched keys to the effective watermark window. See [window replacement](../guide/metadata/watermark-window-replacement.md).",
    "dataflows[].destination.merge_keys": "Required by `merge_upsert`, `scd2`, and key-based `merge_overwrite`; a usable watermark replacement window does not need keys.",
    "dataflows[].transform.schema_hints": "Column-level type declarations. They take effect only when the source connection permits schema hints and the selected engine/type system supports the value. See [dataflow-level hints](../guide/metadata/data-types.md#hints-for-one-dataflow).",
    "schema_hints[].hints": "Shared hints are selected by connection/table identity and are distinct from inline `dataflows[].transform.schema_hints`. See [shared hints](../guide/metadata/data-types.md#global-hints-for-a-source-table).",
    "dataflows[].destination.configure.write_options": "Engine-specific write options for this destination; matching keys override connection write options.",
    "valueRule.operation": "Normalization action applied to the selected columns; conditional fields below depend on this value.",
    "valueRule.columns": "Columns to which this value rule applies.",
    "valueRule.order": "Order among value rules; lower values run first.",
    "valueRule.mode": "Case conversion mode; required when `operation` is `case`.",
    "valueRule.value": "Replacement value required for `fill_null`; also used by the selected operation where applicable.",
    "valueRule.mapping": "Old-to-new value mapping required for the `map` operation.",
    "valueRule.on_unmapped": "For `map`, keep unmatched values or replace them with null.",
    "hashColumn.target_column": "Output column that receives the computed hash.",
    "hashColumn.columns": "Input columns combined to compute the hash.",
    "hashColumn.algorithm": "Hash algorithm; changing it changes persisted values and may change the output type.",
    "hashColumn.serialization": "Portable input serialization contract used before hashing.",
    "maskingRule.method": "Masking operation; method-specific required fields are listed below.",
    "maskingRule.columns": "Columns to which this masking rule applies.",
    "maskingRule.value": "Replacement value required for `redact`.",
    "maskingRule.keep_start": "For partial masking, number of leading characters to keep.",
    "maskingRule.keep_end": "For partial masking, number of trailing characters to keep.",
    "maskingRule.mask_char": "Character used to cover the masked middle portion.",
    "maskingRule.bucket_size": "Numeric bucket width required for `numeric_bucket`.",
    "maskingRule.unit": "Truncation unit required for `date_truncate`.",
}
ENDPOINT_NOTES: dict[tuple[str, str], str] = {
    ("file", "base_path"): "File root. The schema permits omission, but the built-in file reader needs a resolvable root path; see [file connections](../guide/metadata/connections.md#choose-the-endpoint-family).",
    ("lakehouse", "base_path"): "Path-based lakehouse root; omit for a supported named-table/catalog addressing mode. See [lakehouse addressing](../guide/metadata/source-patterns.md#lakehouse-source-delta-or-iceberg).",
    ("api", "base_url"): "Base URL required by the built-in API reader, although the schema does not mark this open-map key as required.",
    ("api", "auth_type"): "API authentication mode; its credential fields depend on the selected mode. See [API authentication cases](../guide/metadata/connections.md#api-authentication).",
}
for _scope, _owner in (("connections[].configure", "connection default"), ("dataflows[].source.configure", "source override")):
    for _unit in ("years", "months", "days", "hours"):
        FIELD_NOTES[f"{_scope}.backward.{_unit}"] = f"Look-back {_unit} within the {_owner}."
    FIELD_NOTES[f"{_scope}.backward.closing_day"] = "Nested day-of-month for the closing-day look-back strategy."

CONDITIONAL_REQUIRED = {
    "dataflows[].source.connection_name",
    "dataflows[].source.connection",
    "dataflows[].destination.connection_name",
    "dataflows[].destination.connection",
}


DISPLAY_NAMES = {
    "Connection": "Connection",
    "DataFlow": "Dataflow",
    "Source": "Source",
    "Destination": "Destination",
    "Transform": "Transform",
    "SchemaHint": "Schema hint",
    "SharedSchemaHint": "Shared schema hint",
    "PartitionColumn": "Partition column",
    "AdditionalColumn": "Additional column",
    "ValueRule": "Value rule",
    "HashColumn": "Hash column",
    "MaskingRule": "Masking rule",
}

PRIMARY_DEFINITIONS = (
    "Connection",
    "DataFlow",
    "Source",
    "Destination",
    "Transform",
    "SchemaHint",
    "SharedSchemaHint",
    "PartitionColumn",
    "AdditionalColumn",
    "ValueRule",
    "HashColumn",
    "MaskingRule",
)

GUIDE_LINKS: dict[str, tuple[tuple[str, str], ...]] = {
    "Connection": (("Connections", "../guide/metadata/connections.md"),),
    "DataFlow": (("Dataflows", "../guide/metadata/dataflows.md"),),
    "Source": (
        ("Source configuration", "../guide/metadata/source-patterns.md"),
    ),
    "Destination": (("Destination and load patterns", "../guide/metadata/destination-and-load-patterns.md"),),
    "Transform": (("Transform patterns", "../guide/metadata/transform-patterns.md"),),
    "SchemaHint": (("Datatypes and schema hints", "../guide/metadata/data-types.md"),),
    "SharedSchemaHint": (("Datatypes and schema hints", "../guide/metadata/data-types.md"),),
    "PartitionColumn": (("Destination partitioning", "../guide/metadata/destination-and-load-patterns.md#partition_columns-partition-the-output-table"),),
    "AdditionalColumn": (("Computed columns (ColumnAdder)", "../guide/metadata/transform-patterns.md#columnadder"),),
    "ValueRule": (("Value normalization (ColumnValueTransformer)", "../guide/metadata/transform-patterns.md#columnvaluetransformer"),),
    "HashColumn": (("Hash columns (HashColumnAdder)", "../guide/metadata/transform-patterns.md#hashcolumnadder"),),
    "MaskingRule": (("Masking (DataMasker)", "../guide/metadata/transform-patterns.md#datamasker"),),
}


def _defs(schema: Mapping[str, Any]) -> Mapping[str, Any]:
    value = schema.get("$defs", {})
    return value if isinstance(value, Mapping) else {}


def _resolve(node: Any, schema: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(node, Mapping):
        return {}
    reference = node.get("$ref")
    if isinstance(reference, str) and reference.startswith("#/$defs/"):
        value = _defs(schema).get(reference.removeprefix("#/$defs/"), {})
        return value if isinstance(value, Mapping) else {}
    return node


def _type_label(node: Any, schema: Mapping[str, Any]) -> str:
    if not isinstance(node, Mapping):
        return "any"
    if "$ref" in node:
        ref_name = str(node["$ref"]).rsplit("/", 1)[-1]
        return DISPLAY_NAMES.get(ref_name, ref_name)
    if "enum" in node:
        values = ", ".join(_inline(value) for value in node["enum"])
        return f"enum: {values}"
    if "oneOf" in node:
        return "one of " + " / ".join(_type_label(item, schema) for item in node["oneOf"])
    if "anyOf" in node:
        return "any of " + " / ".join(_type_label(item, schema) for item in node["anyOf"])
    value = node.get("type")
    if isinstance(value, list):
        return " / ".join(str(item) for item in value)
    if value == "array":
        return f"array of {_type_label(node.get('items', {}), schema)}"
    if value == "object":
        additional = node.get("additionalProperties")
        if additional is True:
            return "object (open map)"
        if isinstance(additional, Mapping):
            return f"object map of {_type_label(additional, schema)}"
        return "object"
    return str(value or "any")


def _inline(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).replace("|", "\\|")


def _default(node: Mapping[str, Any]) -> str:
    if "default" not in node:
        return "—"
    return f"default `{_inline(node['default'])}`"


def _properties(node: Mapping[str, Any], schema: Mapping[str, Any]) -> tuple[dict[str, Any], set[str]]:
    resolved = _resolve(node, schema)
    properties: dict[str, Any] = {}
    required = set(resolved.get("required", ()))
    direct = resolved.get("properties", {})
    if isinstance(direct, Mapping):
        properties.update(direct)
    for branch in resolved.get("allOf", ()):
        branch_resolved = _resolve(branch, schema)
        branch_properties = branch_resolved.get("properties", {})
        if isinstance(branch_properties, Mapping):
            properties.update(branch_properties)
        required.update(branch_resolved.get("required", ()))
    return properties, required


def _conditional_required(node: Mapping[str, Any]) -> set[str]:
    conditional: set[str] = set()
    for branch in node.get("anyOf", ()):
        if isinstance(branch, Mapping):
            conditional.update(branch.get("required", ()))
    return conditional


def _field_anchor(path: str, *, endpoint: str | None = None) -> str:
    """Stable HTML ID for an authored field, independent of render order."""

    prefixes = (
        ("dataflows[].source", "source"),
        ("dataflows[].transform", "transform"),
        ("dataflows[].destination", "destination"),
        ("dataflows[]", "dataflow"),
        ("connections[]", "connection"),
        ("schema_hints[]", "shared-schema-hint"),
        ("schemaHint", "schema-hint"),
        ("partitionColumn", "partition-column"),
        ("additionalColumn", "additional-column"),
        ("valueRule", "value-rule"),
        ("hashColumn", "hash-column"),
        ("maskingRule", "masking-rule"),
    )
    for prefix, owner in prefixes:
        if path == prefix or path.startswith(prefix + "."):
            if path == prefix:
                return "metadata-" + re.sub(r"[^a-z0-9]+", "-", path.lower()).strip("-")
            suffix = path[len(prefix):].lstrip(".")
            if endpoint and owner == "connection" and suffix.startswith("configure."):
                suffix = "configure." + endpoint + "." + suffix.removeprefix("configure.")
            suffix = suffix.replace("backward.", "backward-object.")
            return "-".join(filter(None, (owner, re.sub(r"[^a-z0-9]+", "-", suffix.lower()).strip("-"))))
    return "metadata-" + re.sub(r"[^a-z0-9]+", "-", path.lower()).strip("-")


def _table(rows: list[tuple[str, str, str, str, str]], *, endpoint: str | None = None) -> str:
    lines = [
        "| Authored path | Type | Required / default | Meaning | Constraints |",
        "|---|---|---|---|---|",
    ]
    lines.extend(
        f'| `{path}` | {type_} | {required} | <span id="{_field_anchor(path, endpoint=endpoint)}"></span>{meaning} | {_clean(constraints) or "—"} |'
        for path, type_, required, meaning, constraints in rows
    )
    return "\n".join(lines)


def _definition_rows(
    node: Mapping[str, Any],
    schema: Mapping[str, Any],
    prefix: str,
) -> tuple[list[tuple[str, str, str, str, str]], list[tuple[str, Mapping[str, Any]]]]:
    properties, required = _properties(node, schema)
    conditional_names = _conditional_required(_resolve(node, schema))
    rows: list[tuple[str, str, str, str, str]] = []
    nested: list[tuple[str, Mapping[str, Any]]] = []
    for name, raw in properties.items():
        field = raw if isinstance(raw, Mapping) else {}
        path = f"{prefix}.{name}"
        resolved = _resolve(field, schema)
        description = _clean(resolved.get("description", ""))
        meaning = _clean(FIELD_NOTES.get(path, description)) or f"Authored `{name}` value for this object."
        required_label = "conditional" if path in CONDITIONAL_REQUIRED or name in conditional_names else ("yes" if name in required else "no")
        rows.append((path, _type_label(field, schema), required_label + "; " + _default(resolved), meaning, _constraint_text(resolved)))
        if isinstance(field.get("properties"), Mapping):
            nested.append((path, resolved))
    return rows, nested


def _conditional_connection_rows(schema: Mapping[str, Any]) -> list[tuple[str, list[tuple[str, str, str, str, str]]]]:
    result = []
    for name, raw in _defs(schema).items():
        if not name.startswith("ConnectionConfigure") or not isinstance(raw, Mapping):
            continue
        condition = raw.get("if", {})
        condition_properties = condition.get("properties", {}) if isinstance(condition, Mapping) else {}
        type_node = condition_properties.get("connection_type", {}) if isinstance(condition_properties, Mapping) else {}
        connection_type = type_node.get("const") if isinstance(type_node, Mapping) else None
        then = raw.get("then", {})
        configure = then.get("properties", {}).get("configure", {}) if isinstance(then, Mapping) else {}
        properties = configure.get("properties", {}) if isinstance(configure, Mapping) else {}
        if not connection_type or not isinstance(properties, Mapping):
            continue
        rows = []
        for field_name, field in properties.items():
            resolved = field if isinstance(field, Mapping) else {}
            path = f"connections[].configure.{field_name}"
            meaning = _clean(ENDPOINT_NOTES.get((str(connection_type), field_name), FIELD_NOTES.get(path, resolved.get("description", "")))) or "Type-specific connection setting."
            rows.append((path, _type_label(field, schema), "no; " + _default(resolved), meaning, _constraint_text(resolved)))
            nested_properties = resolved.get("properties", {})
            if isinstance(nested_properties, Mapping):
                for nested_name, nested_field in nested_properties.items():
                    nested_value = nested_field if isinstance(nested_field, Mapping) else {}
                    nested_path = f"{path}.{nested_name}"
                    rows.append(
                        (
                            nested_path,
                            _type_label(nested_field, schema),
                            "no; " + _default(nested_value),
                            _clean(FIELD_NOTES.get(nested_path, nested_value.get("description", ""))) or "Nested connection setting.",
                            _constraint_text(nested_value),
                        )
                    )
        result.append((str(connection_type), rows))
    return result


def _condition_label(condition: Mapping[str, Any]) -> str:
    parts: list[str] = []
    properties = condition.get("properties", {})
    if isinstance(properties, Mapping):
        for name, value in properties.items():
            if not isinstance(value, Mapping):
                continue
            if "const" in value:
                parts.append(f"`{name}` is `{_inline(value['const'])}`")
            elif "enum" in value:
                parts.append(f"`{name}` is one of `{_inline(value['enum'])}`")
            elif value:
                parts.append(f"`{name}` matches the conditional schema")
    required = condition.get("required", ())
    if required:
        parts.append("fields " + ", ".join(f"`{name}`" for name in required) + " are present")
    return " and ".join(parts) or "the condition in the schema is met"


def _constraint_text(node: Mapping[str, Any]) -> str:
    details: list[str] = []
    if "const" in node:
        details.append(f"must be `{_inline(node['const'])}`")
    if "enum" in node:
        details.append("must be one of " + ", ".join(f"`{_inline(value)}`" for value in node["enum"]))
    if "minItems" in node:
        details.append(f"must contain at least {node['minItems']} item(s)")
    if "maxItems" in node:
        details.append(f"must contain at most {node['maxItems']} item(s)")
    if "minLength" in node:
        details.append(f"must contain at least {node['minLength']} character(s)")
    if "maxLength" in node:
        details.append(f"must contain at most {node['maxLength']} character(s)")
    for keyword, label in (
        ("minimum", ">="), ("maximum", "<="),
        ("exclusiveMinimum", ">"), ("exclusiveMaximum", "<"),
    ):
        if keyword in node:
            details.append(f"value {label} `{_inline(node[keyword])}`")
    if "multipleOf" in node:
        details.append(f"multiple of `{_inline(node['multipleOf'])}`")
    if node.get("uniqueItems"):
        details.append("items must be unique")
    if "pattern" in node:
        details.append(f"matches pattern `{node['pattern']}`")
    for keyword, label in (("minProperties", "at least"), ("maxProperties", "at most")):
        if keyword in node:
            details.append(f"must contain {label} {node[keyword]} property/properties")
    items = node.get("items")
    if isinstance(items, Mapping):
        item_constraints = _constraint_text(items)
        if item_constraints:
            details.append(f"each item: {item_constraints}")
    return "; ".join(details)


def _conditional_rule_rows(schema: Mapping[str, Any]) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    definitions = [("Metadata document", schema)] + [
        (DISPLAY_NAMES.get(definition, definition), _defs(schema).get(definition))
        for definition in PRIMARY_DEFINITIONS
    ]
    for title, node in definitions:
        if not isinstance(node, Mapping):
            continue
        for keyword, qualifier in (("anyOf", "at least one"), ("oneOf", "exactly one")):
            alternatives = []
            for branch in node.get(keyword, ()):
                if isinstance(branch, Mapping) and branch.get("required"):
                    alternatives.append("all of " + ", ".join(f"`{name}`" for name in branch["required"]))
            if alternatives:
                rows.append((title, qualifier + " of the schema alternatives", "requires " + " or ".join(alternatives)))
        for branch in node.get("allOf", ()):
            if not isinstance(branch, Mapping) or "$ref" in branch:
                continue
            condition = branch.get("if")
            then = branch.get("then", {})
            if not isinstance(condition, Mapping) or not isinstance(then, Mapping):
                continue
            effects: list[str] = []
            then_required = then.get("required", ())
            if then_required:
                effects.append("requires " + ", ".join(f"`{name}`" for name in then_required))
            then_properties = then.get("properties", {})
            if isinstance(then_properties, Mapping):
                for name, value in then_properties.items():
                    if isinstance(value, Mapping):
                        constraints = _constraint_text(value)
                        if constraints:
                            effects.append(f"`{name}` {constraints}")
            if effects:
                rows.append((title, _condition_label(condition), "; ".join(effects)))
    return rows


def _append_rules(sections: list[str], rules: list[tuple[str, str, str]], title: str, *, level: int = 3) -> None:
    relevant = [(condition, effect) for owner, condition, effect in rules if owner == title]
    if relevant:
        sections.extend(["", "#" * level + " Conditional rules", "", "| Condition | Result |", "|---|---|"])
        sections.extend(f"| {condition} | {effect} |" for condition, effect in relevant)


def _append_nested(
    sections: list[str], node: Mapping[str, Any], schema: Mapping[str, Any],
    prefix: str, *, level: int = 3,
) -> None:
    _, nested = _definition_rows(node, schema, prefix)
    for nested_path, nested_node in nested:
        nested_rows, _ = _definition_rows(nested_node, schema, nested_path)
        sections.extend(["", "#" * min(level, 6) + f" `{nested_path}`", "", _table(nested_rows)])
        _append_nested(sections, nested_node, schema, nested_path, level=level + 1)


def _append_definition(
    sections: list[str], schema: Mapping[str, Any], rules: list[tuple[str, str, str]],
    definition: str, *, level: int = 2,
) -> None:
    node = _defs(schema).get(definition)
    if not isinstance(node, Mapping):
        return
    title = DISPLAY_NAMES.get(definition, definition)
    prefix = {
        "Connection": "connections[]",
        "DataFlow": "dataflows[]",
        "Source": "dataflows[].source",
        "Destination": "dataflows[].destination",
        "Transform": "dataflows[].transform",
        "SharedSchemaHint": "schema_hints[]",
    }.get(definition, f"{definition[0].lower()}{definition[1:]}")
    rows, _ = _definition_rows(node, schema, prefix)
    description = _clean(node.get("description", ""))
    if definition == "Source":
        description = "Read-side pipeline configuration using a named or inline connection."
    elif definition == "Destination":
        description = "Write-side pipeline configuration using a named or inline connection."
    sections.extend(["", "#" * level + f" {title}", ""])
    if description:
        sections.extend([description, ""])
    sections.append(_table(rows))
    links = GUIDE_LINKS.get(definition, ())
    if links:
        sections.extend(["", "How to configure: " + " · ".join(
            f"[{label}]({url})" for label, url in links
        )])
    _append_nested(sections, node, schema, prefix, level=level + 1)
    _append_rules(sections, rules, title, level=level + 1)


def verify_latest_reference(
    latest: Mapping[str, Any], *, version: str, public_url: str, sha256: str, latest_url: str,
) -> None:
    """Refuse to publish a reference that describes a different schema than latest."""

    expected = {"version": version, "source_url": public_url, "sha256": sha256, "url": latest_url}
    mismatches = {key: (latest.get(key), value) for key, value in expected.items() if latest.get(key) != value}
    if mismatches:
        raise ValueError(f"Metadata reference and published latest schema disagree: {mismatches}")


def render_metadata_reference(
    schema: Mapping[str, Any], *, latest_url: str,
) -> str:
    """Return the generated authored metadata reference page."""

    root_rows = []
    root_properties = schema.get("properties", {})
    if isinstance(root_properties, Mapping):
        for name, raw in root_properties.items():
            field = raw if isinstance(raw, Mapping) else {}
            path = f"{name}[]" if field.get("type") == "array" else name
            type_node = field.get("items", field) if field.get("type") == "array" else field
            required = set(schema.get("required", ()))
            conditional = _conditional_required(schema)
            required_label = "conditional" if name in conditional else ("yes" if name in required else "no")
            root_rows.append((path, _type_label(type_node, schema), required_label + "; " + _default(field), _clean(field.get("description", "")) or "Top-level metadata field.", _constraint_text(field)))

    sections: list[str] = [
        "---",
        "title: Metadata reference | DataCoolie",
        "description: Authored DataCoolie metadata fields, types, defaults, conditions, and configuration boundaries.",
        "---",
        "",
        "# Metadata reference",
        "",
        "This page is generated from the current DataCoolie metadata JSON Schema. The schema is the structural source of truth for authored metadata; this page adds readable paths and runtime guidance that the schema cannot express.",
        "",
        f"Authoring schema: [latest]({latest_url})",
        "",
        "For reproducible metadata, pin the versioned URL. `dc validate` uses the schema bundled with the installed framework, not the public URL.",
        "",
        "Use the [metadata guide](../guide/metadata/index.md) for task-based instructions. Each guide case keeps its own focused JSON example beside the explanation. Use the [Python API reference](api/core.md) for hydrated model classes and runtime-only properties.",
        "",
        "## Reading this reference",
        "",
        "Paths ending in `[]` describe one item in an authored array. `Required` describes the local schema object; some fields are required only when another field selects a mode. `default` is the schema default and does not replace a runtime-derived value.",
        "",
        "The Constraints column exposes property limits declared by the schema, including lengths, numeric bounds and uniqueness. Conditional rules below add mode-specific requirements. Use `dc validate` to evaluate the complete schema; these tables are a readable reference, not a separate validator.",
        "",
        "An `enum` lists the exact values accepted by the selected schema. A plain `string` is not automatically an unrestricted runtime option: check the field's behavior note and linked guide for built-in modes. Values such as endpoint paths, API parameter names, SQL expressions, and provider-specific option keys are free-form within their documented purpose.",
        "",
        "`configure`, `read_options`, and `write_options` are open maps for engine- or provider-specific options. The top-level `extensions` object is optional and project-owned: metadata project operations preserve it, but the DataCoolie runtime does not interpret its contents or use them to configure pipeline execution. This page lists DataCoolie-defined keys where the schema defines them; it cannot enumerate third-party options.",
        "",
        "Runtime values such as hydrated `connection`, effective watermark bounds, `date_backward`, deduplication fallback columns, and execution metadata are not authored fields.",
        "",
        "## Metadata document",
        "",
        "How to configure: [Metadata document](../guide/metadata/index.md#metadata-document)",
        "",
        _table(root_rows),
        "",
        "The document must provide the arrays required by the selected schema branch. A typical document contains `connections` and `dataflows`; shared `schema_hints` and `extensions` are optional.",
    ]

    rules = _conditional_rule_rows(schema)
    _append_rules(sections, rules, "Metadata document")
    _append_definition(sections, schema, rules, "SharedSchemaHint", level=3)
    _append_definition(sections, schema, rules, "Connection")
    conditional = _conditional_connection_rows(schema)
    if conditional:
        sections.extend(["", "### Connection settings by endpoint type", "", "These fields are conditional on `connections[].connection_type` and remain under that connection's `configure` object."])
        for connection_type, rows in conditional:
            sections.extend(["", f"#### `connection_type: {connection_type}`", "", _table(rows, endpoint=connection_type)])

    _append_definition(sections, schema, rules, "DataFlow")
    _append_definition(sections, schema, rules, "Source")
    _append_definition(sections, schema, rules, "Transform")
    for definition in ("SchemaHint", "AdditionalColumn", "ValueRule", "HashColumn", "MaskingRule"):
        _append_definition(sections, schema, rules, definition, level=3)
    _append_definition(sections, schema, rules, "Destination")
    _append_definition(sections, schema, rules, "PartitionColumn", level=3)

    sections.extend(
        [
            "",
            "## Cross-boundary combinations",
            "",
            "- [`source.query`](#source-query) determines the read; optional [`source.table`](#source-table) can label the query result. A relative SQL file is still authored in `source.query` and resolved during Driver preparation.",
            "- [`destination.load_type`](#destination-load-type) `merge_overwrite` plus [`replace_by_watermark`](#destination-configure-replace-by-watermark) requires an effective [`source.watermark_columns`](#source-watermark-columns) window and authored [look-back](#source-configure-backward). See [window replacement](../guide/metadata/watermark-window-replacement.md); `date_backward` is computed at runtime.",
            "- [`transform.select_columns`](#transform-select-columns) and [`transform.drop_columns`](#transform-drop-columns) are mutually exclusive. Deduplication and merge also depend on destination keys and source watermarks.",
            "- [Shared schema hints](#shared-schema-hint) and [inline transform hints](#transform-schema-hints) have different scopes. See [Datatypes and schema hints](../guide/metadata/data-types.md).",
            "",
            "## Related documentation",
            "",
            "- [Metadata guide](../guide/metadata/index.md)",
            "- [Metadata model concepts](concepts/metadata-model.md)",
            "- [Python core API](api/core.md)",
            "- [Published schema index](../schema/index.json)",
            "- [Current stable schema](../schema/latest/metadata.schema.json)",
            "",
        ]
    )
    return "\n".join(sections)


__all__ = ["render_metadata_reference"]
