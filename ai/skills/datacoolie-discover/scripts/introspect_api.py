"""introspect_api.py — API spec introspection for OpenAPI, GraphQL, and OData.

Parses API specifications and outputs a standardised CSV matching the
datacoolie discover schema contract.

Usage:
    python introspect_api.py --spec https://api.example.com/openapi.json --source crm
    python introspect_api.py --spec ./petstore.yaml --source pets --output schema.csv
    python introspect_api.py --graphql https://api.example.com/graphql --source crm
    python introspect_api.py --odata https://api.example.com/\\$metadata --source erp
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
import yaml

from _observation_contract import (
    CSV_HEADER as CSV_HEADER,  # noqa: F401 - public cross-probe contract
    atomic_write_observations,
    make_observation,
    write_observations,
)
from _input_safety import validate_nonsecret_locator
from _probe_status import (
    PARTIAL_EXIT_CODE,
    observation_scope,
    write_probe_status,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_REQUEST_TIMEOUT_SECONDS = 30

# GraphQL introspection query
GRAPHQL_INTROSPECTION = """
{
  __schema {
    types {
      name
      kind
      fields {
        name
        type {
          name
          kind
          ofType {
            name
            kind
            ofType {
              name
              kind
              ofType {
                name
                kind
              }
            }
          }
        }
      }
    }
  }
}
"""

# ---------------------------------------------------------------------------
# OpenAPI parsing
# ---------------------------------------------------------------------------

OPENAPI_TYPE_MAP = {
    "integer": "integer",
    "number": "double",
    "string": "string",
    "boolean": "boolean",
    "array": "array",
    "object": "struct",
}

OPENAPI_FORMAT_MAP = {
    "int32": ("integer", ""),
    "int64": ("long", ""),
    "float": ("float", ""),
    "double": ("double", ""),
    "date": ("date", ""),
    "date-time": ("timestamp", ""),
    "time": ("time", ""),
    "byte": ("binary", "base64"),
    "binary": ("binary", ""),
    "uuid": ("string", "uuid"),
    "email": ("string", "email"),
    "uri": ("string", "uri"),
    "uri-reference": ("string", "uri"),
    "password": ("string", ""),
}


MAX_SCHEMA_DEPTH = 10


def _resolve_ref(spec: dict, ref: str) -> dict:
    """Resolve a local ``$ref`` path in an OpenAPI spec.

    This helper intentionally keeps its historical ``{}`` result for an
    unresolved path.  The mapper uses ``_resolve_ref_checked`` when it needs
    to distinguish that condition and report partial evidence.
    """
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict):
            return {}
        node = node.get(part, {})
    return node if isinstance(node, dict) else {}


def _resolve_ref_checked(spec: dict, ref: str) -> dict | None:
    if not isinstance(ref, str) or not ref.startswith("#/"):
        return None
    parts = ref[2:].split("/")
    node: object = spec
    for part in parts:
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node if isinstance(node, dict) else None


def _add_issue(issues: list[str], issue: str) -> None:
    if issue not in issues:
        issues.append(issue)


def _declared_type_values(schema: dict) -> list[str]:
    declared = schema.get("type")
    if isinstance(declared, list):
        return [str(value) for value in declared]
    if isinstance(declared, str):
        return [declared]
    return []


def _branch_is_null(schema: object) -> bool:
    if not isinstance(schema, dict):
        return False
    values = _declared_type_values(schema)
    return bool(values) and all(value == "null" for value in values)


def _native_type(schema: dict, override: str = "") -> str:
    override = override or str(schema.get("__native_type_override", "") or "")
    if override:
        return override
    values = _declared_type_values(schema)
    if values:
        return "|".join(values)
    if "properties" in schema or schema.get("type") == "object":
        return "object"
    if "items" in schema or schema.get("type") == "array":
        return "array"
    return ""


def _schema_nullable(schema: dict) -> str:
    explicit = schema.get("__nullable_override")
    if explicit is not None:
        return "true" if explicit else "false"
    if schema.get("nullable") is True:
        return "true"
    if schema.get("nullable") is False:
        return "false"
    values = _declared_type_values(schema)
    if values:
        return "true" if "null" in values else "false"
    if "properties" in schema or "items" in schema or schema.get("type"):
        return "false"
    return ""


def _wrapper_notes(
    schema: dict,
    *,
    wrapper: str,
    presence: str | None = None,
    element_schema: dict | None = None,
) -> tuple[str, ...]:
    """Capture a container's presence/nullability beside flattened children."""
    notes = [
        f"{wrapper}_nullable={_schema_nullable(schema)}",
        f"{wrapper}_native_type={_native_type(schema)}",
    ]
    if presence is not None:
        notes.insert(0, f"{wrapper}_presence={presence}")
    if element_schema is not None:
        notes.extend([
            f"{wrapper}_element_nullable={_schema_nullable(element_schema)}",
            f"{wrapper}_element_native_type={_native_type(element_schema)}",
        ])
    return tuple(notes)


def _prepare_schema(
    spec: dict,
    schema: object,
    issues: list[str],
    depth: int,
    ref_stack: tuple[str, ...] = (),
) -> dict | None:
    """Resolve and normalize one schema without selecting an ambiguous type."""
    if depth > MAX_SCHEMA_DEPTH:
        _add_issue(
            issues,
            f"OpenAPI schema depth exceeded maximum {MAX_SCHEMA_DEPTH}; deeper fields omitted",
        )
        return None
    if not isinstance(schema, dict):
        _add_issue(issues, "Unsupported OpenAPI schema shape; expected an object")
        return None

    working = dict(schema)
    native_override = str(working.pop("__native_type_override", "") or "")
    nullable_override = working.pop("__nullable_override", None)

    ref = working.get("$ref")
    if ref is not None:
        if not isinstance(ref, str) or not ref.startswith("#/"):
            _add_issue(issues, "External OpenAPI $ref is unsupported; no external fetch performed")
            return None
        if ref in ref_stack:
            _add_issue(issues, "Cyclic OpenAPI $ref is unsupported; recursive fields omitted")
            return None
        resolved = _resolve_ref_checked(spec, ref)
        if resolved is None:
            _add_issue(issues, "Unresolved local OpenAPI $ref; referenced fields omitted")
            return None
        # OpenAPI 3.1 permits siblings next to a $ref.  Applying them here
        # also preserves a local nullable override without changing the
        # historical _resolve_ref helper.
        siblings = {key: value for key, value in working.items() if key != "$ref"}
        working = dict(resolved)
        working.update(siblings)
        prepared = _prepare_schema(
            spec,
            working,
            issues,
            depth + 1,
            (*ref_stack, ref),
        )
        if prepared is None:
            return None
        if native_override:
            prepared["__native_type_override"] = native_override
        if nullable_override is not None:
            prepared["__nullable_override"] = nullable_override
        return prepared

    # OpenAPI 3.1 expresses nullable values as a type union.  A single
    # concrete branch plus null is safe to map; choosing from multiple
    # concrete branches would invent a source type, so leave that field out
    # and make the probe partial instead.
    type_values = _declared_type_values(working)
    if isinstance(working.get("type"), list):
        non_null = [value for value in type_values if value != "null"]
        if len(non_null) != 1 or len(type_values) not in (1, 2):
            _add_issue(
                issues,
                "Unsupported OpenAPI type union with multiple non-null branches; field omitted",
            )
            return None
        native_override = "|".join(type_values)
        working["type"] = non_null[0]
        nullable_override = "null" in type_values

    if working.get("type") == "null":
        _add_issue(
            issues,
            "Unsupported OpenAPI null-only schema; field omitted",
        )
        return None

    for combo_key in ("oneOf", "anyOf"):
        branches = working.get(combo_key)
        if branches is None:
            continue
        if not isinstance(branches, list) or not branches:
            _add_issue(issues, f"Unsupported empty OpenAPI {combo_key} schema; field omitted")
            return None
        non_null_branches = [branch for branch in branches if not _branch_is_null(branch)]
        if len(non_null_branches) != 1:
            _add_issue(
                issues,
                "Unsupported OpenAPI union with multiple non-null branches; field omitted",
            )
            return None
        selected = _prepare_schema(
            spec,
            non_null_branches[0],
            issues,
            depth + 1,
            ref_stack,
        )
        if selected is None:
            return None
        branch_native = _native_type(selected)
        if len(branches) > 1 and any(_branch_is_null(branch) for branch in branches):
            nullable_override = True
            native_override = native_override or f"{branch_native}|null"
        merged = dict(selected)
        # Retain descriptive/constraint siblings while replacing the union
        # keyword itself with its sole representable branch.
        merged.update({key: value for key, value in working.items() if key != combo_key})
        merged.pop("oneOf", None)
        merged.pop("anyOf", None)
        working = merged

    if native_override:
        working["__native_type_override"] = native_override
    if nullable_override is not None:
        working["__nullable_override"] = nullable_override
    return working


def _flatten_schema(
    spec: dict,
    schema: dict,
    prefix: str = "",
    depth: int = 0,
    issues: list[str] | None = None,
    context_notes: tuple[str, ...] = (),
) -> list[dict]:
    """Flatten an OpenAPI schema into field dictionaries.

    ``issues`` is optional to keep the original helper usable by callers that
    only need rows.  The CLI supplies it so unsupported schema pieces produce
    partial evidence instead of silently disappearing.
    """
    issue_list = issues if issues is not None else []
    if depth > MAX_SCHEMA_DEPTH:
        _add_issue(
            issue_list,
            f"OpenAPI schema depth exceeded maximum {MAX_SCHEMA_DEPTH}; deeper fields omitted",
        )
        return []
    prepared = _prepare_schema(spec, schema, issue_list, depth)
    if prepared is None:
        return []

    fields: list[dict] = []
    schema_type = prepared.get("type")

    # allOf is an intersection, so retaining fields from every branch is
    # representable.  oneOf/anyOf were reduced by _prepare_schema when they
    # had exactly one non-null branch.
    all_of = prepared.get("allOf", [])
    if all_of:
        if not isinstance(all_of, list):
            _add_issue(issue_list, "Unsupported OpenAPI allOf shape; branch omitted")
        else:
            for sub in all_of:
                fields.extend(
                    _flatten_schema(
                        spec,
                        sub,
                        prefix,
                        depth + 1,
                        issue_list,
                        context_notes,
                    )
                )

    if schema_type == "array" or "items" in prepared:
        items = prepared.get("items")
        if not isinstance(items, dict) or not items:
            _add_issue(issue_list, "OpenAPI array is missing a representable items schema")
            return fields
        item_schema = _prepare_schema(spec, items, issue_list, depth + 1)
        if item_schema is None:
            return fields
        arr_prefix = f"{prefix}[]" if prefix else "[]"
        if item_schema.get("type") == "object" or "properties" in item_schema or item_schema.get("allOf"):
            fields.extend(
                _flatten_schema(
                    spec,
                    item_schema,
                    arr_prefix + ".",
                    depth + 1,
                    issue_list,
                    context_notes + _wrapper_notes(
                        prepared,
                        wrapper="array",
                        element_schema=item_schema,
                    ),
                )
            )
        else:
            canonical, fmt = _map_openapi_type(item_schema)
            item_native = _native_type(item_schema)
            if _schema_nullable(item_schema) == "true" and "|null" not in item_native:
                item_native = f"{item_native}|null"
            native = f"array<{item_native or canonical}>"
            fields.append({
                "name": arr_prefix.rstrip("."),
                "native_type": native,
                "type": f"array<{canonical}>",
                "format": fmt,
                "precision": "",
                "scale": "",
                "nullable": _schema_nullable(prepared),
                "fk": "",
                "notes": "; ".join(context_notes),
            })
        return fields

    properties = prepared.get("properties", {})
    if properties and not isinstance(properties, dict):
        _add_issue(issue_list, "Unsupported OpenAPI properties shape; fields omitted")
        return fields
    required_fields = {
        str(value) for value in prepared.get("required", [])
    } if isinstance(prepared.get("required", []), list) else set()

    for prop_name, raw_prop_schema in properties.items():
        if not isinstance(prop_name, str):
            _add_issue(issue_list, "Unsupported OpenAPI property name; field omitted")
            continue
        fk_ref = ""
        if isinstance(raw_prop_schema, dict) and isinstance(raw_prop_schema.get("$ref"), str):
            fk_ref = _ref_to_fk(raw_prop_schema["$ref"])
        prop_schema = _prepare_schema(spec, raw_prop_schema, issue_list, depth + 1)
        if prop_schema is None:
            continue

        full_name = f"{prefix}{prop_name}" if prefix else prop_name
        prop_type = prop_schema.get("type")
        presence = "required" if prop_name in required_fields else "optional"
        field_notes = f"presence={presence}"

        if prop_type == "object" or "properties" in prop_schema or prop_schema.get("allOf"):
            fields.extend(
                _flatten_schema(
                    spec,
                    prop_schema,
                    full_name + ".",
                    depth + 1,
                    issue_list,
                    context_notes + _wrapper_notes(
                        prop_schema,
                        wrapper="object",
                        presence=presence,
                    ),
                )
            )
        elif prop_type == "array" or "items" in prop_schema:
            items = prop_schema.get("items")
            if not isinstance(items, dict) or not items:
                _add_issue(issue_list, f"OpenAPI array field {full_name} is missing items schema")
                continue
            item_schema = _prepare_schema(spec, items, issue_list, depth + 1)
            if item_schema is None:
                continue
            arr_name = f"{full_name}[]"
            if item_schema.get("type") == "object" or "properties" in item_schema or item_schema.get("allOf"):
                fields.extend(
                    _flatten_schema(
                        spec,
                        item_schema,
                        arr_name + ".",
                        depth + 1,
                        issue_list,
                        context_notes + _wrapper_notes(
                            prop_schema,
                            wrapper="array",
                            presence=presence,
                            element_schema=item_schema,
                        ),
                    )
                )
            else:
                canonical, fmt = _map_openapi_type(item_schema)
                item_native = _native_type(item_schema)
                if _schema_nullable(item_schema) == "true" and "|null" not in item_native:
                    item_native = f"{item_native}|null"
                fields.append({
                    "name": arr_name,
                    "native_type": f"array<{item_native or canonical}>",
                    "type": f"array<{canonical}>",
                    "format": fmt,
                    "precision": str(item_schema.get("maxLength", "") or ""),
                    "scale": "",
                    "nullable": _schema_nullable(prop_schema),
                    "fk": fk_ref,
                    "notes": "; ".join((*context_notes, field_notes)),
                })
        else:
            canonical, fmt = _map_openapi_type(prop_schema)
            if not _native_type(prop_schema):
                _add_issue(issue_list, f"Unsupported OpenAPI schema for field {full_name}; field omitted")
                continue
            max_len = prop_schema.get("maxLength", "")
            fields.append({
                "name": full_name,
                "native_type": _native_type(prop_schema),
                "type": canonical,
                "format": fmt,
                "precision": str(max_len) if max_len else "",
                "scale": "",
                "nullable": _schema_nullable(prop_schema),
                "fk": fk_ref,
                "notes": "; ".join((*context_notes, field_notes)),
            })

    return fields


def _map_openapi_type(schema: dict) -> tuple[str, str]:
    """Map an OpenAPI property schema to (canonical_type, format)."""
    fmt_val = schema.get("format", "")
    if not isinstance(fmt_val, str):
        fmt_val = str(fmt_val or "")
    values = _declared_type_values(schema)
    non_null = [value for value in values if value != "null"]
    base_type = non_null[0] if len(non_null) == 1 else (values[0] if values else "")

    if fmt_val and fmt_val in OPENAPI_FORMAT_MAP:
        return OPENAPI_FORMAT_MAP[fmt_val]

    canonical = OPENAPI_TYPE_MAP.get(base_type, base_type or "unknown")
    return canonical, fmt_val


def _ref_to_fk(ref: str) -> str:
    """Convert a $ref path to a canonical schema locator."""
    parts = ref.split("/")
    if len(parts) >= 2:
        return parts[-1]
    return ""


def _detect_pagination(params: list[dict]) -> str:
    """Detect pagination type from operation parameters."""
    param_names = {
        p.get("name", "").lower()
        for p in params
        if isinstance(p, dict) and isinstance(p.get("name", ""), str)
    }
    if "cursor" in param_names or "after" in param_names or "page_token" in param_names:
        return "cursor"
    if "offset" in param_names or "skip" in param_names:
        return "offset"
    if "page" in param_names:
        return "page"
    return ""


def _deduplicate_fields(fields: list[dict], issues: list[str]) -> list[dict]:
    """Drop exact ``allOf`` repeats and retain no conflicting guess."""
    by_name: dict[str, dict] = {}
    conflicts: set[str] = set()
    for field in fields:
        name = str(field.get("name", ""))
        if name in conflicts:
            continue
        previous = by_name.get(name)
        if previous is None:
            by_name[name] = field
            continue
        comparable = ("native_type", "type", "format", "precision", "scale", "nullable")
        if all(previous.get(key, "") == field.get(key, "") for key in comparable):
            continue
        _add_issue(
            issues,
            f"Conflicting OpenAPI schemas for field {name}; field omitted",
        )
        by_name.pop(name, None)
        conflicts.add(name)
    return [
        field for field in fields
        if str(field.get("name", "")) not in conflicts
        and by_name.get(str(field.get("name", ""))) is field
    ]


def parse_openapi(
    spec_content: str | dict,
    source: str,
    issues: list[str] | None = None,
) -> list[dict[str, str]]:
    """Parse an OpenAPI spec and return CSV rows (without header).

    ``issues`` is an optional collector for callers that need the CLI's
    complete/partial distinction.  The historical two-argument helper still
    returns only rows.
    """
    issue_list = issues if issues is not None else []
    if isinstance(spec_content, str):
        try:
            spec = json.loads(spec_content)
        except json.JSONDecodeError:
            spec = yaml.safe_load(spec_content)
    else:
        spec = spec_content

    if not isinstance(spec, dict):
        _add_issue(issue_list, "OpenAPI document must be a mapping")
        return []

    rows: list[dict[str, str]] = []
    paths = spec.get("paths", {})
    if not isinstance(paths, dict):
        _add_issue(issue_list, "OpenAPI paths must be a mapping")
        return []

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            _add_issue(issue_list, f"Unsupported OpenAPI path item for {path}; path omitted")
            continue
        for method in ("get", "post", "put", "patch", "delete"):
            operation = path_item.get(method)
            if not operation:
                continue
            if not isinstance(operation, dict):
                _add_issue(issue_list, f"Unsupported OpenAPI operation for {path} {method.upper()}; omitted")
                continue

            # Get response schema
            responses = operation.get("responses", {})
            if not isinstance(responses, dict):
                _add_issue(issue_list, f"Unsupported OpenAPI responses for {path} {method.upper()}; omitted")
                continue
            response_schema = None
            for code in ("200", "201", "default"):
                resp = responses.get(code, {})
                if not isinstance(resp, dict):
                    continue
                content = resp.get("content", {})
                if not isinstance(content, dict):
                    continue
                for mime in ("application/json", "*/*"):
                    if mime in content:
                        media = content[mime]
                        if isinstance(media, dict):
                            response_schema = media.get("schema", {})
                        break
                if response_schema:
                    break

            if not response_schema:
                continue

            # Detect pagination
            operation_params = operation.get("parameters", [])
            path_params = path_item.get("parameters", [])
            if not isinstance(operation_params, list):
                operation_params = []
            if not isinstance(path_params, list):
                path_params = []
            params = operation_params + path_params
            pagination = _detect_pagination(params)

            # Flatten response schema to fields
            fields = _flatten_schema(spec, response_schema, issues=issue_list)
            fields = _deduplicate_fields(fields, issue_list)
            if not fields:
                continue

            for ordinal, field in enumerate(fields, start=1):
                notes_parts = []
                field_note = str(field.get("notes", "")).strip()
                if field_note:
                    notes_parts.append(field_note)
                if pagination:
                    notes_parts.append(f"pagination={pagination}")
                rows.append(make_observation(
                    source=source,
                    object_type="endpoint",
                    object=path,
                    source_operation=method.upper(),
                    column=field["name"],
                    native_type=field.get("native_type", ""),
                    data_type=field["type"],
                    format=field.get("format", ""),
                    precision=field.get("precision", ""),
                    scale=field.get("scale", ""),
                    nullable=field.get("nullable", ""),
                    ordinal=ordinal,
                    reference=field.get("fk", ""),
                    notes="; ".join(notes_parts),
                ))

    return rows


# ---------------------------------------------------------------------------
# GraphQL parsing
# ---------------------------------------------------------------------------

GRAPHQL_SCALAR_MAP = {
    "String": "string",
    "Int": "integer",
    "Float": "double",
    "Boolean": "boolean",
    "ID": "string",
    "DateTime": "timestamp",
    "Date": "date",
    "Time": "time",
    "JSON": "string",
    "BigInt": "long",
    "Decimal": "decimal",
}


def _graphql_named_type(type_obj: object) -> str:
    if not isinstance(type_obj, dict):
        return ""
    kind = type_obj.get("kind")
    if kind in {"NON_NULL", "LIST"}:
        return _graphql_named_type(type_obj.get("ofType"))
    name = type_obj.get("name")
    return str(name) if isinstance(name, str) and name else ""


def _graphql_type_text(type_obj: object, depth: int = 0) -> str:
    if depth > 20 or not isinstance(type_obj, dict):
        return ""
    kind = type_obj.get("kind")
    if kind == "NON_NULL":
        inner = _graphql_type_text(type_obj.get("ofType"), depth + 1)
        return f"{inner}!" if inner else ""
    if kind == "LIST":
        inner = _graphql_type_text(type_obj.get("ofType"), depth + 1)
        return f"[{inner}]" if inner else ""
    if kind in {"SCALAR", "OBJECT", "ENUM", "INPUT_OBJECT"}:
        name = type_obj.get("name")
        return str(name) if isinstance(name, str) and name else ""
    return ""


def _graphql_canonical_type(type_obj: object, depth: int = 0) -> str:
    if depth > 20 or not isinstance(type_obj, dict):
        return ""
    kind = type_obj.get("kind")
    if kind == "NON_NULL":
        return _graphql_canonical_type(type_obj.get("ofType"), depth + 1)
    if kind == "LIST":
        inner = _graphql_canonical_type(type_obj.get("ofType"), depth + 1)
        return f"array<{inner}>" if inner else ""
    name = _graphql_named_type(type_obj)
    return GRAPHQL_SCALAR_MAP.get(name, "struct") if name else ""


def _graphql_is_outer_list(type_obj: object) -> bool:
    if not isinstance(type_obj, dict):
        return False
    if type_obj.get("kind") == "NON_NULL":
        type_obj = type_obj.get("ofType")
    return isinstance(type_obj, dict) and type_obj.get("kind") == "LIST"


def _unwrap_graphql_type(type_obj: dict) -> tuple[str, bool, bool]:
    """Unwrap to ``(base name, outer-list flag, outer non-null flag)``.

    The tuple shape remains compatible with existing callers.  The last flag
    intentionally describes the field/list wrapper only; an element ``!``
    stays in the native type text and does not make the outer list non-null.
    """
    return (
        _graphql_named_type(type_obj) or "String",
        _graphql_is_outer_list(type_obj),
        isinstance(type_obj, dict) and type_obj.get("kind") == "NON_NULL",
    )


def parse_graphql(
    endpoint: str,
    source: str,
    headers: dict | None = None,
    issues: list[str] | None = None,
) -> list[dict[str, str]]:
    """Run GraphQL introspection and return CSV rows."""
    issue_list = issues if issues is not None else []
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)

    resp = requests.post(
        endpoint,
        json={"query": GRAPHQL_INTROSPECTION},
        headers=hdrs,
        timeout=API_REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    data = resp.json()

    if not isinstance(data, dict):
        _add_issue(issue_list, "GraphQL introspection response was not an object")
        return []
    data_payload = data.get("data", {})
    if not isinstance(data_payload, dict):
        _add_issue(issue_list, "GraphQL introspection response has no data object")
        return []
    schema_payload = data_payload.get("__schema", {})
    if not isinstance(schema_payload, dict):
        _add_issue(issue_list, "GraphQL introspection response has no schema object")
        return []
    types = schema_payload.get("types", [])
    if not isinstance(types, list):
        _add_issue(issue_list, "GraphQL introspection returned no usable type list")
        return []

    rows: list[dict[str, str]] = []
    skip_prefixes = ("__",)  # internal introspection types

    for type_def in types:
        if not isinstance(type_def, dict):
            _add_issue(issue_list, "Unsupported GraphQL type definition; type omitted")
            continue
        name = type_def.get("name", "")
        kind = type_def.get("kind", "")
        fields = type_def.get("fields")

        if not fields or kind not in ("OBJECT",) or name.startswith(skip_prefixes):
            continue
        # Skip root types
        if name in ("Query", "Mutation", "Subscription"):
            continue

        for ordinal, field in enumerate(fields, start=1):
            if not isinstance(field, dict) or not isinstance(field.get("name"), str):
                _add_issue(issue_list, f"Unsupported GraphQL field in type {name}; field omitted")
                continue
            field_name = field["name"]
            type_obj = field.get("type")
            native_type = _graphql_type_text(type_obj)
            canonical = _graphql_canonical_type(type_obj)
            if not native_type or not canonical:
                _add_issue(issue_list, f"Unsupported GraphQL type for {name}.{field_name}; field omitted")
                continue
            type_name, is_list, is_non_null = _unwrap_graphql_type(type_obj)

            nullable = "false" if is_non_null else "true"

            # FK: if it resolves to another OBJECT type, it's a reference
            fk = ""
            if type_name not in GRAPHQL_SCALAR_MAP and not is_list:
                fk = type_name

            rows.append(make_observation(
                source=source,
                object_type="graphql_type",
                object=name,
                column=field_name,
                native_type=native_type,
                data_type=canonical,
                nullable=nullable,
                ordinal=ordinal,
                reference=fk,
            ))

    return rows


# ---------------------------------------------------------------------------
# OData parsing
# ---------------------------------------------------------------------------

ODATA_NS = "{http://docs.oasis-open.org/odata/ns/edm}"

EDM_TYPE_MAP = {
    "Edm.String": "string",
    "Edm.Int16": "short",
    "Edm.Int32": "integer",
    "Edm.Int64": "long",
    "Edm.Boolean": "boolean",
    "Edm.Byte": "byte",
    "Edm.SByte": "byte",
    "Edm.Single": "float",
    "Edm.Double": "double",
    "Edm.Decimal": "decimal",
    "Edm.Date": "date",
    "Edm.DateTimeOffset": "timestamp_tz",
    "Edm.TimeOfDay": "time",
    "Edm.Duration": "long",
    "Edm.Guid": "string",
    "Edm.Binary": "binary",
    "Edm.Stream": "binary",
}


def parse_odata(metadata_url: str, source: str, headers: dict | None = None) -> list[dict[str, str]]:
    """Parse OData $metadata EDMX and return CSV rows."""
    hdrs = {"Accept": "application/xml"}
    if headers:
        hdrs.update(headers)

    resp = requests.get(
        metadata_url,
        headers=hdrs,
        timeout=API_REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()

    root = ET.fromstring(resp.text)

    rows: list[dict[str, str]] = []
    # Find all EntityType elements (handle namespace variations)
    for schema_elem in root.iter():
        if schema_elem.tag.endswith("}Schema") or schema_elem.tag == "Schema":
            # Collect entity key properties
            for entity_type in schema_elem:
                if not (entity_type.tag.endswith("}EntityType") or entity_type.tag == "EntityType"):
                    continue

                entity_name = entity_type.get("Name", "")
                # Find key properties
                key_props: set[str] = set()
                for key_elem in entity_type:
                    if key_elem.tag.endswith("}Key") or key_elem.tag == "Key":
                        for prop_ref in key_elem:
                            key_props.add(prop_ref.get("Name", ""))

                ordinal = 0
                for prop in entity_type:
                    if prop.tag.endswith("}Property") or prop.tag == "Property":
                        ordinal += 1
                        prop_name = prop.get("Name", "")
                        edm_type = prop.get("Type", "Edm.String")
                        nullable = prop.get("Nullable", "true").lower()
                        max_length = prop.get("MaxLength", "")
                        precision = prop.get("Precision", "")
                        scale = prop.get("Scale", "")

                        canonical = EDM_TYPE_MAP.get(edm_type, edm_type)
                        fmt = ""

                        # Handle decimal precision
                        if canonical == "decimal" and precision:
                            s = scale or "0"
                            canonical = f"decimal({precision},{s})"

                        # maxLength → precision for strings
                        prec = max_length or precision or ""
                        scl = scale or ""

                        is_pk = "true" if prop_name in key_props else ""

                        rows.append(make_observation(
                            source=source,
                            object_type="entity",
                            object=entity_name,
                            column=prop_name,
                            native_type=edm_type,
                            data_type=canonical,
                            format=fmt,
                            precision=prec,
                            scale=scl,
                            nullable=nullable,
                            ordinal=ordinal,
                            key="primary" if is_pk else "",
                        ))

                    elif prop.tag.endswith("}NavigationProperty") or prop.tag == "NavigationProperty":
                        # Navigation properties → FK references
                        ordinal += 1
                        nav_name = prop.get("Name", "")
                        nav_type = prop.get("Type", "")
                        # Clean collection wrapper
                        ref_entity = nav_type.replace("Collection(", "").rstrip(")")
                        ref_entity = ref_entity.split(".")[-1] if "." in ref_entity else ref_entity
                        nullable_val = prop.get("Nullable", "true").lower()

                        is_collection = "Collection(" in nav_type
                        canonical = f"array<{ref_entity}>" if is_collection else "struct"

                        rows.append(make_observation(
                            source=source,
                            object_type="entity",
                            object=entity_name,
                            column=nav_name,
                            native_type=nav_type,
                            data_type=canonical,
                            nullable=nullable_val,
                            ordinal=ordinal,
                            reference=ref_entity,
                            notes="navigation property",
                        ))

    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _load_spec(path_or_url: str, headers: dict | None = None) -> str:
    """Load spec content from a file path or URL."""
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        hdrs = {}
        if headers:
            hdrs.update(headers)
        resp = requests.get(
            path_or_url,
            headers=hdrs,
            timeout=API_REQUEST_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        return resp.text

    p = Path(path_or_url)
    if not p.exists():
        print(f"ERROR: File not found: {path_or_url}", file=sys.stderr)
        sys.exit(1)
    return p.read_text(encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description="Introspect API specs and output a standardised schema CSV.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--spec", help="OpenAPI spec URL or file path (JSON/YAML)")
    group.add_argument("--graphql", help="GraphQL endpoint URL")
    group.add_argument("--odata", help="OData $metadata endpoint URL")

    parser.add_argument("--source", required=True, help="Source name for CSV output")
    parser.add_argument("--headers-env", default=None, help="Environment variable containing JSON headers")
    parser.add_argument("--output", default=None, help="Output CSV file (default: stdout)")
    parser.add_argument("--status-output", default=None, help="Optional probe status JSON path")

    return parser.parse_args(argv)


def _validate_output_paths(args: argparse.Namespace) -> None:
    output_path = Path(args.output).resolve() if args.output else None
    status_path = Path(args.status_output).resolve() if args.status_output else None
    if output_path is not None and output_path == status_path:
        raise ValueError("Probe status output must be distinct from observation output")

    # A local OpenAPI document is an input artifact.  Refuse aliases before
    # reading/writing so a status or observation artifact cannot replace it.
    if args.spec and "://" not in args.spec:
        input_path = Path(args.spec).resolve()
        if output_path == input_path or status_path == input_path:
            raise ValueError("Probe outputs must be distinct from the local OpenAPI spec")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    locator = args.spec or args.graphql or args.odata
    try:
        _validate_output_paths(args)
        validate_nonsecret_locator(locator, "API locator")
    except ValueError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc

    headers: dict[str, str] | None = None
    if args.headers_env:
        raw_headers = os.environ.get(args.headers_env)
        if raw_headers is None:
            raise SystemExit(f"ERROR: Environment variable '{args.headers_env}' is not set")
        try:
            parsed_headers = json.loads(raw_headers)
        except json.JSONDecodeError as exc:
            raise SystemExit("ERROR: Headers environment value is not valid JSON") from exc
        if not isinstance(parsed_headers, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in parsed_headers.items()
        ):
            raise SystemExit("ERROR: Headers environment value must be a JSON object of strings")
        headers = parsed_headers

    rows: list[dict[str, str]] = []
    issues: list[str] = []

    try:
        if args.spec:
            content = _load_spec(args.spec, headers)
            rows = parse_openapi(content, args.source, issues)
            probe = "openapi"
        elif args.graphql:
            rows = parse_graphql(args.graphql, args.source, headers, issues)
            probe = "graphql-introspection"
        elif args.odata:
            rows = parse_odata(args.odata, args.source, headers)
            probe = "odata-metadata"
    except requests.RequestException as exc:
        print(f"ERROR: API request failed ({type(exc).__name__})", file=sys.stderr)
        sys.exit(1)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        print(f"ERROR: Failed to parse spec: {exc}", file=sys.stderr)
        sys.exit(1)
    except ET.ParseError as exc:
        print(f"ERROR: Failed to parse OData XML: {exc}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        _add_issue(issues, "No schema fields extracted from the requested API surface")
    for issue in issues:
        print(f"WARNING: {issue}", file=sys.stderr)

    output_path = Path(args.output) if args.output else None
    if args.output:
        atomic_write_observations(output_path, rows)
    else:
        write_observations(sys.stdout, rows)
    scope_kind = "source" if not issues else "objects"
    raw_scope = observation_scope(rows, kind=scope_kind, filters={})
    # Status JSON is a transport contract: object identities are arrays of
    # the first six observation key fields, even when a helper returns tuples.
    scope = {
        "kind": scope_kind,
        "filters": dict(raw_scope.get("filters", {})),
        "objects": [list(identity) for identity in raw_scope.get("objects", [])],
    }
    status = write_probe_status(
        Path(args.status_output) if args.status_output else None,
        source=args.source,
        probe=probe,
        row_count=len(rows),
        issues=issues,
        output_path=output_path,
        scope=scope,
        details={
            "bounds": {
                "max_depth": MAX_SCHEMA_DEPTH,
                "request_timeout_seconds": API_REQUEST_TIMEOUT_SECONDS,
            },
        },
    )
    if status == "partial":
        raise SystemExit(PARTIAL_EXIT_CODE)


if __name__ == "__main__":
    main()
