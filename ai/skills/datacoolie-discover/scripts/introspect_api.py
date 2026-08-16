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
from _probe_status import PARTIAL_EXIT_CODE, write_probe_status

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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


def _resolve_ref(spec: dict, ref: str) -> dict:
    """Resolve a $ref path in an OpenAPI spec."""
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        part = part.replace("~1", "/").replace("~0", "~")
        node = node.get(part, {})
    return node


def _flatten_schema(
    spec: dict,
    schema: dict,
    prefix: str = "",
    depth: int = 0,
) -> list[dict]:
    """Flatten an OpenAPI schema into a list of field dicts."""
    if depth > 10:
        return []

    # Resolve $ref
    if "$ref" in schema:
        schema = _resolve_ref(spec, schema["$ref"])

    fields = []
    schema_type = schema.get("type", "object")
    properties = schema.get("properties", {})
    required_fields = set(schema.get("required", []))

    # allOf / oneOf / anyOf
    for combo_key in ("allOf", "oneOf", "anyOf"):
        for sub in schema.get(combo_key, []):
            fields.extend(_flatten_schema(spec, sub, prefix, depth + 1))

    if schema_type == "array":
        items = schema.get("items", {})
        if "$ref" in items:
            items = _resolve_ref(spec, items["$ref"])
        arr_prefix = f"{prefix}[]" if prefix else "[]"
        if items.get("type") == "object" or "properties" in items:
            fields.extend(_flatten_schema(spec, items, arr_prefix + ".", depth + 1))
        else:
            # Scalar array — just note the element type
            canonical, fmt = _map_openapi_type(items)
            fields.append({
                "name": arr_prefix.rstrip("."),
                "native_type": f"array<{items.get('type', 'string')}>",
                "type": f"array<{canonical}>",
                "format": fmt,
                "precision": "",
                "scale": "",
                "nullable": "true",
                "fk": "",
            })
        return fields

    for prop_name, prop_schema in properties.items():
        if "$ref" in prop_schema:
            ref_path = prop_schema["$ref"]
            prop_schema = _resolve_ref(spec, ref_path)
            fk_ref = _ref_to_fk(ref_path)
        else:
            fk_ref = ""

        full_name = f"{prefix}{prop_name}" if prefix else prop_name
        prop_type = prop_schema.get("type", "string")

        if prop_type == "object" or "properties" in prop_schema:
            fields.extend(_flatten_schema(spec, prop_schema, full_name + ".", depth + 1))
        elif prop_type == "array":
            items = prop_schema.get("items", {})
            if "$ref" in items:
                items_ref = items["$ref"]
                items = _resolve_ref(spec, items_ref)
                fk_ref = _ref_to_fk(items_ref) or fk_ref
            arr_name = f"{full_name}[]"
            if items.get("type") == "object" or "properties" in items:
                fields.extend(_flatten_schema(spec, items, arr_name + ".", depth + 1))
            else:
                canonical, fmt = _map_openapi_type(items)
                fields.append({
                    "name": arr_name,
                    "native_type": f"array<{items.get('type', 'string')}>",
                    "type": f"array<{canonical}>",
                    "format": fmt,
                    "precision": str(items.get("maxLength", "")),
                    "scale": "",
                    "nullable": "true" if prop_name not in required_fields else "false",
                    "fk": fk_ref,
                })
        else:
            canonical, fmt = _map_openapi_type(prop_schema)
            max_len = prop_schema.get("maxLength", "")
            fields.append({
                "name": full_name,
                "native_type": prop_schema.get("type", "string"),
                "type": canonical,
                "format": fmt,
                "precision": str(max_len) if max_len else "",
                "scale": "",
                "nullable": "true" if prop_name not in required_fields else "false",
                "fk": fk_ref,
            })

    return fields


def _map_openapi_type(schema: dict) -> tuple[str, str]:
    """Map an OpenAPI property schema to (canonical_type, format)."""
    fmt_val = schema.get("format", "")
    base_type = schema.get("type", "string")

    if fmt_val and fmt_val in OPENAPI_FORMAT_MAP:
        return OPENAPI_FORMAT_MAP[fmt_val]

    canonical = OPENAPI_TYPE_MAP.get(base_type, base_type)
    return canonical, fmt_val


def _ref_to_fk(ref: str) -> str:
    """Convert a $ref path to a canonical schema locator."""
    parts = ref.split("/")
    if len(parts) >= 2:
        return parts[-1]
    return ""


def _detect_pagination(params: list[dict]) -> str:
    """Detect pagination type from operation parameters."""
    param_names = {p.get("name", "").lower() for p in params}
    if "cursor" in param_names or "after" in param_names or "page_token" in param_names:
        return "cursor"
    if "offset" in param_names or "skip" in param_names:
        return "offset"
    if "page" in param_names:
        return "page"
    return ""


def parse_openapi(spec_content: str | dict, source: str) -> list[dict[str, str]]:
    """Parse an OpenAPI spec and return CSV rows (without header)."""
    if isinstance(spec_content, str):
        try:
            spec = json.loads(spec_content)
        except json.JSONDecodeError:
            spec = yaml.safe_load(spec_content)
    else:
        spec = spec_content

    rows: list[dict[str, str]] = []
    paths = spec.get("paths", {})

    for path, path_item in paths.items():
        for method in ("get", "post", "put", "patch", "delete"):
            operation = path_item.get(method)
            if not operation:
                continue

            # Get response schema
            responses = operation.get("responses", {})
            response_schema = None
            for code in ("200", "201", "default"):
                resp = responses.get(code, {})
                content = resp.get("content", {})
                for mime in ("application/json", "*/*"):
                    if mime in content:
                        response_schema = content[mime].get("schema", {})
                        break
                if response_schema:
                    break

            if not response_schema:
                continue

            # Detect pagination
            params = operation.get("parameters", []) + path_item.get("parameters", [])
            pagination = _detect_pagination(params)

            # Flatten response schema to fields
            fields = _flatten_schema(spec, response_schema)
            if not fields:
                continue

            # Build notes
            notes_parts = []
            if pagination:
                notes_parts.append(f"pagination={pagination}")
            notes = "; ".join(notes_parts)

            for ordinal, field in enumerate(fields, start=1):
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
                    nullable=field.get("nullable", "true"),
                    ordinal=ordinal,
                    reference=field.get("fk", ""),
                    notes=notes,
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


def _unwrap_graphql_type(type_obj: dict) -> tuple[str, bool, bool]:
    """Unwrap a GraphQL type to (type_name, is_list, is_non_null)."""
    is_list = False
    is_non_null = False
    t = type_obj

    while t:
        kind = t.get("kind", "")
        if kind == "NON_NULL":
            is_non_null = True
            t = t.get("ofType", {})
        elif kind == "LIST":
            is_list = True
            t = t.get("ofType", {})
        else:
            name = t.get("name") or "String"
            return name, is_list, is_non_null

    return "String", is_list, is_non_null


def parse_graphql(endpoint: str, source: str, headers: dict | None = None) -> list[dict[str, str]]:
    """Run GraphQL introspection and return CSV rows."""
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)

    resp = requests.post(
        endpoint,
        json={"query": GRAPHQL_INTROSPECTION},
        headers=hdrs,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    types = data.get("data", {}).get("__schema", {}).get("types", [])

    rows: list[dict[str, str]] = []
    skip_prefixes = ("__",)  # internal introspection types

    for type_def in types:
        name = type_def.get("name", "")
        kind = type_def.get("kind", "")
        fields = type_def.get("fields")

        if not fields or kind not in ("OBJECT",) or name.startswith(skip_prefixes):
            continue
        # Skip root types
        if name in ("Query", "Mutation", "Subscription"):
            continue

        for ordinal, field in enumerate(fields, start=1):
            field_name = field["name"]
            type_name, is_list, is_non_null = _unwrap_graphql_type(field["type"])

            canonical = GRAPHQL_SCALAR_MAP.get(type_name, "struct")
            if is_list:
                canonical = f"array<{canonical}>"

            nullable = "false" if is_non_null else "true"

            # FK: if it resolves to another OBJECT type, it's a reference
            fk = ""
            if type_name not in GRAPHQL_SCALAR_MAP and not is_list:
                fk = type_name

            native_type = f"[{type_name}]" if is_list else type_name
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

    resp = requests.get(metadata_url, headers=hdrs, timeout=30)
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
        resp = requests.get(path_or_url, headers=hdrs, timeout=30)
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


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    locator = args.spec or args.graphql or args.odata
    try:
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

    try:
        if args.spec:
            content = _load_spec(args.spec, headers)
            rows = parse_openapi(content, args.source)
            probe = "openapi"
        elif args.graphql:
            rows = parse_graphql(args.graphql, args.source, headers)
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

    issues = [] if rows else ["No schema fields extracted from the requested API surface"]
    for issue in issues:
        print(f"WARNING: {issue}", file=sys.stderr)

    if args.output:
        atomic_write_observations(Path(args.output), rows)
    else:
        write_observations(sys.stdout, rows)
    status = write_probe_status(
        Path(args.status_output) if args.status_output else None,
        source=args.source,
        probe=probe,
        row_count=len(rows),
        issues=issues,
    )
    if status == "partial":
        raise SystemExit(PARTIAL_EXIT_CODE)


if __name__ == "__main__":
    main()
