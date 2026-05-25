"""API introspection — parse OpenAPI specs, GraphQL schemas, OData metadata.

Supports: OpenAPI 3.x JSON/YAML (file or URL), GraphQL introspection, OData $metadata.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from _types import ApiEndpoint, ApiParameter, SourceResult


def introspect_api(spec_url: str, api_type: str = "openapi",
                   auth_mode: str | None = None,
                   api_key: str | None = None,
                   api_key_header: str = "X-API-Key",
                   token: str | None = None,
                   sample_call: bool = False) -> SourceResult:
    """Parse API spec and extract endpoint/schema info.

    api_type: 'openapi' (default), 'graphql', 'odata'
    auth_mode: None, 'api_key', 'bearer', 'basic'
    """
    # Resolve env vars in credentials
    import os
    if api_key and api_key.startswith("$"):
        api_key = os.environ.get(api_key[1:], api_key)
    if token and token.startswith("$"):
        token = os.environ.get(token[1:], token)

    headers = _build_auth_headers(auth_mode, api_key, api_key_header, token)

    if api_type == "graphql":
        return _introspect_graphql(spec_url, headers)
    elif api_type == "odata":
        return _introspect_odata(spec_url, headers)
    else:
        result = _introspect_openapi(spec_url, headers)
        if sample_call and result.endpoints:
            _sample_call_inference(result, spec_url, headers)
        return result


def _build_auth_headers(auth_mode: str | None, api_key: str | None,
                        api_key_header: str, token: str | None) -> dict:
    """Build HTTP headers for auth."""
    headers = {"User-Agent": "datacoolie-discover/1.0"}
    if auth_mode == "api_key" and api_key:
        headers[api_key_header] = api_key
    elif auth_mode == "bearer" and token:
        headers["Authorization"] = f"Bearer {token}"
    elif auth_mode == "basic" and token:
        import base64
        headers["Authorization"] = f"Basic {base64.b64encode(token.encode()).decode()}"
    return headers


# ---------------------------------------------------------------------------
# OpenAPI introspection
# ---------------------------------------------------------------------------

def _introspect_openapi(spec_url: str, headers: dict) -> SourceResult:
    """Parse OpenAPI spec and extract endpoint/schema info."""
    result = SourceResult(
        source_type="api",
        spec_url=spec_url,
        api_type="openapi",
    )

    spec_content = _fetch_content(spec_url, headers)
    spec = _parse_spec_content(spec_content)

    paths = spec.get("paths", {})
    for path, methods in paths.items():
        for method, details in methods.items():
            if method in ("get", "post", "put", "patch", "delete"):
                parameters = [
                    ApiParameter(
                        name=param.get("name", ""),
                        location=param.get("in", "query"),
                        param_type=param.get("schema", {}).get("type", "string") if isinstance(param.get("schema"), dict) else "string",
                        required=param.get("required", False),
                    )
                    for param in details.get("parameters", [])
                ]

                responses = details.get("responses", {})
                success = responses.get("200") or responses.get("201") or {}
                content = success.get("content", {})
                json_resp = content.get("application/json", {})
                response_schema = None
                if "schema" in json_resp:
                    response_schema = _simplify_schema(json_resp["schema"], spec)

                # Detect pagination
                pagination_type = _detect_pagination(parameters, response_schema)

                endpoint = ApiEndpoint(
                    path=path,
                    method=method.upper(),
                    summary=details.get("summary", ""),
                    parameters=parameters,
                    response_schema=response_schema,
                    pagination_type=pagination_type,
                )
                result.endpoints.append(endpoint)

    return result


def _detect_pagination(params: list[ApiParameter], schema: dict | None) -> str | None:
    """Detect pagination style from params and response schema."""
    param_names = {p.name.lower() for p in params}
    # Check cursor first (more specific)
    if "cursor" in param_names or "after" in param_names:
        return "cursor"
    if schema and isinstance(schema, dict):
        props = schema.get("properties", {})
        if "next_cursor" in props or "next_page_token" in props:
            return "cursor"
        if "next" in props:
            return "link"
    # Offset patterns
    if {"page", "per_page"} <= param_names or {"page", "page_size"} <= param_names:
        return "offset"
    if {"limit", "offset"} <= param_names:
        return "offset"
    if "page" in param_names:
        return "offset"
    return None


# ---------------------------------------------------------------------------
# GraphQL introspection
# ---------------------------------------------------------------------------

def _introspect_graphql(endpoint_url: str, headers: dict) -> SourceResult:
    """Introspect a GraphQL endpoint via __schema query."""
    result = SourceResult(
        source_type="api",
        spec_url=endpoint_url,
        api_type="graphql",
    )

    query = """
    {
      __schema {
        queryType { name }
        types {
          name
          kind
          fields {
            name
            type { name kind ofType { name kind } }
            args { name type { name kind } }
          }
        }
      }
    }
    """

    import urllib.request
    body = json.dumps({"query": query}).encode("utf-8")
    req = urllib.request.Request(
        endpoint_url,
        data=body,
        headers={**headers, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"ERROR: GraphQL introspection failed for {endpoint_url}: {e}", file=sys.stderr)
        sys.exit(1)

    schema = data.get("data", {}).get("__schema", {})
    types = schema.get("types", [])

    # Extract user-defined object types (exclude built-in __ types and scalars)
    for t in types:
        if t["name"].startswith("__") or t["kind"] != "OBJECT":
            continue
        fields = t.get("fields") or []
        if not fields:
            continue

        parameters = [
            ApiParameter(
                name=f["name"],
                location="field",
                param_type=_graphql_type_name(f.get("type", {})),
                required=False,
            )
            for f in fields
        ]

        endpoint = ApiEndpoint(
            path=t["name"],
            method="QUERY",
            summary=f"GraphQL type: {t['name']}",
            parameters=parameters,
            response_schema={"type": "object", "properties": {f["name"]: _graphql_type_name(f.get("type", {})) for f in fields}},
        )
        result.endpoints.append(endpoint)

    return result


def _graphql_type_name(type_info: dict) -> str:
    """Extract human-readable type name from GraphQL type info."""
    if type_info.get("name"):
        return type_info["name"]
    of_type = type_info.get("ofType", {})
    if of_type and of_type.get("name"):
        kind = type_info.get("kind", "")
        prefix = "LIST:" if kind == "LIST" else ""
        return f"{prefix}{of_type['name']}"
    return "unknown"


# ---------------------------------------------------------------------------
# OData introspection
# ---------------------------------------------------------------------------

def _introspect_odata(metadata_url: str, headers: dict) -> SourceResult:
    """Parse OData $metadata XML and extract entity types."""
    result = SourceResult(
        source_type="api",
        spec_url=metadata_url,
        api_type="odata",
    )

    content = _fetch_content(metadata_url, headers)

    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(content)
    except Exception as e:
        print(f"ERROR: Cannot parse OData metadata XML: {e}", file=sys.stderr)
        sys.exit(1)

    # OData namespace varies; find EntityType elements regardless of namespace
    ns = {"edm": "http://docs.oasis-open.org/odata/ns/edm"}
    # Try common namespaces
    entity_types = root.findall(".//{http://docs.oasis-open.org/odata/ns/edm}EntityType")
    if not entity_types:
        entity_types = root.findall(".//{http://schemas.microsoft.com/ado/2009/11/edm}EntityType")
    if not entity_types:
        # Fallback: find any element named EntityType
        entity_types = [el for el in root.iter() if el.tag.endswith("EntityType")]

    for et in entity_types:
        name = et.get("Name", "Unknown")
        properties = []
        for prop in et:
            if prop.tag.endswith("Property") or prop.tag.endswith("NavigationProperty"):
                properties.append(ApiParameter(
                    name=prop.get("Name", ""),
                    location="property",
                    param_type=prop.get("Type", "Edm.String"),
                    required=prop.get("Nullable", "true").lower() == "false",
                ))

        endpoint = ApiEndpoint(
            path=f"/{name}",
            method="GET",
            summary=f"OData EntityType: {name}",
            parameters=properties,
        )
        result.endpoints.append(endpoint)

    return result


# ---------------------------------------------------------------------------
# Sample call inference (REST)
# ---------------------------------------------------------------------------

def _sample_call_inference(result: SourceResult, base_url: str, headers: dict) -> None:
    """Try calling GET endpoints to infer response schema from live data."""
    import urllib.request
    from urllib.parse import urljoin

    # Only sample GET endpoints without path params
    get_endpoints = [ep for ep in result.endpoints
                     if ep.method == "GET" and "{" not in ep.path]

    for ep in get_endpoints[:5]:  # limit to 5 calls
        url = urljoin(base_url.rsplit("/", 1)[0] + "/", ep.path.lstrip("/"))
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
                body = json.loads(resp.read().decode("utf-8"))
            if isinstance(body, list) and body:
                ep.response_schema = {"type": "array", "items": {"type": "object", "properties": {k: type(v).__name__ for k, v in body[0].items()}}}
            elif isinstance(body, dict):
                # Look for data wrapper patterns
                for key in ("data", "results", "items", "records", "value"):
                    if key in body and isinstance(body[key], list) and body[key]:
                        ep.response_schema = {"type": "array", "items": {"type": "object", "properties": {k: type(v).__name__ for k, v in body[key][0].items()}}}
                        break
                else:
                    ep.response_schema = {"type": "object", "properties": {k: type(v).__name__ for k, v in body.items()}}
        except Exception:
            continue  # sample call failed — skip silently


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _fetch_content(url_or_path: str, headers: dict) -> str:
    """Fetch content from file path or URL."""
    import urllib.request

    spec_path = Path(url_or_path)
    if spec_path.exists():
        with open(spec_path, encoding="utf-8") as f:
            return f.read()

    req = urllib.request.Request(url_or_path, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"ERROR: Cannot fetch from {url_or_path}: {e}", file=sys.stderr)
        sys.exit(1)


def _parse_spec_content(content: str) -> dict:
    """Parse JSON or YAML content."""
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        try:
            import yaml
            return yaml.safe_load(content)
        except ImportError:
            print("ERROR: YAML spec detected but PyYAML not installed.", file=sys.stderr)
            sys.exit(1)


def _simplify_schema(schema: dict, root_spec: dict, depth: int = 0) -> dict | None:
    """Recursively resolve $ref and simplify schema to field list."""
    if depth > 5:
        return {"type": "object", "note": "max depth reached"}

    if "$ref" in schema:
        ref_path = schema["$ref"].lstrip("#/").split("/")
        resolved = root_spec
        for part in ref_path:
            resolved = resolved.get(part, {})
        return _simplify_schema(resolved, root_spec, depth + 1)

    if schema.get("type") == "array":
        items = schema.get("items", {})
        return {"type": "array", "items": _simplify_schema(items, root_spec, depth + 1)}

    if schema.get("type") == "object" or "properties" in schema:
        props = {}
        for name, prop in schema.get("properties", {}).items():
            props[name] = prop.get("type", "string")
        return {"type": "object", "properties": props}

    return {"type": schema.get("type", "unknown")}
