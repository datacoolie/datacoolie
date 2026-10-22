"""Stable and generated identity helpers."""

from __future__ import annotations

import copy
import json
import uuid
from typing import Any


_DATACOOLIE_NS = uuid.UUID("da7ac001-e000-4000-8000-000000000000")


def is_usable_identifier(value: object) -> bool:
    """Return whether *value* can be used as an authored identity.

    Identity fields are strings in the public metadata contract.  Treating
    whitespace-only values as absent prevents a document from receiving a
    deterministic ID for a label that cannot be resolved by a user.
    """

    return isinstance(value, str) and bool(value.strip())


def generate_unique_id(prefix: str = "") -> str:
    """Generate a UUID-4 string, optionally prefixed.

    Args:
        prefix: Optional prefix separated by ``_``.

    Returns:
        Unique identifier string.
    """
    uid = str(uuid.uuid4())
    return f"{prefix}_{uid}" if prefix else uid


def normalize_workspace_id(value: object) -> str | None:
    """Normalize an optional scope without changing authored entity names."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("workspace_id must be a string or null")
    return value.strip() or None


def name_to_uuid(name: str, workspace_id: str | None = None) -> str:
    """Derive a stable UUID from the exact name and optional workspace."""
    if not is_usable_identifier(name):
        raise ValueError("Identity name must be a non-empty string")
    workspace = normalize_workspace_id(workspace_id)
    value = (
        json.dumps(["workspace", workspace, name], ensure_ascii=False, separators=(",", ":"))
        if workspace is not None else name
    )
    return str(uuid.uuid5(_DATACOOLIE_NS, value))


def normalize_document_workspace(
    document: dict[str, Any], default_workspace: str | None = None,
) -> dict[str, Any]:
    """Copy a document and establish scope before generating IDs or resolving refs.

    A single explicit scope supplies omitted top-level scopes. Mixed documents
    must scope every top-level entity. Inline endpoints inherit their flow scope.
    A provider binding supplies a default and rejects foreign explicit scopes.
    Shape validation remains the document/model owner's responsibility.
    """
    result = copy.deepcopy(document)
    default = normalize_workspace_id(default_workspace)
    entities = [
        row for section in ("connections", "dataflows")
        for row in result.get(section, []) if isinstance(row, dict)
    ]
    scopes = {normalize_workspace_id(row.get("workspace_id")) for row in entities}
    explicit = scopes - {None}
    if default is not None and explicit - {default}:
        raise ValueError("Metadata workspace does not match provider workspace")
    if len(explicit) > 1 and None in scopes:
        raise ValueError("Mixed-workspace metadata requires workspace_id on every entity")
    inherited = default or (next(iter(explicit)) if len(explicit) == 1 else None)
    for row in entities:
        scope = normalize_workspace_id(row.get("workspace_id")) or inherited
        if scope is not None or "workspace_id" in row:
            row["workspace_id"] = scope
    for flow in result.get("dataflows", []):
        if not isinstance(flow, dict):
            continue
        scope = normalize_workspace_id(flow.get("workspace_id"))
        for role in ("source", "destination"):
            endpoint = flow.get(role)
            inline = endpoint.get("connection") if isinstance(endpoint, dict) else None
            if not isinstance(inline, dict):
                continue
            connection_scope = normalize_workspace_id(inline.get("workspace_id"))
            if connection_scope is not None and connection_scope != scope:
                raise ValueError(f"Dataflow {flow.get('name')!r} {role} workspace mismatch")
            if scope is not None or "workspace_id" in inline:
                inline["workspace_id"] = scope
    return result
