"""Read exact metadata files and ordered scenario manifests in memory."""
from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from runner.metadata_document_rules import (
    MetadataCompositionError, _hint_group_key, _read_json,
    _require_list, _root_path, _source_candidate,
)


def load_metadata(path, root=None):
    """Read a plain document or a scenario manifest without writing artifacts."""
    root_path = _root_path(root)
    caller = Path(path).expanduser()
    candidate = caller.resolve() if caller.is_file() else _source_candidate(path, root_path)
    raw = _read_json(candidate)
    # JSON parsing owns a fresh tree on every call; no cached/input object is
    # shared with the caller. Return that tree without copying it again.
    if not isinstance(raw, dict):
        return raw
    if "shared_metadata" in raw:
        raise MetadataCompositionError(f"{candidate}: obsolete shared_metadata; select shared section files explicitly")
    if "metadata_documents" not in raw:
        return raw
    # Only manifests are simulator-root-bound. Plain deployed files remain valid.
    _source_candidate(candidate, root_path)
    unknown = set(raw) - {"metadata_documents", "flow_order", "connections_path", "schema_hints_path", "connection_order"}
    if unknown:
        raise MetadataCompositionError(f"{candidate}: unsupported workflow keys: {sorted(unknown)}")
    references = _require_list(raw, "metadata_documents", candidate)
    if not references:
        raise MetadataCompositionError(f"{candidate}: metadata_documents must not be empty")
    paths = []
    for reference in references:
        if not isinstance(reference, str) or not reference.strip():
            raise MetadataCompositionError(f"{candidate}: references must be non-empty local paths")
        source = _source_candidate(reference, root_path)
        document = _read_json(source)
        if isinstance(document, dict) and {"metadata_documents", "shared_metadata"} & document.keys():
            raise MetadataCompositionError(f"{candidate}: workflow references must be plain group files: {reference}")
        paths.append(source)
    order = _require_list(raw, "flow_order", candidate)
    if any(not isinstance(name, str) or not name for name in order) or len(order) != len(set(order)):
        raise MetadataCompositionError(f"{candidate}: flow_order must contain unique non-empty names")
    section_paths = {key: _source_candidate(raw[key], root_path)
                     for key in ("connections_path", "schema_hints_path") if raw.get(key)}
    document = load_metadata_files(paths, root=root_path, **section_paths)
    flow_rows = {}
    for row in document["dataflows"]:
        flow_rows.setdefault(row["name"], []).append(row)
    ambiguous = sorted(name for name, rows in flow_rows.items() if len(rows) > 1)
    if ambiguous:
        raise MetadataCompositionError(
            f"{candidate}: flow_order names are ambiguous across workspaces: {ambiguous}"
        )
    flows = {name: rows[0] for name, rows in flow_rows.items()}
    missing = [name for name in order if name not in flows]
    if missing:
        raise MetadataCompositionError(f"{candidate}: unknown flow_order names: {missing}")
    document["dataflows"] = [flows[name] for name in order]
    connection_order = raw.get("connection_order")
    if connection_order is not None:
        if not isinstance(connection_order, list) or any(not isinstance(name, str) or not name for name in connection_order) or len(connection_order) != len(set(connection_order)):
            raise MetadataCompositionError(f"{candidate}: invalid connection_order")
        connection_rows = {}
        for row in document["connections"]:
            connection_rows.setdefault(row["name"], []).append(row)
        ambiguous = sorted(name for name, rows in connection_rows.items() if len(rows) > 1)
        if ambiguous:
            raise MetadataCompositionError(
                f"{candidate}: connection_order names are ambiguous across workspaces: {ambiguous}"
            )
        connections = {name: rows[0] for name, rows in connection_rows.items()}
        connections_by_id = {
            row.get("connection_id"): row for row in document["connections"]
        }
        required = set()
        for row in document["dataflows"]:
            for side in ("source", "destination"):
                endpoint = row.get(side, {})
                inline = endpoint.get("connection") if isinstance(endpoint, dict) else None
                name = endpoint.get("connection_name") if isinstance(endpoint, dict) else None
                if not name and isinstance(inline, dict):
                    name = inline.get("name")
                if not name:
                    connection_id = (endpoint.get("connection_id") if isinstance(endpoint, dict) else None) or row.get(
                        f"{side}_connection_id"
                    )
                    if not connection_id and isinstance(inline, dict):
                        connection_id = inline.get("connection_id")
                    owner = connections_by_id.get(connection_id)
                    name = owner.get("name") if owner else None
                if name:
                    required.add(name)
                else:
                    raise MetadataCompositionError(
                        f"{candidate}: connection_order cannot resolve {side} reference"
                    )
        if not required <= set(connection_order) or not set(connection_order) <= connections.keys():
            raise MetadataCompositionError(f"{candidate}: connection_order cannot resolve selected flows")
        document["connections"] = [connections[name] for name in connection_order]
        ids = {row.get("connection_id") for row in document["connections"] if row.get("connection_id")}
        document["schema_hints"] = [row for row in document["schema_hints"]
            if row.get("connection_name") in connection_order or row.get("connection_id") in ids]
    return document


def load_metadata_files(
    paths: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
    *,
    connections_path: str | os.PathLike[str] | None = None,
    schema_hints_path: str | os.PathLike[str] | None = None,
    root: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Assemble exact documents in memory for exports and simulator inspection.

    Section paths replace their sections, matching FileProvider's public input
    contract. Ordinary execution uses FileProvider directly; this helper neither
    writes files nor replaces provider model validation.
    """
    sources = [paths] if isinstance(paths, (str, os.PathLike)) else list(paths)
    if not sources or any(not str(path).strip() for path in sources):
        raise MetadataCompositionError("metadata paths must be a non-empty exact file list")
    merged: dict[str, Any] = {"connections": [], "dataflows": [], "schema_hints": []}
    for source in sources:
        document = load_metadata(source, root=root)
        if not isinstance(document, dict):
            raise MetadataCompositionError(f"{source}: metadata must be a JSON object")
        for section in merged:
            rows = document.get(section, [])
            if not isinstance(rows, list):
                raise MetadataCompositionError(f"{source}: '{section}' must be a list")
            # load_metadata transfers freshly parsed rows to this result.
            merged[section].extend(rows)
    for source, section in ((connections_path, "connections"),
                            (schema_hints_path, "schema_hints")):
        if source is not None:
            document = load_metadata(source, root=root)
            if not isinstance(document, dict) or not isinstance(document.get(section), list):
                raise MetadataCompositionError(f"{source}: '{section}' must be a list")
            merged[section] = document[section]
    from datacoolie.utils.identity import (
        is_usable_identifier,
        name_to_uuid,
        normalize_document_workspace,
        normalize_workspace_id,
    )
    try:
        merged = normalize_document_workspace(merged)
    except ValueError as exc:
        raise MetadataCompositionError(str(exc)) from exc

    # Schema-hint groups refer to connection identities, so inherit the
    # connection scope before checking duplicate groups.  Keeping the scope on
    # the group also lets downstream providers resolve equal names in separate
    # workspaces without rebuilding the original document.
    connection_scopes = {}
    connection_names = {}
    for connection in merged["connections"]:
        scope = normalize_workspace_id(connection.get("workspace_id"))
        connection_scopes[connection.get("connection_id")] = scope
        connection_names.setdefault((scope, connection.get("name")), []).append(connection)
    for group in merged["schema_hints"]:
        if not isinstance(group, dict):
            continue
        group_scope = normalize_workspace_id(group.get("workspace_id"))
        connection_id = group.get("connection_id")
        if group_scope is None and connection_id in connection_scopes:
            group_scope = connection_scopes[connection_id]
        if group_scope is None and group.get("connection_name"):
            candidates = [
                row for (scope, name), rows in connection_names.items()
                if name == group["connection_name"]
                for row in rows
            ]
            scopes = {normalize_workspace_id(row.get("workspace_id")) for row in candidates}
            if len(scopes) == 1:
                group_scope = next(iter(scopes))
        if group_scope is not None or "workspace_id" in group:
            group["workspace_id"] = group_scope

    for section, id_field in (("connections", "connection_id"),
                              ("dataflows", "dataflow_id")):
        names, identities = {}, set()
        for row in merged[section]:
            if not isinstance(row, dict):
                raise MetadataCompositionError(f"{section}: entries must be objects")
            name = row.get("name")
            if not is_usable_identifier(name):
                raise MetadataCompositionError(f"{section}: name must be a non-empty string")
            workspace_id = normalize_workspace_id(row.get("workspace_id"))
            identity = row.get(id_field) or name_to_uuid(name, workspace_id)
            if not is_usable_identifier(identity):
                raise MetadataCompositionError(f"{id_field}: must be a non-empty string")
            name_key = (workspace_id, name)
            previous = names.get(name_key)
            explicit_identity = is_usable_identifier(row.get(id_field))
            if previous is not None and not (
                explicit_identity and previous[1] and previous[0] != identity
            ):
                raise MetadataCompositionError(
                    f"Duplicate {section} name in workspace {workspace_id!r}: {name!r}"
                )
            if identity in identities:
                raise MetadataCompositionError(f"Duplicate {id_field}: {identity!r}")
            names[name_key] = (identity, explicit_identity)
            identities.add(identity)
    hint_keys = set()
    for index, group in enumerate(merged["schema_hints"]):
        key = _hint_group_key(group, Path(str(schema_hints_path or sources[0])), index)
        if key in hint_keys:
            raise MetadataCompositionError(f"Duplicate schema hint group: {key!r}")
        hint_keys.add(key)
    return merged
