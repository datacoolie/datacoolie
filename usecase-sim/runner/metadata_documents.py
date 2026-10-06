"""Read-only recursive assembly of shared/group/workflow metadata."""

from __future__ import annotations

import copy
import os
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from runner.metadata_document_rules import (
    MetadataCompositionError, _COMPOSITION_KEYS, _connection_key, _flow_names,
    _hint_group_key, _is_uri, _read_json, _require_list,
    _root_path, _source_candidate, _validate_shared,
)


@dataclass
class _LoadedDocument:
    data: Any
    kind: str
    # Ordered by first reference in a workflow.  A shared profile referenced by
    # several groups appears once, while distinct profiles are checked for
    # duplicate/conflicting connection and hint identities.
    shared: "OrderedDict[Path, dict[str, Any]]" = field(default_factory=OrderedDict)


class _Loader:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._stack: list[Path] = []
        self._cache: dict[Path, _LoadedDocument] = {}

    def _enter(self, path: Path) -> None:
        if path in self._stack:
            cycle = " -> ".join(str(item.relative_to(self.root)) for item in (*self._stack, path))
            raise MetadataCompositionError(f"Metadata composition cycle: {cycle}")
        self._stack.append(path)

    def _leave(self) -> None:
        self._stack.pop()

    def _resolve_ref(self, reference: Any, owner: Path) -> Path:
        if not isinstance(reference, str) or not reference.strip():
            raise MetadataCompositionError(
                f"{owner}: composition references must be non-empty local paths"
            )
        # References are simulator-root-relative by contract.  Do not resolve
        # them relative to the group directory; accepting that would make a
        # path's meaning depend on where it was authored.
        return _source_candidate(reference, self.root)

    def load(self, path: str | os.PathLike[str]) -> _LoadedDocument:
        resolved = _source_candidate(path, self.root)
        if resolved in self._stack:
            self._enter(resolved)
        cached = self._cache.get(resolved)
        if cached is not None:
            return _LoadedDocument(
                data=copy.deepcopy(cached.data),
                kind=cached.kind,
                shared=OrderedDict(
                    (key, copy.deepcopy(value)) for key, value in cached.shared.items()
                ),
            )

        self._enter(resolved)
        try:
            raw = _read_json(resolved)
            if not isinstance(raw, dict):
                # Valid non-object JSON is still a plain document.  Provider
                # validation owns whether it is a usable metadata document.
                loaded = _LoadedDocument(data=copy.deepcopy(raw), kind="plain")
            elif "metadata_documents" in raw:
                loaded = self._load_workflow(raw, resolved)
            elif "shared_metadata" in raw:
                loaded = self._load_group(raw, resolved)
            else:
                # Plain JSON is intentionally not structurally validated here.
                # Some simulator fixtures exercise provider startup failures by
                # carrying malformed dataflows and must remain untouched.
                loaded = _LoadedDocument(data=copy.deepcopy(raw), kind="plain")
            self._cache[resolved] = copy.deepcopy(loaded)
            return loaded
        finally:
            self._leave()

    def _load_shared(self, reference: Any, owner: Path) -> tuple[Path, dict[str, Any]]:
        shared_path = self._resolve_ref(reference, owner)
        shared_loaded = self.load(shared_path)
        if shared_loaded.kind != "plain":
            raise MetadataCompositionError(
                f"{owner}: shared_metadata must reference a plain metadata document: {reference}"
            )
        if not isinstance(shared_loaded.data, dict):
            raise MetadataCompositionError(
                f"{owner}: shared_metadata must contain a JSON object: {reference}"
            )
        _validate_shared(shared_loaded.data, shared_path)
        # Keep only metadata sections in the shared contribution.  Additional
        # authored top-level fields are carried through and conflict-checked.
        return shared_path, copy.deepcopy(shared_loaded.data)

    def _load_group(self, raw: Mapping[str, Any], path: Path) -> _LoadedDocument:
        unknown = set(raw) - ({"shared_metadata", "dataflows"} | _COMPOSITION_KEYS)
        # The set above intentionally includes composition keys only to make
        # the error below explicit if a workflow marker is mixed into a group.
        if "metadata_documents" in raw or "flow_order" in raw:
            raise MetadataCompositionError(f"{path}: group cannot contain workflow keys")
        if unknown:
            raise MetadataCompositionError(
                f"{path}: unsupported group metadata keys: {sorted(unknown)}"
            )
        if not isinstance(raw.get("shared_metadata"), str):
            raise MetadataCompositionError(f"{path}: 'shared_metadata' must be a path string")
        flows = _require_list(raw, "dataflows", path)
        _flow_names(flows, path)
        shared_path, shared = self._load_shared(raw["shared_metadata"], path)
        data: dict[str, Any] = {}
        # Canonical runtime order matches the original simulator documents.
        data["connections"] = copy.deepcopy(shared.get("connections", []))
        data["dataflows"] = copy.deepcopy(flows)
        data["schema_hints"] = copy.deepcopy(shared.get("schema_hints", []))
        for key, value in shared.items():
            if key not in {"connections", "schema_hints"}:
                data[key] = copy.deepcopy(value)
        return _LoadedDocument(
            data=data,
            kind="group",
            shared=OrderedDict([(shared_path, shared)]),
        )

    @staticmethod
    def _merge_shared(
        merged: dict[str, Any],
        contribution: Mapping[str, Any],
        owner: Path,
        connection_sources: dict[str, Path],
        hint_sources: dict[tuple[Any, ...], Path],
    ) -> None:
        _validate_shared(contribution, owner)
        connections = merged.setdefault("connections", [])
        for index, connection in enumerate(contribution.get("connections", [])):
            name = _connection_key(connection, owner, index)
            if name in connection_sources:
                previous = connection_sources[name]
                raise MetadataCompositionError(
                    f"Conflicting or duplicate connection {name!r}: {previous} and {owner}"
                )
            connection_sources[name] = owner
            connections.append(copy.deepcopy(connection))

        hints = merged.setdefault("schema_hints", [])
        for index, group in enumerate(contribution.get("schema_hints", [])):
            key = _hint_group_key(group, owner, index)
            if key in hint_sources:
                previous = hint_sources[key]
                raise MetadataCompositionError(
                    f"Conflicting or duplicate schema hint group {key!r}: {previous} and {owner}"
                )
            hint_sources[key] = owner
            hints.append(copy.deepcopy(group))

        for key, value in contribution.items():
            if key in {"connections", "schema_hints"}:
                continue
            if key in merged and merged[key] != value:
                raise MetadataCompositionError(
                    f"Conflicting shared metadata field {key!r}: {owner}"
                )
            merged.setdefault(key, copy.deepcopy(value))

    def _load_workflow(self, raw: Mapping[str, Any], path: Path) -> _LoadedDocument:
        if "dataflows" in raw:
            raise MetadataCompositionError(f"{path}: workflow manifests cannot duplicate dataflows")
        unknown = set(raw) - {"metadata_documents", "flow_order"}
        if unknown:
            raise MetadataCompositionError(
                f"{path}: unsupported workflow metadata keys: {sorted(unknown)}"
            )
        documents = _require_list(raw, "metadata_documents", path)
        order = _require_list(raw, "flow_order", path)
        if any(not isinstance(item, str) or not item for item in order):
            raise MetadataCompositionError(f"{path}: flow_order entries must be non-empty strings")
        if len(set(order)) != len(order):
            repeats = sorted({name for name in order if order.count(name) > 1})
            raise MetadataCompositionError(f"{path}: repeated flow_order names: {repeats}")

        merged: dict[str, Any] = {"connections": [], "dataflows": [], "schema_hints": []}
        shared_sources: "OrderedDict[Path, dict[str, Any]]" = OrderedDict()
        connection_sources: dict[str, Path] = {}
        hint_sources: dict[tuple[Any, ...], Path] = {}
        flow_sources: dict[str, Path] = {}
        all_flows: dict[str, dict[str, Any]] = {}

        for index, reference in enumerate(documents):
            group_path = self._resolve_ref(reference, path)
            loaded = self.load(group_path)
            if loaded.kind != "group":
                raise MetadataCompositionError(
                    f"{path}: metadata_documents[{index}] must reference a group document: {reference}"
                )
            for shared_path, contribution in loaded.shared.items():
                if shared_path not in shared_sources:
                    shared_sources[shared_path] = contribution
                    self._merge_shared(
                        merged,
                        contribution,
                        shared_path,
                        connection_sources,
                        hint_sources,
                    )
            for flow in loaded.data.get("dataflows", []):
                name = flow["name"]
                if name in all_flows:
                    raise MetadataCompositionError(
                        f"Duplicate dataflow {name!r}: {flow_sources[name]} and {group_path}"
                    )
                all_flows[name] = copy.deepcopy(flow)
                flow_sources[name] = group_path

        actual_names = set(all_flows)
        ordered_names = set(order)
        missing = [name for name in order if name not in actual_names]
        extra = [name for name in all_flows if name not in ordered_names]
        if missing or extra:
            details: list[str] = []
            if missing:
                details.append(f"missing flows in metadata_documents: {missing}")
            if extra:
                details.append(f"flows absent from flow_order: {extra}")
            raise MetadataCompositionError(f"{path}: invalid flow_order ({'; '.join(details)})")

        merged["dataflows"] = [all_flows[name] for name in order]
        return _LoadedDocument(data=merged, kind="workflow", shared=shared_sources)


def load_metadata(
    path: str | os.PathLike[str],
    root: str | os.PathLike[str] | None = None,
) -> Any:
    """Load plain metadata or compose a simulator group/workflow document.

    Relative composition references are always resolved beneath *root* (the
    simulator root by default).  The returned mapping is newly allocated, so
    callers cannot mutate an authored source document through the composition
    cache.
    """

    root_path = _root_path(root)
    caller_path = Path(path).expanduser()
    load_path: str | os.PathLike[str] = (
        caller_path.resolve()
        if not caller_path.is_absolute() and caller_path.is_file()
        else path
    )
    try:
        loader = _Loader(root_path)
        return copy.deepcopy(loader.load(load_path).data)
    except MetadataCompositionError:
        # A plain metadata file may be supplied from a deployed or temporary
        # location outside the simulator tree.  Read it unchanged; a composed
        # document outside the root still fails the root guard.
        if _is_uri(path):
            raise
        candidate = Path(path).expanduser().resolve()
        if not candidate.is_file():
            raise
        raw = _read_json(candidate)
        if isinstance(raw, dict) and ("metadata_documents" in raw or "shared_metadata" in raw):
            raise
        return copy.deepcopy(raw)
