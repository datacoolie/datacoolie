"""Simulator-only metadata bindings for exact canonical file inputs.

The product :class:`~datacoolie.metadata.file_provider.FileProvider` remains
the owner of parsing, model construction, identity validation, and lifecycle.
This module adapts a canonical simulator document for one invocation without
materialising a generated metadata file or touching provider caches.
"""

from __future__ import annotations

from copy import deepcopy
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from os import PathLike
from typing import Any

from datacoolie.metadata.file_provider import FileProvider
from datacoolie.utils.identity import normalize_workspace_id

from runner.metadata_documents import load_metadata_files


SECTION_NAMES = ("connections", "dataflows")
IDENTITY_FIELDS = {
    # A binding is an invocation-level projection.  It may change endpoint
    # configuration, but it must never create a different logical entity or
    # move it to another stage.  Keep these fields explicit instead of
    # relying on model validation after the projection has been applied.
    "connections": frozenset({"name", "connection_id", "workspace_id"}),
    "dataflows": frozenset({"name", "dataflow_id", "workspace_id", "stage"}),
}
_OPERATION_KEYS = frozenset({"replace", "patch", "delete"})


class MetadataBindingError(ValueError):
    """Raised when an invocation binding is malformed or cannot be admitted."""


@dataclass(frozen=True)
class _Operation:
    """One explicit replacement/patch/delete operation for a named row."""

    replace: dict[str, Any] | None = None
    patch: dict[str, Any] | None = None
    delete: tuple[str, ...] = ()


def parse_metadata_bindings(value: str | Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Decode one CLI JSON binding object without changing caller data."""

    if value is None:
        return None
    if isinstance(value, Mapping):
        raw = deepcopy(dict(value))
    elif isinstance(value, str):
        try:
            raw = json.loads(value)
        except json.JSONDecodeError as exc:
            raise MetadataBindingError(
                f"--metadata-bindings must be valid JSON: {exc.msg}"
            ) from exc
    else:
        raise MetadataBindingError("metadata bindings must be a JSON object")
    if not isinstance(raw, dict):
        raise MetadataBindingError("metadata bindings must decode to a JSON object")
    return raw


def _tokens(raw: Mapping[str, Any] | None) -> dict[str, str]:
    value = {} if raw is None else raw.get("tokens", {})
    if not isinstance(value, Mapping):
        raise MetadataBindingError("metadata bindings tokens must be an object")
    result: dict[str, str] = {}
    for token, replacement in value.items():
        if not isinstance(token, str) or not token:
            raise MetadataBindingError("metadata binding token names must be non-empty strings")
        if not isinstance(replacement, str):
            raise MetadataBindingError(
                f"metadata binding token {token!r} must have a string value"
            )
        result[token] = replacement
    return result


def _operation(target: str, value: Any, section: str) -> _Operation:
    label = f"{section} binding {target!r}"
    if not isinstance(target, str) or not target:
        raise MetadataBindingError(f"{section} binding targets must be non-empty names")
    if not isinstance(value, Mapping):
        raise MetadataBindingError(f"{label} must be an object")
    unknown = set(value) - _OPERATION_KEYS
    if unknown:
        raise MetadataBindingError(
            f"{label} has unsupported operation keys: {sorted(map(str, unknown))}"
        )
    has_replace = "replace" in value
    has_patch = "patch" in value
    if has_replace and has_patch:
        raise MetadataBindingError(f"{label} must choose replace or patch")
    replacement: dict[str, Any] | None = None
    patch: dict[str, Any] | None = None
    if has_replace:
        if not isinstance(value["replace"], Mapping):
            raise MetadataBindingError(f"{label}.replace must be an object")
        replacement = deepcopy(dict(value["replace"]))
    elif has_patch:
        if not isinstance(value["patch"], Mapping):
            raise MetadataBindingError(f"{label}.patch must be an object")
        patch = deepcopy(dict(value["patch"]))
    deletes = value.get("delete", [])
    if not isinstance(deletes, list) or any(
        not isinstance(item, str) or not item for item in deletes
    ):
        raise MetadataBindingError(f"{label}.delete must be a list of dotted field names")
    if replacement is not None and deletes:
        raise MetadataBindingError(f"{label}.replace cannot be combined with delete")
    return _Operation(replace=replacement, patch=patch, delete=tuple(deletes))


def _operations(raw: Mapping[str, Any] | None, section: str) -> dict[str, _Operation]:
    if raw is None:
        return {}
    value = raw.get(section, {})
    if value is None:
        return {}
    result: dict[str, _Operation] = {}
    if isinstance(value, Mapping):
        # A mapping is intentionally name -> explicit operation.  This avoids
        # treating an accidental row object as a new metadata entity.
        for target, operation in value.items():
            if not isinstance(target, str) or not target:
                raise MetadataBindingError(
                    f"{section} binding targets must be non-empty names"
                )
            if target in _OPERATION_KEYS:
                raise MetadataBindingError(
                    f"{section} bindings must map names to replace/patch operations"
                )
            if target in result:
                raise MetadataBindingError(f"duplicate {section} binding: {target}")
            result[target] = _operation(target, operation, section)
        return result
    if isinstance(value, list):
        for index, item in enumerate(value):
            if not isinstance(item, Mapping):
                raise MetadataBindingError(f"{section}[{index}] binding must be an object")
            unknown_item = set(item) - {"name", "target", "replace", "patch", "delete"}
            if unknown_item:
                raise MetadataBindingError(
                    f"{section}[{index}] binding has unsupported keys: "
                    f"{sorted(map(str, unknown_item))}"
                )
            target = item.get("name", item.get("target"))
            if not isinstance(target, str) or not target:
                raise MetadataBindingError(f"{section}[{index}] binding needs name")
            if target in result:
                raise MetadataBindingError(f"duplicate {section} binding: {target}")
            operation = {key: item[key] for key in _OPERATION_KEYS if key in item}
            result[target] = _operation(target, operation, section)
        return result
    raise MetadataBindingError(f"metadata bindings {section} must be an object or list")


def _normalise_spec(value: str | Mapping[str, Any] | None) -> tuple[dict[str, str], dict[str, _Operation], dict[str, _Operation]]:
    raw = parse_metadata_bindings(value) or {}
    unknown = set(raw) - {"tokens", "connections", "dataflows"}
    if unknown:
        raise MetadataBindingError(
            f"metadata bindings has unsupported keys: {sorted(map(str, unknown))}"
        )
    return _tokens(raw), _operations(raw, "connections"), _operations(raw, "dataflows")


def _replace_tokens(value: Any, tokens: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        result = value
        for token, replacement in tokens.items():
            result = result.replace(token, replacement)
        return result
    if isinstance(value, list):
        return [_replace_tokens(item, tokens) for item in value]
    if isinstance(value, dict):
        # Tokens are structural values; JSON field names are never rewritten.
        return {key: _replace_tokens(item, tokens) for key, item in value.items()}
    return deepcopy(value)


def _deep_patch(base: dict[str, Any], patch: Mapping[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in patch.items():
        if not isinstance(key, str) or not key:
            raise MetadataBindingError("patch field names must be non-empty strings")
        if "." in key:
            _set_path(result, key.split("."), deepcopy(value), allow_new=True)
        elif isinstance(result.get(key), dict) and isinstance(value, Mapping):
            result[key] = _deep_patch(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _set_path(root: Any, parts: list[str], value: Any, *, allow_new: bool) -> None:
    if not parts or any(not part for part in parts):
        raise MetadataBindingError("binding field paths must contain non-empty segments")
    current = root
    for part in parts[:-1]:
        if isinstance(current, dict):
            if part not in current:
                if not allow_new:
                    raise MetadataBindingError(f"binding field does not exist: {'.'.join(parts)}")
                current[part] = {}
            current = current[part]
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            if index >= len(current):
                raise MetadataBindingError(f"binding field does not exist: {'.'.join(parts)}")
            current = current[index]
        else:
            raise MetadataBindingError(f"binding field is not traversable: {'.'.join(parts)}")
    leaf = parts[-1]
    if isinstance(current, dict):
        if not allow_new and leaf not in current:
            raise MetadataBindingError(f"binding field does not exist: {'.'.join(parts)}")
        current[leaf] = value
    elif isinstance(current, list) and leaf.isdigit():
        index = int(leaf)
        if index >= len(current):
            raise MetadataBindingError(f"binding field does not exist: {'.'.join(parts)}")
        current[index] = value
    else:
        raise MetadataBindingError(f"binding field is not traversable: {'.'.join(parts)}")


def _delete_path(root: Any, path: str, *, section: str) -> None:
    parts = path.split(".")
    if not path or any(not part for part in parts):
        raise MetadataBindingError(f"{section} delete paths must be dotted field names")
    if parts[0] in IDENTITY_FIELDS[section]:
        raise MetadataBindingError(
            f"{section} binding cannot delete identity field: {path}"
        )
    current = root
    for part in parts[:-1]:
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            raise MetadataBindingError(f"{section} delete field does not exist: {path}")
    leaf = parts[-1]
    if isinstance(current, dict) and leaf in current:
        del current[leaf]
    elif isinstance(current, list) and leaf.isdigit() and int(leaf) < len(current):
        del current[int(leaf)]
    else:
        raise MetadataBindingError(f"{section} delete field does not exist: {path}")


def _identity(row: Mapping[str, Any], section: str) -> tuple[Any, ...]:
    """Return the raw identity tuple used by binding immutability checks."""

    return tuple(row.get(field) for field in sorted(IDENTITY_FIELDS[section]))


def _guard_identity(
    before: Mapping[str, Any], after: Mapping[str, Any], *, section: str, target: str
) -> None:
    if _identity(before, section) != _identity(after, section):
        changed = [
            field
            for field in sorted(IDENTITY_FIELDS[section])
            if before.get(field) != after.get(field)
        ]
        raise MetadataBindingError(
            f"{section} binding cannot change identity for {target!r}: "
            f"{', '.join(changed) or 'identity fields'}"
        )


def _apply_operations(
    document: dict[str, Any],
    *,
    tokens: Mapping[str, str],
    connection_ops: Mapping[str, _Operation],
    dataflow_ops: Mapping[str, _Operation],
    require_targets: bool,
) -> dict[str, Any]:
    # Resolve operation targets against the authored document before token
    # substitution.  Tokens are value substitutions only; they never rewrite
    # JSON field names, and replacing an identity value is rejected below.
    # The caller's document is read-only. One editable copy is sufficient;
    # identity comparisons can use the original without duplicating it.
    original = document
    result = deepcopy(document)
    for section, operations in (
        ("connections", connection_ops),
        ("dataflows", dataflow_ops),
    ):
        rows = result.get(section, [])
        if not isinstance(rows, list):
            # Let FileProvider own this diagnostic for an unbound malformed
            # fixture; a binding cannot safely target a non-list section.
            if operations and require_targets:
                raise MetadataBindingError(f"metadata section {section!r} must be a list")
            continue
        indexes: dict[str, list[int]] = {}
        for index, row in enumerate(rows):
            if isinstance(row, Mapping) and isinstance(row.get("name"), str):
                indexes.setdefault(row["name"], []).append(index)
        for target, operation in operations.items():
            candidates = indexes.get(target, [])
            if not candidates:
                if require_targets:
                    raise MetadataBindingError(
                        f"Unknown {section[:-1]} binding target: {target}"
                    )
                # The same named row may be present in another exact source
                # file; source-level transforms ignore absent rows.
                continue
            if len(candidates) != 1:
                raise MetadataBindingError(
                    f"Ambiguous {section[:-1]} binding target {target!r}; "
                    "use an explicit identity"
                )
            index = candidates[0]
            base = rows[index]
            if not isinstance(base, dict):
                raise MetadataBindingError(f"{section} target {target!r} must be an object")
            if operation.replace is not None:
                candidate = deepcopy(operation.replace)
            else:
                candidate = _deep_patch(base, operation.patch or {})
            for path in operation.delete:
                _delete_path(candidate, path, section=section)
            if not isinstance(candidate, dict):
                raise MetadataBindingError(f"{section} replacement {target!r} must be an object")
            _guard_identity(base, candidate, section=section, target=target)
            rows[index] = candidate

    bound = _replace_tokens(result, tokens) if tokens else result
    # A token can affect an identity field even when no explicit operation was
    # supplied.  Compare every surviving row with its authored counterpart so
    # unselected rows cannot silently acquire new names, stages, or IDs.
    for section in SECTION_NAMES:
        before_rows = original.get(section, [])
        after_rows = bound.get(section, [])
        if not isinstance(before_rows, list) or not isinstance(after_rows, list):
            continue
        for index, before in enumerate(before_rows):
            if index >= len(after_rows) or not isinstance(before, Mapping):
                continue
            after = after_rows[index]
            if isinstance(after, Mapping):
                _guard_identity(
                    before,
                    after,
                    section=section,
                    target=str(before.get("name", index)),
                )
    return bound


def _validate_effective_identities(document: Mapping[str, Any]) -> None:
    for section, id_field in (("connections", "connection_id"), ("dataflows", "dataflow_id")):
        rows = document.get(section, [])
        if not isinstance(rows, list):
            return
        names: dict[tuple[str | None, str], list[tuple[str, bool]]] = {}
        identities: set[str] = set()
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                continue
            name = row.get("name")
            if isinstance(name, str):
                scope = normalize_workspace_id(row.get("workspace_id"))
                identity = row.get(id_field)
                names.setdefault((scope, name), []).append(
                    (identity, isinstance(identity, str) and bool(identity.strip()))
                )
            identity = row.get(id_field)
            if isinstance(identity, str):
                if identity in identities:
                    raise MetadataBindingError(f"Duplicate {id_field}: {identity!r}")
                identities.add(identity)
        for (scope, name), candidates in names.items():
            if len(candidates) <= 1:
                continue
            # Equal display names are valid when the authored rows have
            # distinct explicit IDs. A generated/missing ID would collapse
            # to the same identity, so name based bindings must reject it.
            if not all(explicit for _, explicit in candidates) or len(
                {identity for identity, _ in candidates}
            ) != len(candidates):
                label = f" in workspace {scope!r}" if scope is not None else ""
                raise MetadataBindingError(
                    f"Duplicate {section} name{label}: {name!r}; "
                    "use distinct explicit IDs"
                )


def _normalise_selectors(values: Sequence[str] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        values = [values]
    result = tuple(values)
    if any(not isinstance(value, str) or not value for value in result):
        raise MetadataBindingError("--dataflow values must be non-empty names")
    if len(set(result)) != len(result):
        raise MetadataBindingError("duplicate dataflow selector")
    return result


def bind_document(
    source: Mapping[str, Any]
    | str
    | PathLike[str]
    | Sequence[str | PathLike[str]],
    bindings: str | Mapping[str, Any] | None = None,
    *,
    connections_path: str | PathLike[str] | None = None,
    schema_hints_path: str | PathLike[str] | None = None,
    dataflow_names: Sequence[str] | None = None,
    dataflows: Sequence[str] | None = None,
    root: str | PathLike[str] | None = None,
) -> dict[str, Any]:
    """Return a bound copy of one effective exact metadata document.

    Section overlays follow FileProvider semantics: explicit connection and
    schema-hint paths replace those sections.  The function is pure and is
    used for admission/target resolution; actual execution still reads each
    canonical source through :class:`BoundFileProvider`.
    """

    if dataflow_names is not None and dataflows is not None:
        if tuple(dataflow_names) != tuple(dataflows):
            raise MetadataBindingError("use only one of dataflow_names or dataflows")
    selectors_input = dataflow_names if dataflow_names is not None else dataflows

    if isinstance(source, Mapping):
        document = deepcopy(dict(source))
    else:
        document = load_metadata_files(
            source,
            connections_path=connections_path,
            schema_hints_path=schema_hints_path,
            root=root,
        )
    tokens, connection_ops, dataflow_ops = _normalise_spec(bindings)
    bound = _apply_operations(
        document,
        tokens=tokens,
        connection_ops=connection_ops,
        dataflow_ops=dataflow_ops,
        require_targets=True,
    )
    _validate_effective_identities(bound)
    selectors = _normalise_selectors(selectors_input)
    if selectors:
        rows = bound.get("dataflows", [])
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, Mapping) and isinstance(row.get("name"), str):
                    grouped.setdefault(row["name"], []).append(row)
        ambiguous = [name for name in selectors if len(grouped.get(name, [])) > 1]
        if ambiguous:
            raise MetadataBindingError(
                f"Ambiguous dataflow selector(s): {', '.join(ambiguous)}; "
                "use an explicit dataflow ID"
            )
        missing = [name for name in selectors if name not in grouped]
        if missing:
            raise MetadataBindingError(
                f"Unknown dataflow selector(s): {', '.join(missing)}"
            )
    return bound


def _select_models(
    dataflows: list[Any], selectors: tuple[str, ...], *, context: str,
) -> list[Any]:
    if not selectors:
        return dataflows
    grouped: dict[str, list[Any]] = {}
    for dataflow in dataflows:
        name = getattr(dataflow, "name", None)
        if name:
            grouped.setdefault(name, []).append(dataflow)
    ambiguous = [name for name in selectors if len(grouped.get(name, [])) > 1]
    if ambiguous:
        raise MetadataBindingError(
            f"Dataflow selector is ambiguous for {context}: "
            f"{', '.join(ambiguous)}; use an explicit dataflow ID"
        )
    by_name = {name: rows[0] for name, rows in grouped.items()}
    missing = [name for name in selectors if name not in grouped]
    if missing:
        raise MetadataBindingError(
            f"Dataflow selector is incompatible with {context}: {', '.join(missing)}"
        )
    return [by_name[name] for name in selectors]


class BoundFileProvider(FileProvider):
    """FileProvider variant with immutable per-invocation raw bindings."""

    def __init__(
        self,
        *args: Any,
        metadata_bindings: str | Mapping[str, Any] | None = None,
        bindings: str | Mapping[str, Any] | None = None,
        dataflow_names: Sequence[str] | None = None,
        dataflows: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        provider_args = args
        # Keep a compact positional form useful for scenario helpers:
        # ``BoundFileProvider(config_path, bindings, dataflows=[...])``.
        # A normal FileProvider's second positional argument is a platform
        # object, so only mapping/string values are interpreted as bindings.
        if (
            len(args) >= 2
            and metadata_bindings is None
            and bindings is None
            and isinstance(args[1], (str, Mapping))
        ):
            bindings = args[1]
            provider_args = (args[0],) + args[2:]
        if metadata_bindings is not None and bindings is not None:
            raise MetadataBindingError("use only one of metadata_bindings or bindings")
        self._binding_value = metadata_bindings if metadata_bindings is not None else bindings
        self._tokens, self._connection_ops, self._dataflow_ops = _normalise_spec(self._binding_value)
        if dataflow_names is not None and dataflows is not None:
            if tuple(dataflow_names) != tuple(dataflows):
                raise MetadataBindingError("use only one of dataflow_names or dataflows")
        self._selectors = _normalise_selectors(
            dataflow_names if dataflow_names is not None else dataflows
        )
        # Keep the exact source contract in adapter-owned state.  The
        # FileProvider implementation deliberately keeps its path attributes
        # private; preflight must not depend on those implementation details.
        self._binding_config_path = kwargs.get("config_path", provider_args[0] if provider_args else None)
        self._binding_connections_path = kwargs.get("connections_path")
        self._binding_schema_hints_path = kwargs.get("schema_hints_path")
        self._preflight_done = False
        super().__init__(*provider_args, **kwargs)

    def _preflight(self) -> None:
        if self._preflight_done:
            return
        paths = self._binding_config_path
        if paths is None or (isinstance(paths, Sequence) and not isinstance(paths, (str, bytes)) and not list(paths)):
            raise MetadataBindingError(
                "metadata bindings/dataflow selectors require explicit metadata_path files"
            )
        bind_document(
            paths,
            self._binding_value,
            connections_path=self._binding_connections_path,
            schema_hints_path=self._binding_schema_hints_path,
            dataflow_names=self._selectors,
        )
        self._preflight_done = True

    def initialize(self) -> None:
        # Admission runs before provider I/O, while the Driver still owns the
        # normal startup boundary.  FileProvider then performs the real read
        # and full model validation through its ordinary lifecycle.
        self._preflight()
        super().initialize()

    def _load_file(self, path: str) -> dict[str, Any]:
        document = super()._load_file(path)
        return _apply_operations(
            document,
            tokens=self._tokens,
            connection_ops=self._connection_ops,
            dataflow_ops=self._dataflow_ops,
            require_targets=False,
        )

    def get_dataflows(
        self,
        *,
        stage: Any = None,
        active_only: bool = True,
        attach_schema_hints: bool = True,
    ) -> list[Any]:
        if not self._selectors:
            return super().get_dataflows(
                stage=stage,
                active_only=active_only,
                attach_schema_hints=attach_schema_hints,
            )

        # Resolve explicit selectors against the authored full stage scope.
        # FileProvider's active-only projection must happen after selection:
        # an inactive row is a valid authored selector and is simply omitted
        # from an active execution request.
        dataflows = super().get_dataflows(
            stage=stage,
            active_only=False,
            attach_schema_hints=attach_schema_hints,
        )
        selected = _select_models(dataflows, self._selectors, context="stage selection")
        if active_only:
            selected = [dataflow for dataflow in selected if dataflow.is_active]
        return selected

    def get_maintenance_dataflows(
        self,
        *,
        connection: Any = None,
        active_only: bool = True,
        attach_schema_hints: bool = False,
    ) -> list[Any]:
        if not self._selectors:
            return super().get_maintenance_dataflows(
                connection=connection,
                active_only=active_only,
                attach_schema_hints=attach_schema_hints,
            )

        # Validate every explicit selector against the authored inventory
        # before maintenance's lakehouse/connection filters.  A broad
        # maintenance scenario intentionally supplies selectors for all
        # workflow stages; only the eligible intersection is returned.
        authored = super().get_dataflows(
            active_only=False,
            attach_schema_hints=False,
        )
        _select_models(authored, self._selectors, context="maintenance selection")
        eligible = super().get_maintenance_dataflows(
            connection=connection,
            active_only=False,
            attach_schema_hints=attach_schema_hints,
        )
        eligible_by_name: dict[str, list[Any]] = {}
        for dataflow in eligible:
            if dataflow.name is not None:
                eligible_by_name.setdefault(dataflow.name, []).append(dataflow)
        ambiguous = [name for name in self._selectors if len(eligible_by_name.get(name, [])) > 1]
        if ambiguous:
            raise MetadataBindingError(
                "Maintenance dataflow selector is ambiguous: "
                f"{', '.join(ambiguous)}; use an explicit dataflow ID"
            )
        by_name = {name: rows[0] for name, rows in eligible_by_name.items()}
        selected = [by_name[name] for name in self._selectors if name in by_name]
        if active_only:
            selected = [dataflow for dataflow in selected if dataflow.is_active]
        return selected


class MemoryFileProvider(FileProvider):
    """FileProvider backed by one immutable in-memory document.

    Inline scenario projections must still use the product FileProvider model
    and lifecycle validation.  The generated ``memory://`` coordinate gives
    validation/origin diagnostics a stable provenance label while the
    protected loader prevents any platform or local-file read.
    """

    MEMORY_CONFIG_PATH = "memory://metadata.json"

    def __init__(self, document: Mapping[str, Any], **kwargs: Any) -> None:
        if not isinstance(document, Mapping):
            raise MetadataBindingError("memory metadata document must be an object")
        if "config_path" in kwargs:
            raise MetadataBindingError(
                "MemoryFileProvider accepts document instead of config_path"
            )
        if kwargs.get("metadata_base_path") is not None:
            raise MetadataBindingError(
                "MemoryFileProvider cannot be combined with metadata_base_path"
            )
        self._memory_snapshot = deepcopy(dict(document))
        super().__init__(config_path=self.MEMORY_CONFIG_PATH, **kwargs)

    def _load_file(self, path: str) -> dict[str, Any]:
        if path != self.MEMORY_CONFIG_PATH:
            return super()._load_file(path)
        return deepcopy(self._memory_snapshot)


def build_file_provider(
    *,
    config_path: str | Sequence[str] | None = None,
    connections_path: str | None = None,
    schema_hints_path: str | None = None,
    metadata_bindings: str | Mapping[str, Any] | None = None,
    dataflow_names: Sequence[str] | None = None,
    dataflows: Sequence[str] | None = None,
    **kwargs: Any,
) -> FileProvider:
    """Build the normal provider or the simulator adapter when requested."""

    if dataflow_names is not None and dataflows is not None:
        if tuple(dataflow_names) != tuple(dataflows):
            raise MetadataBindingError("use only one of dataflow_names or dataflows")
    selectors = dataflow_names if dataflow_names is not None else dataflows
    if metadata_bindings is None and not selectors:
        # Resolve the ordinary provider at call time.  The simulator adapter
        # must not freeze a product-provider replacement used by an embedding
        # application or a diagnostic test.
        from datacoolie.metadata.file_provider import FileProvider as ProductFileProvider
        return ProductFileProvider(
            config_path=config_path,
            connections_path=connections_path,
            schema_hints_path=schema_hints_path,
            **kwargs,
        )
    return BoundFileProvider(
        config_path=config_path,
        connections_path=connections_path,
        schema_hints_path=schema_hints_path,
        metadata_bindings=metadata_bindings,
        dataflows=selectors,
        **kwargs,
    )


__all__ = [
    "BoundFileProvider",
    "MemoryFileProvider",
    "MetadataBindingError",
    "bind_document",
    "build_file_provider",
    "parse_metadata_bindings",
]
